/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.island.ohara.configurator

import java.util.concurrent.{ExecutionException, Executors, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{handleRejections, path, _}
import akka.http.scaladsl.server.{ExceptionHandler, MalformedRequestContentRejection, RejectionHandler}
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import com.island.ohara.agent._
import com.island.ohara.agent.docker.ServiceCollieImpl
import com.island.ohara.agent.k8s.K8SClient
import com.island.ohara.client.HttpExecutor
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.MetricsApi.Meter
import com.island.ohara.client.configurator.v0.StreamApi.StreamClusterInfo
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.client.configurator.v0._
import com.island.ohara.common.util.{CommonUtils, Releasable, ReleaseOnce}
import com.island.ohara.configurator.Configurator.Mode
import com.island.ohara.configurator.route._
import com.island.ohara.configurator.store.{DataStore, MeterCache}
import com.typesafe.scalalogging.Logger
import spray.json.DeserializationException

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.TimeoutException

import scala.util.control.Breaks._

/**
  * A simple impl from Configurator. This impl maintains all subclass from ohara data in a single ohara store.
  * NOTED: there are many route requiring the implicit variables so we make them be implicit in construction.
  *
  */
class Configurator private[configurator] (val hostname: String, val port: Int)(
  implicit val store: DataStore,
  val dataCollie: DataCollie,
  val serviceCollie: ServiceCollie,
  val k8sClient: Option[K8SClient]
) extends ReleaseOnce {
  private[this] val threadMax = {
    val value = Runtime.getRuntime.availableProcessors()
    if (value <= 1)
      throw new IllegalArgumentException(
        s"I'm sorry that your machine is too weak to run Ohara Configurator." +
          s" The required number of core must be bigger than 2, but actual number is $value"
      )
    value
  }

  private[this] val threadPool = Executors.newFixedThreadPool(threadMax)

  private[this] val loopOfUpdatingNode: Releasable = {
    val pool = if (k8sClient.nonEmpty) Executors.newSingleThreadExecutor() else null
    if (pool != null) pool.execute(() => {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          TimeUnit.SECONDS.sleep(5)
          Await.result(addK8SNodes(), 5 seconds)
        } catch {
          case timeoutException: TimeoutException => log.error("timeout exception", timeoutException)
          case interrupException: InterruptedException => {
            log.error("interrup exception", interrupException)
            break
          }
          case e: Throwable => throw e
        }
      }
    })

    () => if (pool != null) pool.shutdownNow()
  }

  private[this] implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  /**
    * this timeout is used to wait the socket server to be ready to accept connection.
    */
  private[this] val initializationTimeout = 10 seconds

  /**
    * this timeout is used to
    * 1) unbind the socket server
    * 2) reject all incoming requests
    * 3) wait and then terminate in-flight requests
    * A small timeout can reduce the time to close configurator, and it is useful for testing. Perhaps we should expose this timeout to production
    * purpose. However, we have not met related use cases or bugs and hence we leave a constant timeout here.
    */
  private[this] val terminateTimeout = 3 seconds
  private[this] val cacheTimeout     = 3 seconds
  private[this] val cleanupTimeout   = 30 seconds

  private[configurator] def size: Int = store.size()

  private[this] val log = Logger(classOf[Configurator])

  private[this] implicit val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
  private[this] implicit val brokerCollie: BrokerCollie       = serviceCollie.brokerCollie
  private[this] implicit val workerCollie: WorkerCollie       = serviceCollie.workerCollie
  private[this] implicit val streamCollie: StreamCollie       = serviceCollie.streamCollie
  private[this] implicit val adminCleaner: AdminCleaner       = new AdminCleaner(cleanupTimeout)
  private[this] implicit val objectChecker: ObjectChecker     = ObjectChecker()

  def mode: Mode = serviceCollie match {
    case _: ServiceCollieImpl                                    => Mode.DOCKER
    case _: com.island.ohara.agent.k8s.K8SServiceCollieImpl      => Mode.K8S
    case _: com.island.ohara.configurator.fake.FakeServiceCollie => Mode.FAKE
    case _                                                       => throw new IllegalArgumentException(s"unknown cluster collie: ${serviceCollie.getClass.getName}")
  }

  private[this] def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case e @ (_: DeserializationException | _: ParsingException | _: IllegalArgumentException |
        _: NoSuchElementException) =>
      extractRequest { request =>
        log.error(s"Request to ${request.uri} with ${request.entity} is wrong", e)
        complete(StatusCodes.BadRequest -> ErrorApi.of(e))
      }
    case e: Throwable =>
      extractRequest { request =>
        log.error(s"Request to ${request.uri} with ${request.entity} could not be handled normally", e)
        complete(StatusCodes.InternalServerError -> ErrorApi.of(e))
      }
  }

  /**
    *Akka use rejection to wrap error message
    */
  private[this] def rejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        // seek the true exception
        case MalformedRequestContentRejection(_, cause) if cause != null => throw cause
        case e: ExecutionException if e.getCause != null                 => throw e.getCause
      }
      .result()

  private[this] implicit val meterCache: MeterCache = {
    def metrics(collie: Collie, clusterInfos: Seq[ClusterInfo]): Future[Map[ClusterInfo, Map[String, Seq[Meter]]]] =
      Future
        .sequence(clusterInfos.map { clusterInfo =>
          collie
            .counters(clusterInfo.key)
            .map(m => Some(clusterInfo -> m))
            .recover {
              case e: Throwable =>
                log.error(
                  s"failed to get metrics of service:${clusterInfo.key} from ${collie.getClass.getSimpleName}",
                  e
                )
                None
            }
        })
        .map(_.flatten.toMap)

    MeterCache.builder
      .refresher { () =>
        // we do the sync here to simplify the interface
        Await.result(
          for {
            zks <- serviceCollie.brokerCollie
              .clusters()
              .map(_.map(_.key))
              .flatMap(keys => Future.traverse(keys)(store.get[ZookeeperClusterInfo]))
              .map(_.flatten)
              .flatMap(clusterInfos => metrics(serviceCollie.zookeeperCollie, clusterInfos))
            bks <- serviceCollie.brokerCollie
              .clusters()
              .map(_.map(_.key))
              .flatMap(keys => Future.traverse(keys)(store.get[BrokerClusterInfo]))
              .map(_.flatten)
              .flatMap(clusterInfos => metrics(serviceCollie.brokerCollie, clusterInfos))
            wks <- serviceCollie.workerCollie
              .clusters()
              .map(_.map(_.key))
              .flatMap(keys => Future.traverse(keys)(store.get[WorkerClusterInfo]))
              .map(_.flatten)
              .flatMap(clusterInfos => metrics(serviceCollie.workerCollie, clusterInfos))
            streams <- serviceCollie.streamCollie
              .clusters()
              .map(_.map(_.key))
              .flatMap(keys => Future.traverse(keys)(store.get[StreamClusterInfo]))
              .map(_.flatten)
              .flatMap(clusterInfos => metrics(serviceCollie.streamCollie, clusterInfos))
          } yield zks ++ bks ++ wks ++ streams,
          cacheTimeout * 5
        )
      }
      .frequency(cacheTimeout)
      .build
  }

  private[configurator] def addK8SNodes(): Future[Seq[NodeApi.Node]] = {
    log.info("Running check Kubernetes node")
    val nodeApi           = NodeApi.access.hostname(hostname).port(port)
    val client: K8SClient = this.k8sClient.getOrElse(throw new RuntimeException("K8SClient object isn't exist"))
    client
      .nodes()
      .flatMap(
        nodes =>
          Future.sequence(nodes.map { k8sNode =>
            nodeApi
              .list()
              .map(
                nodes =>
                  if (nodes.map(_.hostname).contains(k8sNode.nodeName)) Seq.empty
                  else Seq(nodeApi.request.hostname(k8sNode.nodeName).create())
              )
          })
      )
      .flatMap(x => Future.sequence(x.flatten))
  }

  /**
    * the version of APIs supported by Configurator.
    * We are not ready to support multiples version APIs so it is ok to make a constant string.
    */
  private[this] val version = com.island.ohara.client.configurator.v0.V0

  /**
    * the full route consists from all routes against all subclass from ohara data and a final route used to reject other requests.
    */
  private[this] def basicRoute: server.Route =
    pathPrefix(version)(
      Seq[server.Route](
        TopicRoute.apply,
        PipelineRoute.apply,
        ValidationRoute.apply,
        ConnectorRoute.apply,
        InspectRoute.apply(mode),
        StreamRoute.apply,
        ShabondiRoute.apply,
        NodeRoute.apply,
        ZookeeperRoute.apply,
        BrokerRoute.apply,
        WorkerRoute.apply,
        FileInfoRoute.apply(hostname, port, version),
        LogRoute.apply,
        ObjectRoute.apply,
        ContainerRoute.apply
      ).reduce[server.Route]((a, b) => a ~ b)
    )

  private[this] def finalRoute: server.Route =
    path(Remaining)(routeToOfficialUrl)

  private[this] implicit val actorSystem: ActorSystem             = ActorSystem(s"${classOf[Configurator].getSimpleName}-system")
  private[this] implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  private[this] val httpServer: Http.ServerBinding =
    try Await.result(
      Http().bindAndHandle(
        handler = handleExceptions(exceptionHandler)(handleRejections(rejectionHandler)(basicRoute) ~ finalRoute),
        // we bind the service on all network adapter.
        interface = CommonUtils.anyLocalAddress(),
        port = port
      ),
      initializationTimeout.toMillis milliseconds
    )
    catch {
      case e: Throwable =>
        Releasable.close(this)
        throw e
    }

  /**
    * Do what you want to do when calling closing.
    */
  override protected def doClose(): Unit = {
    log.info("start to close Ohara Configurator")
    val start = CommonUtils.current()
    // close the cache thread in order to avoid cache error in log
    Releasable.close(meterCache)
    val onceHttpTerminated =
      if (httpServer != null)
        Some(httpServer.terminate(terminateTimeout).flatMap(_ => actorSystem.terminate()))
      else if (actorSystem == null) None
      else Some(actorSystem.terminate())
    // manually close materializer in order to speedup the release of http server and actor system
    if (actorMaterializer != null) actorMaterializer.shutdown()
    onceHttpTerminated.foreach { f =>
      try Await.result(f, terminateTimeout)
      catch {
        case e: Throwable =>
          log.error("failed to close http server and actor system", e)
      }
    }
    if (threadPool != null) {
      threadPool.shutdownNow()
      if (!threadPool.awaitTermination(terminateTimeout.toMillis, TimeUnit.MILLISECONDS))
        log.error("failed to terminate all running threads!!!")
    }

    Releasable.close(loopOfUpdatingNode)
    Releasable.close(serviceCollie)
    Releasable.close(store)
    Releasable.close(adminCleaner)
    log.info(s"succeed to close Ohara Configurator. elapsed:${CommonUtils.current() - start} ms")
  }
}

object Configurator {
  def builder: ConfiguratorBuilder = new ConfiguratorBuilder()

  //----------------[main]----------------//
  private[configurator] lazy val LOG                = Logger(Configurator.getClass)
  private[configurator] val HELP_KEY                = "--help"
  private[configurator] val FOLDER_KEY              = "--folder"
  private[configurator] val HOSTNAME_KEY            = "--hostname"
  private[configurator] val K8S_NAMESPACE_KEY       = "--k8s-namespace"
  private[configurator] val K8S_METRICS_SERVICE_KEY = "--k8s-metrics-server"
  private[configurator] val K8S_KEY                 = "--k8s"
  private[configurator] val FAKE_KEY                = "--fake"
  private[configurator] val PORT_KEY                = "--port"
  private val USAGE                                 = s"[Usage] $FOLDER_KEY $HOSTNAME_KEY $PORT_KEY $K8S_KEY $FAKE_KEY"

  /**
    * parse input arguments and then generate a Configurator instance.
    * @param args input arguments
    * @return configurator instance
    */
  private[configurator] def configurator(args: Array[String]): Configurator = {
    val configuratorBuilder = Configurator.builder
    try {
      args.sliding(2, 2).foreach {
        case Array(FOLDER_KEY, value)              => configuratorBuilder.homeFolder(value)
        case Array(HOSTNAME_KEY, value)            => configuratorBuilder.hostname(value)
        case Array(PORT_KEY, value)                => configuratorBuilder.port(value.toInt)
        case Array(K8S_NAMESPACE_KEY, value)       => configuratorBuilder.k8sNamespace(value)
        case Array(K8S_METRICS_SERVICE_KEY, value) => configuratorBuilder.k8sMetricsServerURL(value)
        case Array(K8S_KEY, value)                 => configuratorBuilder.k8sApiServer(value)
        case Array(FAKE_KEY, value) =>
          if (value.toBoolean) configuratorBuilder.fake()
        case _ =>
          configuratorBuilder.cleanup()
          throw new IllegalArgumentException(s"input:${args.mkString(" ")}. $USAGE")
      }
      configuratorBuilder.build()
    } catch {
      case e: Throwable =>
        // release all pre-created objects
        configuratorBuilder.cleanup()
        throw e
    }
  }

  def main(args: Array[String]): Unit =
    try {
      if (args.length == 1 && args(0) == HELP_KEY) {
        println(USAGE)
        return
      }
      if (GLOBAL_CONFIGURATOR != null) throw new RuntimeException("configurator is running!!!")

      GLOBAL_CONFIGURATOR = configurator(args)

      LOG.info(
        s"start a configurator built on hostname:${GLOBAL_CONFIGURATOR.hostname} and port:${GLOBAL_CONFIGURATOR.port}"
      )
      LOG.info("enter ctrl+c to terminate the configurator")

      while (!GLOBAL_CONFIGURATOR_SHOULD_CLOSE) {
        TimeUnit.SECONDS.sleep(2)
        LOG.info(s"Current data size:${GLOBAL_CONFIGURATOR.size}")
      }
    } finally {
      Releasable.close(GLOBAL_CONFIGURATOR)
      GLOBAL_CONFIGURATOR = null

      /**
        * the akka http executor is shared globally so we have to close it in this final block
        */
      HttpExecutor.close()
    }

  /**
    * visible for testing.
    */
  @volatile private[configurator] var GLOBAL_CONFIGURATOR: Configurator = _

  /**
    * visible for testing.
    */
  private[configurator] def GLOBAL_CONFIGURATOR_RUNNING: Boolean = GLOBAL_CONFIGURATOR != null

  /**
    * visible for testing.
    */
  @volatile private[configurator] var GLOBAL_CONFIGURATOR_SHOULD_CLOSE = false

  abstract sealed class Mode

  /**
    * show the mode of running configurator.
    */
  object Mode extends com.island.ohara.client.Enum[Mode] {
    /**
      * No extra services are running. Configurator fake all content of response for all requests. This mode is useful to test only the APIs
      */
    case object FAKE   extends Mode
    case object DOCKER extends Mode
    case object K8S    extends Mode
  }
}
