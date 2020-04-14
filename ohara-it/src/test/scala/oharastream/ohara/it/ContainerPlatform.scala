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

package oharastream.ohara.it

import java.net.URL
import java.util.Objects

import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.agent.k8s.K8SClient
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.client.configurator.v0.{InspectApi, NodeApi}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.configurator.Configurator
import oharastream.ohara.it.ContainerPlatform.ResourceRef
import org.junit.AssumptionViolatedException

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
trait ContainerPlatform {
  /**
    * setup all runtime services. The return object must be released after completing test case. Normally, it should be
    * called by after phase
    * @return a object amassing runtime object. For example, configurator service and container client
    */
  def setup(): ResourceRef

  /**
    * @return the node names exists on Configurator
    */
  def nodeNames: Set[String]
}

object ContainerPlatform {
  trait ResourceRef extends Releasable {
    def configuratorHostname: String
    def configuratorPort: Int
    def containerClient: ContainerClient
  }

  private[ContainerPlatform] def result[T](f: Future[T]): T = Await.result(f, 120 seconds)

  val K8S_MASTER_KEY: String    = "ohara.it.k8s"
  val K8S_METRICS_SERVER_URL    = "ohara.it.k8s.metrics.server"
  val K8S_NAMESPACE_KEY: String = "ohara.it.k8s.namespace"

  /**
    * form: user:password@hostname:port.
    * NOTED: this key need to be matched with another key value in ohara-it/build.gradle
    */
  val DOCKER_NODES_KEY = "ohara.it.docker"
  private[this] def _k8sMode: Option[ContainerPlatform] =
    sys.env
      .get(ContainerPlatform.K8S_MASTER_KEY)
      .map { masterUrl =>
        def createClient(): K8SClient = {
          val metricsUrl = sys.env.get(ContainerPlatform.K8S_METRICS_SERVER_URL).orNull
          K8SClient.builder
            .apiServerURL(masterUrl)
            .namespace(sys.env.getOrElse(K8S_NAMESPACE_KEY, "default"))
            .metricsApiServerURL(metricsUrl)
            .build()
        }
        val containerClient = createClient()
        try ContainerPlatform.builder
        // the master node is NOT able to run pods by default so we must exclude it
          .excludedHostname(new URL(masterUrl).getHost)
          .modeName("K8S")
          .nodes(result(containerClient.nodes()).map(_.nodeName).map(Node.apply))
          .clientCreator(() => createClient())
          .arguments(
            Seq(
              "--k8s",
              masterUrl
            ) ++ containerClient.metricsUrl.map(s => Seq("--k8s-metrics-server", s)).getOrElse(Seq.empty)
          )
          .build
        finally Releasable.close(containerClient)
      }

  /**
    * @return k8s platform information. Or skip test
    */
  def k8sMode: ContainerPlatform =
    _k8sMode.getOrElse(
      throw new AssumptionViolatedException(s"set ${ContainerPlatform.K8S_MASTER_KEY} to run IT on k8s mode")
    )

  private[this] def parserNode(nodeInfo: String): Node = {
    val user     = nodeInfo.split(":").head
    val password = nodeInfo.split("@").head.split(":").last
    val hostname = nodeInfo.split("@").last.split(":").head
    val port     = nodeInfo.split("@").last.split(":").last.toInt
    Node(
      hostname = hostname,
      port = Some(port),
      user = Some(user),
      password = Some(password),
      services = Seq.empty,
      state = State.AVAILABLE,
      error = None,
      lastModified = CommonUtils.current(),
      resources = Seq.empty,
      tags = Map.empty
    )
  }

  private[this] def _dockerMode: Option[ContainerPlatform] = {
    sys.env
      .get(ContainerPlatform.DOCKER_NODES_KEY)
      .map(plainString => plainString.split(",").map(parserNode).toSeq)
      .map { nodes =>
        ContainerPlatform.builder
          .modeName("DOCKER")
          .nodes(nodes)
          .clientCreator(() => DockerClient(DataCollie(nodes)))
          .build
      }
  }

  /**
    * @return docker platform information. Or skip test
    */
  def dockerMode: ContainerPlatform =
    _dockerMode.getOrElse(
      throw new AssumptionViolatedException(s"set ${ContainerPlatform.DOCKER_NODES_KEY} to run IT on docker mode")
    )

  private[this] val CUSTOM_CONFIGURATOR_KEY = "ohara.it.configurator"

  /**
    * custom mode accepts an existent Configurator service to run IT. In this mode, the nodes must be stored in Configurator
    * already. otherwise, RuntimeException is thrown. The container client is setup according to mode of Configurator.
    * 1) k8s mode => k8s client
    * 2) docker mode => docker client
    * 3) throw RuntimeException
    *
    * @return
    */
  private[this] def customMode: Option[ContainerPlatform] = sys.env.get(CUSTOM_CONFIGURATOR_KEY).map(_.split(":")).map {
    case Array(hostname, portString) =>
      val port  = portString.toInt
      val nodes = result(NodeApi.access.hostname(hostname).port(port).list())
      if (nodes.isEmpty) throw new RuntimeException(s"the configurator run on $hostname:$port does not store any nodes")
      val info = result(InspectApi.access.hostname(hostname).port(port).configuratorInfo())
      val clientCreator = Configurator.Mode.forName(info.mode) match {
        case Configurator.Mode.K8S =>
          () =>
            K8SClient.builder
              .apiServerURL(info.k8sUrls.get.masterUrl)
              .metricsApiServerURL(info.k8sUrls.get.metricsUrl.orNull)
              .build()
        case Configurator.Mode.DOCKER =>
          () => DockerClient(DataCollie(nodes))
        case _ =>
          throw new RuntimeException(s"${info.mode} is not supported!!!!")
      }
      new ContainerPlatform {
        override def setup(): ResourceRef = new ResourceRef {
          override def configuratorHostname: String = hostname
          override def configuratorPort: Int        = port
          override def close(): Unit = {
            // nothing
          }
          override lazy val containerClient: ContainerClient = clientCreator()
        }
        override def nodeNames: Set[String] = nodes.map(_.hostname).toSet
        override def toString: String       = "CUSTOM"
      }
    case _ =>
      throw new RuntimeException(
        s"the value of $CUSTOM_CONFIGURATOR_KEY should be <hostname>:<port> but actual is ${sys.env.get(CUSTOM_CONFIGURATOR_KEY)}"
      )
  }

  private[this] val ERROR_MESSAGE = s"please set ${ContainerPlatform.K8S_MASTER_KEY} and ${ContainerPlatform.K8S_METRICS_SERVER_URL}" +
    s"to run the IT on k8s mode; Or set ${ContainerPlatform.DOCKER_NODES_KEY} to run IT on docker mode"

  /**
    * The order of lookup is shown below.
    * 1) k8s setting - PlatformModeInfo.K8S_MASTER_KEY and PlatformModeInfo.K8S_METRICS_SERVER_URL
    * 2) docker setting - PlatformModeInfo.DOCKER_NODES_KEY
    * @return one of k8s or docker. If they are nonexistent, a AssumptionViolatedException is thrown
    */
  def default: ContainerPlatform =
    _k8sMode.orElse(_dockerMode).getOrElse(throw new AssumptionViolatedException(ERROR_MESSAGE))

  /**
    * @return k8s + docker. Or empty collection
    */
  def all: Seq[ContainerPlatform] = (_dockerMode ++ _k8sMode ++ customMode).toSeq

  /**
    * @return a empty platform that all methods throw AssumptionViolatedException
    */
  def empty: ContainerPlatform = new ContainerPlatform {
    private[this] val exception = new AssumptionViolatedException(ERROR_MESSAGE)

    override def nodeNames: Set[String] = throw exception

    override def toString: String = "EMPTY"

    override def setup(): ResourceRef = throw exception
  }

  def builder = new Builder

  private[ContainerPlatform] class Builder extends oharastream.ohara.common.pattern.Builder[ContainerPlatform] {
    private[this] var modeName: String                     = _
    private[this] var nodes: Seq[Node]                     = Seq.empty
    private[this] var arguments: Seq[String]               = Seq.empty
    private[this] var excludedHostname: Option[String]     = None
    private[this] var clientCreator: () => ContainerClient = _

    def modeName(modeName: String): Builder = {
      this.modeName = CommonUtils.requireNonEmpty(modeName)
      this
    }

    def nodes(nodes: Seq[Node]): Builder = {
      this.nodes = CommonUtils.requireNonEmpty(nodes.asJava).asScala
      this
    }

    def clientCreator(clientCreator: () => ContainerClient): Builder = {
      this.clientCreator = Objects.requireNonNull(clientCreator)
      this
    }

    def arguments(arguments: Seq[String]): Builder = {
      this.arguments = Objects.requireNonNull(arguments)
      this
    }

    /**
      * don't select this node as configurator node
      * @param hostname excluded hostname
      * @return this builder
      */
    def excludedHostname(hostname: String): Builder = {
      this.excludedHostname = Some(hostname)
      this
    }

    private[this] def createConfigurator(containerClient: ContainerClient): (String, String, Int) =
      try {
        val configuratorHostname = {
          val images = result(containerClient.imageNames())
          images
            .filter(_._2.contains(s"oharastream/configurator:${VersionUtils.VERSION}"))
            .filterNot(e => excludedHostname.contains(e._1))
            .keys
            .headOption
            .getOrElse(
              throw new RuntimeException(
                s"failed to find oharastream/configurator:${VersionUtils.VERSION} from nodes:${images.keySet.mkString(",")}"
              )
            )
        }

        val configuratorName = s"configurator-${CommonUtils.randomString(10)}"
        val configuratorPort = CommonUtils.availablePort()
        result(
          containerClient.containerCreator
            .nodeName(configuratorHostname)
            .imageName(s"oharastream/configurator:${VersionUtils.VERSION}")
            .portMappings(Map(configuratorPort -> configuratorPort))
            .arguments(
              Seq(
                "--hostname",
                configuratorHostname,
                "--port",
                configuratorPort.toString
              ) ++ Objects.requireNonNull(arguments)
            )
            // add the routes manually since not all envs have deployed the DNS.
            .routes(nodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap)
            .name(configuratorName)
            .create()
        )
        try {
          val nodeApi = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)
          // wait configurator to run and update the nodes to configurator
          CommonUtils.await(
            () => {
              val existentNodes = try result(nodeApi.list())
              catch {
                case _: Throwable => Seq.empty
              }
              nodes
                .filterNot(node => existentNodes.exists(_.hostname == node.hostname))
                .foreach(node => Releasable.close(() => result(nodeApi.request.node(node).create())))
              existentNodes.size == nodes.size
            },
            java.time.Duration.ofSeconds(60)
          )
          (configuratorName, configuratorHostname, configuratorPort)
        } catch {
          case e: Throwable =>
            Releasable.close(() => result(containerClient.forceRemove(configuratorName)))
            Releasable.close(containerClient)
            throw e
        }
      } finally Releasable.close(containerClient)

    override def build: ContainerPlatform = {
      Objects.requireNonNull(arguments)
      new ContainerPlatform {
        override def setup(): ResourceRef = {
          val (configuratorName, hostname, port) = createConfigurator(clientCreator())
          new ResourceRef {
            override def configuratorHostname: String = hostname
            override def configuratorPort: Int        = port
            override def close(): Unit = {
              Releasable.close(() => result(containerClient.forceRemove(configuratorName)))
              Releasable.close(containerClient)
            }
            override lazy val containerClient: ContainerClient = clientCreator()
          }
        }

        override val nodeNames: Set[String] =
          CommonUtils.requireNonEmpty(Builder.this.nodes.asJava).asScala.map(_.hostname).toSet

        override val toString: String = CommonUtils.requireNonEmpty(modeName)
      }
    }
  }
}
