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

package oharastream.ohara.configurator

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import oharastream.ohara.agent._
import oharastream.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.v0.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.v0.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.client.configurator.v0.{ClusterCreation, ClusterInfo, ClusterUpdating, ErrorApi, JsonRefiner}
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}
import oharastream.ohara.kafka.TopicAdmin
import spray.json.{DeserializationException, JsArray, JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}
package object route {
  /** default we restrict the jar size to 50MB */
  private[route] val DEFAULT_FILE_SIZE_BYTES = 50 * 1024 * 1024L
  private[route] val NAME_KEY: String        = oharastream.ohara.client.configurator.v0.NAME_KEY
  private[route] val GROUP_KEY: String       = oharastream.ohara.client.configurator.v0.GROUP_KEY
  private[route] val GROUP_DEFAULT: String   = oharastream.ohara.client.configurator.v0.GROUP_DEFAULT
  private[route] val TAGS_KEY: String        = oharastream.ohara.client.configurator.v0.TAGS_KEY
  private[this] val FORCE_KEY: String        = oharastream.ohara.client.configurator.v0.FORCE_KEY
  private[route] val START_COMMAND: String   = oharastream.ohara.client.configurator.v0.START_COMMAND
  private[route] val STOP_COMMAND: String    = oharastream.ohara.client.configurator.v0.STOP_COMMAND
  private[route] val PAUSE_COMMAND: String   = oharastream.ohara.client.configurator.v0.PAUSE_COMMAND
  private[route] val RESUME_COMMAND: String  = oharastream.ohara.client.configurator.v0.RESUME_COMMAND

  /**
    * this is a variety to basic route of all APIs to access ohara's "cluster" data.
    * It implements 1) get, 2) list, 3) delete, 4) add, 5) update, 6) start and 7) stop function.
    * The CREATION is routed to "POST  /root"
    * The UPDATE is routed to "PUT /root/name"
    * The GET is routed to "GET /root/name"
    * The LIST is routed to "GET /root"
    * The DELETE is routed to "DELETE /root/name"
    * The START is routed to "PUT /root/name/start"
    * The STOP is routed to "PUT /root/name/stop"
    *
    * The following routes are added autmatically.
    * The GET/LIST route auto-update the state of cluster
    * The DELETE route reject the request to a running cluster
    * @param root path to root
    * @param hookOfCreation used to convert request to response for Add function
    * @param hookOfUpdating used to convert request to response for Update function
    * @param hookOfStart used to handle start command
    * @param hookBeforeStop used to perform checks before stopping cluster
    * @param store data store
    * @param rm marshalling of creation
    * @param rm1 marshalling of update
    * @param rm2 marshalling of response
    * @param executionContext thread pool
    * @tparam Creation creation request for cluster resources
    * @tparam Updating creation request
    * @tparam Cluster cluster info
    * @return route
    */
  def clusterRoute[
    Cluster <: ClusterInfo: ClassTag,
    Creation <: ClusterCreation,
    Updating <: ClusterUpdating
  ](
    root: String,
    hookOfCreation: HookOfCreation[Creation, Cluster],
    hookOfUpdating: HookOfUpdating[Updating, Cluster],
    hookOfStart: HookOfAction[Cluster],
    hookBeforeStop: HookOfAction[Cluster],
    hookBeforeDelete: HookBeforeDelete
  )(
    implicit store: DataStore,
    objectChecker: ObjectChecker,
    meterCache: MetricsCache,
    collie: Collie,
    serviceCollie: ServiceCollie,
    rm: JsonRefiner[Creation],
    rm1: RootJsonFormat[Updating],
    rm2: JsonRefiner[Cluster],
    executionContext: ExecutionContext
  ): server.Route =
    RouteBuilder[Creation, Updating, Cluster]()
      .root(root)
      .hookOfCreation(hookOfCreation)
      .hookOfUpdating(hookOfUpdating)
      .hookOfGet(updateState[Cluster])
      .hookOfList((clusters: Seq[Cluster]) => Future.traverse(clusters)(updateState[Cluster]))
      .hookBeforeDelete(
        (key: ObjectKey) =>
          store.get[Cluster](key).flatMap {
            _.fold(Future.unit) { info =>
              updateState[Cluster](info)
                .flatMap(cluster => hookBeforeDelete(cluster.key).map(_ => cluster))
                .map(_.state)
                .map {
                  case None => ()
                  case Some(_) =>
                    throw new IllegalArgumentException(
                      s"You cannot delete a non-stopped ${classTag[Cluster].runtimeClass.getSimpleName} :$key"
                    )
                }
            }
          }
      )
      .hookOfPutAction(
        START_COMMAND,
        (clusterInfo: Cluster, subName: String, params: Map[String, String]) =>
          collie.exist(clusterInfo.key).flatMap {
            if (_) {
              // this cluster already exists, return OK
              Future.unit
            } else
              checkResourcesConflict(clusterInfo).flatMap(_ => hookOfStart(clusterInfo, subName, params))
          }
      )
      .hookOfPutAction(
        STOP_COMMAND,
        (clusterInfo: Cluster, subName: String, params: Map[String, String]) =>
          hookBeforeStop(clusterInfo, subName, params).flatMap(
            _ =>
              collie
                .clusters()
                .flatMap { clusters =>
                  if (!clusters.map(_.key).contains(clusterInfo.key)) Future.unit
                  else if (params.get(FORCE_KEY).exists(_.toLowerCase == "true")) collie.forceRemove(clusterInfo.key)
                  else collie.remove(clusterInfo.key)
                }
                .flatMap(_ => Future.unit)
          )
      )
      .hookOfFinalPutAction((clusterInfo: Cluster, nodeName: String, _: Map[String, String]) => {
        // A BIT hard-code here to reuse the checker :(
        rm.check[JsArray]("nodeNames", JsArray(JsString(nodeName)))
        collie.creator
          .settings(clusterInfo.settings)
          .nodeName(nodeName)
          .threadPool(executionContext)
          .create()
          // we have to update the nodeNames of stored cluster info. Otherwise, the following Get/List request
          // will see the out-of-date nodeNames
          .flatMap(_ => store.add(clusterInfo.newNodeNames(clusterInfo.nodeNames + nodeName)))
          .flatMap(_ => Future.unit)
      })
      .hookOfFinalDeleteAction(
        (key: ObjectKey, nodeName: String, _: Map[String, String]) =>
          store.get[Cluster](key).flatMap {
            clusterOption =>
              clusterOption
                .filter(_.nodeNames.contains(nodeName))
                .map { cluster =>
                  // there is two kind Public Deletion APIs.
                  // 1) delete a node
                  // 2) delete whole cluster
                  // and we don't want to encourage user to call "delete a node" to "remove whole cluster"
                  // hence, we throw exception here
                  if (cluster.nodeNames.size <= 1)
                    Future.failed(
                      new IllegalStateException(
                        s"there is only one instance. Please call remove to delete whole cluster"
                      )
                    )
                  else
                    collie
                      .removeNode(key, nodeName)
                      .flatMap(
                        _ =>
                          store.addIfPresent(cluster.newNodeNames(cluster.nodeNames - nodeName).asInstanceOf[Cluster])
                      )
                      .flatMap(_ => Future.unit)
                }
                .getOrElse(Future.unit)
          }
      )
      .build()

  /**
    * the url to official APIs documentation.
    * @return url string
    */
  def apiUrl: String = {
    val docVersion = if (VersionUtils.BRANCH == "master") "latest" else VersionUtils.BRANCH
    s"https://ohara.readthedocs.io/en/$docVersion/rest/index.html"
  }

  private[this] def errorWithOfficialApis(inputPath: String): ErrorApi.Error = ErrorApi.Error(
    code = s"Unsupported API: $inputPath",
    message = s"please see link to find the available APIs. input url:$inputPath",
    stack = "N/A",
    apiUrl = Some(apiUrl)
  )

  def routeToOfficialUrl(inputPath: String): server.Route =
    complete(StatusCodes.NotFound -> errorWithOfficialApis(inputPath))

  /**
    * Test whether this cluster satisfied the following rules:
    * <p>
    * 1) cluster image in all nodes
    * 2) name should not conflict
    * 3) port should not conflict
    *
    * @param serviceCollie serviceCollie instance
    * @param req cluster creation request
    * @param executionContext execution context
    * @tparam Cluster type of request
    * @return clusters that fitted the requires
    */
  private[this] def checkResourcesConflict[Cluster <: ClusterInfo: ClassTag](req: Cluster)(
    implicit executionContext: ExecutionContext,
    objectChecker: ObjectChecker,
    serviceCollie: ServiceCollie
  ): Future[Unit] =
    objectChecker.checkList
      .nodeNames(
        if (req.nodeNames.isEmpty) throw DeserializationException("node names can't be empty")
        else req.nodeNames
      )
      .allZookeepers()
      .allBrokers()
      .allWorkers()
      .allStreams()
      .allShabondis()
      .check()
      .map(
        report =>
          report.runningZookeepers ++ report.runningBrokers ++ report.runningWorkers ++ report.runningStreams ++ report.runningShabondis
      )
      // check the docker images
      .flatMap { clusters =>
        serviceCollie.imageNames().map { nodesImages =>
          req.nodeNames.foreach { nodeName =>
            val images = nodesImages.find(_._1 == nodeName).map(_._2).getOrElse(Seq.empty)
            if (!images.contains(req.imageName))
              throw new IllegalArgumentException(
                s"$nodeName does not have image:${req.imageName}. It has images:${images.mkString(",")}"
              )
          }
          clusters
        }
      }
      // check resources
      .map { clusters =>
        def serviceName(cluster: ClusterInfo): String = cluster match {
          case _: ZookeeperClusterInfo => s"zookeeper cluster:${cluster.key}"
          case _: BrokerClusterInfo    => s"broker cluster:${cluster.key}"
          case _: WorkerClusterInfo    => s"worker cluster:${cluster.key}"
          case _: StreamClusterInfo    => s"stream cluster:${cluster.key}"
          case _: ShabondiClusterInfo  => s"shabondi cluster:${cluster.key}"
          case _                       => s"cluster:${cluster.key}"
        }
        // check name conflict
        clusters
          .filter(c => classTag[Cluster].runtimeClass.isInstance(c))
          .map(_.asInstanceOf[Cluster])
          .find(_.key == req.key)
          .foreach(conflictCluster => throw new IllegalArgumentException(s"${serviceName(conflictCluster)} is running"))

        // check port conflict
        Some(
          clusters
            .flatMap { cluster =>
              val conflictPorts = cluster.ports.intersect(req.ports)
              if (conflictPorts.isEmpty) None
              else Some(cluster -> conflictPorts)
            }
            .map {
              case (cluster, conflictPorts) =>
                s"ports:${conflictPorts.mkString(",")} are used by ${serviceName(cluster)} (the port is generated randomly if it is ignored from request)"
            }
            .mkString(";")
        ).filter(_.nonEmpty).foreach(s => throw new IllegalArgumentException(s))
        ()
      }

  private[this] def updateState[Cluster <: ClusterInfo: ClassTag](
    cluster: Cluster
  )(implicit meterCache: MetricsCache, collie: Collie, executionContext: ExecutionContext): Future[Cluster] =
    collie
      .clusters()
      .map(
        statuses =>
          statuses.find(_.key == cluster.key) match {
            case None =>
              // no running cluster. It means no state and no dead nodes.
              // noted that the failed containers should still exist and we can "get" the cluster from collie.
              // the case of getting nothing from collie is only one that there is absolutely no containers and
              // we assume the cluster is NOT running.
              cluster match {
                case c: ZookeeperClusterInfo =>
                  c.copy(
                    aliveNodes = Set.empty,
                    state = None,
                    error = None
                  )
                case c: BrokerClusterInfo =>
                  c.copy(
                    aliveNodes = Set.empty,
                    state = None,
                    error = None
                  )
                case c: WorkerClusterInfo =>
                  c.copy(
                    aliveNodes = Set.empty,
                    state = None,
                    error = None
                  )
                case c: StreamClusterInfo =>
                  c.copy(
                    aliveNodes = Set.empty,
                    state = None,
                    error = None,
                    // the cluster is stooped (all containers are gone) so we don't need to fetch metrics.
                    nodeMetrics = Map.empty
                  )
                case c: ShabondiClusterInfo =>
                  c.copy(
                    aliveNodes = Set.empty,
                    state = None,
                    error = None,
                    nodeMetrics = Map.empty
                  )
              }
            case Some(status) =>
              // no running cluster. It means no state and no dead nodes.
              // noted that the failed containers should still exist and we can "get" the cluster from collie.
              // the case of getting nothing from collie is only one that there is absolutely no containers and
              // we assume the cluster is NOT running.
              cluster match {
                case c: ZookeeperClusterInfo =>
                  c.copy(
                    aliveNodes = status.aliveNodes,
                    state = status.state,
                    error = status.error,
                    lastModified = CommonUtils.current()
                  )
                case c: BrokerClusterInfo =>
                  c.copy(
                    aliveNodes = status.aliveNodes,
                    state = status.state,
                    error = status.error,
                    lastModified = CommonUtils.current()
                  )
                case c: WorkerClusterInfo =>
                  c.copy(
                    aliveNodes = status.aliveNodes,
                    state = status.state,
                    error = status.error,
                    lastModified = CommonUtils.current()
                  )
                case c: StreamClusterInfo =>
                  c.copy(
                    aliveNodes = status.aliveNodes,
                    state = status.state,
                    error = status.error,
                    lastModified = CommonUtils.current(),
                    // the cluster is stooped (all containers are gone) so we don't need to fetch metrics.
                    nodeMetrics = meterCache.meters(c, c.key)
                  )
                case c: ShabondiClusterInfo =>
                  c.copy(
                    aliveNodes = status.aliveNodes,
                    state = status.state,
                    error = status.error,
                    lastModified = CommonUtils.current(),
                    nodeMetrics = meterCache.meters(c, c.key)
                  )
              }
          }
      )
      // the actual type is erased since the clone method returns the ClusterInfo type.
      // However, it is safe to case the type to the input type since all sub classes of ClusterInfo should work well.
      .map(_.asInstanceOf[Cluster])

  /**
    * Create a topic admin according to passed cluster name.
    * Noted: if target cluster doesn't exist, an future with exception will return
    * @param clusterKey target cluster
    * @return cluster info and topic admin
    */
  def topicAdmin(clusterKey: ObjectKey)(
    implicit brokerCollie: BrokerCollie,
    store: DataStore,
    cleaner: AdminCleaner,
    executionContext: ExecutionContext
  ): Future[(BrokerClusterInfo, TopicAdmin)] =
    store.value[BrokerClusterInfo](clusterKey).flatMap(cluster => topicAdmin(cluster).map(cluster -> _))

  /**
    * Create a worker client according to passed cluster name.
    * Noted: if target cluster doesn't exist, an future with exception will return
    * @param clusterKey target cluster
    * @return cluster info and client
    */
  def connectorAdmin(clusterKey: ObjectKey)(
    implicit workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext
  ): Future[(WorkerClusterInfo, ConnectorAdmin)] =
    store
      .value[WorkerClusterInfo](clusterKey)
      .flatMap(cluster => workerCollie.connectorAdmin(cluster).map(cluster -> _))

  /**
    * create worker client and topic admin based on input worker cluster key.
    */
  def both[T](workerClusterKey: ObjectKey)(
    implicit brokerCollie: BrokerCollie,
    store: DataStore,
    cleaner: AdminCleaner,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): Future[(BrokerClusterInfo, TopicAdmin, WorkerClusterInfo, ConnectorAdmin)] =
    connectorAdmin(workerClusterKey).flatMap {
      case (wkInfo, wkClient) =>
        topicAdmin(wkInfo.brokerClusterKey).map {
          case (bkInfo, topicAdmin) => (bkInfo, cleaner.add(topicAdmin), wkInfo, wkClient)
        }
    }

  /**
    * create a topic admin and then add it to cleanup process in order to avoid resource leak.
    */
  def topicAdmin(brokerClusterInfo: BrokerClusterInfo)(
    implicit brokerCollie: BrokerCollie,
    adminCleaner: AdminCleaner,
    executionContext: ExecutionContext
  ): Future[TopicAdmin] = brokerCollie.topicAdmin(brokerClusterInfo).map(adminCleaner.add)

  /**
    * a helper method to Updating request that it remove all fields declared as non-updatable.
    * @param settings origin settings
    * @param settingDefs definitions
    * @return settings have no non-updatable
    */
  def keepEditableFields(settings: Map[String, JsValue], settingDefs: Seq[SettingDef]): Map[String, JsValue] =
    settings.filter {
      case (k, _) =>
        settingDefs.find(_.key() == k).forall(_.permission() == Permission.EDITABLE)
    }
}
