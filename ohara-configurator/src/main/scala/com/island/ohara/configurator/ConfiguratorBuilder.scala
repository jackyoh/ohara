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

import java.io.File
import java.util.Objects

import com.island.ohara.agent._
import com.island.ohara.agent.k8s.K8SClient
import com.island.ohara.client.configurator.Data
import com.island.ohara.client.configurator.v0.BrokerApi.{BrokerClusterInfo, BrokerClusterStatus}
import com.island.ohara.client.configurator.v0.NodeApi.{Node, NodeService}
import com.island.ohara.client.configurator.v0.WorkerApi.{WorkerClusterInfo, WorkerClusterStatus}
import com.island.ohara.client.configurator.v0.ZookeeperApi.{ZookeeperClusterInfo, ZookeeperClusterStatus}
import com.island.ohara.client.configurator.v0.{BrokerApi, NodeApi, WorkerApi, ZookeeperApi}
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.annotations.{Optional, VisibleForTesting}
import com.island.ohara.common.pattern.Builder
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.fake._
import com.island.ohara.configurator.store.DataStore
import org.rocksdb.RocksDBException

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
class ConfiguratorBuilder private[configurator] extends Builder[Configurator] {
  private[this] var hostname: String = _
  private[this] var port: Int = -1
  private[this] var homeFolder: String = _
  private[this] var store: DataStore = _
  private[this] var serviceCollie: ServiceCollie = _
  private[this] var k8sClient: K8SClient = _
  private[this] var k8sNamespace: String = K8SClient.NAMESPACE_DEFAULT_VALUE

  @Optional("default is random folder")
  def homeFolder(homeFolder: String): ConfiguratorBuilder = doOrReleaseObjects {
    if (this.homeFolder != null) throw new IllegalArgumentException(alreadyExistMessage("homeFolder"))
    if (this.store != null) throw new IllegalArgumentException(alreadyExistMessage("store"))
    if (this.homeFolder != null) throw new IllegalArgumentException(alreadyExistMessage("homeFolder"))
    val f = new File(CommonUtils.requireNonEmpty(homeFolder))
    if (!f.exists() && !f.mkdirs()) throw new IllegalArgumentException(s"failed to mkdir on $homeFolder")
    this.homeFolder = CommonUtils.requireFolder(f).getCanonicalPath
    this
  }

  /**
    * set advertised hostname which will be exposed by configurator.
    *
    * @param hostname used to build the rest server
    * @return this builder
    */
  @Optional("default is localhost")
  def hostname(hostname: String): ConfiguratorBuilder = doOrReleaseObjects {
    if (this.hostname != null) throw new IllegalArgumentException(alreadyExistMessage("hostname"))
    this.hostname = CommonUtils.requireNonEmpty(hostname)
    this
  }

  /**
    * configurator is bound on this port also.
    * @param port used to build the rest server
    * @return this builder
    */
  @Optional("default is random port")
  def port(port: Int): ConfiguratorBuilder = doOrReleaseObjects {
    if (this.port > 0) throw new IllegalArgumentException(alreadyExistMessage("port"))
    this.port = if (port == 0) CommonUtils.availablePort() else CommonUtils.requireConnectionPort(port)
    this
  }

  /**
    * set all client to fake mode with a pre-created broker cluster and worker cluster.
    *
    * @return this builder
    */
  @VisibleForTesting
  private[configurator] def fake(): ConfiguratorBuilder = fake(1, 1)

  /**
    * set all client to fake mode but broker client and worker client is true that they are connecting to embedded cluster.
    *
    * @return this builder
    */
  @VisibleForTesting
  private[configurator] def fake(bkConnectionProps: String, wkConnectionProps: String): ConfiguratorBuilder =
    doOrReleaseObjects {
      if (this.k8sClient != null) throw new IllegalArgumentException(alreadyExistMessage("k8sClient"))
      if (this.serviceCollie != null) throw new IllegalArgumentException(alreadyExistMessage("serviceCollie"))
      val store = getOrCreateStore()
      val embeddedZkName = ObjectKey.of(com.island.ohara.client.configurator.v0.GROUP_DEFAULT, "embeddedzk")
      val embeddedBrokerKey = ObjectKey.of(com.island.ohara.client.configurator.v0.GROUP_DEFAULT, "embeddedbk")
      val embeddedWorkerKey = ObjectKey.of(com.island.ohara.client.configurator.v0.GROUP_DEFAULT, "embeddedwk")
      // we fake nodes for embedded bk and wk
      def nodes(s: String): Seq[String] = s.split(",").map(_.split(":").head)
      import scala.concurrent.ExecutionContext.Implicits.global

      (nodes(bkConnectionProps) ++ nodes(wkConnectionProps))
      // DON'T add duplicate nodes!!!
        .toSet[String]
        .map { nodeName =>
          Node(
            hostname = nodeName,
            services = (if (bkConnectionProps.contains(nodeName))
                          Seq(NodeService(NodeApi.BROKER_SERVICE_NAME, Seq(embeddedBrokerKey)))
                        else Seq.empty)
              ++ (if (wkConnectionProps.contains(nodeName))
                    Seq(NodeService(NodeApi.WORKER_SERVICE_NAME, Seq(embeddedWorkerKey)))
                  else Seq.empty),
            port = Some(22),
            user = Some("fake"),
            password = Some("fake"),
            lastModified = CommonUtils.current(),
            validationReport = None,
            tags = Map.empty
          )
        }
        .foreach(r => store.addIfAbsent(r))
      val collie = new FakeServiceCollie(createCollie(), store, bkConnectionProps, wkConnectionProps)
      val bkCluster = {
        val pair = bkConnectionProps.split(",")
        val host = pair.map(_.split(":").head).head
        val port = pair.map(_.split(":").last).head.toInt
        BrokerClusterInfo(
          settings = BrokerApi.access.request
            .key(embeddedBrokerKey)
            .imageName("None")
            .zookeeperClusterKey(embeddedZkName)
            .clientPort(port)
            .nodeName(host)
            .creation
            .settings,
          aliveNodes = Set(host),
          // In fake mode, we need to assign a state in creation for "GET" method to act like real case
          state = Some(ServiceState.RUNNING.name),
          error = None,
          lastModified = CommonUtils.current(),
          topicDefinition = BrokerApi.TOPIC_DEFINITION
        )
      }
      val wkCluster = {
        val pair = wkConnectionProps.split(",")
        val host = pair.map(_.split(":").head).head
        val port = pair.map(_.split(":").last).head.toInt
        WorkerClusterInfo(
          settings = WorkerApi.access.request
            .key(embeddedWorkerKey)
            .brokerClusterKey(bkCluster.key)
            .clientPort(port)
            .nodeName(host)
            .creation
            .settings,
          connectorDefinitions = Await.result(WorkerClient(wkConnectionProps).connectorDefinitions(), 10 seconds),
          aliveNodes = Set(host),
          // In fake mode, we need to assign a state in creation for "GET" method to act like real case
          state = Some(ServiceState.RUNNING.name),
          error = None,
          lastModified = CommonUtils.current()
        )
      }
      //TODO: we need to add data into store to use the APIs
      //TODO: refactor this if cluster data could be stored automatically...by Sam
      store.addIfAbsent[BrokerClusterInfo](bkCluster)
      store.addIfAbsent[WorkerClusterInfo](wkCluster)

      collie.brokerCollie.addCluster(
        new BrokerClusterStatus(
          group = bkCluster.group,
          name = bkCluster.name,
          // TODO: we should check the supported arguments by the running broker images
          topicDefinition = BrokerApi.TOPIC_DEFINITION,
          aliveNodes = bkCluster.nodeNames,
          // In fake mode, we need to assign a state in creation for "GET" method to act like real case
          state = Some(ServiceState.RUNNING.name),
          error = None
        ),
        bkCluster.imageName,
        bkCluster.ports
      )
      collie.workerCollie.addCluster(
        new WorkerClusterStatus(
          group = wkCluster.group,
          name = wkCluster.name,
          connectorDefinitions = wkCluster.connectorDefinitions,
          aliveNodes = wkCluster.nodeNames,
          // In fake mode, we need to assign a state in creation for "GET" method to act like real case
          state = Some(ServiceState.RUNNING.name),
          error = None
        ),
        wkCluster.imageName,
        wkCluster.ports
      )
      serviceCollie(collie)
    }

  /**
    * Create a fake collie with specified number of broker/worker cluster.
    * @param numberOfBrokerCluster number of broker cluster
    * @param numberOfWorkerCluster number of worker cluster
    * @return this builder
    */
  @VisibleForTesting
  private[configurator] def fake(numberOfBrokerCluster: Int,
                                 numberOfWorkerCluster: Int,
                                 zkClusterNamePrefix: String = "fakezkcluster",
                                 bkClusterNamePrefix: String = "fakebkcluster",
                                 wkClusterNamePrefix: String = "fakewkcluster"): ConfiguratorBuilder =
    doOrReleaseObjects {
      if (this.k8sClient != null) throw new IllegalArgumentException(alreadyExistMessage("k8sClient"))
      if (this.serviceCollie != null) throw new IllegalArgumentException(alreadyExistMessage("serviceCollie"))
      if (numberOfBrokerCluster < 0)
        throw new IllegalArgumentException(s"numberOfBrokerCluster:$numberOfBrokerCluster should be positive")
      if (numberOfWorkerCluster < 0)
        throw new IllegalArgumentException(s"numberOfWorkerCluster:$numberOfWorkerCluster should be positive")
      if (numberOfBrokerCluster <= 0 && numberOfWorkerCluster > 0)
        throw new IllegalArgumentException(s"you must initialize bk cluster before you initialize wk cluster")
      val store = getOrCreateStore()
      val collie = new FakeServiceCollie(createCollie(), store)

      import scala.concurrent.ExecutionContext.Implicits.global
      val zkCreations = (0 until numberOfBrokerCluster).map { index =>
        val nodeNames = (0 to 2).map(_ => CommonUtils.randomString(5)).toSet
        val creation = ZookeeperApi.access.request
          .name(s"$zkClusterNamePrefix$index")
          .imageName(s"fakeImage$index")
          .nodeNames(nodeNames)
          .creation
        collie.zookeeperCollie.addCluster(
          new ZookeeperClusterStatus(
            group = creation.group,
            name = creation.name,
            aliveNodes = nodeNames,
            // In fake mode, we need to assign a state in creation for "GET" method to act like real case
            state = Some(ServiceState.RUNNING.name),
            error = None
          ),
          creation.imageName,
          creation.ports
        )
        creation
      }

      // add broker cluster
      val bkCreations = zkCreations.zipWithIndex.map {
        case (zkCreation, index) =>
          val creation = BrokerApi.access.request
            .name(s"$bkClusterNamePrefix$index")
            .imageName(s"fakeImage$index")
            .zookeeperClusterKey(zkCreation.key)
            .nodeNames(zkCreation.nodeNames)
            .creation
          collie.brokerCollie.addCluster(
            new BrokerClusterStatus(
              group = creation.group,
              name = creation.name,
              // TODO: we should check the supported arguments by the running broker images
              topicDefinition = BrokerApi.TOPIC_DEFINITION,
              aliveNodes = zkCreation.nodeNames,
              // In fake mode, we need to assign a state in creation for "GET" method to act like real case
              state = Some(ServiceState.RUNNING.name),
              error = None
            ),
            creation.imageName,
            creation.ports
          )
          creation
      }

      val wkCreations = (0 until numberOfWorkerCluster).map { _ =>
        val bkCreation = bkCreations((Math.random() % bkCreations.size).asInstanceOf[Int])
        val creation =
          WorkerApi.access.request.brokerClusterKey(bkCreation.key).nodeNames(bkCreation.nodeNames).creation
        collie.workerCollie.addCluster(
          new WorkerClusterStatus(
            group = creation.group,
            name = creation.name,
            connectorDefinitions = ReflectionUtils.localConnectorDefinitions,
            aliveNodes = bkCreation.nodeNames,
            // In fake mode, we need to assign a state in creation for "GET" method to act like real case
            state = Some(ServiceState.RUNNING.name),
            error = None
          ),
          creation.imageName,
          creation.ports
        )
        creation
      }

      //TODO: we need to add data into store to use the APIs
      //TODO: refactor this if cluster data could be stored automatically...by Sam
      zkCreations
        .map(
          creation =>
            ZookeeperClusterInfo(
              settings = creation.settings,
              aliveNodes = creation.nodeNames,
              // In fake mode, we need to assign a state in creation for "GET" method to act like real case
              state = Some(ServiceState.RUNNING.name),
              error = None,
              lastModified = CommonUtils.current()
          ))
        .foreach(store.addIfAbsent[ZookeeperClusterInfo])
      bkCreations
        .map(
          creation =>
            BrokerClusterInfo(
              settings = creation.settings,
              // TODO: we should check the supported arguments by the running broker images
              topicDefinition = BrokerApi.TOPIC_DEFINITION,
              aliveNodes = creation.nodeNames,
              // In fake mode, we need to assign a state in creation for "GET" method to act like real case
              state = Some(ServiceState.RUNNING.name),
              error = None,
              lastModified = CommonUtils.current()
          ))
        .foreach(store.addIfAbsent[BrokerClusterInfo])
      wkCreations
        .map(
          creation =>
            WorkerClusterInfo(
              settings = creation.settings,
              connectorDefinitions = ReflectionUtils.localConnectorDefinitions,
              aliveNodes = creation.nodeNames,
              // In fake mode, we need to assign a state in creation for "GET" method to act like real case
              state = Some(ServiceState.RUNNING.name),
              error = None,
              lastModified = CommonUtils.current()
          ))
        .foreach(store.addIfAbsent[WorkerClusterInfo])

      // fake nodes
      zkCreations
        .flatMap(_.nodeNames)
        // DON'T add duplicate nodes!!!
        .toSet[String]
        .map(name =>
          Node(
            hostname = name,
            port = Some(22),
            user = Some("fake"),
            password = Some("fake"),
            services = Seq.empty,
            lastModified = CommonUtils.current(),
            validationReport = None,
            tags = Map.empty
        ))
        .foreach(store.addIfAbsent[Node])
      serviceCollie(collie)
    }

  @VisibleForTesting
  @Optional("default is implemented by ssh")
  private[configurator] def serviceCollie(serviceCollie: ServiceCollie): ConfiguratorBuilder = doOrReleaseObjects {
    if (this.serviceCollie != null) throw new IllegalArgumentException(alreadyExistMessage("serviceCollie"))
    this.serviceCollie = Objects.requireNonNull(serviceCollie)
    this
  }

  /**
    * Set a k8s client to enable container collie to use k8s platform. If you don't set it, the default implementation apply the ssh connection
    * to control containers on remote nodes.
    * @param k8sClient k8s client
    * @return this builder
    */
  @Optional("default is null")
  def k8sClient(k8sClient: K8SClient): ConfiguratorBuilder = doOrReleaseObjects {
    if (this.k8sClient != null) throw new IllegalArgumentException(alreadyExistMessage("k8sClient"))
    if (this.serviceCollie != null) throw new IllegalArgumentException(alreadyExistMessage("serviceCollie"))
    this.k8sClient = Objects.requireNonNull(k8sClient)
    this
  }

  /**
    * Set a k8s namespace to use k8s platform
    * @param namespace k8s namespace
    * @return this builder
    */
  @Optional("default value is default")
  private[configurator] def k8sNamespace(namespace: String): ConfiguratorBuilder = {
    this.k8sNamespace = namespace
    this
  }

  /**
    * Set a k8s api server url
    * @param url k8s api server url
    * @return this builder
    */
  @Optional("default value is null")
  private[configurator] def k8sApiServer(url: String): ConfiguratorBuilder = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val client = K8SClient(url, k8sNamespace)
    try if (Await.result(client.nodeNameIPInfo(), 30 seconds).isEmpty)
      throw new IllegalArgumentException("your k8s clusters is empty!!!")
    catch {
      case e: Throwable =>
        throw new IllegalArgumentException(s"unable to access k8s cluster:$url", e)
    }
    this.k8sClient(client)
    this
  }

  private[configurator] def createCollie(): DataCollie = {
    val store = getOrCreateStore()
    new DataCollie {
      override def value[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[T] =
        store.value[T](key)
      override def values[T <: Data: ClassTag]()(implicit executor: ExecutionContext): Future[Seq[T]] =
        store.values[T]()
    }
  }

  override def build(): Configurator = doOrReleaseObjects(
    new Configurator(hostname = getOrCreateHostname(), port = getOrCreatePort())(store = getOrCreateStore(),
                                                                                 dataCollie = createCollie(),
                                                                                 serviceCollie = getOrCreateCollie(),
                                                                                 k8sClient = Option(k8sClient)))

  private[this] def folder(prefix: String): String =
    new File(CommonUtils.requireNonEmpty(getOrCreateHomeFolder()), prefix).getCanonicalPath

  private[this] def getOrCreateHostname(): String = {
    if (hostname == null) hostname = CommonUtils.hostname()
    hostname
  }

  private[this] def getOrCreatePort(): Int = {
    if (port <= 0) port = CommonUtils.availablePort()
    port
  }

  private[this] def getOrCreateHomeFolder(): String = {
    if (homeFolder == null) homeFolder = CommonUtils.createTempFolder("configurator").getCanonicalPath
    homeFolder
  }

  private[this] def getOrCreateStore(): DataStore = if (store == null) try {
    store = DataStore.builder.persistentFolder(folder("store")).build()
    store
  } catch {
    case e: RocksDBException =>
      if (e.getMessage.contains("Permission denied"))
        throw new RuntimeException(
          "Permission denied! if you are trying to mount host folder to Configurator container, " +
            "the UID of the host folder must be 1000 since the user of Configurator container is ohara and its UID is 1000",
          e
        )
      else throw e
  } else store

  private[this] def getOrCreateCollie(): ServiceCollie = if (serviceCollie == null) {
    this.serviceCollie =
      if (k8sClient == null) ServiceCollie.builderOfSsh.dataCollie(createCollie()).build
      else ServiceCollie.builderOfK8s().dataCollie(createCollie()).k8sClient(k8sClient).build()
    serviceCollie
  } else serviceCollie

  /**
    * do the action and auto-release all internal objects if the action fails.
    * @param f action
    * @tparam T return type
    * @return object created by action
    */
  private[this] def doOrReleaseObjects[T](f: => T): T = try f
  catch {
    case t: Throwable =>
      Configurator.LOG.error("failed to pre-create resource", t)
      cleanup()
      throw t
  }

  private[this] def alreadyExistMessage(key: String) = s"$key already exists!!!"

  /**
    * Configurator Builder take many resources so as to create a Configurator. However, in testing we may fail in assigning a part of resources
    * and the others are leak. It does hurt production since we can't do anything if we fail to start up a configurator. However, in testing we
    * have to keep running the testing...
    */
  private[configurator] def cleanup(): Unit = {
    Releasable.close(store)
    store = null
    Releasable.close(serviceCollie)
    serviceCollie = null
    k8sClient = null
  }
}
