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

package oharastream.ohara.it.collie

import java.time.Duration
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.client.configurator.v0.{BrokerApi, ClusterInfo, ContainerApi, LogApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.exception.{ExecutionException, TimeoutException}
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.{PaltformModeInfo, WithRemoteConfigurator}
import oharastream.ohara.kafka.{Consumer, Producer, TopicAdmin}
import oharastream.ohara.metrics.BeanChannel
import com.typesafe.scalalogging.Logger
import oharastream.ohara.it.category.CollieGroup
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.junit.experimental.categories.Category
import org.junit.Test
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * This abstract class extracts the "required" information of running tests on true env.
  * All checks are verified in this class but we do run all test cases on different test in order to avoid
  * slow test cases run by single test jvm.
  *
  * NOTED: this test will forward random ports so it would be better to "close" firewall of remote node.
  *
  * Noted: this test depends on the ClusterNameHolder which helps us to cleanup all containers for all tests cases.
  * Hence, you don't need to add "finally" hook to do the cleanup.
  */
@Category(Array(classOf[CollieGroup]))
class TestCollie(paltform: PaltformModeInfo) extends WithRemoteConfigurator(paltform: PaltformModeInfo) {
  private[this] val log              = Logger(classOf[TestCollie])
  private[this] val numberOfClusters = 2

  private[this] def zkApi = ZookeeperApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def bkApi = BrokerApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def wkApi = WorkerApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def logApi = LogApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def containerApi = ContainerApi.access.hostname(configuratorHostname).port(configuratorPort)

  //--------------------------------------------------[zk operations]--------------------------------------------------//
  private[this] def zk_exist(clusterKey: ObjectKey): Future[Boolean] =
    zkApi.list().map(_.exists(_.key == clusterKey))
  private[this] def zk_create(
    clusterKey: ObjectKey,
    jmxPort: Int,
    clientPort: Int,
    electionPort: Int,
    peerPort: Int,
    nodeNames: Set[String]
  ): Future[ZookeeperClusterInfo] =
    zkApi.request
      .key(clusterKey)
      .jmxPort(jmxPort)
      .clientPort(clientPort)
      .electionPort(electionPort)
      .peerPort(peerPort)
      .nodeNames(nodeNames)
      .create()
  private[this] def zk_start(clusterKey: ObjectKey): Future[Unit] = zkApi.start(clusterKey)
  private[this] def zk_stop(clusterKey: ObjectKey): Future[Unit] =
    zkApi.forceStop(clusterKey).map(_ => Unit)
  private[this] def zk_cluster(clusterKey: ObjectKey): Future[ZookeeperClusterInfo] =
    zk_clusters().map(_.find(_.key == clusterKey).get)
  private[this] def zk_clusters(): Future[Seq[ZookeeperApi.ZookeeperClusterInfo]] = zkApi.list()

  private[this] def zk_logs(clusterKey: ObjectKey): Future[Seq[String]] =
    logApi.log4ZookeeperCluster(clusterKey).map(_.logs.map(_.value))

  private[this] def zk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def zk_delete(clusterKey: ObjectKey): Future[Unit] = zkApi.delete(clusterKey)

  //--------------------------------------------------[bk operations]--------------------------------------------------//
  private[this] def bk_exist(clusterKey: ObjectKey): Future[Boolean] =
    bkApi.list().map(_.exists(_.key == clusterKey))

  private[this] def bk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    jmxPort: Int,
    zookeeperClusterKey: ObjectKey,
    nodeNames: Set[String]
  ): Future[BrokerApi.BrokerClusterInfo] =
    bkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .zookeeperClusterKey(zookeeperClusterKey)
      .nodeNames(nodeNames)
      .create()

  def bk_cluster(clusterKey: ObjectKey): Future[BrokerClusterInfo] =
    bk_clusters().map(_.find(_.key == clusterKey).get)

  private[this] def bk_start(clusterKey: ObjectKey): Future[Unit] = bkApi.start(clusterKey)

  private[this] def bk_stop(clusterKey: ObjectKey): Future[Unit] =
    bkApi.forceStop(clusterKey).map(_ => Unit)

  private[this] def bk_clusters(): Future[Seq[BrokerApi.BrokerClusterInfo]] = bkApi.list()

  private[this] def bk_logs(clusterKey: ObjectKey): Future[Seq[String]] =
    logApi.log4BrokerCluster(clusterKey).map(_.logs.map(_.value))

  private[this] def bk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def bk_delete(clusterKey: ObjectKey): Future[Unit] = bkApi.delete(clusterKey)

  private[this] def bk_addNode(clusterKey: ObjectKey, nodeName: String): Future[BrokerApi.BrokerClusterInfo] =
    bkApi.addNode(clusterKey, nodeName).flatMap(_ => bkApi.get(clusterKey))

  private[this] def bk_removeNode(clusterKey: ObjectKey, nodeName: String): Future[Unit] =
    bkApi.removeNode(clusterKey, nodeName)

  //--------------------------------------------------[wk operations]--------------------------------------------------//
  private[this] def wk_exist(clusterKey: ObjectKey): Future[Boolean] =
    wkApi.list().map(_.exists(_.key == clusterKey))

  private[this] def wk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    jmxPort: Int,
    brokerClusterKey: ObjectKey,
    nodeNames: Set[String]
  ): Future[WorkerApi.WorkerClusterInfo] =
    wkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .brokerClusterKey(brokerClusterKey)
      .nodeNames(nodeNames)
      .create()

  private[this] def wk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    jmxPort: Int,
    groupId: String,
    configTopicName: String,
    statusTopicName: String,
    offsetTopicName: String,
    brokerClusterKey: ObjectKey,
    nodeNames: Set[String]
  ): Future[WorkerApi.WorkerClusterInfo] =
    wkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .brokerClusterKey(brokerClusterKey)
      .nodeNames(nodeNames)
      .groupId(groupId)
      .configTopicName(configTopicName)
      .statusTopicName(statusTopicName)
      .offsetTopicName(offsetTopicName)
      .create()

  private[this] def wk_start(clusterKey: ObjectKey): Future[Unit] = wkApi.start(clusterKey)

  private[this] def wk_stop(clusterKey: ObjectKey): Future[Unit] =
    wkApi.forceStop(clusterKey).map(_ => Unit)

  private[this] def wk_clusters(): Future[Seq[WorkerApi.WorkerClusterInfo]] = wkApi.list()

  private[this] def wk_logs(clusterKey: ObjectKey): Future[Seq[String]] =
    logApi.log4WorkerCluster(clusterKey).map(_.logs.map(_.value))

  private[this] def wk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def wk_delete(clusterKey: ObjectKey): Future[Unit] = wkApi.delete(clusterKey)

  private[this] def wk_addNode(clusterKey: ObjectKey, nodeName: String): Future[WorkerApi.WorkerClusterInfo] =
    wkApi.addNode(clusterKey, nodeName).flatMap(_ => wkApi.get(clusterKey))

  private[this] def wk_removeNode(clusterKey: ObjectKey, nodeName: String): Future[Unit] =
    wkApi.removeNode(clusterKey, nodeName)
  private[this] def wk_cluster(clusterKey: ObjectKey): Future[WorkerClusterInfo] =
    wk_clusters().map(_.find(_.key == clusterKey).get)

  @Test
  def testZk(): Unit = {
    log.info("start to run zookeeper cluster")
    val nodeName: String = nodes.head.name
    val clusterKey       = serviceNameHolder.generateClusterKey()
    result(zk_exist(clusterKey)) shouldBe false
    val jmxPort      = CommonUtils.availablePort()
    val clientPort   = CommonUtils.availablePort()
    val electionPort = CommonUtils.availablePort()
    val peerPort     = CommonUtils.availablePort()
    def assert(zkCluster: ZookeeperClusterInfo): ZookeeperClusterInfo = {
      zkCluster.key shouldBe clusterKey
      zkCluster.nodeNames.head shouldBe nodeName
      zkCluster.jmxPort shouldBe jmxPort
      zkCluster.clientPort shouldBe clientPort
      zkCluster.peerPort shouldBe peerPort
      zkCluster.electionPort shouldBe electionPort
      zkCluster
    }

    val zkCluster = assert(
      result(
        zk_create(
          clusterKey = clusterKey,
          jmxPort = jmxPort,
          clientPort = clientPort,
          electionPort = electionPort,
          peerPort = peerPort,
          nodeNames = Set(nodeName)
        )
      )
    )
    result(zk_start(zkCluster.key))
    assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    assert(result(zk_cluster(zkCluster.key)))
    log.info("start to run zookeeper cluster ... done")
    result(zk_exist(zkCluster.key)) shouldBe true
    // we can't assume the size since other tests may create zk cluster at the same time
    result(zk_clusters()).isEmpty shouldBe false
    log.info(s"verify number of zk clusters... done")
    result(zk_logs(clusterKey)).size shouldBe 1
    log.info(s"verify number of log... done")
    result(zk_logs(clusterKey)).foreach(
      log =>
        withClue(log) {
          log.toLowerCase().contains("exception") shouldBe false
          log.isEmpty shouldBe false
        }
    )
    log.info(s"verify log of zk clusters... done")
    result(zk_containers(clusterKey)).foreach { container =>
      container.nodeName shouldBe nodeName
      container.name.contains(clusterKey.name()) shouldBe true
      container.name should not be container.hostname
      container.name.length should be > container.hostname.length
      container.portMappings.size shouldBe 4
      container.portMappings.exists(_.containerPort == clientPort) shouldBe true
      container.portMappings.exists(_.containerPort == electionPort) shouldBe true
      container.portMappings.exists(_.containerPort == peerPort) shouldBe true
    }
    log.info(s"get containers from zk:$clusterKey... done")
    testJmx(zkCluster)
    log.info(s"test jmx from zk:$clusterKey... done")
    result(zk_stop(clusterKey))
    await(() => {
      // In configurator mode: clusters() will return the "stopped list" in normal case
      // In collie mode: clusters() will return the "cluster list except stop one" in normal case
      // we should consider these two cases by case...
      val clusters = result(zk_clusters())
      !clusters.map(_.key).contains(clusterKey) || clusters.find(_.key == clusterKey).get.state.isEmpty
    })
    // the cluster is stopped actually, delete the data
    zk_delete(clusterKey)
  }

  @Test
  def testBroker(): Unit = {
    val zkCluster = result(
      zk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        jmxPort = CommonUtils.availablePort(),
        clientPort = CommonUtils.availablePort(),
        electionPort = CommonUtils.availablePort(),
        peerPort = CommonUtils.availablePort(),
        nodeNames = Set(nodes.head.name)
      )
    )
    result(zk_start(zkCluster.key))
    assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    log.info("[BROKER] start to run broker cluster")
    val clusterKey = serviceNameHolder.generateClusterKey()
    result(bk_exist(clusterKey)) shouldBe false
    log.info(s"[BROKER] verify existence of broker cluster:$clusterKey...done")
    val nodeName: String = nodes.head.name
    val clientPort       = CommonUtils.availablePort()
    val jmxPort          = CommonUtils.availablePort()
    def assert(brokerCluster: BrokerClusterInfo): BrokerClusterInfo = {
      brokerCluster.key shouldBe clusterKey
      brokerCluster.zookeeperClusterKey shouldBe zkCluster.key
      brokerCluster.nodeNames.head shouldBe nodeName
      brokerCluster.clientPort shouldBe clientPort
      brokerCluster.jmxPort shouldBe jmxPort
      brokerCluster
    }

    val bkCluster = assert(
      result(
        bk_create(
          clusterKey = clusterKey,
          clientPort = clientPort,
          jmxPort = jmxPort,
          zookeeperClusterKey = zkCluster.key,
          nodeNames = Set(nodeName)
        )
      )
    )
    result(bk_start(bkCluster.key))
    log.info("[BROKER] start to run broker cluster...done")
    assertCluster(() => result(bk_clusters()), () => result(bk_containers(bkCluster.key)), bkCluster.key)
    assert(result(bk_cluster(bkCluster.key)))
    log.info("[BROKER] verify cluster api...done")
    result(bk_exist(bkCluster.key)) shouldBe true
    // we can't assume the size since other tests may create zk cluster at the same time
    result(bk_clusters()).isEmpty shouldBe false
    result(bk_containers(clusterKey)).foreach { container =>
      container.nodeName shouldBe nodeName
      container.name.contains(clusterKey.name) shouldBe true
      container.name should not be container.hostname
      container.name.length should be > container.hostname.length
      container.portMappings.size shouldBe 2
      container.portMappings.exists(_.containerPort == clientPort) shouldBe true
    }
    result(bk_logs(clusterKey)).size shouldBe 1
    result(bk_logs(clusterKey)).foreach(
      log =>
        withClue(log) {
          log.toLowerCase().contains("exception") shouldBe false
          log.isEmpty shouldBe false
        }
    )
    log.info("[BROKER] verify:log done")
    var curCluster = bkCluster
    testTopic(curCluster)
    testJmx(curCluster)
    curCluster = testAddNodeToRunningBrokerCluster(curCluster)
    testTopic(curCluster)
    testJmx(curCluster)
    // we can't remove the node used to set up broker cluster since the internal topic is hosted by it.
    // In this test case, the internal topic has single replication. If we remove the node host the internal topic
    // the broker cluster will be out-of-sync and all data operation will be frozen.
    curCluster = testRemoveNodeToRunningBrokerCluster(curCluster, nodeName)
    testTopic(curCluster)
    testJmx(curCluster)
    result(bk_stop(clusterKey))
    await(() => {
      // In configurator mode: clusters() will return the "stopped list" in normal case
      // In collie mode: clusters() will return the "cluster list except stop one" in normal case
      // we should consider these two cases by case...
      val clusters = result(bk_clusters())
      !clusters.map(_.key).contains(clusterKey) || clusters.find(_.key == clusterKey).get.state.isEmpty
    })
    // the cluster is stopped actually, delete the data
    bk_delete(clusterKey)
  }

  private[this] def testAddNodeToRunningBrokerCluster(previousCluster: BrokerClusterInfo): BrokerClusterInfo = {
    await(() => result(bk_exist(previousCluster.key)))
    log.info(s"[BROKER] nodes:$nodes previous:${previousCluster.nodeNames}")
    an[IllegalArgumentException] should be thrownBy result(
      bk_removeNode(previousCluster.key, previousCluster.nodeNames.head)
    )
    val freeNodes = nodes.filterNot(node => previousCluster.nodeNames.contains(node.name))
    if (freeNodes.nonEmpty) {
      await { () =>
        // nothing happens if we add duplicate nodes
        result(bk_addNode(previousCluster.key, previousCluster.nodeNames.head))
        // we can't add a nonexistent node
        // we always get IllegalArgumentException if we sent request by restful api
        // However, if we use collie impl, an NoSuchElementException will be thrown...
        an[Throwable] should be thrownBy result(bk_addNode(previousCluster.key, CommonUtils.randomString()))
        val newNode = freeNodes.head.name
        log.info(s"[BROKER] add new node:$newNode to cluster:${previousCluster.key}")
        val newCluster = result(bk_addNode(previousCluster.key, newNode))
        log.info(s"[BROKER] add new node:$newNode to cluster:${previousCluster.key}...done")
        newCluster.key shouldBe previousCluster.key
        newCluster.imageName shouldBe previousCluster.imageName
        newCluster.zookeeperClusterKey shouldBe previousCluster.zookeeperClusterKey
        newCluster.clientPort shouldBe previousCluster.clientPort
        newCluster.nodeNames.size - previousCluster.nodeNames.size shouldBe 1
        result(bk_cluster(newCluster.key)).aliveNodes.contains(newNode)
      }
      result(bk_cluster(previousCluster.key))
    } else previousCluster
  }

  private[this] def testTopic(cluster: BrokerClusterInfo): Unit = {
    val topicName = CommonUtils.randomString()
    val brokers   = cluster.nodeNames.map(_ + s":${cluster.clientPort}").mkString(",")
    log.info(s"[BROKER] start to create topic:$topicName on broker cluster:$brokers")
    val topicAdmin = TopicAdmin.of(brokers)
    try {
      log.info(s"[BROKER] start to check the sync information. active broker nodes:${cluster.nodeNames}")
      // make sure all active broker nodes are sync!
      await(() => topicAdmin.brokerPorts().toCompletableFuture.get().size() == cluster.nodeNames.size)
      log.info(s"[BROKER] start to check the sync information. active broker nodes:${cluster.nodeNames} ... done")
      log.info(s"[BROKER] number of replications:${cluster.nodeNames.size}")
      await(
        () =>
          try {
            topicAdmin.topicCreator().numberOfPartitions(1).numberOfReplications(1).topicName(topicName).create()
            true
          } catch {
            case e: ExecutionException =>
              e.getCause match {
                // the new broker needs time to sync information to other existed bk nodes.
                case _: InvalidReplicationFactorException => false
                case _: Throwable                         => throw e.getCause
              }
            case e: TimeoutException =>
              log.error(s"[BROKER] create topic error ${e.getCause}")
              false
          }
      )
      log.info(s"[BROKER] start to create topic:$topicName on broker cluster:$brokers ... done")
      val producer = Producer
        .builder()
        .connectionProps(brokers)
        .allAcks()
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .build()
      val numberOfRecords = 5
      log.info(s"[BROKER] start to send $numberOfRecords data")
      (0 until numberOfRecords).foreach(
        index => producer.sender().key(index.toString).value(index.toString).topicName(topicName).send()
      )
      producer.flush()
      producer.close()
      log.info(s"[BROKER] start to send data ... done")
      log.info(s"[BROKER] start to receive data")
      val consumer = Consumer
        .builder()
        .connectionProps(brokers)
        .offsetFromBegin()
        .topicName(topicName)
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .build()
      try {
        val records = consumer.poll(Duration.ofSeconds(30), numberOfRecords)
        records.size() shouldBe numberOfRecords
        records.stream().forEach(record => record.key().get() shouldBe record.value().get())
        log.info(s"[BROKER] start to receive data ... done")
      } finally consumer.close()
      topicAdmin.deleteTopic(topicName)
    } finally topicAdmin.close()
  }

  private[this] def testRemoveNodeToRunningBrokerCluster(
    previousCluster: BrokerClusterInfo,
    excludedNode: String
  ): BrokerClusterInfo = {
    await(() => result(bk_exist(previousCluster.key)))
    if (previousCluster.nodeNames.size > 1) {
      val beRemovedNode: String = previousCluster.nodeNames.filter(_ != excludedNode).head
      await { () =>
        log.info(
          s"[BROKER] start to remove node:$beRemovedNode from bk cluster:${previousCluster.key} nodes:${previousCluster.nodeNames
            .mkString(",")}"
        )
        result(bk_removeNode(previousCluster.key, beRemovedNode))
        log.info(s"[BROKER] start to remove node:$beRemovedNode from bk cluster:${previousCluster.key} ... done")
        val newCluster = result(bk_cluster(previousCluster.key))
        newCluster.key == previousCluster.key &&
        newCluster.imageName == previousCluster.imageName &&
        newCluster.zookeeperClusterKey == previousCluster.zookeeperClusterKey &&
        newCluster.clientPort == previousCluster.clientPort &&
        previousCluster.nodeNames.size - newCluster.nodeNames.size == 1 &&
        !result(bk_cluster(newCluster.key)).aliveNodes.contains(beRemovedNode)
      }
      result(bk_cluster(previousCluster.key))
    } else previousCluster
  }

  @Test
  def testWorker(): Unit = {
    val zkCluster = result(
      zk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        jmxPort = CommonUtils.availablePort(),
        clientPort = CommonUtils.availablePort(),
        electionPort = CommonUtils.availablePort(),
        peerPort = CommonUtils.availablePort(),
        nodeNames = Set(nodes.head.name)
      )
    )
    result(zk_start(zkCluster.key))
    assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    val bkCluster = result(
      bk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        clientPort = CommonUtils.availablePort(),
        jmxPort = CommonUtils.availablePort(),
        zookeeperClusterKey = zkCluster.key,
        nodeNames = Set(nodes.head.name)
      )
    )
    result(bk_start(bkCluster.key))
    assertCluster(() => result(bk_clusters()), () => result(bk_containers(bkCluster.key)), bkCluster.key)
    log.info("[WORKER] start to test worker")
    val nodeName   = nodes.head.name
    val clusterKey = serviceNameHolder.generateClusterKey()
    result(wk_exist(clusterKey)) shouldBe false
    log.info("[WORKER] verify:nonExists done")
    val clientPort = CommonUtils.availablePort()
    val jmxPort    = CommonUtils.availablePort()
    def assert(workerCluster: WorkerClusterInfo): WorkerClusterInfo = {
      workerCluster.brokerClusterKey shouldBe bkCluster.key
      workerCluster.key shouldBe clusterKey
      workerCluster.nodeNames.head shouldBe nodeName
      workerCluster.clientPort shouldBe clientPort
      workerCluster.jmxPort shouldBe jmxPort
      workerCluster.configTopicPartitions shouldBe 1
      workerCluster.configTopicReplications shouldBe 1
      workerCluster.statusTopicPartitions shouldBe 1
      workerCluster.statusTopicReplications shouldBe 1
      workerCluster.offsetTopicPartitions shouldBe 1
      workerCluster.offsetTopicReplications shouldBe 1
      workerCluster
    }
    log.info("[WORKER] create ...")
    val wkCluster = assert(
      result(
        wk_create(
          clusterKey = clusterKey,
          clientPort = clientPort,
          jmxPort = jmxPort,
          brokerClusterKey = bkCluster.key,
          nodeNames = Set(nodeName)
        )
      )
    )
    log.info("[WORKER] create done")
    result(wk_start(wkCluster.key))
    log.info("[WORKER] start done")
    assertCluster(() => result(wk_clusters()), () => result(wk_containers(wkCluster.key)), wkCluster.key)
    log.info("[WORKER] check existence")
    assert(result(wk_cluster(wkCluster.key)))
    log.info("[WORKER] verify:create done")
    result(wk_exist(wkCluster.key)) shouldBe true
    log.info("[WORKER] verify:exist done")
    // we can't assume the size since other tests may create zk cluster at the same time
    result(wk_clusters()).isEmpty shouldBe false
    log.info("[WORKER] verify:list done")
    result(wk_containers(clusterKey)).foreach { container =>
      container.nodeName shouldBe nodeName
      container.name.contains(clusterKey.name) shouldBe true
      container.name should not be container.hostname
      container.name.length should be > container.hostname.length
      // [BEFORE] ServiceCollieImpl applies --network=host to all worker containers so there is no port mapping.
      // The following checks are disabled rather than deleted since it seems like a bug if we don't check the port mapping.
      // [AFTER] ServiceCollieImpl use bridge network now
      container.portMappings.size shouldBe 2
      container.portMappings.exists(_.containerPort == clientPort) shouldBe true
    }
    val logs = result(wk_logs(clusterKey))
    logs.size shouldBe 1
    logs.foreach(
      log =>
        withClue(log) {
          log.contains("- ERROR") shouldBe false
          // we cannot assume "k8s get logs" are complete since log may rotate
          // so log could be empty in k8s environment
          // also see : https://github.com/kubernetes/kubernetes/issues/11046#issuecomment-121140315
        }
    )
    log.info("[WORKER] verify:log done")
    var curCluster = wkCluster
    testConnectors(curCluster)
    testJmx(curCluster)
    curCluster = testAddNodeToRunningWorkerCluster(curCluster)
    testConnectors(curCluster)
    testJmx(curCluster)
    // The issue of inconsistent settings after adding/removing nodes.
    // This issue is going to resolve in #2677.
    curCluster = testRemoveNodeToRunningWorkerCluster(curCluster, nodeName)
    testConnectors(curCluster)
    testJmx(curCluster)
    result(wk_stop(clusterKey))
    await(() => {
      // In configurator mode: clusters() will return the "stopped list" in normal case
      // In collie mode: clusters() will return the "cluster list except stop one" in normal case
      // we should consider these two cases by case...
      val clusters = result(wk_clusters())
      !clusters.map(_.key).contains(clusterKey) || clusters.find(_.key == clusterKey).get.state.isEmpty
    })
    // the cluster is stopped actually, delete the data
    wk_delete(clusterKey)
  }

  private[this] def testConnectors(cluster: WorkerClusterInfo): Unit =
    await(
      () =>
        try {
          log.info(s"worker node head: ${cluster.nodeNames.head}:${cluster.clientPort}")
          result(ConnectorAdmin(cluster).connectorDefinitions()).nonEmpty
        } catch {
          case e: Throwable =>
            log.info(s"[WORKER] worker cluster:${cluster.name} is starting ... retry", e)
            false
        }
    )

  private[this] def testJmx(cluster: ClusterInfo): Unit =
    cluster.nodeNames.foreach(
      node =>
        await(
          () =>
            try BeanChannel.builder().hostname(node).port(cluster.jmxPort).build().nonEmpty()
            catch {
              // the jmx service may be not ready.
              case _: Throwable =>
                false
            }
        )
    )

  private[this] def testAddNodeToRunningWorkerCluster(previousCluster: WorkerClusterInfo): WorkerClusterInfo = {
    await(() => result(wk_exist(previousCluster.key)))
    an[IllegalArgumentException] should be thrownBy result(
      wk_removeNode(previousCluster.key, previousCluster.nodeNames.head)
    )
    val freeNodes = nodes.filterNot(node => previousCluster.nodeNames.contains(node.name))
    if (freeNodes.nonEmpty) {
      val newNode = freeNodes.head.name
      // it is ok to add duplicate nodes
      result(wk_addNode(previousCluster.key, previousCluster.nodeNames.head))
      // we can't add a nonexistent node
      // we always get IllegalArgumentException if we sent request by restful api
      // However, if we use collie impl, an NoSuchElementException will be thrown...
      an[Throwable] should be thrownBy result(wk_addNode(previousCluster.key, CommonUtils.randomString()))
      log.info(s"[WORKER] start to add node:$newNode to a running worker cluster")
      val newCluster = result(wk_addNode(previousCluster.key, newNode))
      newCluster.key shouldBe previousCluster.key
      newCluster.imageName shouldBe previousCluster.imageName
      newCluster.configTopicName shouldBe previousCluster.configTopicName
      newCluster.statusTopicName shouldBe previousCluster.statusTopicName
      newCluster.offsetTopicName shouldBe previousCluster.offsetTopicName
      newCluster.groupId shouldBe previousCluster.groupId
      newCluster.brokerClusterKey shouldBe previousCluster.brokerClusterKey
      newCluster.clientPort shouldBe previousCluster.clientPort
      newCluster.nodeNames.size - previousCluster.nodeNames.size shouldBe 1
      await(() => result(wk_cluster(newCluster.key)).nodeNames.contains(newNode))
      newCluster
    } else previousCluster
  }

  private[this] def testRemoveNodeToRunningWorkerCluster(
    previousCluster: WorkerClusterInfo,
    excludedNode: String
  ): WorkerClusterInfo = {
    await(() => result(wk_exist(previousCluster.key)))
    if (previousCluster.nodeNames.size > 1) {
      val beRemovedNode = previousCluster.nodeNames.filter(_ != excludedNode).head
      await(() => {
        log.info(s"[WORKER] start to remove node:$beRemovedNode from ${previousCluster.key}")
        result(wk_removeNode(previousCluster.key, beRemovedNode))
        log.info(s"[WORKER] start to remove node:$beRemovedNode from ${previousCluster.key} ... done")
        val newCluster = result(wk_cluster(previousCluster.key))
        newCluster.key == previousCluster.key &&
        newCluster.imageName == previousCluster.imageName &&
        newCluster.configTopicName == previousCluster.configTopicName &&
        newCluster.statusTopicName == previousCluster.statusTopicName &&
        newCluster.offsetTopicName == previousCluster.offsetTopicName &&
        newCluster.groupId == previousCluster.groupId &&
        newCluster.brokerClusterKey == previousCluster.brokerClusterKey &&
        newCluster.clientPort == previousCluster.clientPort &&
        previousCluster.nodeNames.size - newCluster.nodeNames.size == 1 &&
        !result(wk_cluster(newCluster.key)).nodeNames.contains(beRemovedNode)
      })
      result(wk_cluster(previousCluster.key))
    } else previousCluster
  }

  @Test
  def testMultiZkClustersOnSameNodes(): Unit = {
    if (nodes.size < 2) skipTest("the size of nodes must be bigger than 1")
    val keys = (0 until numberOfClusters).map(_ => serviceNameHolder.generateClusterKey())
    val zkClusters = keys.map { key =>
      result(
        zk_create(
          clusterKey = key,
          jmxPort = CommonUtils.availablePort(),
          clientPort = CommonUtils.availablePort(),
          electionPort = CommonUtils.availablePort(),
          peerPort = CommonUtils.availablePort(),
          nodeNames = nodes.map(_.name).toSet
        ).flatMap(info => zk_start(info.key).flatMap(_ => zk_cluster(info.key)))
      )
    }
    // add a bit wait to make sure the cluster is up
    TimeUnit.SECONDS.sleep(5)
    zkClusters.foreach(
      zkCluster => assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    )
    val clusters2 = result(zk_clusters())
    zkClusters.foreach { c =>
      val another = clusters2.find(_.key == c.key).get
      another.key shouldBe c.key
      another.peerPort shouldBe c.peerPort
      another.clientPort shouldBe c.clientPort
      another.imageName shouldBe c.imageName
      another.electionPort shouldBe c.electionPort
      another.nodeNames shouldBe c.nodeNames
      result(zk_logs(c.key)).foreach { log =>
        // If we start a single-node zk cluster, zk print a "error" warning to us to say that you are using a single-node,
        // and we won't see the connection exception since there is only a node.
        if (nodes.size == 1) withClue(log)(log.toLowerCase.contains("exception") shouldBe false)
        // By contrast, if we start a true zk cluster, the exception ensues since the connections between nodes fail in beginning.
        else withClue(log)(log.toLowerCase.contains("- ERROR") shouldBe false)
        log.isEmpty shouldBe false
      }
    }
  }

  @Test
  def testMultiBkClustersOnSameNodes(): Unit = {
    if (nodes.size < 2) skipTest("the size of nodes must be bigger than 1")
    val zkKeys = (0 until numberOfClusters).map(_ => serviceNameHolder.generateClusterKey())
    val bkKeys = (0 until numberOfClusters).map(_ => serviceNameHolder.generateClusterKey())
    // NOTED: It is illegal to run multi bk clusters on same zk cluster so we have got to instantiate multi zk clusters first.
    val zkClusters = zkKeys.map { key =>
      result(
        zk_create(
          clusterKey = key,
          jmxPort = CommonUtils.availablePort(),
          clientPort = CommonUtils.availablePort(),
          electionPort = CommonUtils.availablePort(),
          peerPort = CommonUtils.availablePort(),
          nodeNames = Set(nodes.head.name)
        ).flatMap(info => zk_start(info.key).flatMap(_ => zk_cluster(info.key)))
      )
    }
    zkClusters.foreach(
      zkCluster => assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    )
    zkClusters.zipWithIndex.foreach {
      case (zk, index) =>
        val bkCluster = result(
          bk_create(
            clusterKey = bkKeys(index),
            clientPort = CommonUtils.availablePort(),
            jmxPort = CommonUtils.availablePort(),
            zookeeperClusterKey = zk.key,
            nodeNames = Set(nodes.head.name)
          )
        )
        result(bk_start(bkCluster.key))
        testTopic(bkCluster)
    }
  }

  @Test
  def testMultiWkClustersOnSameNodes(): Unit = {
    if (nodes.size < 2) skipTest("the size of nodes must be bigger than 1")
    val zkKey            = serviceNameHolder.generateClusterKey()
    val bkKey            = serviceNameHolder.generateClusterKey()
    val wkKeys           = (0 until numberOfClusters).map(_ => serviceNameHolder.generateClusterKey())
    val groupIds         = (0 until numberOfClusters).map(_ => CommonUtils.randomString(10))
    val configTopicNames = (0 until numberOfClusters).map(_ => CommonUtils.randomString(10))
    val offsetTopicNames = (0 until numberOfClusters).map(_ => CommonUtils.randomString(10))
    val statusTopicNames = (0 until numberOfClusters).map(_ => CommonUtils.randomString(10))
    log.info(s"start to run zk cluster:$zkKey")
    val zkCluster = result(
      zk_create(
        clusterKey = zkKey,
        jmxPort = CommonUtils.availablePort(),
        clientPort = CommonUtils.availablePort(),
        electionPort = CommonUtils.availablePort(),
        peerPort = CommonUtils.availablePort(),
        nodeNames = Set(nodes.head.name)
      )
    )
    result(zk_start(zkCluster.key))
    assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    log.info(s"start to run bk cluster:$bkKey")
    val bkCluster = result(
      bk_create(
        clusterKey = bkKey,
        clientPort = CommonUtils.availablePort(),
        jmxPort = CommonUtils.availablePort(),
        zookeeperClusterKey = zkCluster.key,
        nodeNames = Set(nodes.head.name)
      )
    )
    result(bk_start(bkCluster.key))
    assertCluster(() => result(bk_clusters()), () => result(bk_containers(bkCluster.key)), bkCluster.key)
    log.info(s"start to run multi wk clusters:$wkKeys")
    val wkClusters = wkKeys.zipWithIndex.map {
      case (wkKey, index) =>
        result(
          wk_create(
            clusterKey = wkKey,
            clientPort = CommonUtils.availablePort(),
            jmxPort = CommonUtils.availablePort(),
            brokerClusterKey = bkCluster.key,
            groupId = groupIds(index),
            configTopicName = configTopicNames(index),
            offsetTopicName = offsetTopicNames(index),
            statusTopicName = statusTopicNames(index),
            nodeNames = nodes.map(_.hostname).toSet
          )
        )
    }
    wkClusters.foreach(wk => result(wk_start(wk.key)))
    log.info(s"check multi wk clusters:$wkKeys")
    // add a bit wait to make sure the cluster is up
    TimeUnit.SECONDS.sleep(10)
    wkClusters.foreach(
      wkCluster => assertCluster(() => result(wk_clusters()), () => result(wk_containers(wkCluster.key)), wkCluster.key)
    )
    wkKeys.zipWithIndex.map {
      case (wkKey, index) =>
        wkClusters.find(_.key == wkKey).get.groupId shouldBe groupIds(index)
        wkClusters.find(_.key == wkKey).get.configTopicName shouldBe configTopicNames(index)
        wkClusters.find(_.key == wkKey).get.offsetTopicName shouldBe offsetTopicNames(index)
        wkClusters.find(_.key == wkKey).get.statusTopicName shouldBe statusTopicNames(index)
        wkClusters.find(_.key == wkKey).get.brokerClusterKey shouldBe bkCluster.key
    }

    log.info(s"check multi wk clusters:$wkKeys by list")
    result(wk_clusters()).foreach { cluster =>
      testConnectors(cluster)
      testJmx(cluster)
    }
  }
}
