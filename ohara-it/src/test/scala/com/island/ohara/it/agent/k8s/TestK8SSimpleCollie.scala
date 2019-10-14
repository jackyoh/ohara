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

package com.island.ohara.it.agent.k8s

import com.island.ohara.agent._
import com.island.ohara.agent.docker.ContainerState
import com.island.ohara.agent.k8s.{K8SClient, K8sContainerState}
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterStatus
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterStatus
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterStatus
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.it.agent.ClusterNameHolder
import com.island.ohara.it.{EnvTestingUtils, IntegrationTest}
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try
//Test jenkins environment
class TestK8SSimpleCollie extends IntegrationTest with Matchers {
  private[this] val nodes: Seq[Node] = EnvTestingUtils.k8sNodes()
  private[this] val nodeNames: Set[String] = nodes.map(_.hostname).toSet
  private[this] val nodeCollie: NodeCollie = NodeCollie(nodes)
  private[this] val nameHolder: ClusterNameHolder = ClusterNameHolder(nodes, EnvTestingUtils.k8sClient())
  private[this] val serviceCollie: ServiceCollie =
    ServiceCollie.builderOfK8s().nodeCollie(nodeCollie).k8sClient(EnvTestingUtils.k8sClient()).build()
  private[this] val TIMEOUT: FiniteDuration = 30 seconds

  @Before
  def before(): Unit = if (nodes.size < 2) skipTest("TestK8SSimpleCollie requires two nodes at least")

  private[this] def waitZookeeperCluster(clusterKey: ObjectKey): ZookeeperClusterStatus = {
    await(() => result(serviceCollie.zookeeperCollie.clusters()).exists(_._1.key == clusterKey))
    // since we only get "active" containers, all containers belong to the cluster should be running.
    // Currently, both k8s and pure docker have the same context of "RUNNING".
    // It is ok to filter container via RUNNING state.
    await(() => {
      val containers = result(serviceCollie.zookeeperCollie.containers(clusterKey))
      containers.nonEmpty && containers.map(_.state).forall(_.equals(ContainerState.RUNNING.name))
    })
    result(serviceCollie.zookeeperCollie.cluster(clusterKey))._1
  }

  private[this] def waitBrokerCluster(clusterKey: ObjectKey): BrokerClusterStatus = {
    await(() => result(serviceCollie.brokerCollie.clusters()).exists(_._1.key == clusterKey))
    // since we only get "active" containers, all containers belong to the cluster should be running.
    // Currently, both k8s and pure docker have the same context of "RUNNING".
    // It is ok to filter container via RUNNING state.
    await(() => {
      val containers = result(serviceCollie.brokerCollie.containers(clusterKey))
      containers.nonEmpty && containers.map(_.state).forall(_.equals(ContainerState.RUNNING.name))
    })
    result(serviceCollie.brokerCollie.cluster(clusterKey))._1
  }

  private[this] def waitWorkerCluster(clusterKey: ObjectKey): WorkerClusterStatus = {
    await(() => result(serviceCollie.workerCollie.clusters()).exists(_._1.key == clusterKey))
    // since we only get "active" containers, all containers belong to the cluster should be running.
    // Currently, both k8s and pure docker have the same context of "RUNNING".
    // It is ok to filter container via RUNNING state.
    await(() => {
      val containers = result(serviceCollie.workerCollie.containers(clusterKey))
      containers.nonEmpty && containers.map(_.state).forall(_.equals(ContainerState.RUNNING.name))
    })
    result(serviceCollie.workerCollie.cluster(clusterKey))._1
  }

  private[this] def node(hostname: String): Node = Node(
    hostname = hostname,
    port = Some(22),
    user = Some("fake"),
    password = Some("fake"),
    services = Seq.empty,
    lastModified = CommonUtils.current(),
    validationReport = None,
    tags = Map.empty
  )

  @Test
  def testZookeeperCollie(): Unit = {
    val firstNode: String = nodeNames.head
    val secondNode: String = nodeNames.last
    firstNode should not be secondNode

    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie

    val clientPort: Int = CommonUtils.availablePort()
    val peerPort: Int = CommonUtils.availablePort()
    val electionPort: Int = CommonUtils.availablePort()

    val zookeeperClusterStatus =
      createZookeeperCollie(zookeeperCollie,
                            nameHolder.generateClusterKey(),
                            firstNode,
                            clientPort,
                            peerPort,
                            electionPort)

    zookeeperClusterStatus.aliveNodes shouldBe Set(firstNode)
  }

  @Test
  def testBrokerCollie(): Unit = {
    val firstNode: String = nodeNames.head

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()
    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)

    //Create broker cluster service
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()
    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie
    val brokerClusterStatus = createBrokerCollie(brokerCollie,
                                                 nameHolder.generateClusterKey(),
                                                 firstNode,
                                                 brokerClientPort,
                                                 brokerExporterPort,
                                                 zk.key)

    //Check broker info
    brokerClusterStatus.aliveNodes shouldBe Set(firstNode)
  }

  @Test
  def testWorkerCollie(): Unit = {
    val firstNode: String = nodeNames.head

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()

    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)

    //Create broker cluster service
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()
    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie
    val brokerClusterInfo = createBrokerCollie(brokerCollie,
                                               nameHolder.generateClusterKey(),
                                               firstNode,
                                               brokerClientPort,
                                               brokerExporterPort,
                                               zk.key)

    //Create worker cluster service
    val workerCollie: WorkerCollie = serviceCollie.workerCollie
    val workerClientPort: Int = CommonUtils.availablePort()
    val workerClusterStatus =
      createWorkerCollie(workerCollie,
                         nameHolder.generateClusterKey(),
                         firstNode,
                         workerClientPort,
                         brokerClusterInfo.key)
    workerClusterStatus.aliveNodes shouldBe Set(firstNode)

    // Confirm pod name is a common format
    val wkPodName = Await.result(workerCollie.cluster(workerClusterStatus.key), TIMEOUT)._2.head.name
    val wkPodNameFieldSize = wkPodName.split("-").length
    val expectWKPodNameField =
      Collie.containerName("k8soccl", workerClusterStatus.group, workerClusterStatus.name, "wk").split("-")
    wkPodNameFieldSize shouldBe expectWKPodNameField.size

  }

  @Test
  def testAddBrokerNode(): Unit = {
    val firstNode: String = nodeNames.head
    val secondNode: String = nodeNames.last

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()

    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)

    //Create broker cluster service
    val brokerClusterKey = nameHolder.generateClusterKey()
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()

    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie

    val brokerClusterStatus1 =
      createBrokerCollie(brokerCollie, brokerClusterKey, firstNode, brokerClientPort, brokerExporterPort, zk.key)

    //Test add broker node
    val brokerClusterStatus2 =
      createBrokerCollie(brokerCollie, brokerClusterKey, secondNode, brokerClientPort, brokerExporterPort, zk.key)

    brokerClusterStatus1.aliveNodes shouldBe Set(firstNode)
    brokerClusterStatus2.aliveNodes shouldBe Set(firstNode, secondNode)
  }

  @Test
  def testAddWorkerNode(): Unit = {
    val firstNode: String = nodeNames.head
    val secondNode: String = nodeNames.last

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()

    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)

    //Create broker cluster service
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()
    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie

    val brokerClusterStatus =
      createBrokerCollie(brokerCollie,
                         nameHolder.generateClusterKey(),
                         firstNode,
                         brokerClientPort,
                         brokerExporterPort,
                         zk.key)

    //Create worker cluster service
    val workerClusterKey = nameHolder.generateClusterKey()
    val workerCollie: WorkerCollie = serviceCollie.workerCollie
    val workerClientPort: Int = CommonUtils.availablePort()
    val workerClusterStatus1 =
      createWorkerCollie(workerCollie, workerClusterKey, firstNode, workerClientPort, brokerClusterStatus.key)
    //Test add worker node
    val workerClusterStatus2 =
      createWorkerCollie(workerCollie, workerClusterKey, secondNode, workerClientPort, brokerClusterStatus.key)
    workerClusterStatus1.aliveNodes shouldBe Set(firstNode)
    workerClusterStatus2.aliveNodes shouldBe Set(firstNode, secondNode)
  }

  @Test
  def testRemoveBrokerNode(): Unit = {
    val firstNode: String = nodeNames.head
    val secondNode: String = nodeNames.last

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()
    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)
    //Create broker cluster service
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()
    val brokerClusterInfo =
      createBrokerCollie(brokerCollie,
                         nameHolder.generateClusterKey(),
                         firstNode,
                         brokerClientPort,
                         brokerExporterPort,
                         zk.key)
    val firstContainerName: String = result(brokerCollie.cluster(brokerClusterInfo.key))._2.head.hostname

    createBrokerCollie(brokerCollie, brokerClusterInfo.key, secondNode, brokerClientPort, brokerExporterPort, zk.key)
    result(brokerCollie.cluster(brokerClusterInfo.key))._2.size shouldBe 2
    result(brokerCollie.removeNode(brokerClusterInfo.key, firstNode))

    val k8sClient: K8SClient = EnvTestingUtils.k8sClient()
    await(() => !Await.result(k8sClient.containers(), TIMEOUT).exists(c => c.hostname.contains(firstContainerName)))
    result(brokerCollie.cluster(brokerClusterInfo.key))._2.size shouldBe 1
  }
  @Test
  def testClusters(): Unit = {
    val firstNode: String = nodeNames.head

    //Create zookeeper cluster for start broker service
    val zkClusterKey1 = nameHolder.generateClusterKey()
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    // for zookeeper, we assign node name to "hostname" field
    result(zookeeperCollie.clusters())
      .flatMap(x => x._2)
      .count(
        x =>
          x.name.contains(zkClusterKey1.group())
            && x.name.contains(zkClusterKey1.name())
            && x.nodeName == firstNode) shouldBe 0

    val zkClientPort1: Int = CommonUtils.availablePort()
    val zkPeerPort1: Int = CommonUtils.availablePort()
    val zkElectionPort1: Int = CommonUtils.availablePort()

    createZookeeperCollie(zookeeperCollie, zkClusterKey1, firstNode, zkClientPort1, zkPeerPort1, zkElectionPort1)
    // for zookeeper, we assign node name to "hostname" field
    result(zookeeperCollie.clusters())
      .flatMap(x => x._2)
      .count(
        x =>
          x.name.contains(zkClusterKey1.group())
            && x.name.contains(zkClusterKey1.name())
            && x.hostname == firstNode) shouldBe 1

  }

  @Test
  def testLog(): Unit = {
    val firstNode: String = nodeNames.head

    //Create zookeeper cluster for start broker service
    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val zkClientPort: Int = CommonUtils.availablePort()
    val zkPeerPort: Int = CommonUtils.availablePort()
    val zkElectionPort: Int = CommonUtils.availablePort()

    val zk = createZookeeperCollie(zookeeperCollie,
                                   nameHolder.generateClusterKey(),
                                   firstNode,
                                   zkClientPort,
                                   zkPeerPort,
                                   zkElectionPort)

    //Create broker cluster service
    val brokerClientPort = CommonUtils.availablePort()
    val brokerExporterPort = CommonUtils.availablePort()
    val brokerCollie: BrokerCollie = serviceCollie.brokerCollie
    val brokerClusterInfo = createBrokerCollie(brokerCollie,
                                               nameHolder.generateClusterKey(),
                                               firstNode,
                                               brokerClientPort,
                                               brokerExporterPort,
                                               zk.key)
    //Create worker cluster service
    val workerCollie: WorkerCollie = serviceCollie.workerCollie
    val workerClientPort: Int = CommonUtils.availablePort()
    val workerClusterInfo =
      createWorkerCollie(workerCollie,
                         nameHolder.generateClusterKey(),
                         firstNode,
                         workerClientPort,
                         brokerClusterInfo.key)
    val workerContainerHostName: String = result(workerCollie.cluster(workerClusterInfo.key))._2.head.hostname

    val k8sClient: K8SClient = EnvTestingUtils.k8sClient()
    await(
      () =>
        Await
          .result(k8sClient.containers(), TIMEOUT)
          .count(c => c.hostname.contains(workerContainerHostName) && c.state == K8sContainerState.RUNNING.name) == 1
    )

    val logMessage: String = "Kafka Connect distributed worker initialization"
    await(() => result(workerCollie.logs(workerClusterInfo.key)).head._2.contains(logMessage))
    val workerLogs: Map[ContainerInfo, String] = result(workerCollie.logs(workerClusterInfo.key))
    workerLogs.size shouldBe 1

    workerLogs.head._2.contains(logMessage) shouldBe true
  }

  @Test
  def testVerifyNode(): Unit = {
    val firstNode: String = nodeNames.head
    val verifyNode: Try[String] =
      Await.result(
        serviceCollie.verifyNode(node(firstNode)),
        30 seconds
      )
    verifyNode.get.contains("node is running.") shouldBe true

    val unknownNode: Try[String] = Await.result(
      serviceCollie.verifyNode(node("unknow-node")),
      30 seconds
    )
    unknownNode.isFailure shouldBe true
  }

  @Test
  def testNameAndGroupLimitInZookeeper(): Unit = {
    // generate the max length name and group (the sum of length is equal to LIMIT_OF_KEY_LENGTH)
    val key = ObjectKey.of(
      CommonUtils.randomString(com.island.ohara.client.configurator.v0.LIMIT_OF_KEY_LENGTH / 2),
      CommonUtils.randomString(com.island.ohara.client.configurator.v0.LIMIT_OF_KEY_LENGTH / 2)
    )
    nameHolder.addClusterKey(key)

    val firstNode: String = nodeNames.head
    val secondNode: String = nodeNames.last
    firstNode should not be secondNode

    val zookeeperCollie: ZookeeperCollie = serviceCollie.zookeeperCollie
    val clientPort: Int = CommonUtils.availablePort()
    val peerPort: Int = CommonUtils.availablePort()
    val electionPort: Int = CommonUtils.availablePort()

    val zookeeperClusterStatus =
      createZookeeperCollie(zookeeperCollie, key, nodeNames.head, clientPort, peerPort, electionPort)

    // create zookeeper should be ok
    zookeeperClusterStatus.key shouldBe key
  }

  private[this] def createZookeeperCollie(zookeeperCollie: ZookeeperCollie,
                                          clusterKey: ObjectKey,
                                          nodeName: String,
                                          clientPort: Int,
                                          peerPort: Int,
                                          electionPort: Int): ZookeeperClusterStatus = {
    result(
      zookeeperCollie.creator
        .key(clusterKey)
        .clientPort(clientPort)
        .peerPort(peerPort)
        .electionPort(electionPort)
        .nodeName(nodeName)
        .create())
    waitZookeeperCluster(clusterKey)
  }

  private[this] def createBrokerCollie(brokerCollie: BrokerCollie,
                                       clusterKey: ObjectKey,
                                       nodeName: String,
                                       clientPort: Int,
                                       exporterPort: Int,
                                       zookeeperClusterKey: ObjectKey): BrokerClusterStatus = {
    result(
      brokerCollie.creator
        .key(clusterKey)
        .clientPort(clientPort)
        .exporterPort(exporterPort)
        .zookeeperClusterKey(zookeeperClusterKey)
        .nodeName(nodeName)
        .create())
    waitBrokerCluster(clusterKey)
  }

  private[this] def createWorkerCollie(workerCollie: WorkerCollie,
                                       clusterKey: ObjectKey,
                                       nodeName: String,
                                       clientPort: Int,
                                       brokerClusterKey: ObjectKey): WorkerClusterStatus = {
    result(
      workerCollie.creator
        .key(clusterKey)
        .clientPort(clientPort)
        .brokerClusterKey(brokerClusterKey)
        .nodeName(nodeName)
        .create())
    waitWorkerCluster(clusterKey)
  }

  @After
  final def tearDown(): Unit = {
    Releasable.close(serviceCollie)
    Releasable.close(nameHolder)
  }
}
