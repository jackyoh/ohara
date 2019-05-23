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

package com.island.ohara.agent
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.common.rule.SmallTest
import org.junit.Test
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class TestContainerCollie extends SmallTest with Matchers {
  private[this] val fakeClusterName: String = FakeContainerCollie.clusterName
  private[this] val TIMEOUT = 10 seconds

  @Test
  def testAddNodeNameUpperCase(): Unit = {
    validErrorNodeName("Node1", "Your node name can't uppercase")
  }

  @Test
  def testAddNodeNameEmpty(): Unit = {
    validErrorNodeName("", "cluster and node name can't empty")
  }

  @Test
  def testAddNodeNormal(): Unit = {
    val nodeName1 = "node1"
    val nodeName2 = "node2"

    val node1 = Node(nodeName1, 22, "user1", "123456")
    val node2 = Node(nodeName2, 22, "user1", "123456")

    val container1 =
      ContainerInfo(nodeName1, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx")

    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1, node2)), Seq(container1))
    val cluster = Await.result(fakeContainerCollie.cluster(FakeContainerCollie.clusterName), TIMEOUT)
    cluster._1.nodeNames.size shouldBe 1

    val result: FakeContainerCollieClusterInfo =
      Await.result(fakeContainerCollie.addNode(FakeContainerCollie.clusterName, nodeName2), TIMEOUT)
    result.nodeNames.size shouldBe 2
    result.nodeNames(0) shouldBe "node1"
    result.nodeNames(1) shouldBe "node2"
  }

  @Test
  def testRemoveEmptyNode(): Unit = {
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq.empty), Seq.empty)
    val removeNode: Future[Boolean] = fakeContainerCollie.removeNode(fakeClusterName, "node1")
    Await.result(removeNode, TIMEOUT) shouldBe false
  }

  @Test
  def testRemoveContainerNodeNameNotExists(): Unit = {
    val node1Name = "node1"
    val node1 = Node(node1Name, 22, "user1", "123456")
    val containerInfo =
      ContainerInfo("node2", "0", "fakeimage", "", "", "", "container1", "0", Seq.empty, Map.empty, s"xxx")
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1)), Seq(containerInfo))
    val removeNode: Future[Boolean] = fakeContainerCollie.removeNode(fakeClusterName, node1Name)
    Await.result(removeNode, TIMEOUT) shouldBe false
  }

  @Test
  def testRemoveSingleNode(): Unit = {
    val node1Name = "node1"
    val node1 = Node(node1Name, 22, "user1", "123456")
    val containerInfo =
      ContainerInfo(node1Name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1Name}")
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1)), Seq(containerInfo))
    val removeNode: Future[Boolean] = fakeContainerCollie.removeNode(fakeClusterName, node1Name)
    intercept[IllegalArgumentException] {
      Await.result(removeNode, TIMEOUT)
    }.getMessage shouldBe s"${FakeContainerCollie.clusterName} is a single-node cluster. You can't remove the last node by removeNode(). Please use remove(clusterName) instead"
  }

  @Test
  def testRemoveNotExistsNode(): Unit = {
    val node1Name = "node1"
    val node1 = Node(node1Name, 22, "user1", "123456")
    val containerInfo =
      ContainerInfo(node1Name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1Name}")
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1)), Seq(containerInfo))
    val removeNode: Future[Boolean] = fakeContainerCollie.removeNode(fakeClusterName, "node3")
    Await.result(removeNode, TIMEOUT) shouldBe false
  }

  @Test
  def testRemoveNodeNormal(): Unit = {
    val node1Name = "node1"
    val node2Name = "node2"

    val node1 = Node(node1Name, 22, "user1", "123456")
    val node2 = Node(node2Name, 22, "user1", "123456")
    val container1 =
      ContainerInfo(node1Name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1Name}")

    val container2 =
      ContainerInfo(node2Name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1Name}")

    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1, node2)), Seq(container1, container2))
    val removeNode: Future[Boolean] = fakeContainerCollie.removeNode(fakeClusterName, "node1")
    Await.result(removeNode, TIMEOUT) shouldBe true
  }

  @Test
  def testZKCreator(): Unit = {
    val node1Name = "node1"
    val node1 = Node(node1Name, 22, "user1", "123456")

    val node2Name = "node2"
    val node2 = Node(node2Name, 22, "user1", "123456")

    val containerInfo =
      ContainerInfo(node1.name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1.name}")
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1, node2)), Seq(containerInfo))
    val zkCreator: Future[ZookeeperClusterInfo] =
      fakeContainerCollie.zkCreator("fakeprefix", "cluster1", "image1", 2181, 2182, 2183, Seq(node1Name, node2Name))(
        ExecutionContext.Implicits.global)

    val zookeeperClusterInfo = Await.result(zkCreator, TIMEOUT)
    zookeeperClusterInfo.clientPort shouldBe 2181
    zookeeperClusterInfo.peerPort shouldBe 2182
    zookeeperClusterInfo.electionPort shouldBe 2183
    zookeeperClusterInfo.nodeNames.size shouldBe 2
    zookeeperClusterInfo.nodeNames(0) shouldBe node1Name
    zookeeperClusterInfo.nodeNames(1) shouldBe node2Name
    zookeeperClusterInfo.ports.size shouldBe 3
  }

  private[this] def validErrorNodeName(nodeName: String, expectErrMsg: String): Unit = {
    val node1 = Node(nodeName, 22, "user1", "123456")
    val containerInfo =
      ContainerInfo(node1.name, "0", "fakeimage", "", "", "", "container1", "0", Seq(), Map(), s"xxx-${node1.name}")
    val fakeContainerCollie = new FakeContainerCollie(NodeCollie(Seq(node1)), Seq(containerInfo))
    val addNode: Future[FakeContainerCollieClusterInfo] = fakeContainerCollie.addNode(fakeClusterName, nodeName)
    intercept[IllegalArgumentException] {
      Await.result(addNode, TIMEOUT)
    }.getMessage shouldBe expectErrMsg
  }
}
