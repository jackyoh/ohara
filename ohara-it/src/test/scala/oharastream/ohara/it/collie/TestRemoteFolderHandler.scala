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

import oharastream.ohara.agent.{
  DataCollie,
  FolderInfo,
  RemoteFolderCommandResult,
  RemoteFolderHandler,
  RemoteFolderState
}
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.ContainerPlatform
import org.junit.{AssumptionViolatedException, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.scalatest.matchers.should.Matchers._

class TestRemoteFolderHandler extends OharaTest {
  private[this] val nodeInfos: String = sys.env.getOrElse(
    ContainerPlatform.DOCKER_NODES_KEY,
    throw new AssumptionViolatedException(s"${ContainerPlatform.DOCKER_NODES_KEY} the key is not exists")
  )

  private[this] val nodes: Seq[Node] = nodeInfos.split(",").toSeq.map(nodeInfo => parserNode(nodeInfo))

  @Test
  def testValidateError(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).hostNames(hostnames).build()

    val commandResult: Map[String, RemoteFolderCommandResult] = result(
      remoteNodeHandler.validateFolder("/home/ohara100")
    )
    commandResult.foreach { node =>
      hostnames.exists(_ == node._1) shouldBe true
      node._2.message.contains("Folder validate failed") shouldBe true
      node._2.state shouldBe RemoteFolderState.FAILED
    }
  }

  @Test
  def testValidateSuccess(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).hostNames(hostnames).build()

    val fileName = CommonUtils.randomString(5)
    val path     = s"/tmp/${fileName}"
    try {
      result(remoteNodeHandler.mkDir(path))
      val commandResult = result(remoteNodeHandler.validateFolder(path))
      commandResult.foreach { node =>
        hostnames.exists(_ == node._1) shouldBe true
        node._2.message.contains("Folder validate success") shouldBe true
        node._2.state shouldBe RemoteFolderState.SUCCESS
      }
    } finally {
      result(remoteNodeHandler.deleteDir(path))
    }
  }

  @Test
  def testMkDirAndDelete(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val fileName          = CommonUtils.randomString(5)
    val path              = s"/tmp/${fileName}"
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).hostNames(hostnames).build()
    try {
      val commandResult = result(remoteNodeHandler.mkDir(path))
      commandResult.foreach { node =>
        hostnames.exists(_ == node._1) shouldBe true
        node._2.message.contains("create folder success") shouldBe true
        node._2.state shouldBe RemoteFolderState.SUCCESS
      }

      val nodeListDir = result(remoteNodeHandler.listDir("/tmp"))
      nodeListDir.foreach { result =>
        result._2.exists(_.fileName == fileName) shouldBe true
      }
    } finally {
      val commandResult = result(remoteNodeHandler.deleteDir(path))
      commandResult.foreach { node =>
        node._2.message shouldBe "Delete folder success"
        node._2.state shouldBe RemoteFolderState.SUCCESS
      }
    }
  }

  @Test
  def testRemoveFolderError(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).hostNames(hostnames).build()
    val commandResult: Map[String, RemoteFolderCommandResult] = result(
      remoteNodeHandler.deleteDir(s"/tmp/${CommonUtils.randomString(5)}")
    )
    commandResult.foreach { nodes =>
      hostnames.exists(_ == nodes._1) shouldBe true
      nodes._2.state shouldBe RemoteFolderState.FAILED
      nodes._2.message shouldBe "Folder is not exists"
    }
  }

  @Test
  def testListDir(): Unit = {
    val dataCollie                                  = DataCollie(nodes)
    val hostnames                                   = nodes.map(_.hostname)
    val remoteNodeHandler                           = RemoteFolderHandler.builder().dataCollie(dataCollie).hostNames(hostnames).build()
    val commandResult: Map[String, Seq[FolderInfo]] = result(remoteNodeHandler.listDir("/tmp"))
    commandResult.foreach { node =>
      node._2.size > 0 shouldBe true
    }
  }

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

  private[this] def result[T](f: Future[T]): T = Await.result(f, 2 minutes)
}
