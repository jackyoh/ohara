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

import oharastream.ohara.agent.{DataCollie, FolderInfo, RemoteFolderHandler}
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
  def testFolderNotExists(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).build()
    hostnames.foreach { hostname =>
      result(remoteNodeHandler.exists(hostname, "/home/ohara100")) shouldBe false
    }
  }

  @Test
  def testFolderExists(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).build()

    val fileName = CommonUtils.randomString(5)
    val path     = s"/tmp/${fileName}"
    try {
      hostnames.foreach { hostname =>
        result(remoteNodeHandler.mkFolder(hostname, path))
        result(remoteNodeHandler.exists(hostname, path)) shouldBe true
      }
    } finally {
      hostnames.foreach { hostname =>
        result(remoteNodeHandler.deleteFolder(hostname, path))
      }
    }
  }

  @Test
  def testMkFolderAndDelete(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostname          = nodes.map(_.hostname).head
    val fileName          = CommonUtils.randomString(5)
    val path              = s"/tmp/${fileName}"
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).build()
    try {
      result(remoteNodeHandler.mkFolder(hostname, path))

      val listFolder = result(remoteNodeHandler.listFolder(hostname, "/tmp"))
      listFolder.exists(_.fileName == fileName) shouldBe true
    } finally {
      result(remoteNodeHandler.deleteFolder(hostname, path))
    }
  }

  @Test
  def testRemoveFolderError(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostname          = nodes.map(_.hostname).head
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).build()
    an[IllegalArgumentException] should be thrownBy {
      result(
        remoteNodeHandler.deleteFolder(hostname, s"/tmp/${CommonUtils.randomString(5)}")
      )
    }
  }

  @Test
  def testListFolder(): Unit = {
    val dataCollie        = DataCollie(nodes)
    val hostnames         = nodes.map(_.hostname)
    val remoteNodeHandler = RemoteFolderHandler.builder().dataCollie(dataCollie).build()
    hostnames.foreach { hostname =>
      val folders: Seq[FolderInfo] = result(remoteNodeHandler.listFolder(hostname, "/tmp"))
      folders.size > 0 shouldBe true
      folders.foreach { fileInfo =>
        fileInfo.uid >= 0 shouldBe true
      }
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
