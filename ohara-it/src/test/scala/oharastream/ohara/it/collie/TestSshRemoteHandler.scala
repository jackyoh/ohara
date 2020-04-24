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

import oharastream.ohara.agent.{DataCollie, SshRemoteNodeHandler}
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.ContainerPlatform
import org.junit.{AssumptionViolatedException, Test}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.scalatest.Matchers._

class TestSshRemoteHandler extends OharaTest {
  private[this] val nodeInfo: String = sys.env.getOrElse(
    ContainerPlatform.DOCKER_NODES_KEY,
    throw new AssumptionViolatedException(s"${ContainerPlatform.DOCKER_NODES_KEY} the key is not exists")
  )
  private[this] val node: Node = parserNode(nodeInfo)

  @Test
  def testValidateError(): Unit = {
    val dataCollie        = DataCollie(Seq(node))
    val nodeName          = node.hostname
    val remoteNodeHandler = new SshRemoteNodeHandler(dataCollie)
    val response          = result(remoteNodeHandler.validateFolder(nodeName, "/home/ohara100"))
    response.message shouldBe "Folder validate failed"
  }

  @Test
  def testValidateSuccess(): Unit = {
    val dataCollie        = DataCollie(Seq(node))
    val nodeName          = node.hostname
    val remoteNodeHandler = new SshRemoteNodeHandler(dataCollie)
    val fileName          = CommonUtils.randomString(5)
    val path              = s"/tmp/${fileName}"
    try {
      result(remoteNodeHandler.mkDir(nodeName, path))
      val response = result(remoteNodeHandler.validateFolder(nodeName, path))
      response.message shouldBe "Folder validate success"
    } finally {
      result(remoteNodeHandler.deleteDir(nodeName, path))
    }
  }

  @Test
  def testMkDirAndDelete(): Unit = {
    val dataCollie        = DataCollie(Seq(node))
    val nodeName          = node.hostname
    val fileName          = CommonUtils.randomString(5)
    val path              = s"/tmp/${fileName}"
    val remoteNodeHandler = new SshRemoteNodeHandler(dataCollie)
    try {
      val response = result(remoteNodeHandler.mkDir(nodeName, path))
      response.message shouldBe "create folder success"
      val listDir = result(remoteNodeHandler.listDir(nodeName, "/tmp"))
      listDir.message.contains(fileName) shouldBe true
    } finally {
      val response = result(remoteNodeHandler.deleteDir(nodeName, path))
      response.message shouldBe "delete folder success"
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
