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

import java.util.concurrent.TimeUnit

import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{After, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(value = classOf[Parameterized])
class TestContainerClient(platform: ContainerPlatform) extends IntegrationTest {
  private[this] val containerClient = platform.setupContainerClient()

  private[this] def createBusyBox(name: String, arguments: Seq[String]): Unit =
    result(
      containerClient.containerCreator
        .nodeName(platform.nodeNames.head)
        .name(name)
        .imageName("busybox")
        .arguments(arguments)
        .create()
    )

  private[this] def log(name: String, sinceSeconds: Option[Long]): String =
    result(containerClient.logs(name, sinceSeconds)).head._2

  @Test
  def testLog(): Unit = {
    val name = CommonUtils.randomString(10)
    createBusyBox(name, Seq("sh", "-c", "while true; do $(echo date); sleep 1; done"))
    try {
      // wait the container
      await(() => log(name, None).contains("UTC"))
      val lastLine = log(name, None).split("\n").last
      TimeUnit.SECONDS.sleep(3)
      log(name, Some(1)) should not include lastLine
      log(name, Some(10)) should include(lastLine)
    } finally Releasable.close(() => result(containerClient.forceRemove(name)))
  }

  @Test
  def testVolume(): Unit = {
    val names = Seq(CommonUtils.randomString(), CommonUtils.randomString())
    checkVolumeExists(names)
    try {
      names.foreach(
        name =>
          result(
            containerClient.volumeCreator
              .name(name)
              .nodeName(platform.nodeNames.head)
              .path("/tmp")
              .create()
          )
      )
      names.foreach { name =>
        result(containerClient.volumes()).size shouldBe names.size
        result(containerClient.volumes(name)).head.path shouldBe "/tmp"
        result(containerClient.volumes(name)).head.name shouldBe name
        result(containerClient.volumes(name)).head.nodeName shouldBe platform.nodeNames.head
      }
    } finally {
      names.foreach(name => Releasable.close(() => result(containerClient.removeVolumes(name))))
      checkVolumeExists(names)
    }
  }

  private[this] def checkVolumeExists(names: Seq[String]): Unit = {
    names.foreach { volumeName =>
      await(() => !result(containerClient.volumes()).exists(_.name == volumeName))
    }
  }

  @After
  def tearDown(): Unit = Releasable.close(containerClient)
}

object TestContainerClient {
  @Parameters(name = "{index} mode = {0}")
  def parameters: java.util.Collection[ContainerPlatform] = {
    val modes = ContainerPlatform.all
    if (modes.isEmpty) java.util.Collections.singletonList(ContainerPlatform.empty)
    else modes.asJava
  }
}
