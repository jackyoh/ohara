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

import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.docker.{ContainerState, DockerClient}
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import org.junit.{After, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * all test cases here are executed on remote node. If no remote node is defined, all tests are skipped.
  * You can run following command to pass the information of remote node.
  * $ ./gradlew clean ohara-it:test --tests *TestDockerClient -PskipManager -Pohara.it.docker=$user:$password@$hostname:$port
  */
class TestDockerClient extends IntegrationTest {
  private[this] val platform = ContainerPlatform.dockerMode
  private[this] val webHost  = "www.google.com.tw"

  private[this] val client: DockerClient = platform.setupContainerClient().asInstanceOf[DockerClient]

  private[this] val remoteHostname: String = platform.nodeNames.head

  private[this] val imageName = "centos:7"

  private[this] val name = CommonUtils.randomString(10)

  @Test
  def testLog(): Unit = {
    // For QA test
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    result(client.log(name, None)).head._2 should include(webHost)
  }

  @Test
  def testList(): Unit = {
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    result(client.containerNames()).map(_.name) should contain(name)
  }

  @Test
  def testNonCleanup(): Unit = {
    // ping google 3 times
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost -c 3\"""")
        .create()
    )
    result(client.containerNames()).map(_.name) should contain(name)
    TimeUnit.SECONDS.sleep(3)
    result(client.containers()).find(_.name == name).get.state.toUpperCase shouldBe ContainerState.EXITED.name
  }

  @Test
  def testRoute(): Unit = {
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .routes(Map("ABC" -> "192.168.123.123"))
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    val hostFile = result(client.containerInspector.name(name).cat("/etc/hosts")).head._2
    hostFile should include("192.168.123.123")
    hostFile should include("ABC")
  }

  @Test
  def testPortMapping(): Unit = {
    val availablePort = CommonUtils.availablePort()
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .portMappings(Map(availablePort -> availablePort))
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )

    val container = result(client.containers()).find(_.name == name).get
    container.portMappings.size shouldBe 1
    container.portMappings.size shouldBe 1
    container.portMappings.head.hostPort shouldBe availablePort
    container.portMappings.head.containerPort shouldBe availablePort
  }

  @Test
  def testSetEnv(): Unit = {
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .envs(Map("abc" -> "123", "ccc" -> "ttt"))
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    val container = result(client.containers()).find(_.name == name).get
    container.environments("abc") shouldBe "123"
    container.environments("ccc") shouldBe "ttt"
  }

  @Test
  def testHostname(): Unit = {
    val hostname = CommonUtils.randomString(5)
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .hostname(hostname)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    result(client.containers()).find(_.name == name).get.hostname shouldBe hostname
  }

  @Test
  def testNodeName(): Unit = {
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    result(client.containers()).find(_.name == name).get.nodeName shouldBe remoteHostname
  }

  @Test
  def testAppend(): Unit = {
    result(
      client.containerCreator
        .nodeName(remoteHostname)
        .name(name)
        .imageName(imageName)
        .command(s"""/bin/bash -c \"ping $webHost\"""")
        .create()
    )
    val container = result(client.containers()).find(_.name == name).get
    result(client.containerInspector.name(container.name).append("/tmp/ttt", "abc")).head._2 shouldBe "abc\n"
    result(client.containerInspector.name(container.name).append("/tmp/ttt", "abc")).head._2 shouldBe "abc\nabc\n"
    result(client.containerInspector.name(container.name).append("/tmp/ttt", Seq("t", "z"))).head._2 shouldBe "abc\nabc\nt\nz\n"
  }

  @Test
  def testResources(): Unit = result(client.resources()) should not be Map.empty

  @Test
  def testResourcesOfUnavailableNode(): Unit = {
    val c = DockerClient(
      DataCollie(
        Seq(
          Node(
            hostname = "abc",
            port = Some(22),
            user = Some("user"),
            password = Some("password"),
            services = Seq.empty,
            state = State.AVAILABLE,
            error = None,
            lastModified = CommonUtils.current(),
            resources = Seq.empty,
            tags = Map.empty
          )
        )
      )
    )
    try result(c.resources()) shouldBe Map.empty
    finally c.close()
  }

  @After
  def tearDown(): Unit = {
    Releasable.close(() => result(client.forceRemove(name)))
    Releasable.close(client)
  }
}
