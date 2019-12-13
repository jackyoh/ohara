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

package com.island.ohara.it

import java.util.concurrent.TimeUnit

import com.island.ohara.agent.DataCollie
import com.island.ohara.agent.docker.DockerClient
import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import org.junit.{After, Before}
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * a basic setup offering a configurator running on remote node.
  * this stuff is also in charge of releasing the configurator after testing.
  */
abstract class WithRemoteConfigurator extends IntegrationTest {
  private[this] val nodes: Seq[Node]                = EnvTestingUtils.dockerNodes()
  protected val nodeNames: Seq[String]              = nodes.map(_.hostname)
  private[this] val containerClient                 = DockerClient(DataCollie(nodes))
  protected val serviceNameHolder: ServiceKeyHolder = ServiceKeyHolder(containerClient, false)
  private[this] val configuratorContainerKey        = serviceNameHolder.generateClusterKey()
  protected val configuratorHostname: String        = nodes.head.hostname
  protected val configuratorPort: Int               = CommonUtils.availablePort()

  /**
    * we have to combine the group and name in order to make name holder to delete related container.
    */
  protected val configuratorContainerName: String =
    s"${configuratorContainerKey.group()}-${configuratorContainerKey.name()}"

  private[this] val imageName = s"oharastream/configurator:${VersionUtils.VERSION}"

  @Before
  def setupConfigurator(): Unit = {
    result(containerClient.imageNames(configuratorHostname)) should contain(imageName)
    result(
      containerClient.containerCreator
        .nodeName(configuratorHostname)
        .imageName(imageName)
        .portMappings(Map(configuratorPort -> configuratorPort))
        .command(s"--hostname $configuratorHostname --port $configuratorPort")
        // add the routes manually since not all envs have deployed the DNS.
        .routes(nodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap)
        .name(configuratorContainerName)
        .create()
    )
    // wait for configurator
    TimeUnit.SECONDS.sleep(10)
    nodes.foreach { node =>
      result(
        NodeApi.access
          .hostname(configuratorHostname)
          .port(configuratorPort)
          .request
          .hostname(node.hostname)
          .port(node.port.get)
          .user(node.user.get)
          .password(node.password.get)
          .create()
      )
    }
  }

  @After
  def releaseConfigurator(): Unit = {
    Releasable.close(serviceNameHolder)
    // the client is used by name holder so we have to close it later
    Releasable.close(containerClient)
  }
}
