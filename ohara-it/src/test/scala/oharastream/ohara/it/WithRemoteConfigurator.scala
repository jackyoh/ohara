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

package oharastream.ohara.it

import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.client.configurator.v0.NodeApi
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import org.junit.{After, AssumptionViolatedException, Before}
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * a basic setup offering a configurator running on remote node.
  * this stuff is also in charge of releasing the configurator after testing.
  */
abstract class WithRemoteConfigurator extends IntegrationTest {
  private[this] val CONFIURATOR_NODENAME_KEY = "ohara.it.performance.configurator.node"

  private[this] val configuratorNodeInfo: String = sys.env.getOrElse(
    CONFIURATOR_NODENAME_KEY,
    throw new AssumptionViolatedException(s"$CONFIURATOR_NODENAME_KEY does not exists!!!")
  )
  private[this] val configuratorNode = Node(
    hostname = configuratorNodeInfo.split("@").last.split(":").head,
    port = Some(configuratorNodeInfo.split("@").last.split(":").last.toInt),
    user = Some(configuratorNodeInfo.split(":").head),
    password = Some(configuratorNodeInfo.split("@").head.split(":").last),
    services = Seq.empty,
    state = State.AVAILABLE,
    error = None,
    lastModified = CommonUtils.current(),
    resources = Seq.empty,
    tags = Map.empty
  )

  private[this] val configuratorContainerClient = DockerClient(DataCollie(Seq(configuratorNode)))
  private[this] val configuratorServiceKeyHolder: ServiceKeyHolder =
    ServiceKeyHolder(configuratorContainerClient, false)
  private[this] val configuratorContainerKey = configuratorServiceKeyHolder.generateClusterKey()
  protected val configuratorHostname: String = configuratorNode.hostname
  protected val configuratorPort: Int        = CommonUtils.availablePort()

  /**
    * we have to combine the group and name in order to make name holder to delete related container.
    */
  protected val configuratorContainerName: String =
    s"${configuratorContainerKey.group()}-${configuratorContainerKey.name()}"

  protected[this] var nodes: Seq[Node]                   = _
  protected[this] var serviceKeyHolder: ServiceKeyHolder = _
  private var k8sArgument                                = ""

  private[this] val k8sApiURL: Option[String] = sys.env.get("ohara.it.k8s").orElse(Option.empty)

  if (k8sApiURL.nonEmpty) { // Run K8S mode for the performance test
    val containerClient = EnvTestingUtils.k8sClient()
    nodes = EnvTestingUtils.k8sNodes()
    serviceKeyHolder = ServiceKeyHolder(containerClient, false)
    k8sArgument = s"--k8s ${k8sApiURL.get}"
  } else { // Run docker mode for the performance test
    nodes = EnvTestingUtils.dockerNodes()
    val containerClient = DockerClient(DataCollie(nodes))
    serviceKeyHolder = ServiceKeyHolder(containerClient, false)
  }

  protected[this] var nodeNames: Seq[String] = nodes.map(_.hostname)
  private[this] val imageName                = s"oharastream/configurator:${VersionUtils.VERSION}"

  @Before
  def setupConfigurator(): Unit = {
    // Start configurator from docker client
    result(configuratorContainerClient.imageNames(configuratorHostname)) should contain(imageName)
    result(
      configuratorContainerClient.containerCreator
        .nodeName(configuratorHostname)
        .imageName(imageName)
        .portMappings(Map(configuratorPort -> configuratorPort))
        .command(
          s"--hostname $configuratorHostname --port $configuratorPort $k8sArgument"
        )
        // add the routes manually since not all envs have deployed the DNS.
        .routes(Map(configuratorNode.hostname -> CommonUtils.address(configuratorNode.hostname)))
        .name(configuratorContainerName)
        .create()
    )

    // wait for configurator
    TimeUnit.SECONDS.sleep(10)
    val nodeApi      = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)
    val hostNameList = result(nodeApi.list()).map(_.hostname)
    nodes.foreach { node =>
      if (!hostNameList.contains(node.hostname)) {
        nodeApi.request
          .hostname(node.hostname)
          .port(node.port.get)
          .user(node.user.get)
          .password(node.password.get)
          .create()
      }
    }
  }

  @After
  def releaseConfigurator(): Unit = {
    Releasable.close(serviceKeyHolder)
    Releasable.close(configuratorServiceKeyHolder)
    // the client is used by name holder so we have to close it later
    Releasable.close(configuratorContainerClient)
  }
}
