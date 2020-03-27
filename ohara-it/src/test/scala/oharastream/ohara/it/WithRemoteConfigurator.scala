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

import com.typesafe.scalalogging.Logger
import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.client.configurator.v0.NodeApi
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{After, AssumptionViolatedException, Before}
import org.scalatest.Matchers._

import collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * a basic setup offering a configurator running on remote node.
  * this stuff is also in charge of releasing the configurator after testing.
  */
@RunWith(value = classOf[Parameterized])
abstract class WithRemoteConfigurator(paltform: PaltformModeInfo) extends IntegrationTest {
  private[this] val log: Logger = Logger(classOf[WithRemoteConfigurator])
  log.info(s"Running the ${paltform.modeName} mode")
  private[this] val nodes: Seq[Node] = paltform.nodes
  private[this] val containerClient  = paltform.containerClient
  private[this] val k8sURL: Option[String] =
    sys.env.get(EnvTestingUtils.K8S_MASTER_KEY).map(url => s"--k8s ${url}").orElse(Option.empty)

  protected val nodeNames: Seq[String]              = nodes.map(_.hostname)
  protected val serviceNameHolder: ServiceKeyHolder = ServiceKeyHolder(containerClient, false)
  private[this] val configuratorContainerKey        = serviceNameHolder.generateClusterKey()

  private[this] val configuratorNodeInfo: String = sys.env.getOrElse(
    EnvTestingUtils.CONFIURATOR_NODENAME_KEY,
    throw new AssumptionViolatedException(s"${EnvTestingUtils.CONFIURATOR_NODENAME_KEY} does not exists!!!")
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

  protected val configuratorHostname: String = configuratorNode.hostname
  protected val configuratorPort: Int        = CommonUtils.availablePort()

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
        .command(
          s"--hostname $configuratorHostname --port $configuratorPort ${k8sURL.getOrElse("")}"
        )
        // add the routes manually since not all envs have deployed the DNS.
        .routes(nodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap)
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
    Releasable.close(serviceNameHolder)
    // the client is used by name holder so we have to close it later
    Releasable.close(containerClient)
  }
}

object WithRemoteConfigurator {
  @Parameters
  def parameters: java.util.Collection[PaltformModeInfo] = {
    Seq(
      {
        val k8sNode: Seq[Node]         = EnvTestingUtils.k8sNodes()
        val k8sClient: ContainerClient = EnvTestingUtils.k8sClient()
        PaltformModeInfo("K8S", k8sNode, k8sClient)
      }, {
        val dockerNode: Seq[Node]         = EnvTestingUtils.dockerNodes()
        val dockerClient: ContainerClient = DockerClient(DataCollie(dockerNode))
        PaltformModeInfo("DOCKER", dockerNode, dockerClient)
      }
    ).asJava
  }
}

case class PaltformModeInfo(modeName: String, nodes: Seq[Node], containerClient: ContainerClient)
