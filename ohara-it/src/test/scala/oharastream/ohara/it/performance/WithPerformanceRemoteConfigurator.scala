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

package oharastream.ohara.it.performance

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.it.{EnvTestingUtils, IntegrationTest, ServiceKeyHolder}
import org.junit.{After, AssumptionViolatedException, Before}
import org.scalatest.Matchers.contain
import org.scalatest.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

abstract class WithPerformanceRemoteConfigurator extends IntegrationTest {
  private[this] val log: Logger = Logger(classOf[WithPerformanceRemoteConfigurator])

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
  private[this] val configuratorContainerName: String =
    s"${configuratorContainerKey.group()}-${configuratorContainerKey.name()}"

  private[this] val k8sURL: Option[String] =
    sys.env.get("ohara.it.k8s").map(url => s"--k8s ${url}").orElse(Option.empty)

  private[this] val serviceInfo: (Seq[Node], ContainerClient) = k8sURL
    .map { argument =>
      log.info(s"Running the K8S mode, K8S API argument is ${argument}")
      val nodes: Seq[Node]                 = EnvTestingUtils.k8sNodes()
      val containerClient: ContainerClient = EnvTestingUtils.k8sClient()
      (nodes, containerClient)
    }
    .getOrElse {
      log.info(s"Running the Docker mode")
      val nodes: Seq[Node]                 = EnvTestingUtils.dockerNodes()
      val containerClient: ContainerClient = DockerClient(DataCollie(nodes))
      (nodes, containerClient)
    }

  private[this] val imageName = s"oharastream/configurator:${VersionUtils.VERSION}"

  protected val nodes: Seq[Node]                 = serviceInfo._1
  protected val containerClient: ContainerClient = serviceInfo._2

  @Before
  def setupConfigurator(): Unit = {
    result(configuratorContainerClient.imageNames(configuratorHostname)) should contain(imageName)

    val routes
      : Map[String, String] = Map(configuratorNode.hostname -> CommonUtils.address(configuratorNode.hostname)) ++
      nodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap ++
      k8sURL
        .map { url =>
          val k8sMasterNodeName = url.split("http://").last.split(":").head
          Map(k8sMasterNodeName -> CommonUtils.address(k8sMasterNodeName))
        }
        .getOrElse(Map.empty)

    result(
      configuratorContainerClient.containerCreator
        .nodeName(configuratorHostname)
        .imageName(imageName)
        .portMappings(Map(configuratorPort -> configuratorPort))
        .command(
          s"--hostname $configuratorHostname --port $configuratorPort ${k8sURL.getOrElse("")}"
        )
        // add the routes manually since not all envs have deployed the DNS.
        .routes(routes)
        .name(configuratorContainerName)
        .create()
    )

    // Wait configurator start completed
    TimeUnit.SECONDS.sleep(10)
  }

  @After
  def releaseConfigurator(): Unit = {
    Releasable.close(configuratorServiceKeyHolder)
    // the client is used by name holder so we have to close it later
    Releasable.close(configuratorContainerClient)
  }
}
