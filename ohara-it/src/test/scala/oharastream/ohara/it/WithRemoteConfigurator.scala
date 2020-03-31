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
import oharastream.ohara.client.configurator.v0.NodeApi.Node
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

  protected val containerClient = paltform.containerClient.getOrElse(
    throw new AssumptionViolatedException(s"Please setting the K8S or Docker config key for the integration test")
  )
  protected[this] val nodes: Seq[Node]              = paltform.nodes
  protected val nodeNames: Seq[String]              = nodes.map(_.hostname)
  protected val serviceNameHolder: ServiceKeyHolder = ServiceKeyHolder(containerClient, false)

  private[this] val configuratorNode            = EnvTestingUtils.configuratorNode()
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

  private[this] val imageName = s"oharastream/configurator:${VersionUtils.VERSION}"

  @Before
  def setupConfigurator(): Unit = {
    result(configuratorContainerClient.imageNames(configuratorHostname)) should contain(imageName)
    result(
      configuratorContainerClient.containerCreator
        .nodeName(configuratorHostname)
        .imageName(imageName)
        .portMappings(Map(configuratorPort -> configuratorPort))
        .command(
          s"--hostname $configuratorHostname --port $configuratorPort ${paltform.args}"
        )
        // add the routes manually since not all envs have deployed the DNS.
        .routes(EnvTestingUtils.routes(nodes))
        .name(configuratorContainerName)
        .create()
    )

    // wait for configurator
    TimeUnit.SECONDS.sleep(20)

    val nodeApi = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)
    (nodes ++ Seq(configuratorNode)).foreach { node =>
      val hostNameList = result(nodeApi.list()).map(_.hostname)
      if (!hostNameList.contains(node.hostname)) {
        result(
          nodeApi.request
            .hostname(node.hostname)
            .port(node.port.get)
            .user(node.user.get)
            .password(node.password.get)
            .create()
        )
      }
    }
  }

  @After
  def releaseConfigurator(): Unit = {
    Releasable.close(serviceNameHolder)
    // the client is used by name holder so we have to close it later
    Releasable.close(containerClient)

    Releasable.close(configuratorServiceKeyHolder)
    Releasable.close(configuratorContainerClient)
  }
}

object WithRemoteConfigurator {
  @Parameters(name = "{index} mode = {0}")
  def parameters: java.util.Collection[PaltformModeInfo] = {
    val k8s: Option[String]    = sys.env.get(EnvTestingUtils.K8S_MASTER_KEY)
    val docker: Option[String] = sys.env.get(EnvTestingUtils.DOCKER_NODES_KEY)
    if (k8s.isEmpty && docker.isEmpty)
      Seq(PaltformModeInfo("Empty", Seq.empty, Option.empty, "")).asJava
    else
      ((if (k8s.nonEmpty) {
          val k8sNode: Seq[Node]         = EnvTestingUtils.k8sNodes()
          val k8sClient: ContainerClient = EnvTestingUtils.k8sClientWithMetricsServer()
          val k8sURL: String = sys.env.getOrElse(
            EnvTestingUtils.K8S_MASTER_KEY,
            throw new AssumptionViolatedException(s"${EnvTestingUtils.K8S_MASTER_KEY} does not exists!!!")
          )
          val k8sMetricsURL: String = sys.env.getOrElse(
            EnvTestingUtils.K8S_METRICS_SERVER_URL,
            throw new AssumptionViolatedException(s"${EnvTestingUtils.K8S_METRICS_SERVER_URL} does not exists!!!")
          )

          Seq(
            PaltformModeInfo(
              "K8S",
              k8sNode,
              Option(k8sClient),
              s"--k8s ${k8sURL} --k8s-metrics-server ${k8sMetricsURL}"
            )
          )
        } else Seq.empty) ++

        (if (docker.nonEmpty) {
           val dockerNode: Seq[Node]         = EnvTestingUtils.dockerNodes()
           val dockerClient: ContainerClient = DockerClient(DataCollie(dockerNode))
           Seq(PaltformModeInfo("DOCKER", dockerNode, Option(dockerClient), ""))
         } else Seq.empty)).asJava
  }
}

case class PaltformModeInfo(
  modeName: String,
  nodes: Seq[Node],
  containerClient: Option[ContainerClient],
  args: String
) {
  override def toString(): String = {
    modeName
  }
}
