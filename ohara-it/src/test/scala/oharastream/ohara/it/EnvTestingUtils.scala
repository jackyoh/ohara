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

import oharastream.ohara.agent.k8s.K8SClient
import oharastream.ohara.client.configurator.v0.NodeApi.{Node, State}
import oharastream.ohara.common.util.CommonUtils
import org.junit.AssumptionViolatedException

/**
  * includes helper methods to fetch important information from env variables. This class reduce the duplicate codes
  * from each ITs.
  */
object EnvTestingUtils {
  val K8S_MASTER_KEY: String                  = "ohara.it.k8s"
  val K8S_METRICS_SERVER_URL                  = "ohara.it.k8s.metrics.server"
  private[this] val K8S_NODES_KEY: String     = "ohara.it.k8s.nodename"
  private[this] val K8S_NAMESPACE_KEY: String = "ohara.it.k8s.namespace"

  def k8sClient(): K8SClient = {
    val k8sApiServer =
      sys.env.getOrElse(K8S_MASTER_KEY, throw new AssumptionViolatedException(s"$K8S_MASTER_KEY does not exists!!!"))
    val namespace = sys.env.getOrElse(K8S_NAMESPACE_KEY, "default")
    K8SClient.builder.apiServerURL(k8sApiServer).namespace(namespace).build()
  }

  def k8sClientWithMetricsServer(): K8SClient = {
    val k8sApiServer =
      sys.env.getOrElse(K8S_MASTER_KEY, throw new AssumptionViolatedException(s"$K8S_MASTER_KEY does not exists!!!"))
    val namespace = sys.env.getOrElse(K8S_NAMESPACE_KEY, "default")
    val k8sMetricsServerURL = sys.env.getOrElse(
      K8S_METRICS_SERVER_URL,
      throw new AssumptionViolatedException(s"$K8S_METRICS_SERVER_URL does not exists!!!")
    )
    K8SClient.builder.apiServerURL(k8sApiServer).namespace(namespace).metricsApiServerURL(k8sMetricsServerURL).build()
  }

  def k8sNodes(): Seq[Node] =
    sys.env
      .get(K8S_NODES_KEY)
      .map(
        _.split(",")
          .map(Node.apply)
          .toSeq
      )
      .getOrElse(throw new AssumptionViolatedException(s"$K8S_NODES_KEY does not exists!!!"))

  /**
    * form: user:password@hostname:port.
    * NOTED: this key need to be matched with another key value in ohara-it/build.gradle
    */
  val DOCKER_NODES_KEY = "ohara.it.docker"

  def dockerNodes(): Seq[Node] =
    sys.env
      .get(DOCKER_NODES_KEY)
      .map(_.split(",").map(nodeInfo => parserNode(nodeInfo)).toSeq)
      .getOrElse(throw new AssumptionViolatedException(s"$DOCKER_NODES_KEY does not exists!!!"))

  val CONFIURATOR_NODENAME_KEY = "ohara.it.configurator.node"

  def configuratorNode(): Node =
    sys.env
      .get(CONFIURATOR_NODENAME_KEY)
      .map(parserNode(_))
      .getOrElse(throw new AssumptionViolatedException(s"$CONFIURATOR_NODENAME_KEY does not exists!!!"))

  def routes(nodes: Seq[Node]): Map[String, String] = {
    Map(configuratorNode.hostname -> CommonUtils.address(configuratorNode.hostname)) ++
      nodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap ++
      sys.env
        .get(K8S_MASTER_KEY)
        .map { url =>
          val k8sMasterNodeName = url.split("http://").last.split(":").head
          Map(k8sMasterNodeName -> CommonUtils.address(k8sMasterNodeName))
        }
        .getOrElse(Map.empty)
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
}
