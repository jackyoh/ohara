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

import com.island.ohara.agent.k8s.K8SClient
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.util.CommonUtils
import org.junit.AssumptionViolatedException

/**
  * includes helper methods to fetch important information from env variables. This class reduce the duplicate codes
  * from each ITs.
  */
object EnvTestingUtils {

  val K8S_MASTER_KEY: String = "ohara.it.k8s"
  private[this] val K8S_METRICS_SERVER_URL = "ohara.it.k8s.metrics.server"
  private[this] val K8S_NODES_KEY: String = "ohara.it.k8s.nodename"
  private[this] val K8S_NAMESPACE_KEY: String = "ohara.it.k8s.namespace"
  private[this] val CONFIGURATOR_HOSTNAME_KEY: String = "ohara.it.hostname"
  private[this] val CONFIGURATOR_HOSTPORT_KEY: String = "ohara.it.port"

  def configuratorHostName(): String = sys.env.getOrElse(
    CONFIGURATOR_HOSTNAME_KEY,
    throw new AssumptionViolatedException(s"$CONFIGURATOR_HOSTNAME_KEY does not exists!!!"))

  def configuratorHostPort(): Int = sys.env
    .getOrElse(CONFIGURATOR_HOSTPORT_KEY,
               throw new AssumptionViolatedException(s"$CONFIGURATOR_HOSTPORT_KEY does not exists!!!"))
    .toInt

  def k8sClient(): K8SClient = K8SClient(
    sys.env.getOrElse(K8S_MASTER_KEY, throw new AssumptionViolatedException(s"$K8S_MASTER_KEY does not exists!!!")),
    sys.env.getOrElse(K8S_NAMESPACE_KEY, K8SClient.NAMESPACE_DEFAULT_VALUE)
  )

  def k8sClientWithMetricsServer(): K8SClient = {
    val client = k8sClient()
    client.k8sMetricsAPIServerURL(
      sys.env.getOrElse(K8S_METRICS_SERVER_URL,
                        throw new AssumptionViolatedException(s"$K8S_METRICS_SERVER_URL does not exists!!!")))
    client
  }

  def k8sNodes(): Seq[Node] = sys.env
    .get(K8S_NODES_KEY)
    .map(
      _.split(",")
        .map(node =>
          Node(
            hostname = node,
            port = Some(22),
            user = Some("fake"),
            password = Some("fake"),
            services = Seq.empty,
            lastModified = CommonUtils.current(),
            validationReport = None,
            resources = Seq.empty,
            tags = Map.empty
        ))
        .toSeq)
    .getOrElse(throw new AssumptionViolatedException(s"$K8S_NODES_KEY does not exists!!!"))

  /**
    * form: user:password@hostname:port.
    * NOTED: this key need to be matched with another key value in ohara-it/build.gradle
    */
  private[this] val SSH_NODES_KEY = "ohara.it.docker"

  def sshNodes(): Seq[Node] = sys.env
    .get(SSH_NODES_KEY)
    .map(_.split(",").map { nodeInfo =>
      val user = nodeInfo.split(":").head
      val password = nodeInfo.split("@").head.split(":").last
      val hostname = nodeInfo.split("@").last.split(":").head
      val port = nodeInfo.split("@").last.split(":").last.toInt
      Node(
        hostname = hostname,
        port = Some(port),
        user = Some(user),
        password = Some(password),
        services = Seq.empty,
        lastModified = CommonUtils.current(),
        validationReport = None,
        resources = Seq.empty,
        tags = Map.empty
      )
    }.toSeq)
    .getOrElse(throw new AssumptionViolatedException(s"$SSH_NODES_KEY does not exists!!!"))
}
