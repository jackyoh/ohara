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
  private[this] val K8S_NODES_KEY: String = "ohara.it.k8s.nodename"

  def configuratorHostName(): String = "10.100.0.138"

  def configuratorHostPort(): Int = 12345

  def k8sClient(): K8SClient = K8SClient("http://ohara-jenkins-it-00:8080/api/v1")

  def k8sNodes(): Seq[Node] = Option("ohara-jenkins-it-01,ohara-jenkins-it-02")
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
        tags = Map.empty
      )
    }.toSeq)
    .getOrElse(throw new AssumptionViolatedException(s"$SSH_NODES_KEY does not exists!!!"))
}
