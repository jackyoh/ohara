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

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.client.configurator.v0.NodeApi.Node
import oharastream.ohara.it.EnvTestingUtils

class TestPerformance4JDBCSourceToHDFSSinkOnK8S extends TestPerformance4FtpSourceToHDFSSinkOnDocker {
  private[this] val k8sURL: Option[String] =
    sys.env.get(EnvTestingUtils.K8S_MASTER_KEY).map(url => s"--k8s ${url}").orElse(Option.empty)

  override protected val nodes: Seq[Node]                 = EnvTestingUtils.k8sNodes()
  override protected val containerClient: ContainerClient = EnvTestingUtils.k8sClient()

  override protected def otherRoutes(): Map[String, String] = EnvTestingUtils.k8sMasterRoute()

  override protected def otherContainerCommand(): String = k8sURL.getOrElse("")
}
