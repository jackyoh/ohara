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

package com.island.ohara.it.agent

import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.util.Releasable
import com.island.ohara.configurator.Configurator
import com.island.ohara.it.IntegrationTest
import org.junit.{After, Test}
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

abstract class BasicTests4Resources extends IntegrationTest {
  protected def nodes: Seq[Node]
  protected def configurator: Configurator
  protected def nameHolder: ClusterNameHolder

  private[this] def nodeApi = NodeApi.access.hostname(configurator.hostname).port(configurator.port)

  @Test
  def test(): Unit = {
    val nodes = result(nodeApi.list())
    nodes should not be Seq.empty
    nodes.foreach { node =>
      nodes.exists(_.hostname == node.hostname) shouldBe true
      node.resources should not be Seq.empty
      node.resources.size should be >= 1
      node.resources.foreach { resource =>
        resource.value.toInt should be >= 1
        resource.name.isEmpty shouldBe false
        resource.unit.isEmpty shouldBe false
      }
    }
  }

  @After
  def cleanAllContainers(): Unit = {
    Releasable.close(configurator)
    Releasable.close(nameHolder)
  }
}
