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

package oharastream.ohara.it.client

import oharastream.ohara.client.configurator.v0.NodeApi
import oharastream.ohara.it.category.ClientGroup
import oharastream.ohara.it.{PaltformModeInfo, WithRemoteConfigurator}
import org.junit.Test
import org.junit.experimental.categories.Category
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[ClientGroup]))
class TestNodeResource(paltform: PaltformModeInfo) extends WithRemoteConfigurator(paltform: PaltformModeInfo) {
  private[this] def nodeApi: NodeApi.Access = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)

  @Test
  def testResources(): Unit = {
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

  @Test
  def testStatus(): Unit = {
    val nodes = result(nodeApi.list())
    nodes should not be Seq.empty
    nodes.foreach { node =>
      nodes.exists(_.hostname == node.hostname) shouldBe true
      node.state shouldBe NodeApi.State.AVAILABLE
      node.error shouldBe None
    }
  }
}
