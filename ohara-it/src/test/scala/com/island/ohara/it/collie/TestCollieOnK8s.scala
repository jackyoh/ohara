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

package com.island.ohara.it.collie

import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.configurator.Configurator
import com.island.ohara.it.{EnvTestingUtils, ServiceKeyHolder}
import com.island.ohara.it.category.CollieGroup
import org.junit.Before
import org.junit.experimental.categories.Category
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
@Category(Array(classOf[CollieGroup]))
class TestCollieOnK8s extends BasicTests4Collie {
  // Modify test code for jenkins test
  override protected val nodes: Seq[Node] = EnvTestingUtils.k8sNodes()
  override protected val configurator: Configurator =
    Configurator.builder.k8sClient(EnvTestingUtils.k8sClient()).build()
  override protected val nameHolder: ServiceKeyHolder = ServiceKeyHolder(EnvTestingUtils.k8sClient())

  @Before
  final def setup(): Unit =
    if (nodes.isEmpty) skipTest(s"You must assign nodes for collie tests")
    else {
      val nodeApi = NodeApi.access.hostname(configurator.hostname).port(configurator.port)
      nodes.foreach(node => result(nodeApi.request.node(node).create()))
      result(nodeApi.list()).size shouldBe nodes.size
      result(nodeApi.list()).foreach(node => nodes.exists(_.name == node.name) shouldBe true)
    }
}
