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

package com.island.ohara.configurator

import com.island.ohara.agent.fake.FakeK8SClient
import com.island.ohara.agent.k8s.K8SNodeReport
import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.common.rule.OharaTest
import org.junit.Test
import org.scalatest.Matchers._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}

class TestConfigurator extends OharaTest {
  @Test
  def testK8SNodes(): Unit = {
    val k8sClient = new FakeK8SClient(true, None, "") {
      override def nodes()(implicit executionContext: ExecutionContext): Future[Seq[K8SNodeReport]] =
        Future.successful(Seq(K8SNodeReport("node1"), K8SNodeReport("node2"), K8SNodeReport("node3")))
    }
    val configurator = Configurator.builder.k8sClient(k8sClient).build()

    val nodeApi    = NodeApi.access.hostname(configurator.hostname).port(configurator.port)
    val createNode = Await.result(nodeApi.request.hostname("node1").create(), 15 seconds)
    createNode.hostname shouldBe "node1"

    val nodes = Await.result(configurator.addK8SNodes(), 15 seconds)
    nodes.size shouldBe 2
    nodes(0).hostname shouldBe "node2"
    nodes(1).hostname shouldBe "node3"
  }
}
