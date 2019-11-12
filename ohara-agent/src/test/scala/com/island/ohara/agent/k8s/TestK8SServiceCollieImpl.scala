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

package com.island.ohara.agent.k8s

import com.island.ohara.agent.DataCollie
import com.island.ohara.agent.fake.FakeK8SClient
import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.client.configurator.v0.NodeApi.{Node, Resource}
import com.island.ohara.common.rule.OharaTest
import org.junit.Test

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.scalatest.Matchers._

class TestK8SServiceCollieImpl extends OharaTest {

  @Test
  def testResource(): Unit = {
    val nodeCache = (1 to 3).map(x => Node(s"node${x}", None, None, None, Seq.empty, 0L, None, Seq.empty, Map.empty))
    val dataCollie = DataCollie(nodeCache)

    val k8sClient = new FakeK8SClient(false, None, "container1") {
      override def resources()(
        implicit executionContext: ExecutionContext): Future[Map[String, Seq[NodeApi.Resource]]] =
        Future.successful {
          Map(
            "node1" -> Seq(Resource.cpu(8, Option(2.0)), Resource.memory(1024 * 1024 * 1024 * 100, Option(5.0))),
            "node2" -> Seq(Resource.cpu(8, Option(1.0)), Resource.memory(1024 * 1024 * 1024 * 100, Option(5.0))),
            "node3" -> Seq(Resource.cpu(8, Option(3.0)), Resource.memory(1024 * 1024 * 1024 * 100, Option(5.0)))
          )
        }
    }

    val k8sServiceCollieImpl = new K8SServiceCollieImpl(dataCollie, k8sClient)
    val resource: Map[Node, Seq[Resource]] = result(k8sServiceCollieImpl.resources())
    resource.size shouldBe 3
    val nodeNames = resource.map(_._1.hostname).toSeq
    nodeNames(0) shouldBe "node1"
    nodeNames(1) shouldBe "node2"
    nodeNames(2) shouldBe "node3"

    val node1Resource: Seq[Resource] =
      resource.map(x => (x._1.hostname, x._2)).filter(x => x._1 == "node1").flatMap(x => x._2).toSeq

    node1Resource(0).name shouldBe "CPU"
    node1Resource(0).unit shouldBe "cores"
    node1Resource(0).used.get shouldBe 2.0
    node1Resource(0).value shouldBe 8

    node1Resource(1).name shouldBe "Memory"
    node1Resource(1).unit shouldBe "bytes"
    node1Resource(1).used.get shouldBe 5.0
    node1Resource(1).value shouldBe 1024 * 1024 * 1024 * 100
  }

  @Test
  def testEmptyResource(): Unit = {
    val nodeCache = (1 to 3).map(x => Node(s"node${x}", None, None, None, Seq.empty, 0L, None, Seq.empty, Map.empty))
    val dataCollie = DataCollie(nodeCache)

    val k8sClient = new FakeK8SClient(false, None, "container1") {
      override def resources()(
        implicit executionContext: ExecutionContext): Future[Map[String, Seq[NodeApi.Resource]]] =
        Future.successful(Map.empty)
    }

    val k8sServiceCollieImpl = new K8SServiceCollieImpl(dataCollie, k8sClient)
    val resource: Map[Node, Seq[Resource]] = result(k8sServiceCollieImpl.resources())
    resource.size shouldBe 0
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)
}
