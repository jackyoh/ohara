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

package oharastream.ohara.it.collie

import oharastream.ohara.client.configurator.v0.{ContainerApi, NodeApi, ZookeeperApi}
import oharastream.ohara.it.category.CollieGroup
import oharastream.ohara.it.{PaltformModeInfo, WithRemoteConfigurator}
import org.junit.experimental.categories.Category
import org.junit.{Before, Test}
import org.scalatest.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[CollieGroup]))
class TestGetNodeWithRunningCluster(paltform: PaltformModeInfo)
    extends WithRemoteConfigurator(paltform: PaltformModeInfo) {
  @Before
  def setup(): Unit = {
    val images = result(containerClient.imageNames())
    nodes.foreach { node =>
      withClue(s"failed to find ${ZookeeperApi.IMAGE_NAME_DEFAULT}")(
        images(node.hostname) should contain(ZookeeperApi.IMAGE_NAME_DEFAULT)
      )
    }
  }

  @Test
  def test(): Unit = {
    val cluster = result(
      ZookeeperApi.access
        .hostname(configuratorHostname)
        .port(configuratorPort)
        .request
        .key(serviceNameHolder.generateClusterKey())
        .nodeNames(nodes.map(_.name).toSet)
        .create()
        .flatMap(
          info =>
            ZookeeperApi.access
              .hostname(configuratorHostname)
              .port(configuratorPort)
              .start(info.key)
              .flatMap(_ => ZookeeperApi.access.hostname(configuratorHostname).port(configuratorPort).get(info.key))
        )
    )
    assertCluster(
      () => result(ZookeeperApi.access.hostname(configuratorHostname).port(configuratorPort).list()),
      () =>
        result(
          ContainerApi.access
            .hostname(configuratorHostname)
            .port(configuratorPort)
            .get(cluster.key)
            .map(_.flatMap(_.containers))
        ),
      cluster.key
    )
    result(NodeApi.access.hostname(configuratorHostname).port(configuratorPort).list()).isEmpty shouldBe false
    result(NodeApi.access.hostname(configuratorHostname).port(configuratorPort).list()).foreach { node =>
      node.services.isEmpty shouldBe false
      withClue(s"${node.services}")(node.services.map(_.clusterKeys.size).sum > 0 shouldBe true)
    }
  }
}
