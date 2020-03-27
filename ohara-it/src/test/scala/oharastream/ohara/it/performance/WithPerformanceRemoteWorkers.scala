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

import oharastream.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.v0.{BrokerApi, NodeApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.it.ServiceKeyHolder
import org.junit.{After, Before}

import scala.concurrent.ExecutionContext.Implicits.global

abstract class WithPerformanceRemoteWorkers extends WithPerformanceRemoteConfigurator {
  private[this] val zkInitHeap                         = sys.env.get("ohara.it.zk.xms").map(_.toInt).getOrElse(1024)
  private[this] val zkMaxHeap                          = sys.env.get("ohara.it.zk.xmx").map(_.toInt).getOrElse(1024)
  private[this] val bkInitHeap                         = sys.env.get("ohara.it.bk.xms").map(_.toInt).getOrElse(1024)
  private[this] val bkMaxHeap                          = sys.env.get("ohara.it.bk.xmx").map(_.toInt).getOrElse(1024)
  private[this] val wkInitHeap                         = sys.env.get("ohara.it.wk.xms").map(_.toInt).getOrElse(1024)
  private[this] val wkMaxHeap                          = sys.env.get("ohara.it.wk.xmx").map(_.toInt).getOrElse(1024)
  private[this] var serviceKeyHolder: ServiceKeyHolder = _

  private[this] var zkKey: ObjectKey = _
  private[this] def zkApi =
    ZookeeperApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)

  private[this] var bkKey: ObjectKey = _
  private[this] def bkApi =
    BrokerApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)
  protected def brokerClusterInfo: BrokerClusterInfo = result(bkApi.get(bkKey))

  private[this] var wkKey: ObjectKey = _
  private[this] def wkApi =
    WorkerApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)
  protected def workerClusterInfo: WorkerClusterInfo = result(wkApi.get(wkKey))

  /**
    * set the extra routes to all services
    * @return routes
    */
  protected def routes: Map[String, String] = Map.empty

  protected def sharedJars: Set[ObjectKey] = Set.empty

  @Before
  def setupWorkers(): Unit = {
    val nodeNames: Seq[String] = nodes.map(_.hostname)
    serviceKeyHolder = ServiceKeyHolder(containerClient, false)

    val nodeApi = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)

    nodes.foreach { node =>
      val hostNameList = result(nodeApi.list()).map(_.hostname)
      if (!hostNameList.contains(node.hostname)) {
        nodeApi.request
          .hostname(node.hostname)
          .port(node.port.get)
          .user(node.user.get)
          .password(node.password.get)
          .create()
      }
    }

    zkKey = serviceKeyHolder.generateClusterKey()
    bkKey = serviceKeyHolder.generateClusterKey()
    wkKey = serviceKeyHolder.generateClusterKey()

    // single zk
    result(
      zkApi.request
        .key(zkKey)
        .nodeName(nodeNames.head)
        .routes(routes)
        .initHeap(zkInitHeap)
        .maxHeap(zkMaxHeap)
        .create()
        .map(_.key)
        .flatMap(zkApi.start)
    )
    await(() => result(zkApi.get(zkKey)).state.isDefined)

    result(
      bkApi.request
        .key(bkKey)
        .zookeeperClusterKey(zkKey)
        .nodeNames(nodeNames.toSet)
        .routes(routes)
        .initHeap(bkInitHeap)
        .maxHeap(bkMaxHeap)
        .create()
        .map(_.key)
        .flatMap(bkApi.start)
    )
    await(() => result(bkApi.get(bkKey)).state.isDefined)

    result(
      wkApi.request
        .key(wkKey)
        .brokerClusterKey(bkKey)
        .nodeNames(nodeNames.toSet)
        .freePort(CommonUtils.availablePort())
        .routes(routes)
        .sharedJarKeys(sharedJars)
        .initHeap(wkInitHeap)
        .maxHeap(wkMaxHeap)
        .create()
        .map(_.key)
        .flatMap(wkApi.start)
    )
    await(() => result(wkApi.get(wkKey)).state.isDefined)
  }

  @After
  def releaseService(): Unit = {
    Releasable.close(serviceKeyHolder)
  }
}
