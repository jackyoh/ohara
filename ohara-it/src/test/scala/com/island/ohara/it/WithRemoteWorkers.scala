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

import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.configurator.v0.{BrokerApi, WorkerApi, ZookeeperApi}
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.CommonUtils
import org.junit.Before
import scala.concurrent.ExecutionContext.Implicits.global

abstract class WithRemoteWorkers extends WithRemoteConfigurator {
  private[this] val zkInitHeap       = sys.env.get("ohara.it.zk.xms").map(_.toInt).getOrElse(1024)
  private[this] val zkMaxHeap        = sys.env.get("ohara.it.zk.xmx").map(_.toInt).getOrElse(1024)
  private[this] val bkInitHeap       = sys.env.get("ohara.it.bk.xms").map(_.toInt).getOrElse(1024)
  private[this] val bkMaxHeap        = sys.env.get("ohara.it.bk.xmx").map(_.toInt).getOrElse(1024)
  private[this] val wkInitHeap       = sys.env.get("ohara.it.wk.xms").map(_.toInt).getOrElse(1024)
  private[this] val wkMaxHeap        = sys.env.get("ohara.it.wk.xmx").map(_.toInt).getOrElse(1024)
  private[this] val zkKey: ObjectKey = serviceNameHolder.generateClusterKey()
  private[this] def zkApi =
    ZookeeperApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)

  private[this] val bkKey: ObjectKey = serviceNameHolder.generateClusterKey()
  private[this] def bkApi =
    BrokerApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)
  protected def brokerClusterInfo: BrokerClusterInfo = result(bkApi.get(bkKey))

  private[this] val wkKey: ObjectKey = serviceNameHolder.generateClusterKey()
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
}
