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

import com.island.ohara.agent._
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerName
import com.island.ohara.client.configurator.v0.NodeApi.{Node, Resource}
import com.island.ohara.common.util.ReleaseOnce

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

// accessible to configurator
private[ohara] class K8SServiceCollieImpl(dataCollie: DataCollie, k8sClient: K8SClient)
    extends ReleaseOnce
    with ServiceCollie {

  override val zookeeperCollie: ZookeeperCollie = new K8SZookeeperCollieImpl(dataCollie, k8sClient)

  override val brokerCollie: BrokerCollie = new K8SBrokerCollieImpl(dataCollie, zookeeperCollie, k8sClient)

  override val workerCollie: WorkerCollie = new K8SWorkerCollieImpl(dataCollie, brokerCollie, k8sClient)

  override val streamCollie: StreamCollie = new K8SStreamCollieImpl(dataCollie, brokerCollie, k8sClient)

  override protected def doClose(): Unit = {
    //Nothing
  }

  override def images()(implicit executionContext: ExecutionContext): Future[Map[Node, Seq[String]]] =
    dataCollie.values[Node]().flatMap { nodes =>
      Future
        .traverse(nodes) { node =>
          k8sClient.images(node.name).map(images => node -> images)
        }
        .map(_.toMap)
    }

  override def verifyNode(node: Node)(implicit executionContext: ExecutionContext): Future[Try[String]] =
    k8sClient
      .checkNode(node.name)
      .map(report => {
        val statusInfo = report.statusInfo.getOrElse(K8SStatusInfo(false, s"${node.name} node doesn't exists."))
        if (statusInfo.isHealth)
          Try(s"${node.name} node is running.")
        else
          Failure(
            new IllegalStateException(s"${node.name} node doesn't running container. cause: ${statusInfo.message}"))
      })

  override def containerNames()(implicit executionContext: ExecutionContext): Future[Seq[ContainerName]] =
    k8sClient
      .containers()
      .map(
        _.map(
          container =>
            new ContainerName(
              id = container.id,
              name = container.name,
              imageName = container.imageName,
              nodeName = container.nodeName
          )))

  override def logs()(implicit executionContext: ExecutionContext): Future[Map[ContainerName, String]] =
    containerNames()
      .flatMap(containerNames =>
        Future.sequence(containerNames.map { containerName =>
          k8sClient.log(containerName.name).map(containerName -> _)
        }))
      .map(_.toMap)

  override def resources()(implicit executionContext: ExecutionContext): Future[Map[Node, Seq[Resource]]] = {
    k8sClient
      .resources()
      .flatMap(k8sNodeResource =>
        dataCollie.values[Node]().map {
          _.map(node =>
            if (k8sNodeResource.contains(node.hostname)) Seq(node -> k8sNodeResource(node.hostname))
            else Seq.empty).flatten.toMap
      })
  }
}
