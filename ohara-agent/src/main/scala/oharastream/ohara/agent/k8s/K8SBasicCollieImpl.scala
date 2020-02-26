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

package oharastream.ohara.agent.k8s

import oharastream.ohara.agent.container.ContainerName
import oharastream.ohara.agent.{Collie, DataCollie, ServiceState}
import oharastream.ohara.client.configurator.v0.ClusterStatus
import oharastream.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import oharastream.ohara.client.configurator.v0.NodeApi.Node
import oharastream.ohara.common.setting.ObjectKey

import scala.concurrent.{ExecutionContext, Future}

private[this] abstract class K8SBasicCollieImpl(val dataCollie: DataCollie, k8sClient: K8SClient) extends Collie {
  override protected def doRemove(clusterInfo: ClusterStatus, beRemovedContainer: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    Future.sequence(beRemovedContainer.map(c => k8sClient.remove(c.name))).map(_ => Unit)

  override protected def doForceRemove(clusterInfo: ClusterStatus, containerInfos: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    Future.sequence(containerInfos.map(c => k8sClient.forceRemove(c.name))).map(_ => Unit)

  override def logs(key: ObjectKey, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[Map[ContainerName, String]] =
    cluster(key)
      .map(_.containers)
      .flatMap(
        cs =>
          Future.sequence(
            cs.map(container => k8sClient.log(container.name, sinceSeconds))
          )
      )
      .map(_.toMap)

  override def clusters()(
    implicit executionContext: ExecutionContext
  ): Future[Seq[ClusterStatus]] =
    k8sClient
      .containers()
      .map(_.filter(container => Collie.matched(container.name, kind)))
      .map(
        _.map(container => Collie.objectKeyOfContainerName(container.name) -> container)
          .groupBy(_._1)
          .map {
            case (objectKey, value) => objectKey -> value.map(_._2)
          }
          .map {
            case (objectKey, containers) => toStatus(objectKey, containers)
          }
          .toSeq
      )
      .flatMap(Future.sequence(_))

  override protected def toClusterState(containers: Seq[ContainerInfo]): Option[ServiceState] =
    if (containers.isEmpty) None
    else {
      // we use a "pod" as a container of ohara cluster, so it is more easy to define a cluster state than docker
      // since a "pod" in k8s is actually an application with multiple containers...
      if (containers.exists(_.state == K8sContainerState.RUNNING.name)) Some(ServiceState.RUNNING)
      else if (containers.exists(_.state == K8sContainerState.FAILED.name)) Some(ServiceState.FAILED)
      else if (containers.exists(_.state == K8sContainerState.PENDING.name)) Some(ServiceState.PENDING)
      // All Containers in the Pod have terminated in success, BUT it is still failed :(
      else if (containers.exists(_.state == K8sContainerState.SUCCEEDED.name)) Some(ServiceState.FAILED)
      else Some(ServiceState.UNKNOWN)
    }

  //----------------------------[override helper methods]----------------------------//
  override protected def doCreator(
    executionContext: ExecutionContext,
    containerInfo: ContainerInfo,
    node: Node,
    volume: Option[String],
    route: Map[String, String],
    arguments: Seq[String]
  ): Future[Unit] =
    k8sClient.containerCreator
      .imageName(containerInfo.imageName)
      .portMappings(
        containerInfo.portMappings.map(portMapping => portMapping.hostPort -> portMapping.containerPort).toMap
      )
      .nodeName(containerInfo.nodeName)
      /**
        * the hostname of k8s/docker container has strict limit. Fortunately, we are aware of this issue and the hostname
        * passed to this method is legal to k8s/docker. Hence, assigning the hostname is very safe to you :)
        */
      .hostname(containerInfo.hostname)
      .envs(containerInfo.environments)
      .name(containerInfo.name)
      .threadPool(executionContext)
      .arguments(arguments)
      .create()

  override protected def postCreate(
    clusterStatus: ClusterStatus,
    existentNodes: Map[Node, ContainerInfo],
    routes: Map[String, String]
  )(implicit executionContext: ExecutionContext): Future[Unit] = Future.unit
}
