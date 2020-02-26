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

package oharastream.ohara.agent.docker

import oharastream.ohara.agent.container.ContainerName
import oharastream.ohara.agent.{Collie, DataCollie, ServiceState}
import oharastream.ohara.client.configurator.v0.ClusterStatus
import oharastream.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import oharastream.ohara.client.configurator.v0.NodeApi.Node
import oharastream.ohara.common.setting.ObjectKey

import scala.concurrent.{ExecutionContext, Future}
private abstract class BasicCollieImpl(
  val dataCollie: DataCollie,
  dockerClient: DockerClient,
  clusterCache: ServiceCache
) extends Collie {
  final override def clusters()(implicit executionContext: ExecutionContext): Future[Seq[ClusterStatus]] =
    Future.successful(clusterCache.snapshot.filter(_.kind == kind))

  protected def updateRoute(existentNodes: Map[Node, ContainerInfo], routes: Map[String, String])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    Future
      .traverse(existentNodes.values.map(_.name))(
        name =>
          dockerClient.containerInspector
            .name(name)
            .asRoot()
            .append("/etc/hosts", routes.map {
              case (hostname, ip) => s"$ip $hostname"
            }.toSeq)
      )
      .map(_ => Unit)

  override protected def doForceRemove(clusterInfo: ClusterStatus, beRemovedContainers: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    remove(clusterInfo, beRemovedContainers, true)

  override protected def doRemove(clusterInfo: ClusterStatus, beRemovedContainers: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    remove(clusterInfo, beRemovedContainers, false)

  private[this] def remove(clusterInfo: ClusterStatus, beRemovedContainers: Seq[ContainerInfo], force: Boolean)(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    Future
      .traverse(beRemovedContainers)(
        containerInfo =>
          if (force) dockerClient.forceRemove(containerInfo.name)
          else dockerClient.remove(containerInfo.name)
      )
      .map { _ =>
        val newContainers =
          clusterInfo.containers.filterNot(container => beRemovedContainers.exists(_.name == container.name))
        if (newContainers.isEmpty) clusterCache.remove(clusterInfo)
        else clusterCache.put(clusterInfo.copy(containers = newContainers))
      }

  override def logs(key: ObjectKey, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[Map[ContainerName, String]] =
    cluster(key)
      .map(_.containers)
      .flatMap { containers =>
        Future
          .sequence(containers.map { container =>
            dockerClient.log(container.name, sinceSeconds)
          })
          .map(_.toMap)
      }

  override protected def toClusterState(containers: Seq[ContainerInfo]): Option[ServiceState] =
    if (containers.isEmpty) None
    else {
      // one of the containers in pending state means cluster pending
      if (containers.exists(_.state == ContainerState.CREATED.name)) Some(ServiceState.PENDING)
      // not pending, if one of the containers in running state means cluster running (even other containers are in
      // restarting, paused, exited or dead state
      else if (containers.exists(_.state == ContainerState.RUNNING.name)) Some(ServiceState.RUNNING)
      // since cluster(collie) is a collection of long running containers,
      // we could assume cluster failed if containers are run into "exited" or "dead" state
      else if (containers.forall(c => c.state == ContainerState.EXITED.name || c.state == ContainerState.DEAD.name))
        Some(ServiceState.FAILED)
      // we set failed state is ok here
      // since there are too many cases that we could not handle for now, we should open the door for whitelist only
      else Some(ServiceState.FAILED)
    }

  //----------------------------[override helper methods]----------------------------//
  override protected def doCreator(
    executionContext: ExecutionContext,
    containerInfo: ContainerInfo,
    node: Node,
    volume: Option[String],
    routes: Map[String, String],
    arguments: Seq[String]
  ): Future[Unit] =
    dockerClient.containerCreator
      .imageName(containerInfo.imageName)
      .portMappings(
        containerInfo.portMappings.map(portMapping => portMapping.hostPort -> portMapping.containerPort).toMap
      )
      .hostname(containerInfo.hostname)
      .envs(containerInfo.environments)
      .name(containerInfo.name)
      .routes(routes)
      .arguments(arguments)
      .nodeName(node.hostname)
      .volume(volume)
      .threadPool(executionContext)
      .create()

  override protected def postCreate(
    clusterStatus: ClusterStatus,
    existentNodes: Map[Node, ContainerInfo],
    routes: Map[String, String]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    updateRoute(existentNodes, routes)
      .map { _ =>
        clusterCache.put(clusterStatus)
        Unit
      }
}
