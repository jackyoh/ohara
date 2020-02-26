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

package oharastream.ohara.agent.container

import java.util.Objects

import oharastream.ohara.agent.container.ContainerClient.ContainerCreator
import oharastream.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import oharastream.ohara.client.configurator.v0.NodeApi.Resource
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.util.{CommonUtils, Releasable}

import scala.concurrent.{ExecutionContext, Future}

/**
  * a interface of Ohara containers. It is able to operate all containers created by ohara across all nodes.
  * For k8s mode, the entrypoint is k8s master. By contrast, for docker mode, the implementation traverse all nodes
  * to handle requests.
  *
  * Noted: You should use this interface rather than docker client or k8s client since this interface consists of common
  * functions supported by all implementations, and your services SHOULD NOT assume the container implementations.
  */
trait ContainerClient extends Releasable {
  def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]]
  def container(name: String)(implicit executionContext: ExecutionContext): Future[ContainerInfo] =
    containers()
      .map(
        _.find(_.name == name)
          .getOrElse(throw new NoSuchElementException(s"$name does not exists!!!"))
      )

  def containerNames()(implicit executionContext: ExecutionContext): Future[Seq[ContainerName]] =
    containers().map(_.map { container =>
      new ContainerName(
        id = container.id,
        name = container.name,
        imageName = container.imageName,
        nodeName = container.nodeName
      )
    })

  def containerName(name: String)(implicit executionContext: ExecutionContext): Future[ContainerName] =
    containerNames()
      .map(
        _.find(_.name == name)
          .getOrElse(throw new NoSuchElementException(s"$name does not exists!!!"))
      )

  def remove(name: String)(implicit executionContext: ExecutionContext): Future[Unit]
  def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[Unit]
  def log(name: String, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[(ContainerName, String)]
  def containerCreator: ContainerCreator
  def imageNames()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[String]]]
  def imageNames(nodeName: String)(implicit executionContext: ExecutionContext): Future[Seq[String]] =
    imageNames()
      .map(_.getOrElse(nodeName, throw new NoSuchElementException(s"$nodeName does not exists!!!")))

  def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]]
}

object ContainerClient {
  trait ContainerCreator extends oharastream.ohara.common.pattern.Creator[Future[Unit]] {
    private[this] var nodeName: String                            = _
    private[this] var hostname: String                            = CommonUtils.randomString()
    private[this] implicit var executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    private[this] var imageName: String                           = _
    private[this] var name: String                                = CommonUtils.randomString()
    private[this] var command: Option[String]                     = None
    private[this] var mappingPath: Option[String]                 = Option.empty
    private[this] var arguments: Seq[String]                      = Seq.empty
    private[this] var ports: Map[Int, Int]                        = Map.empty
    private[this] var envs: Map[String, String]                   = Map.empty
    private[this] var routes: Map[String, String]                 = Map.empty

    /**
      * set container's name. default is a random string
      *
      * @param name container name
      * @return this builder
      */
    @Optional("default is random string")
    def name(name: String): ContainerCreator.this.type = {
      this.name = CommonUtils.requireNonEmpty(name)
      this
    }

    def nodeName(nodeName: String): ContainerCreator.this.type = {
      this.nodeName = CommonUtils.requireNonEmpty(nodeName)
      this
    }

    /**
      * set target image
      *
      * @param imageName docker image
      * @return this builder
      */
    def imageName(imageName: String): ContainerCreator.this.type = {
      this.imageName = CommonUtils.requireNonEmpty(imageName)
      this
    }

    /**
      * @param hostname the hostname of container
      * @return this builder
      */
    @Optional("default is random string")
    def hostname(hostname: String): ContainerCreator.this.type = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    /**
      * @param envs the env variables exposed to container
      * @return this builder
      */
    @Optional("default is empty")
    def envs(envs: Map[String, String]): ContainerCreator.this.type = {
      this.envs = Objects.requireNonNull(envs)
      this
    }

    /**
      * @param routes the pre-defined route to container. hostname -> ip
      * @return this builder
      */
    @Optional("default is empty")
    def routes(routes: Map[String, String]): ContainerCreator.this.type = {
      this.routes = Objects.requireNonNull(routes)
      this
    }

    /**
      * forward the port from host to container.
      * NOTED: currently we don't support to specify the network interface so the forwarded port is bound on all network adapters.
      *
      * @param ports port mapping (host's port -> container's port)
      * @return this builder
      */
    @Optional("default is empty")
    def portMappings(ports: Map[Int, Int]): ContainerCreator.this.type = {
      this.ports = Objects.requireNonNull(ports)
      this
    }

    /**
      * the arguments passed to docker container
      *
      * @param arguments arguments
      * @return this builder
      */
    @Optional("default is empty")
    def arguments(arguments: Seq[String]): ContainerCreator.this.type = {
      this.arguments = Objects.requireNonNull(arguments)
      this
    }

    /**
      * the command passed to docker container
      *
      * @param command command
      * @return this builder
      */
    @Optional("default is empty")
    def command(command: String): ContainerCreator.this.type = {
      this.command = Some(Objects.requireNonNull(command))
      this
    }

    @Optional("default pool is scala.concurrent.ExecutionContext.Implicits.global")
    def threadPool(executionContext: ExecutionContext): ContainerCreator.this.type = {
      this.executionContext = Objects.requireNonNull(executionContext)
      this
    }

    @Optional("default is empty")
    def volume(mappingPath: Option[String]): ContainerCreator.this.type = {
      this.mappingPath = mappingPath
      this
    }

    final override def create(): Future[Unit] = doCreate(
      nodeName = CommonUtils.requireNonEmpty(nodeName),
      hostname = CommonUtils.requireNonEmpty(hostname),
      imageName = CommonUtils.requireNonEmpty(imageName),
      name = CommonUtils.requireNonEmpty(name),
      command = Objects.requireNonNull(command),
      volume = Objects.requireNonNull(mappingPath),
      arguments = Objects.requireNonNull(arguments),
      ports = Objects.requireNonNull(ports),
      envs = Objects.requireNonNull(envs),
      routes = Objects.requireNonNull(routes),
      executionContext = Objects.requireNonNull(executionContext)
    )

    protected def doCreate(
      nodeName: String,
      hostname: String,
      imageName: String,
      name: String,
      command: Option[String],
      volume: Option[String],
      arguments: Seq[String],
      ports: Map[Int, Int],
      envs: Map[String, String],
      routes: Map[String, String],
      executionContext: ExecutionContext
    ): Future[Unit]
  }
}
