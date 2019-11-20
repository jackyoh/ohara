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

package com.island.ohara.agent

import java.util.Objects

import com.island.ohara.agent.docker.ContainerState
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping}
import com.island.ohara.client.configurator.v0.FileInfoApi.FileInfo
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.StreamApi
import com.island.ohara.client.configurator.v0.StreamApi.{Creation, StreamClusterInfo, StreamClusterStatus}
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.metrics.BeanChannel
import com.island.ohara.metrics.basic.CounterMBean
import com.island.ohara.streams.Stream
import com.island.ohara.streams.config.StreamDefUtils
import spray.json.JsString

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * An interface of controlling stream cluster.
  * It isolates the implementation of container manager from Configurator.
  */
trait StreamCollie extends Collie[StreamClusterStatus] {
  override def creator: StreamCollie.ClusterCreator =
    (executionContext, creation) => {
      implicit val exec: ExecutionContext = executionContext

      val resolveRequiredInfos = for {
        allNodes <- dataCollie.valuesByNames[Node](creation.nodeNames)
        existentNodes <- clusters().map(_.find(_._1.key == creation.key)).flatMap {
          case Some(value) =>
            dataCollie
              .valuesByNames[Node](value._1.aliveNodes)
              .map(nodes => nodes.map(node => node -> value._2.find(_.nodeName == node.hostname).get).toMap)
          case None => Future.successful(Map.empty[Node, ContainerInfo])
        }
        brokerClusterInfo <- dataCollie.value[BrokerClusterInfo](creation.brokerClusterKey)
        fileInfo <- dataCollie.value[FileInfo](creation.jarKey)
      } yield
        (existentNodes,
         allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
         brokerClusterInfo,
         fileInfo)

      resolveRequiredInfos
        .map {
          case (existentNodes, newNodes, brokerClusterInfo, fileInfo) =>
            if (existentNodes.nonEmpty)
              throw new UnsupportedOperationException(s"stream collie doesn't support to add node to a running cluster")
            else (newNodes, brokerClusterInfo, fileInfo)
        }
        .flatMap {
          case (newNodes, brokerClusterInfo, fileInfo) =>
            val successfulContainersFuture =
              if (newNodes.isEmpty) Future.successful(Seq.empty)
              else {
                val route = resolveHostNames(
                  (newNodes.map(_.hostname)
                    ++ brokerClusterInfo.nodeNames
                  // make sure the stream can connect to configurator
                    ++ Seq(fileInfo.url.get.getHost)).toSet
                )
                // ssh connection is slow so we submit request by multi-thread
                Future.sequence(newNodes.map { newNode =>
                  val containerInfo = ContainerInfo(
                    nodeName = newNode.name,
                    id = Collie.UNKNOWN,
                    imageName = creation.imageName,
                    // this fake container will be cached before refreshing cache so we make it running.
                    // other, it will be filtered later ...
                    state = ContainerState.RUNNING.name,
                    kind = Collie.UNKNOWN,
                    name = Collie.containerName(prefixKey, creation.group, creation.name, serviceName),
                    size = -1,
                    portMappings = creation.ports
                      .map(
                        port =>
                          PortMapping(
                            hostIp = Collie.UNKNOWN,
                            hostPort = port,
                            containerPort = port
                        ))
                      .toSeq,
                    environments = creation.settings.map {
                      case (k, v) =>
                        k -> (v match {
                          // the string in json representation has quote in the beginning and end.
                          // we don't like the quotes since it obstruct us to cast value to pure string.
                          case JsString(s) => s
                          // save the json string for all settings
                          // StreamDefUtils offers the helper method to turn them back.
                          case _ => CommonUtils.toEnvString(v.toString)
                        })
                    } ++ Map(
                      "JMX_PORT" -> creation.jmxPort.toString,
                      "JMX_HOSTNAME" -> newNode.hostname
                    )
                    // define the urls as string list so as to simplify the script for stream
                      + ("STREAM_JAR_URLS" -> fileInfo.url.get.toURI.toASCIIString),
                    // we should set the hostname to container name in order to avoid duplicate name with other containers
                    hostname = Collie.containerHostName(prefixKey, creation.group, creation.name, serviceName)
                  )
                  val arguments =
                    Seq(classOf[Stream].getName,
                        s"${StreamDefUtils.CLASS_NAME_DEFINITION.key()}=${creation.className.get}")

                  doCreator(executionContext, containerInfo, newNode, route, arguments)
                    .map(_ => Some(containerInfo))
                    .recover {
                      case _: Throwable =>
                        None
                    }
                })
              }

            successfulContainersFuture.map(_.flatten.toSeq).map { aliveContainers =>
              val state = toClusterState(aliveContainers).map(_.name)
              postCreate(
                new StreamClusterStatus(
                  group = creation.group,
                  name = creation.name,
                  aliveNodes = aliveContainers.map(_.nodeName).toSet,
                  state = state,
                  error = None
                ),
                aliveContainers
              )
            }
        }
    }

  /**
    * Get all counter beans from cluster
    * @param cluster cluster
    * @return counter beans
    */
  def counters(cluster: StreamClusterInfo): Seq[CounterMBean] = cluster.aliveNodes.flatMap { node =>
    BeanChannel.builder().hostname(node).port(cluster.jmxPort).build().counterMBeans().asScala
  }.toSeq

  override protected[agent] def toStatus(key: ObjectKey, containers: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext): Future[StreamClusterStatus] =
    Future.successful(
      new StreamClusterStatus(
        group = key.group(),
        name = key.name(),
        // Currently, docker naming rule for "Running" and Kubernetes naming rule for "PENDING"
        // it is ok that we use the containerState.RUNNING or containerState.PENDING here.
        aliveNodes = containers
          .filter(c => c.state == ContainerState.RUNNING.name || c.state == ContainerState.PENDING.name)
          .map(_.nodeName)
          .toSet,
        state = toClusterState(containers).map(_.name),
        error = None
      ))

  protected def dataCollie: DataCollie

  /**
    * Define prefixKey by different environment
    * @return prefix key
    */
  protected def prefixKey: String

  override val serviceName: String = StreamApi.STREAM_SERVICE_NAME

  protected def doCreator(executionContext: ExecutionContext,
                          containerInfo: ContainerInfo,
                          node: Node,
                          route: Map[String, String],
                          arguments: Seq[String]): Future[Unit]

  protected def postCreate(clusterStatus: StreamClusterStatus, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default do nothing
  }

}

object StreamCollie {
  trait ClusterCreator extends Collie.ClusterCreator with StreamApi.Request {
    override def create(): Future[Unit] = doCreate(
      executionContext = Objects.requireNonNull(executionContext),
      creation = creation
    )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
