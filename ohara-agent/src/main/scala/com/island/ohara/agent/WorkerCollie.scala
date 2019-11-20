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
import com.island.ohara.client.configurator.v0.WorkerApi
import com.island.ohara.client.configurator.v0.WorkerApi.{Creation, WorkerClusterInfo, WorkerClusterStatus}
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.metrics.BeanChannel
import com.island.ohara.metrics.basic.CounterMBean

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

trait WorkerCollie extends Collie[WorkerClusterStatus] {

  override val serviceName: String = WorkerApi.WORKER_SERVICE_NAME

  // TODO: remove this hard code (see #2957)
  private[this] val homeFolder: String = WorkerApi.WORKER_HOME_FOLDER
  private[this] val configPath: String = s"$homeFolder/config/worker.config"

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each worker container should assign "docker host name/port" to advertised name/port
    * 4) add broker routes to all worker containers (worker needs to connect to broker cluster)
    * 5) Add worker routes to all worker containers
    * 6) update existed containers (if we are adding new node into a running cluster)
    * @return description of worker cluster
    */
  override def creator: WorkerCollie.ClusterCreator = (executionContext, creation) => {
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
      pluginInfos <- dataCollie.values[FileInfo](creation.pluginKeys)
      sharedJarInfos <- dataCollie.values[FileInfo](creation.sharedJarKeys)
    } yield
      (existentNodes,
       allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
       brokerClusterInfo,
       pluginInfos,
       sharedJarInfos)

    resolveRequiredInfos.flatMap {
      case (existentNodes, newNodes, brokerClusterInfo, pluginInfos, sharedJarInfos) =>
        val successfulContainersFuture =
          if (newNodes.isEmpty) Future.successful(Seq.empty)
          else {
            val brokers =
              brokerClusterInfo.nodeNames.map(nodeName => s"$nodeName:${brokerClusterInfo.clientPort}").mkString(",")

            val route = resolveHostNames(
              (existentNodes.keys.map(_.hostname)
                ++ newNodes.map(_.hostname)
                ++ brokerClusterInfo.nodeNames).toSet
              // make sure the worker can connect to configurator for downloading jars
              // Normally, the jar host name should be resolvable by worker since
              // we should add the "hostname" to configurator for most cases...
              // This is for those configurators that have no hostname (for example, temp configurator)
                ++ pluginInfos.map(_.url.get.getHost).toSet)
            existentNodes.foreach {
              case (node, container) => hookOfNewRoute(node, container, route)
            }

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
                environments = Map(
                  "KAFKA_JMX_OPTS" -> (s"-Dcom.sun.management.jmxremote" +
                    s" -Dcom.sun.management.jmxremote.authenticate=false" +
                    s" -Dcom.sun.management.jmxremote.ssl=false" +
                    s" -Dcom.sun.management.jmxremote.port=${creation.jmxPort}" +
                    s" -Dcom.sun.management.jmxremote.rmi.port=${creation.jmxPort}" +
                    s" -Djava.rmi.server.hostname=${newNode.hostname}"),
                  // define the urls as string list so as to simplify the script for worker
                  "WORKER_PLUGIN_URLS" -> pluginInfos.map(_.url.get.toURI.toASCIIString).mkString(","),
                  "WORKER_SHARED_JAR_URLS" -> sharedJarInfos.map(_.url.get.toURI.toASCIIString).mkString(",")
                ),
                hostname = Collie.containerHostName(prefixKey, creation.group, creation.name, serviceName)
              )

              /**
                * Construct the required configs for current container
                * we will loop all the files in FILE_DATA of arguments : --file A --file B --file C
                * the format of A, B, C should be file_name=k1=v1,k2=v2,k3,k4=v4...
                */
              val arguments = ArgumentsBuilder()
                .mainConfigFile(configPath)
                .file(configPath)
                .append("bootstrap.servers", brokers)
                .append(WorkerApi.GROUP_ID_DEFINITION.key(), creation.groupId)
                .append(WorkerApi.CONFIG_TOPIC_NAME_DEFINITION.key(), creation.configTopicName)
                .append(WorkerApi.CONFIG_TOPIC_REPLICATIONS_DEFINITION.key(), creation.configTopicReplications)
                .append(WorkerApi.OFFSET_TOPIC_NAME_DEFINITION.key(), creation.offsetTopicName)
                .append(WorkerApi.OFFSET_TOPIC_PARTITIONS_DEFINITION.key(), creation.offsetTopicPartitions)
                .append(WorkerApi.OFFSET_TOPIC_REPLICATIONS_DEFINITION.key(), creation.offsetTopicReplications)
                .append(WorkerApi.STATUS_TOPIC_NAME_DEFINITION.key(), creation.statusTopicName)
                .append(WorkerApi.STATUS_TOPIC_PARTITIONS_DEFINITION.key(), creation.statusTopicPartitions)
                .append(WorkerApi.STATUS_TOPIC_REPLICATIONS_DEFINITION.key(), creation.statusTopicReplications)
                .append("rest.port", creation.clientPort)
                .append("rest.advertised.host.name", newNode.hostname)
                .append("rest.advertised.port", creation.clientPort)
                // We offers the kafka recommend settings since we always overwrite the converter in starting connector
                // (see ConnectorFormatter)
                // If users want to deploy connectors manually, this default settings can simplify their life from coming
                // across the schema error :)
                .append("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .append("key.converter.schemas.enable", true)
                .append("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .append("value.converter.schemas.enable", true)
                .done
                .build
              doCreator(executionContext, containerInfo, newNode, route, arguments)
                .map(_ => Some(containerInfo))
                .recover {
                  case _: Throwable =>
                    None
                }
            })
          }
        successfulContainersFuture.map(_.flatten.toSeq).map { successfulContainers =>
          val aliveContainers = existentNodes.values.toSeq ++ successfulContainers
          val state = toClusterState(aliveContainers).map(_.name)
          postCreate(
            new WorkerClusterStatus(
              group = creation.group,
              name = creation.name,
              aliveNodes = aliveContainers.map(_.nodeName).toSet,
              state = state,
              error = None
            ),
            successfulContainers
          )
        }
    }
  }

  protected def dataCollie: DataCollie

  /**
    * Implement prefix name for paltform
    */
  protected def prefixKey: String

  /**
    * there is new route to the node. the sub class can update the running container to apply new route.
    */
  protected def hookOfNewRoute(node: Node, container: ContainerInfo, route: Map[String, String]): Unit = {
    //Nothing
  }

  /**
    * Please implement this function to create the container to a different platform
    * @param executionContext execution context
    * @param containerInfo container information
    * @param node node object
    * @param route ip-host mapping
    */
  protected def doCreator(executionContext: ExecutionContext,
                          containerInfo: ContainerInfo,
                          node: Node,
                          route: Map[String, String],
                          arguments: Seq[String]): Future[Unit]

  /**
    * After the worker container creates complete, you maybe need to do other things.
    */
  protected def postCreate(clusterStatus: WorkerClusterStatus, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default Nothing
  }

  /**
    * Create a worker client according to passed cluster.
    * Noted: this method is placed at collie so as to enable fake collie be available to route.
    * @param workerClusterInfo target cluster
    * @return worker client
    */
  def workerClient(workerClusterInfo: WorkerClusterInfo)(
    implicit executionContext: ExecutionContext): Future[WorkerClient] =
    cluster(workerClusterInfo.key).map(_ => WorkerClient(workerClusterInfo))

  /**
    * Get all counter beans from specific worker cluster
    * @param cluster cluster
    * @return counter beans
    */
  def counters(cluster: WorkerClusterInfo): Seq[CounterMBean] = cluster.aliveNodes.flatMap { node =>
    BeanChannel.builder().hostname(node).port(cluster.jmxPort).build().counterMBeans().asScala
  }.toSeq

  override protected[agent] def toStatus(key: ObjectKey, containers: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext): Future[WorkerClusterStatus] =
    Future.successful(
      new WorkerClusterStatus(
        group = key.group(),
        name = key.name(),
        // Currently, docker naming rule for "Running" and Kubernetes naming rule for "PENDING"
        // it is ok that we use the containerState.RUNNING or containerState.PENDING here.
        aliveNodes = containers
          .filter(c => c.state == ContainerState.RUNNING.name || c.state == ContainerState.PENDING.name)
          .map(_.nodeName)
          .toSet,
        state = toClusterState(containers).map(_.name),
        // TODO how could we fetch the error?...by Sam
        error = None
      ))
}

object WorkerCollie {
  trait ClusterCreator extends Collie.ClusterCreator with WorkerApi.Request {
    override def create(): Future[Unit] =
      doCreate(
        executionContext = Objects.requireNonNull(executionContext),
        creation = creation
      )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
