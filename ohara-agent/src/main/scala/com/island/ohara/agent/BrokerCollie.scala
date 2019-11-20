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
import com.island.ohara.client.configurator.v0.BrokerApi
import com.island.ohara.client.configurator.v0.BrokerApi.{BrokerClusterInfo, BrokerClusterStatus, Creation}
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping}
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.client.kafka.TopicAdmin
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.metrics.BeanChannel
import com.island.ohara.metrics.kafka.TopicMeter

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

trait BrokerCollie extends Collie[BrokerClusterStatus] {

  override val serviceName: String = BrokerApi.BROKER_SERVICE_NAME

  // TODO: remove this hard code (see #2957)
  private[this] val homeFolder: String = BrokerApi.BROKER_HOME_FOLDER
  private[this] val configPath: String = s"$homeFolder/config/broker.config"

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each broker container should assign "docker host name/port" to advertised name/port
    * 4) add zookeeper routes to all broker containers (broker needs to connect to zookeeper cluster)
    * 5) Add broker routes to all broker containers
    * 6) update existed containers (if we are adding new node into a running cluster)
    * @return creator of broker cluster
    */
  override def creator: BrokerCollie.ClusterCreator = (executionContext, creation) => {
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
      zookeeperClusterInfo <- dataCollie.value[ZookeeperClusterInfo](creation.zookeeperClusterKey)
    } yield
      (existentNodes,
       allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
       zookeeperClusterInfo)

    resolveRequiredInfos.flatMap {
      case (existentNodes, newNodes, zookeeperClusterInfo) =>
        val successfulContainersFuture =
          if (newNodes.isEmpty) Future.successful(Seq.empty)
          else {
            val zookeepers = zookeeperClusterInfo.nodeNames
              .map(nodeName => s"$nodeName:${zookeeperClusterInfo.clientPort}")
              .mkString(",")

            val route = resolveHostNames(
              (existentNodes.keys.map(_.hostname) ++ newNodes.map(_.hostname) ++ zookeeperClusterInfo.nodeNames).toSet)
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
                    s" -Djava.rmi.server.hostname=${newNode.hostname}")
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
                .append("zookeeper.connect", zookeepers)
                .append(BrokerApi.LOG_DIRS_DEFINITION.key(), creation.logDirs)
                .append(BrokerApi.NUMBER_OF_PARTITIONS_DEFINITION.key(), creation.numberOfPartitions)
                .append(BrokerApi.NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFINITION.key(),
                        creation.numberOfReplications4OffsetsTopic)
                .append(BrokerApi.NUMBER_OF_NETWORK_THREADS_DEFINITION.key(), creation.numberOfNetworkThreads)
                .append(BrokerApi.NUMBER_OF_IO_THREADS_DEFINITION.key(), creation.numberOfIoThreads)
                .append(s"listeners=PLAINTEXT://:${creation.clientPort}")
                .append(s"advertised.listeners=PLAINTEXT://${newNode.hostname}:${creation.clientPort}")
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
          val status = new BrokerClusterStatus(
            group = creation.group,
            name = creation.name,
            // TODO: we should check the supported arguments by the running broker images
            topicDefinition = BrokerApi.TOPIC_DEFINITION,
            aliveNodes = aliveContainers.map(_.nodeName).toSet,
            state = state,
            error = None
          )
          postCreate(status, successfulContainers)
        }
    }
  }

  protected def dataCollie: DataCollie

  /**
    *  Implement prefix name for the platform
    * @return
    */
  protected def prefixKey: String

  /**
    * Update exist node info
    * @param node node object
    * @param container container information
    * @param route ip-host mapping list
    */
  protected def hookUpdate(node: Node, container: ContainerInfo, route: Map[String, String]): Unit = {
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
    * After creating the broker, need to processor other things
    * @param clusterStatus broker cluster information
    * @param successfulContainers successful created containers
    */
  protected def postCreate(clusterStatus: BrokerClusterStatus, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default Nothing
  }

  /**
    * Create a topic admin according to passed cluster.
    * Noted: the input cluster MUST be running. otherwise, a exception is returned.
    * @param brokerClusterInfo target cluster
    * @return topic admin
    */
  def topicAdmin(brokerClusterInfo: BrokerClusterInfo)(
    implicit executionContext: ExecutionContext): Future[TopicAdmin] =
    cluster(brokerClusterInfo.key).map(_ => TopicAdmin(brokerClusterInfo.connectionProps))

  /**
    * Get all meter beans from specific broker cluster
    * @param cluster cluster
    * @return meter beans
    */
  def topicMeters(cluster: BrokerClusterInfo): Seq[TopicMeter] = cluster.nodeNames.flatMap { node =>
    BeanChannel.builder().hostname(node).port(cluster.jmxPort).build().topicMeters().asScala
  }.toSeq

  override protected[agent] def toStatus(key: ObjectKey, containers: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext): Future[BrokerClusterStatus] =
    Future.successful(
      new BrokerClusterStatus(
        group = key.group(),
        name = key.name(),
        // TODO: we should check the supported arguments by the running broker images
        topicDefinition = BrokerApi.TOPIC_DEFINITION,
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

  /**
    * there is new route to the node. the sub class can update the running container to apply new route.
    */
  protected def hookOfNewRoute(node: Node, container: ContainerInfo, route: Map[String, String]): Unit = {
    //Nothing
  }
}

object BrokerCollie {

  trait ClusterCreator extends Collie.ClusterCreator with BrokerApi.Request {
    override def create(): Future[Unit] =
      doCreate(
        executionContext = Objects.requireNonNull(executionContext),
        creation = creation
      )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
