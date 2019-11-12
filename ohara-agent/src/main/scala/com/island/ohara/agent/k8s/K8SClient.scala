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

import java.util.Objects

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.island.ohara.agent.k8s.K8SClient.ContainerCreator
import com.island.ohara.agent.k8s.K8SJson._
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping}
import com.island.ohara.client.configurator.v0.NodeApi.Resource
import com.island.ohara.client.{Enum, HttpExecutor}
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.CommonUtils

import scala.concurrent.{Await, ExecutionContext, Future}

case class K8SStatusInfo(isHealth: Boolean, message: String)
case class K8SNodeReport(nodeName: String)
case class Report(nodeName: String, isK8SNode: Boolean, statusInfo: Option[K8SStatusInfo])

trait K8SClient {
  def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]]
  def remove(name: String)(implicit executionContext: ExecutionContext): Future[ContainerInfo]
  def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[ContainerInfo]
  def removeNode(containerName: String, nodeName: String, serviceName: String)(
    implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]]
  def log(name: String)(implicit executionContext: ExecutionContext): Future[String]
  def nodeNameIPInfo()(implicit executionContext: ExecutionContext): Future[Seq[HostAliases]]
  def containerCreator()(implicit executionContext: ExecutionContext): ContainerCreator
  def images(nodeName: String)(implicit executionContext: ExecutionContext): Future[Seq[String]]
  def checkNode(nodeName: String)(implicit executionContext: ExecutionContext): Future[Report]
  def nodes()(implicit executionContext: ExecutionContext): Future[Seq[K8SNodeReport]]
  def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]]
  def k8sMetricsAPIServerURL(metricsAPIServerURL: String): Unit
}

object K8SClient {
  import scala.concurrent.duration._
  val NAMESPACE_DEFAULT_VALUE: String = "default"

  private[this] val TIMEOUT: FiniteDuration = 30 seconds
  private[agent] val K8S_KIND_NAME = "K8S"

  def apply(k8sApiServerURL: String): K8SClient = apply(k8sApiServerURL, NAMESPACE_DEFAULT_VALUE)

  def apply(k8sApiServerURL: String, namespace: String): K8SClient = {
    if (k8sApiServerURL.isEmpty) throw new IllegalArgumentException(s"invalid kubernetes api:$k8sApiServerURL")

    new K8SClient() with SprayJsonSupport {
      private[this] var metricsAPIServerURL: String = _

      override def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] =
        HttpExecutor.SINGLETON
          .get[PodList, K8SErrorResponse](s"$k8sApiServerURL/namespaces/$namespace/pods")
          .map(podList =>
            podList.items.map(pod => {
              val spec: PodSpec = pod.spec.getOrElse(
                throw new RuntimeException(s"the container doesn't have spec : ${pod.metadata.name}"))
              val containerInfo: Container = spec.containers.head
              val phase = pod.status.map(_.phase).getOrElse("Unknown")
              val hostIP = pod.status.fold("Unknown")(_.hostIP.getOrElse("Unknown"))
              ContainerInfo(
                nodeName = spec.nodeName.getOrElse("Unknown"),
                id = pod.metadata.uid.getOrElse("Unknown"),
                imageName = containerInfo.image,
                state = K8sContainerState.all
                  .find(s => phase.toLowerCase().contains(s.name.toLowerCase))
                  .getOrElse(K8sContainerState.UNKNOWN)
                  .name,
                kind = K8S_KIND_NAME,
                size = -1,
                name = pod.metadata.name,
                portMappings =
                  containerInfo.ports.getOrElse(Seq.empty).map(x => PortMapping(hostIP, x.hostPort, x.containerPort)),
                environments = containerInfo.env.getOrElse(Seq()).map(x => x.name -> x.value.getOrElse("")).toMap,
                hostname = spec.hostname
              )
            }))

      override def images(nodeName: String)(implicit executionContext: ExecutionContext): Future[Seq[String]] =
        HttpExecutor.SINGLETON
          .get[NodeItems, K8SErrorResponse](s"$k8sApiServerURL/nodes/$nodeName")
          .map(x => {
            x.status.images.flatMap(_.names)
          })

      override def checkNode(nodeName: String)(implicit executionContext: ExecutionContext): Future[Report] =
        HttpExecutor.SINGLETON.get[K8SNodeInfo, K8SErrorResponse](s"$k8sApiServerURL/nodes").map { r =>
          val filterNode: Seq[NodeItems] = r.items.filter(x => x.metadata.name.equals(nodeName))
          val isK8SNode: Boolean = filterNode.size == 1
          var statusInfo: Option[K8SStatusInfo] = None
          if (isK8SNode)
            statusInfo = Some(
              filterNode
                .flatMap(x => {
                  x.status.conditions.filter(y => y.conditionType.equals("Ready"))
                }.map(x => {
                  if (x.status.equals("True")) K8SStatusInfo(true, x.message)
                  else K8SStatusInfo(false, x.message)
                }))
                .head)
          Report(nodeName, isK8SNode, statusInfo)
        }

      override def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[ContainerInfo] =
        removePod(name, true)

      override def remove(name: String)(implicit executionContext: ExecutionContext): Future[ContainerInfo] =
        removePod(name, false)

      override def removeNode(containerName: String, nodeName: String, serviceName: String)(
        implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] = {
        containers()
          .map(cs => cs.filter(c => c.name == containerName && c.nodeName == nodeName))
          .flatMap(
            cs =>
              Future.sequence(
                cs.map(container => {
                  remove(container.name)
                })
            ))
          .map(cs => {
            cs.map(container => {
              //Kubernetes remove pod container is async to need to await container remove completely to return the
              //container info.
              var isRemovedContainer: Boolean = false
              while (!isRemovedContainer) {
                if (!Await
                      .result(containers(), TIMEOUT)
                      .exists(c => c.name == containerName && c.nodeName == nodeName)) {
                  isRemovedContainer = true
                }
              }
              container
            })
          })
      }

      override def log(name: String)(implicit executionContext: ExecutionContext): Future[String] =
        HttpExecutor.SINGLETON
          .getOnlyMessage(s"$k8sApiServerURL/namespaces/$namespace/pods/$name/log")
          .map(msg => if (msg.contains("ERROR:")) throw new IllegalArgumentException(msg) else msg)

      override def nodeNameIPInfo()(implicit executionContext: ExecutionContext): Future[Seq[HostAliases]] =
        HttpExecutor.SINGLETON
          .get[K8SNodeInfo, K8SErrorResponse](s"$k8sApiServerURL/nodes")
          .map(nodeInfo =>
            nodeInfo.items.map(item => {
              val internalIP: String =
                item.status.addresses.filter(node => node.nodeType.equals("InternalIP")).head.nodeAddress
              val hostName: String =
                item.status.addresses.filter(node => node.nodeType.equals("Hostname")).head.nodeAddress
              HostAliases(internalIP, Seq(hostName))
            }))

      override def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]] = {
        if (metricsAPIServerURL == null) Future.successful(Map.empty)
        else {
          // Get K8S metrics
          val nodeResourceUsage: Future[Map[String, K8SJson.K8SMetricsUsage]] = HttpExecutor.SINGLETON
            .get[K8SMetrics, K8SErrorResponse](s"$metricsAPIServerURL/metrics.k8s.io/v1beta1/nodes")
            .map(metrics => {
              metrics.items
                .flatMap(nodeMetricsInfo => {
                  Seq(nodeMetricsInfo.metadata.name ->
                    K8SMetricsUsage(nodeMetricsInfo.usage.cpu, nodeMetricsInfo.usage.memory))
                })
                .toMap
            })

          // Get K8S Node info
          HttpExecutor.SINGLETON
            .get[K8SNodeInfo, K8SErrorResponse](s"$k8sApiServerURL/nodes")
            .map(nodeInfo =>
              nodeInfo.items
                .map { item =>
                  val allocatable =
                    item.status.allocatable.getOrElse(Allocatable(None, None))
                  (item.metadata.name, allocatable.cpu, allocatable.memory)
                }
                .map { nodeResource =>
                  nodeResourceUsage.map {
                    resourceUsage =>
                      val nodeName: String = nodeResource._1
                      val cpuValue: Int = nodeResource._2.getOrElse("0").toInt
                      val memoryValue: Long = nodeResource._3.getOrElse("0").replace("Ki", "").toLong
                      if (resourceUsage.contains(nodeName)) {
                        val cpuUsage: Option[Double] =
                          Option(
                            resourceUsage(nodeName).cpu.replace("n", "").toLong / (1000.0 * 1000.0 * 1000.0 * cpuValue))

                        val memoryUsage: Option[Double] =
                          Option(resourceUsage(nodeName).memory.replace("Ki", "").toDouble / memoryValue)

                        nodeName -> Seq(Resource.cpu(cpuValue, cpuUsage),
                                        Resource.memory(memoryValue * 1024, memoryUsage))
                      } else nodeName -> Seq.empty
                  }
              })
            .flatMap(Future.sequence(_))
            .map(_.toMap)
        }

      }

      override def nodes()(implicit executionContext: ExecutionContext): Future[Seq[K8SNodeReport]] = {
        HttpExecutor.SINGLETON
          .get[K8SNodeInfo, K8SErrorResponse](s"$k8sApiServerURL/nodes")
          .map(nodeInfo => nodeInfo.items.map(item => K8SNodeReport(item.metadata.name)))
      }

      /**
        * Set K8S metrics server URL
        * @param metricsAPIServerURL metrics api server url to get K8S node informat
        */
      override def k8sMetricsAPIServerURL(metricsAPIServerURL: String): Unit = {
        this.metricsAPIServerURL = metricsAPIServerURL
      }

      override def containerCreator()(implicit executionContext: ExecutionContext): ContainerCreator =
        new ContainerCreator() {
          private[this] var name: String = CommonUtils.randomString()
          private[this] var imagePullPolicy: ImagePullPolicy = ImagePullPolicy.IFNOTPRESENT
          private[this] var restartPolicy: RestartPolicy = RestartPolicy.Never
          private[this] var imageName: String = _
          private[this] var hostname: String = _
          private[this] var nodeName: String = _
          private[this] var domainName: String = _
          private[this] var labelName: String = _
          private[this] var envs: Map[String, String] = Map.empty
          private[this] var ports: Map[Int, Int] = Map.empty
          private[this] var routes: Map[String, String] = Map.empty
          private[this] var command: Seq[String] = Seq.empty
          private[this] var args: Seq[String] = Seq.empty
          private[this] implicit var executionContext: ExecutionContext =
            scala.concurrent.ExecutionContext.Implicits.global

          override def name(name: String): ContainerCreator = {
            this.name = CommonUtils.requireNonEmpty(name)
            this
          }

          override def imageName(imageName: String): ContainerCreator = {
            this.imageName = CommonUtils.requireNonEmpty(imageName)
            this
          }

          override def hostname(hostname: String): ContainerCreator = {
            this.hostname = CommonUtils.requireNonEmpty(hostname)
            this
          }

          override def envs(envs: Map[String, String]): ContainerCreator = {
            this.envs = Objects.requireNonNull(envs)
            this
          }

          override def portMappings(ports: Map[Int, Int]): ContainerCreator = {
            this.ports = Objects.requireNonNull(ports)
            this
          }

          override def routes(routes: Map[String, String]): ContainerCreator = {
            this.routes = Objects.requireNonNull(routes)
            this
          }

          override def nodeName(nodeName: String): ContainerCreator = {
            this.nodeName = CommonUtils.requireNonEmpty(nodeName)
            this
          }

          override def domainName(domainName: String): ContainerCreator = {
            this.domainName = CommonUtils.requireNonEmpty(domainName)
            this
          }

          override def labelName(labelName: String): ContainerCreator = {
            this.labelName = CommonUtils.requireNonEmpty(labelName)
            this
          }

          @Optional
          override def pullImagePolicy(imagePullPolicy: ImagePullPolicy): ContainerCreator = {
            this.imagePullPolicy = Objects.requireNonNull(imagePullPolicy, "pullImagePolicy should not be null")
            this
          }

          @Optional("default is Never")
          override def restartPolicy(restartPolicy: RestartPolicy): ContainerCreator = {
            this.restartPolicy = Objects.requireNonNull(restartPolicy, "restartPolicy should not be null")
            this
          }

          @Optional("default is empty")
          override def command(command: Seq[String]): ContainerCreator = {
            this.command = Objects.requireNonNull(command)
            this
          }

          @Optional("default is empty")
          override def args(args: Seq[String]): ContainerCreator = {
            this.args = Objects.requireNonNull(args)
            this
          }

          override def threadPool(executionContext: ExecutionContext): ContainerCreator = {
            this.executionContext = Objects.requireNonNull(executionContext)
            this
          }

          override def create(): Future[Option[ContainerInfo]] = {
            // required fields
            CommonUtils.requireNonEmpty(nodeName)
            CommonUtils.requireNonEmpty(hostname)
            CommonUtils.requireNonEmpty(domainName)
            CommonUtils.requireNonEmpty(imageName)
            CommonUtils.requireNonEmpty(labelName)

            nodeNameIPInfo
              .map { ipInfo =>
                PodSpec(
                  nodeSelector = Some(NodeSelector(nodeName)),
                  hostname = hostname, //hostname is container name
                  subdomain = Some(domainName),
                  hostAliases = Some(ipInfo ++ routes.map { case (host, ip) => HostAliases(ip, Seq(host)) }),
                  containers = Seq(Container(
                    name = labelName,
                    image = imageName,
                    env = if (envs.isEmpty) None else Some(envs.map(x => EnvVar(x._1, Some(x._2))).toSeq),
                    ports = if (ports.isEmpty) None else Some(ports.map(x => ContainerPort(x._1, x._2)).toSeq),
                    imagePullPolicy = Some(imagePullPolicy),
                    volumeMounts = None,
                    command = if (command.isEmpty) None else Some(command),
                    args = if (args.isEmpty) None else Some(args)
                  )),
                  restartPolicy = Some(restartPolicy),
                  nodeName = None,
                  volumes = None
                )
              }
              .flatMap(podSpec => { //name is pod name
                val request = Pod(Metadata(None, name, Some(Label(labelName)), None), Some(podSpec), None)
                HttpExecutor.SINGLETON.post[Pod, Pod, K8SErrorResponse](s"$k8sApiServerURL/namespaces/$namespace/pods",
                                                                        request)
              })
              .map(pod => {
                Option(
                  ContainerInfo(
                    nodeName = nodeName,
                    id = pod.metadata.uid.getOrElse("Unknown"),
                    imageName = imageName,
                    state = K8sContainerState.all
                      .find(s => pod.status.fold(false)(_.phase.toLowerCase.contains(s.name.toLowerCase)))
                      .getOrElse(K8sContainerState.UNKNOWN)
                      .name,
                    kind = K8S_KIND_NAME,
                    name = pod.metadata.name,
                    size = -1,
                    portMappings = ports.map(x => PortMapping(hostname, x._1, x._2)).toSeq,
                    environments = envs,
                    hostname = hostname
                  ))
              })
          }
        }

      private[this] def removePod(name: String, isForce: Boolean)(
        implicit executionContext: ExecutionContext): Future[ContainerInfo] =
        containers()
          .map(_.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Name:$name doesn't exist")))
          .flatMap(container => {
            if (isForce)
              HttpExecutor.SINGLETON.delete[K8SErrorResponse](
                s"$k8sApiServerURL/namespaces/$namespace/pods/${container.name}?gracePeriodSeconds=0")
            else
              HttpExecutor.SINGLETON
                .delete[K8SErrorResponse](s"$k8sApiServerURL/namespaces/$namespace/pods/${container.name}")

            Future.successful(container)
          })
    }
  }

  trait ContainerCreator extends com.island.ohara.common.pattern.Creator[Future[Option[ContainerInfo]]] {
    def name(name: String): ContainerCreator

    def imageName(imageName: String): ContainerCreator

    def hostname(hostname: String): ContainerCreator

    def envs(envs: Map[String, String]): ContainerCreator

    def portMappings(ports: Map[Int, Int]): ContainerCreator

    def routes(routes: Map[String, String]): ContainerCreator

    def nodeName(nodeName: String): ContainerCreator

    /**
      * set the thread pool used to execute request
      * @param executionContext thread pool
      * @return this creator
      */
    @Optional("default pool is scala.concurrent.ExecutionContext.Implicits.global")
    def threadPool(executionContext: ExecutionContext): ContainerCreator

    override def create(): Future[Option[ContainerInfo]]

    def domainName(domainName: String): ContainerCreator

    def labelName(labelName: String): ContainerCreator

    def pullImagePolicy(imagePullPolicy: ImagePullPolicy): ContainerCreator

    def restartPolicy(restartPolicy: RestartPolicy): ContainerCreator

    def command(command: Seq[String]): ContainerCreator

    def args(args: Seq[String]): ContainerCreator
  }

  sealed abstract class ImagePullPolicy
  object ImagePullPolicy extends Enum[ImagePullPolicy] {

    case object ALWAYS extends ImagePullPolicy {
      override def toString: String = "Always"
    }
    case object NEVER extends ImagePullPolicy {
      override def toString: String = "Never"
    }
    case object IFNOTPRESENT extends ImagePullPolicy {
      override def toString: String = "IfNotPresent"
    }
  }

  sealed abstract class RestartPolicy
  object RestartPolicy extends Enum[RestartPolicy] {
    case object Always extends RestartPolicy {
      override def toString: String = "Always"
    }

    case object OnFailure extends RestartPolicy {
      override def toString: String = "OnFailure"
    }

    case object Never extends RestartPolicy {
      override def toString: String = "Never"
    }
  }

}
