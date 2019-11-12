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

import com.island.ohara.agent.k8s.K8SClient.{ImagePullPolicy, RestartPolicy}
import com.island.ohara.client.HttpExecutor
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

object K8SJson {
  //for show container information
  final case class EnvVar(name: String, value: Option[String])
  implicit val ENVINFO_JSON_FORM: RootJsonFormat[EnvVar] = jsonFormat2(EnvVar)

  final case class ContainerPort(hostPort: Int, containerPort: Int)
  implicit val PORTINFO_JSON_FORMAT: RootJsonFormat[ContainerPort] = jsonFormat2(ContainerPort)

  final case class VolumeMount(name: String, mountPath: String)
  implicit val VOLUME_MOUNT_JSON_FORMAT: RootJsonFormat[VolumeMount] = jsonFormat2(VolumeMount)

  implicit val IMAGE_PULL_POLICY_FORMAT: RootJsonFormat[ImagePullPolicy] = new RootJsonFormat[ImagePullPolicy] {
    override def read(json: JsValue): ImagePullPolicy = ImagePullPolicy.forName(json.convertTo[String])

    override def write(obj: ImagePullPolicy): JsValue = JsString(obj.toString)
  }

  final case class Container(name: String,
                             image: String,
                             ports: Option[Seq[ContainerPort]],
                             env: Option[Seq[EnvVar]],
                             imagePullPolicy: Option[ImagePullPolicy],
                             volumeMounts: Option[Seq[VolumeMount]],
                             command: Option[Seq[String]],
                             args: Option[Seq[String]])
  implicit val CONTAINER_JSON_FORMAT: RootJsonFormat[Container] = jsonFormat8(Container)

  final case class ConfigMapVolumeSource(name: String)
  implicit val CONFIGMAP_VOLUME_SOURCE_JSON_FORMAT: RootJsonFormat[ConfigMapVolumeSource] = jsonFormat1(
    ConfigMapVolumeSource)

  final case class Volume(name: String, configMap: Option[ConfigMapVolumeSource])
  implicit val VOLUME_JSON_FORMAT: RootJsonFormat[Volume] = jsonFormat2(Volume)

  implicit val RESTART_POLICY_JSON_FORMAT: RootJsonFormat[RestartPolicy] = new RootJsonFormat[RestartPolicy] {
    override def read(json: JsValue): RestartPolicy = RestartPolicy.forName(json.convertTo[String])

    override def write(obj: RestartPolicy): JsValue = JsString(obj.toString)
  }

  final case class HostAliases(ip: String, hostnames: Seq[String])
  implicit val HOST_ALIASES_FORMAT: RootJsonFormat[HostAliases] = jsonFormat2(HostAliases)

  final case class Label(name: String)
  implicit val CREATEPOD_LABEL_FORMAT: RootJsonFormat[Label] = jsonFormat1(Label)

  final case class NodeSelector(hostname: String)
  implicit val CREATEPOD_NODESELECTOR_FORMAT: RootJsonFormat[NodeSelector] =
    new RootJsonFormat[NodeSelector] {
      override def read(json: JsValue): NodeSelector =
        json.asJsObject.getFields("kubernetes.io/hostname") match {
          case Seq(JsString(hostname)) =>
            NodeSelector(hostname)
          case other: Any =>
            throw DeserializationException(s"${classOf[NodeSelector].getSimpleName} expected but $other")
        }

      override def write(obj: NodeSelector) = JsObject(
        "kubernetes.io/hostname" -> JsString(obj.hostname)
      )
    }

  final case class PodSpec(nodeSelector: Option[NodeSelector],
                           hostname: String,
                           hostAliases: Option[Seq[HostAliases]],
                           subdomain: Option[String],
                           nodeName: Option[String],
                           containers: Seq[Container],
                           restartPolicy: Option[RestartPolicy],
                           volumes: Option[Seq[Volume]])
  implicit val SPEC_JSON_FORMAT: RootJsonFormat[PodSpec] = jsonFormat8(PodSpec)

  final case class Metadata(uid: Option[String], name: String, labels: Option[Label], creationTimestamp: Option[String])
  implicit val METADATA_JSON_FORMAT: RootJsonFormat[Metadata] = jsonFormat4(Metadata)

  final case class Status(phase: String, hostIP: Option[String])
  implicit val STATUS_JSON_FORMAT: RootJsonFormat[Status] = jsonFormat2(Status)

  final case class Pod(metadata: Metadata, spec: Option[PodSpec], status: Option[Status])
  implicit val ITEMS_JSON_FORMAT: RootJsonFormat[Pod] = jsonFormat3(Pod)

  final case class PodList(items: Seq[Pod])
  implicit val K8SPODINFO_JSON_FORMAT: RootJsonFormat[PodList] = jsonFormat1(PodList)

  //for show node information

  final case class NodeAddresses(nodeType: String, nodeAddress: String)
  implicit val NODE_HOSTINFO_FORMAT: RootJsonFormat[NodeAddresses] =
    new RootJsonFormat[NodeAddresses] {
      override def write(obj: NodeAddresses): JsValue = JsObject(
        "type" -> JsString(obj.nodeType),
        "address" -> JsString(obj.nodeAddress)
      )

      override def read(json: JsValue): NodeAddresses =
        json.asJsObject.getFields("type", "address") match {
          case Seq(JsString(nodeType), JsString(nodeAddress)) =>
            NodeAddresses(nodeType, nodeAddress)
          case other: Any =>
            throw DeserializationException(s"${classOf[NodeAddresses].getSimpleName} expected but $other")
        }
    }

  final case class ImageNames(names: Seq[String])
  implicit val NODE_IMAGENAMES_FORMAT: RootJsonFormat[ImageNames] = jsonFormat1(ImageNames)

  final case class Condition(conditionType: String, status: String, message: String)
  implicit val CONDITION_JSON_FORMAT: RootJsonFormat[Condition] =
    new RootJsonFormat[Condition] {
      override def read(json: JsValue): Condition =
        json.asJsObject.getFields("type", "status", "message") match {
          case Seq(JsString(conditionType), JsString(status), JsString(message)) =>
            Condition(conditionType, status, message)
          case other: Any =>
            throw DeserializationException(s"${classOf[Condition].getSimpleName} expected but $other")
        }

      override def write(obj: Condition): JsValue = JsObject(
        "type" -> JsString(obj.conditionType),
        "status" -> JsString(obj.status),
        "message" -> JsString(obj.message)
      )
    }

  final case class Allocatable(cpu: Option[String], memory: Option[String])
  implicit val ALLOCATABLE_JSON_FORMAT: RootJsonFormat[Allocatable] = jsonFormat2(Allocatable)

  final case class NodeStatus(allocatable: Option[Allocatable],
                              addresses: Seq[NodeAddresses],
                              images: Seq[ImageNames],
                              conditions: Seq[Condition])
  implicit val NODESTATUS_JSON_FORMAT: RootJsonFormat[NodeStatus] = jsonFormat4(NodeStatus)

  final case class NodeMetaData(name: String)
  implicit val NODEMETADATA_JSON_FORMAT: RootJsonFormat[NodeMetaData] = jsonFormat1(NodeMetaData)

  final case class NodeItems(status: NodeStatus, metadata: NodeMetaData)
  implicit val NODEITEMS_JSON_FORMAT: RootJsonFormat[NodeItems] = jsonFormat2(NodeItems)

  final case class K8SNodeInfo(items: Seq[NodeItems])
  implicit val K8SNODEINFO_JSON_FORMAT: RootJsonFormat[K8SNodeInfo] = jsonFormat1(K8SNodeInfo)

  final case class ConfigMap(apiVersion: String, kind: String, data: Map[String, String], metadata: Metadata)
  implicit val CONFIGMAP_FORMAT: RootJsonFormat[ConfigMap] = jsonFormat4(ConfigMap)

  //for node metrics
  final case class K8SMetricsMetadata(name: String)
  implicit val K8SMETRICSMETADATA_JSON_FORMAT: RootJsonFormat[K8SMetricsMetadata] = jsonFormat1(K8SMetricsMetadata)

  final case class K8SMetricsUsage(cpu: String, memory: String)
  implicit val K8SMETRICSUSAGE_JSON_FORMAT: RootJsonFormat[K8SMetricsUsage] = jsonFormat2(K8SMetricsUsage)

  final case class K8SMetricsItem(metadata: K8SMetricsMetadata, usage: K8SMetricsUsage)
  implicit val K8SMETRICSITEM_JSON_FORMAT: RootJsonFormat[K8SMetricsItem] = jsonFormat2(K8SMetricsItem)

  final case class K8SMetrics(items: Seq[K8SMetricsItem])
  implicit val K8SMETRICS_JSON_FORMAT: RootJsonFormat[K8SMetrics] = jsonFormat1(K8SMetrics)

  //for error
  final case class K8SErrorResponse(message: String) extends HttpExecutor.Error
  implicit val K8SERROR_RESPONSE_FORMAT: RootJsonFormat[K8SErrorResponse] = jsonFormat1(K8SErrorResponse)
}
