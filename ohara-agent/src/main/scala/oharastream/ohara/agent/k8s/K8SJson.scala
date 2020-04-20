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

import oharastream.ohara.client.HttpExecutor
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

  final case class Container(
    name: String,
    image: String,
    ports: Option[Seq[ContainerPort]],
    env: Option[Seq[EnvVar]],
    imagePullPolicy: Option[ImagePullPolicy],
    command: Option[Seq[String]],
    args: Option[Seq[String]]
  )
  implicit val CONTAINER_JSON_FORMAT: RootJsonFormat[Container] = jsonFormat7(Container)

  implicit val RESTART_POLICY_JSON_FORMAT: RootJsonFormat[RestartPolicy] = new RootJsonFormat[RestartPolicy] {
    override def read(json: JsValue): RestartPolicy = RestartPolicy.forName(json.convertTo[String])

    override def write(obj: RestartPolicy): JsValue = JsString(obj.toString)
  }

  final case class HostAliases(ip: String, hostnames: Seq[String])
  implicit val HOST_ALIASES_FORMAT: RootJsonFormat[HostAliases] = jsonFormat2(HostAliases)

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

  final case class PodSpec(
    nodeSelector: Option[NodeSelector],
    hostname: String,
    hostAliases: Option[Seq[HostAliases]],
    subdomain: Option[String],
    nodeName: Option[String],
    containers: Seq[Container],
    restartPolicy: Option[RestartPolicy]
  )
  implicit val SPEC_JSON_FORMAT: RootJsonFormat[PodSpec] = jsonFormat7(PodSpec)

  final case class Metadata(
    uid: Option[String],
    name: String,
    labels: Option[Map[String, String]],
    creationTimestamp: Option[String]
  )
  implicit val METADATA_JSON_FORMAT: RootJsonFormat[Metadata] = jsonFormat4(Metadata)

  final case class Status(phase: String, hostIP: Option[String])
  implicit val STATUS_JSON_FORMAT: RootJsonFormat[Status] = jsonFormat2(Status)

  final case class Pod(metadata: Metadata, spec: Option[PodSpec], status: Option[Status])
  implicit val ITEMS_JSON_FORMAT: RootJsonFormat[Pod] = jsonFormat3(Pod)

  final case class PodList(items: Seq[Pod])
  implicit val PODINFO_JSON_FORMAT: RootJsonFormat[PodList] = jsonFormat1(PodList)

  //for show node information

  final case class NodeAddresses(nodeType: String, nodeAddress: String)
  implicit val NODE_HOSTINFO_FORMAT: RootJsonFormat[NodeAddresses] =
    new RootJsonFormat[NodeAddresses] {
      override def write(obj: NodeAddresses): JsValue = JsObject(
        "type"    -> JsString(obj.nodeType),
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
        "type"    -> JsString(obj.conditionType),
        "status"  -> JsString(obj.status),
        "message" -> JsString(obj.message)
      )
    }

  final case class Allocatable(cpu: Option[String], memory: Option[String])
  implicit val ALLOCATABLE_JSON_FORMAT: RootJsonFormat[Allocatable] = jsonFormat2(Allocatable)

  final case class NodeStatus(
    allocatable: Option[Allocatable],
    addresses: Seq[NodeAddresses],
    images: Seq[ImageNames],
    conditions: Seq[Condition]
  )
  implicit val NODESTATUS_JSON_FORMAT: RootJsonFormat[NodeStatus] = jsonFormat4(NodeStatus)

  final case class NodeMetaData(name: String)
  implicit val NODEMETADATA_JSON_FORMAT: RootJsonFormat[NodeMetaData] = jsonFormat1(NodeMetaData)

  final case class NodeItems(status: NodeStatus, metadata: NodeMetaData)
  implicit val NODEITEMS_JSON_FORMAT: RootJsonFormat[NodeItems] = jsonFormat2(NodeItems)

  final case class NodeInfo(items: Seq[NodeItems])
  implicit val NODEINFO_JSON_FORMAT: RootJsonFormat[NodeInfo] = jsonFormat1(NodeInfo)

  //for node metrics
  final case class MetricsMetadata(name: String)
  implicit val METRICSMETADATA_JSON_FORMAT: RootJsonFormat[MetricsMetadata] = jsonFormat1(MetricsMetadata)

  final case class MetricsUsage(cpu: String, memory: String)
  implicit val METRICSUSAGE_JSON_FORMAT: RootJsonFormat[MetricsUsage] = jsonFormat2(MetricsUsage)

  final case class MetricsItem(metadata: MetricsMetadata, usage: MetricsUsage)
  implicit val METRICSITEM_JSON_FORMAT: RootJsonFormat[MetricsItem] = jsonFormat2(MetricsItem)

  final case class Metrics(items: Seq[MetricsItem])
  implicit val METRICS_JSON_FORMAT: RootJsonFormat[Metrics] = jsonFormat1(Metrics)

  //for create persistent volume
  final case class PVMetadata(name: String)
  implicit val PVMETADATA_JSON_FORMAT: RootJsonFormat[PVMetadata] = jsonFormat1(PVMetadata)

  final case class PVCapacity(storage: String)
  implicit val PVCAPACITY_JSON_FORMAT: RootJsonFormat[PVCapacity] = jsonFormat1(PVCapacity)

  final case class PVHostPath(path: String, hostPathType: String)
  implicit val PVHOSTPATH_JSON_FORMAT: RootJsonFormat[PVHostPath] =
    new RootJsonFormat[PVHostPath] {
      override def read(json: JsValue): PVHostPath = json.asJsObject.getFields("path", "type") match {
        case Seq(JsString(path), JsString(hostPathType)) =>
          PVHostPath(path, hostPathType)
        case other: Any =>
          throw DeserializationException(s"${classOf[PVHostPath].getSimpleName} expected but $other")
      }

      override def write(obj: PVHostPath): JsValue = JsObject(
        "path" -> JsString(obj.path),
        "type" -> JsString(obj.hostPathType)
      )
    }

  final case class PVMatchExpression(key: String, operator: String, values: Seq[String])
  implicit val PVMATCHEXPRESSION_JSON_FORMAT: RootJsonFormat[PVMatchExpression] = jsonFormat3(
    PVMatchExpression
  )

  final case class PVNodeSelectorTerm(matchExpressions: Seq[PVMatchExpression])
  implicit val PVNODESELECTORTERM_JSON_FORMAT: RootJsonFormat[PVNodeSelectorTerm] = jsonFormat1(
    PVNodeSelectorTerm
  )

  final case class PVRequired(nodeSelectorTerms: Seq[PVNodeSelectorTerm])
  implicit val PVNODESELECTORTERMS_JSON_FORMAT: RootJsonFormat[PVRequired] = jsonFormat1(PVRequired)

  final case class PVNodeAffinity(required: PVRequired)
  implicit val PVREQUIRED_JSON_FORMAT: RootJsonFormat[PVNodeAffinity] = jsonFormat1(PVNodeAffinity)

  final case class PVSpec(
    capacity: PVCapacity,
    accessModes: Seq[String],
    persistentVolumeReclaimPolicy: String,
    storageClassName: String,
    hostPath: PVHostPath,
    nodeAffinity: PVNodeAffinity
  )
  implicit val PVSPEC_JSON_FORMAT: RootJsonFormat[PVSpec] = jsonFormat6(PVSpec)

  final case class PersistentVolume(metadata: PVMetadata, spec: PVSpec)
  implicit val PERSISTENTVOLUME_JSON_FORMAT: RootJsonFormat[PersistentVolume] = jsonFormat2(PersistentVolume)

  // for create persistent volume claim
  final case class PVCRequests(storage: String)
  implicit val PVCREQUEST_JSON_FORMAT: RootJsonFormat[PVCRequests] = jsonFormat1(PVCRequests)

  final case class PVCResources(requests: PVCRequests)
  implicit val PVCRESOURCES_JSON_FORMAT: RootJsonFormat[PVCResources] = jsonFormat1(PVCResources)

  final case class PVCSpec(storageClassName: String, accessModes: Seq[String], resources: PVCResources)
  implicit val PVCSPEC_JSON_FORMAT: RootJsonFormat[PVCSpec] = jsonFormat3(PVCSpec)

  final case class PVCMetadata(name: String)
  implicit val PVCMETADATA_JSON_FORMAT: RootJsonFormat[PVCMetadata] = jsonFormat1(PVCMetadata)

  final case class PersistentVolumeClaim(metadata: PVCMetadata, spec: PVCSpec)
  implicit val PERSISTENTVOLUMECLAIM_JSON_FORMAT: RootJsonFormat[PersistentVolumeClaim] = jsonFormat2(
    PersistentVolumeClaim
  )

  // for persistent volume info
  final case class PVSpecInfo(
    capacity: PVCapacity,
    hostPath: PVHostPath,
    volumeMode: String,
    nodeAffinity: Option[PVNodeAffinity]
  )
  implicit val PVSPECINFO_JSON_FORMAT: RootJsonFormat[PVSpecInfo] = jsonFormat4(PVSpecInfo)

  final case class PersistentVolumeItem(metadata: PVMetadata, spec: PVSpecInfo)
  implicit val PERSISTENTVOLUMEITEM_JSON_FORMAT: RootJsonFormat[PersistentVolumeItem] = jsonFormat2(
    PersistentVolumeItem
  )

  final case class PersistentVolumeInfo(items: Seq[PersistentVolumeItem])
  implicit val PERSISTENTVOLUMEINFO_JSON_FORMAT: RootJsonFormat[PersistentVolumeInfo] = jsonFormat1(
    PersistentVolumeInfo
  )

  //for error
  final case class ErrorResponse(message: String) extends HttpExecutor.Error
  implicit val ERROR_RESPONSE_FORMAT: RootJsonFormat[ErrorResponse] = jsonFormat1(ErrorResponse)
}
