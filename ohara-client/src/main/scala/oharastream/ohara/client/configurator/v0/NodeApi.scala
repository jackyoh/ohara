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

package oharastream.ohara.client.configurator.v0
import java.util.Objects

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
object NodeApi {
  val KIND: String = "node"
  // We use the hostname field as "spec.hostname" label in k8s, which has a limit length <= 63
  // also see https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
  val LIMIT_OF_HOSTNAME_LENGTH: Int = 63

  /**
    * node does not support group. However, we are in group world and there are many cases of inputting key (group, name)
    * to access resource. This method used to generate key for hostname of node.
    * @param hostname hostname
    * @return object key
    */
  def key(hostname: String): ObjectKey = ObjectKey.of(GROUP_DEFAULT, hostname)

  val NODES_PREFIX_PATH: String = "nodes"

  val ZOOKEEPER_SERVICE_NAME: String    = "zookeeper"
  val BROKER_SERVICE_NAME: String       = "broker"
  val WORKER_SERVICE_NAME: String       = "connect-worker"
  val STREAM_SERVICE_NAME: String       = "stream"
  val CONFIGURATOR_SERVICE_NAME: String = "configurator"

  case class Updating(
    port: Option[Int],
    user: Option[String],
    password: Option[String],
    tags: Option[Map[String, JsValue]]
  )
  implicit val UPDATING_JSON_FORMAT: RootJsonFormat[Updating] =
    JsonRefinerBuilder[Updating].format(jsonFormat4(Updating)).requireConnectionPort("port").rejectEmptyString().build

  case class Creation(
    hostname: String,
    port: Option[Int],
    user: Option[String],
    password: Option[String],
    tags: Map[String, JsValue]
  ) extends oharastream.ohara.client.configurator.v0.BasicCreation {
    override def group: String = GROUP_DEFAULT
    override def name: String  = hostname
  }
  implicit val CREATION_JSON_FORMAT: JsonRefiner[Creation] =
    JsonRefinerBuilder[Creation]
      .format(jsonFormat5(Creation))
      // default implementation of node is ssh, we use "default ssh port" here
      .nullToInt("port", 22)
      .requireConnectionPort("port")
      .rejectEmptyString()
      .stringRestriction("hostname", SettingDef.HOSTNAME_REGEX)
      .nullToEmptyObject(TAGS_KEY)
      .build

  case class NodeService(name: String, clusterKeys: Seq[ObjectKey])
  implicit val NODE_SERVICE_JSON_FORMAT: RootJsonFormat[NodeService] = jsonFormat2(NodeService)

  case class Resource(name: String, value: Double, unit: String, used: Option[Double])
  object Resource {
    /**
      * generate a resource based on cores if the number of core is large than 1. Otherwise, the size is in core.
      * @param cores cores
      * @param used used
      * @return memory resource
      */
    def cpu(cores: Int, used: Option[Double]): Resource = Resource(
      name = "CPU",
      value = cores,
      unit = if (cores > 1) "cores" else "core",
      used = used
    )

    /**
      * generate a resource based on MB if the input value is large than MB. Otherwise, the size is in bytes.
      * @param bytes bytes
      * @param used used
      * @return memory resource
      */
    def memory(bytes: Long, used: Option[Double]): Resource =
      if (bytes < 1024)
        Resource(
          name = "Memory",
          value = bytes.toDouble,
          unit = "bytes",
          used = used
        )
      else if (bytes < 1024 * 1024)
        Resource(
          name = "Memory",
          value = bytes / 1024f,
          unit = "KB",
          used = used
        )
      else if (bytes < 1024 * 1024 * 1024)
        Resource(
          name = "Memory",
          value = bytes / 1024f / 1024f,
          unit = "MB",
          used = used
        )
      else
        Resource(
          name = "Memory",
          value = bytes / 1024f / 1024f / 1024f,
          unit = "GB",
          used = used
        )
  }
  implicit val RESOURCE_JSON_FORMAT: RootJsonFormat[Resource] = jsonFormat4(Resource.apply)

  sealed abstract class State
  object State extends oharastream.ohara.client.Enum[State] {
    case object AVAILABLE   extends State
    case object UNAVAILABLE extends State
  }
  implicit val STATE_JSON_FORMAT: RootJsonFormat[State] = new RootJsonFormat[State] {
    override def write(obj: State): JsValue = JsString(obj.toString)

    override def read(json: JsValue): State = State.forName(json.convertTo[String])
  }

  /**
    * NOTED: the field "services" is filled at runtime. If you are in testing, it is ok to assign empty to it.
    */
  case class Node(
    hostname: String,
    port: Option[Int],
    user: Option[String],
    password: Option[String],
    services: Seq[NodeService],
    state: State,
    error: Option[String],
    lastModified: Long,
    resources: Seq[Resource],
    tags: Map[String, JsValue]
  ) extends Data {
    // Node does not support to define group
    override def group: String                 = GROUP_DEFAULT
    private[this] def msg(key: String): String = s"$key is required since Ohara Configurator is in docker mode"
    def _port: Int                             = port.getOrElse(throw new NoSuchElementException(msg("port")))
    def _user: String                          = user.getOrElse(throw new NoSuchElementException(msg("user")))
    def _password: String                      = password.getOrElse(throw new NoSuchElementException(msg("password")))
    override def name: String                  = hostname
    override def kind: String                  = KIND

    override protected def raw: Map[String, JsValue] = NODE_JSON_FORMAT.write(this).asJsObject.fields
  }

  object Node {
    /**
      * create a node with only hostname. It means this node is illegal to ssh mode.
      * @param hostname hostname
      * @return node
      */
    def apply(hostname: String): Node = Node(
      hostname = hostname,
      port = None,
      user = None,
      password = None,
      services = Seq.empty,
      state = State.AVAILABLE,
      error = None,
      lastModified = CommonUtils.current(),
      resources = Seq.empty,
      tags = Map.empty
    )
  }

  implicit val NODE_JSON_FORMAT: JsonRefiner[Node] = JsonRefinerBuilder[Node].format(jsonFormat10(Node.apply)).build

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  trait Request {
    /**
      * a specific setter that user can generate a request according to a existent node.
      * Noted that not all fields are copy to server. the included fields are shown below.
      * 1) hostname
      * 2) port
      * 3) user
      * 4) password
      * 5) tags
      * @param node node info
      * @return request
      */
    def node(node: Node): Request

    def hostname(hostname: String): Request

    @Optional("it is ignorable if you are going to send update request")
    def port(port: Int): Request

    @Optional("it is ignorable if you are going to send update request")
    def user(user: String): Request

    @Optional("it is ignorable if you are going to send update request")
    def password(password: String): Request

    @Optional("default value is empty array")
    def tags(tags: Map[String, JsValue]): Request

    /**
      * Retrieve the inner object of request payload. Noted, it throw unchecked exception if you haven't filled all required fields
      * @return the payload of creation
      */
    @VisibleForTesting
    private[v0] def creation: Creation

    /**
      * Retrieve the inner object of request payload. Noted, it throw unchecked exception if you haven't filled all required fields
      * @return the payload of creation
      */
    @VisibleForTesting
    private[v0] def updating: Updating

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[Node]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[Node]
  }

  class Access private[v0]
      extends oharastream.ohara.client.configurator.v0.Access[Creation, Updating, Node](NODES_PREFIX_PATH) {
    def request: Request = new Request {
      private[this] var hostname: String           = _
      private[this] var port: Option[Int]          = None
      private[this] var user: Option[String]       = None
      private[this] var password: Option[String]   = None
      private[this] var tags: Map[String, JsValue] = _

      override def node(node: Node): Request = {
        this.hostname = node.hostname
        this.port = node.port
        this.user = node.user
        this.password = node.password
        this.tags = node.tags
        this
      }

      override def hostname(hostname: String): Request = {
        this.hostname = CommonUtils.requireNonEmpty(hostname)
        this
      }
      override def port(port: Int): Request = {
        this.port = Some(CommonUtils.requireConnectionPort(port))
        this
      }
      override def user(user: String): Request = {
        this.user = Some(CommonUtils.requireNonEmpty(user))
        this
      }
      override def password(password: String): Request = {
        this.password = Some(CommonUtils.requireNonEmpty(password))
        this
      }

      override def tags(tags: Map[String, JsValue]): Request = {
        this.tags = Objects.requireNonNull(tags)
        this
      }

      override private[v0] def creation: Creation =
        // auto-complete the creation via our refiner
        CREATION_JSON_FORMAT.read(
          CREATION_JSON_FORMAT.write(
            Creation(
              hostname = CommonUtils.requireNonEmpty(hostname),
              user = user.map(CommonUtils.requireNonEmpty),
              password = password.map(CommonUtils.requireNonEmpty),
              port = port.map(CommonUtils.requireConnectionPort),
              tags = if (tags == null) Map.empty else tags
            )
          )
        )

      override private[v0] def updating: Updating =
        // auto-complete the updating via our refiner
        UPDATING_JSON_FORMAT.read(
          UPDATING_JSON_FORMAT.write(
            Updating(
              port = port.map(CommonUtils.requireConnectionPort),
              user = user.map(CommonUtils.requireNonEmpty),
              password = password.map(CommonUtils.requireNonEmpty),
              tags = Option(tags)
            )
          )
        )

      override def create()(implicit executionContext: ExecutionContext): Future[Node] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[Node] =
        put(ObjectKey.of(GROUP_DEFAULT, hostname), updating)
    }
  }

  def access: Access = new Access
}
