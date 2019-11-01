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

package com.island.ohara.client.configurator.v0

import java.util.Objects

import com.island.ohara.client.configurator.QueryRequest
import com.island.ohara.client.configurator.v0.ClusterAccess.Query
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.setting.SettingDef.{Reference, Type}
import com.island.ohara.common.setting.{ObjectKey, SettingDef}
import com.island.ohara.common.util.{CommonUtils, VersionUtils}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
object BrokerApi {

  val BROKER_PREFIX_PATH: String = "brokers"

  val BROKER_SERVICE_NAME: String = "bk"

  /**
    * the default docker image used to run containers of broker cluster
    */
  val IMAGE_NAME_DEFAULT: String = s"oharastream/broker:${VersionUtils.VERSION}"

  //------------------------ The key name list in settings field ---------------------------------/
  val BROKER_HOME_FOLDER: String = "/home/ohara/default"
  private[this] val _DEFINITIONS = mutable.Map[String, SettingDef]()
  private[this] def createDef(f: SettingDef.Builder => SettingDef): SettingDef = {
    val settingDef = f(SettingDef.builder().orderInGroup(_DEFINITIONS.size).group("core"))
    assert(!_DEFINITIONS.contains(settingDef.key()), "duplicate key is illegal")
    _DEFINITIONS += (settingDef.key() -> settingDef)
    settingDef
  }

  val GROUP_DEFINITION: SettingDef = createDef(
    _.key(GROUP_KEY).documentation("group of this worker cluster").optional(GROUP_DEFAULT).build())
  val NAME_DEFINITION: SettingDef = createDef(
    _.key(NAME_KEY).documentation("name of this worker cluster").optional().build())
  val NODE_NAMES_DEFINITION: SettingDef =
    createDef(_.key(NODE_NAMES_KEY).documentation("the nodes hosting this cluster").valueType(Type.ARRAY).build())
  val TAGS_DEFINITION: SettingDef =
    createDef(_.key(TAGS_KEY).documentation("the tags to this cluster").valueType(Type.TAGS).optional().build())
  private[this] val ZOOKEEPER_CLUSTER_KEY_KEY: String = "zookeeperClusterKey"
  val ZOOKEEPER_CLUSTER_KEY_DEFINITION: SettingDef = createDef(
    _.key(ZOOKEEPER_CLUSTER_KEY_KEY)
      .documentation("the zookeeper cluster used to manage broker nodes")
      .valueType(Type.OBJECT_KEYS)
      .reference(Reference.ZOOKEEPER_CLUSTER)
      .build())
  val CLIENT_PORT_DEFINITION: SettingDef = createDef(
    _.key(CLIENT_PORT_KEY)
      .documentation("the port exposed to client to connect to broker")
      .valueType(Type.PORT)
      .optional()
      .build())
  private[this] val JMX_PORT_KEY: String = "jmxPort"
  val JMX_PORT_DEFINITION: SettingDef = createDef(
    _.key(JMX_PORT_KEY)
      .documentation("the port exposed to client to connect to broker jmx")
      .valueType(Type.PORT)
      .optional()
      .build())
  private[this] val LOG_DIRS_KEY: String = "log.dirs"
  private[this] val LOG_DIRS_DEFAULT = s"$BROKER_HOME_FOLDER/data"
  val LOG_DIRS_DEFINITION: SettingDef = createDef(
    _.key(LOG_DIRS_KEY).documentation("the folder used to store data of broker").optional(LOG_DIRS_DEFAULT).build())
  private[this] val NUMBER_OF_PARTITIONS_KEY: String = "num.partitions"
  private[this] val NUMBER_OF_PARTITIONS_DEFAULT = 1
  val NUMBER_OF_PARTITIONS_DEFINITION: SettingDef = createDef(
    _.key(NUMBER_OF_PARTITIONS_KEY)
      .documentation("the number of partitions for all topics by default")
      .valueType(Type.INT)
      .optional(NUMBER_OF_PARTITIONS_DEFAULT)
      .build())
  private[this] val NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY: String = "offsets.topic.replication.factor"
  private[this] val NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFAULT = 1
  val NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFINITION: SettingDef = createDef(
    _.key(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY)
      .documentation("the number of replications for internal offset topic")
      .valueType(Type.SHORT)
      .optional(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFAULT)
      .build())
  private[this] val NUMBER_OF_NETWORK_THREADS_KEY: String = "num.network.threads"
  private[this] val NUMBER_OF_NETWORK_THREADS_DEFAULT = 1
  val NUMBER_OF_NETWORK_THREADS_DEFINITION: SettingDef = createDef(
    _.key(NUMBER_OF_NETWORK_THREADS_KEY)
      .documentation("the number of threads used to accept network requests")
      .valueType(Type.INT)
      .optional(NUMBER_OF_NETWORK_THREADS_DEFAULT)
      .build())
  private[this] val NUMBER_OF_IO_THREADS_KEY: String = "num.io.threads"
  private[this] val NUMBER_OF_IO_THREADS_DEFAULT = 1
  val NUMBER_OF_IO_THREADS_DEFINITION: SettingDef = createDef(
    _.key(NUMBER_OF_IO_THREADS_KEY)
      .documentation("the number of threads used to process network requests")
      .valueType(Type.INT)
      .optional(NUMBER_OF_IO_THREADS_DEFAULT)
      .build())

  /**
    * all public configs
    */
  def DEFINITIONS: Seq[SettingDef] = _DEFINITIONS.values.toSeq

  val TOPIC_DEFINITION: TopicDefinition = TopicDefinition(TopicApi.DEFINITIONS)

  final class Creation(val settings: Map[String, JsValue]) extends ClusterCreation {

    /**
      * reuse the parser from Update.
      * @param settings settings
      * @return update
      */
    private[this] implicit def update(settings: Map[String, JsValue]): Updating = new Updating(noJsNull(settings))
    // the name and group fields are used to identify zookeeper cluster object
    // we should give them default value in JsonRefiner
    override def name: String = settings.name.get
    override def group: String = settings.group.get
    // helper method to get the key
    private[ohara] def key: ObjectKey = ObjectKey.of(group, name)

    override def imageName: String = settings.imageName.get
    override def nodeNames: Set[String] = settings.nodeNames.get
    override def ports: Set[Int] = Set(clientPort, jmxPort)
    override def tags: Map[String, JsValue] = settings.tags.get

    def clientPort: Int = settings.clientPort.get
    def jmxPort: Int = settings.jmxPort.get
    def zookeeperClusterKey: ObjectKey = settings.zookeeperClusterKey.get
    def logDirs: String = settings.logDirs.get
    def numberOfPartitions: Int = settings.numberOfPartitions.get
    def numberOfReplications4OffsetsTopic: Int =
      settings.numberOfReplications4OffsetsTopic.get
    def numberOfNetworkThreads: Int = settings.numberOfNetworkThreads.get
    def numberOfIoThreads: Int = settings.numberOfIoThreads.get
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val BROKER_CREATION_JSON_FORMAT: OharaJsonFormat[Creation] =
    basicRulesOfCreation[Creation](IMAGE_NAME_DEFAULT)
      .format(new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(noJsNull(obj.settings))
        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      })
      .nullToRandomPort(CLIENT_PORT_KEY)
      .requireBindPort(CLIENT_PORT_KEY)
      .nullToRandomPort(JMX_PORT_KEY)
      .requireBindPort(JMX_PORT_KEY)
      .requireKey(ZOOKEEPER_CLUSTER_KEY_KEY)
      .nullToString(LOG_DIRS_KEY, LOG_DIRS_DEFAULT)
      .nullToInt(NUMBER_OF_PARTITIONS_KEY, NUMBER_OF_PARTITIONS_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_PARTITIONS_KEY)
      .nullToInt(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY, NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY)
      .nullToInt(NUMBER_OF_NETWORK_THREADS_KEY, NUMBER_OF_NETWORK_THREADS_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_NETWORK_THREADS_KEY)
      .nullToInt(NUMBER_OF_IO_THREADS_KEY, NUMBER_OF_IO_THREADS_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_IO_THREADS_KEY)
      .refine

  final class Updating(val settings: Map[String, JsValue]) extends ClusterUpdating {
    // We use the update parser to get the name and group
    private[BrokerApi] def name: Option[String] = noJsNull(settings).get(NAME_KEY).map(_.convertTo[String])
    private[BrokerApi] def group: Option[String] = noJsNull(settings).get(GROUP_KEY).map(_.convertTo[String])
    override def imageName: Option[String] = noJsNull(settings).get(IMAGE_NAME_KEY).map(_.convertTo[String])
    override def nodeNames: Option[Set[String]] =
      noJsNull(settings).get(NODE_NAMES_KEY).map(_.convertTo[Seq[String]].toSet)
    override def tags: Option[Map[String, JsValue]] = noJsNull(settings).get(TAGS_KEY).map {
      case s: JsObject => s.fields
      case other: JsValue =>
        throw new IllegalArgumentException(s"the type of tags should be JsObject, actual type is ${other.getClass}")
    }

    def clientPort: Option[Int] = noJsNull(settings).get(CLIENT_PORT_KEY).map(_.convertTo[Int])
    def jmxPort: Option[Int] = noJsNull(settings).get(JMX_PORT_KEY).map(_.convertTo[Int])

    def zookeeperClusterKey: Option[ObjectKey] =
      noJsNull(settings).get(ZOOKEEPER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])

    def logDirs: Option[String] =
      noJsNull(settings).get(LOG_DIRS_KEY).map(_.convertTo[String])

    def numberOfPartitions: Option[Int] =
      noJsNull(settings).get(NUMBER_OF_PARTITIONS_KEY).map(_.convertTo[Int])

    def numberOfReplications4OffsetsTopic: Option[Int] =
      noJsNull(settings).get(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY).map(_.convertTo[Int])

    def numberOfNetworkThreads: Option[Int] =
      noJsNull(settings).get(NUMBER_OF_NETWORK_THREADS_KEY).map(_.convertTo[Int])

    def numberOfIoThreads: Option[Int] =
      noJsNull(settings).get(NUMBER_OF_IO_THREADS_KEY).map(_.convertTo[Int])
  }

  implicit val BROKER_UPDATING_JSON_FORMAT: OharaJsonFormat[Updating] =
    basicRulesOfUpdating[Updating]
      .format(new RootJsonFormat[Updating] {
        override def write(obj: Updating): JsValue = JsObject(noJsNull(obj.settings))
        override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
      })
      .requireBindPort(CLIENT_PORT_KEY)
      .requireBindPort(JMX_PORT_KEY)
      .refine

  final case class TopicDefinition(settingDefinitions: Seq[SettingDef])

  implicit val TOPIC_DEFINITION_JSON_FORMAT: OharaJsonFormat[TopicDefinition] =
    JsonRefiner[TopicDefinition].format(jsonFormat1(TopicDefinition)).rejectEmptyString().refine

  class BrokerClusterStatus(val group: String,
                            val name: String,
                            val topicDefinition: TopicDefinition,
                            val aliveNodes: Set[String],
                            val state: Option[String],
                            val error: Option[String])
      extends ClusterStatus

  final case class BrokerClusterInfo private[BrokerApi] (settings: Map[String, JsValue],
                                                         aliveNodes: Set[String],
                                                         lastModified: Long,
                                                         state: Option[String],
                                                         error: Option[String],
                                                         topicDefinition: TopicDefinition)
      extends ClusterInfo {

    /**
      * update the runtime information for this cluster info
      * @param status runtime information
      * @return a updated cluster info
      */
    def update(status: BrokerClusterStatus): BrokerClusterInfo = copy(
      topicDefinition = status.topicDefinition,
      aliveNodes = status.aliveNodes,
      state = status.state,
      error = status.error,
      lastModified = CommonUtils.current()
    )

    /**
      * reuse the parser from Creation.
      * @param settings settings
      * @return creation
      */
    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(noJsNull(settings))
    override def name: String = settings.name
    override def group: String = settings.group
    override def kind: String = BROKER_SERVICE_NAME
    override def ports: Set[Int] = Set(clientPort, jmxPort)
    override def tags: Map[String, JsValue] = settings.tags
    def nodeNames: Set[String] = settings.nodeNames

    /**
      * the node names is not equal to "running" nodes. The connection props may reference to invalid nodes and the error
      * should be handled by the client code.
      * @return a string host_0:port,host_1:port
      */
    def connectionProps: String = if (nodeNames.isEmpty) throw new IllegalArgumentException("there is no nodes!!!")
    else nodeNames.map(n => s"$n:$clientPort").mkString(",")

    override def imageName: String = settings.imageName
    def clientPort: Int = settings.clientPort
    def jmxPort: Int = settings.jmxPort
    def zookeeperClusterKey: ObjectKey = settings.zookeeperClusterKey
    def logDirs: String = settings.logDirs
    def numberOfPartitions: Int = settings.numberOfPartitions
    def numberOfReplications4OffsetsTopic: Int = settings.numberOfReplications4OffsetsTopic
    def numberOfNetworkThreads: Int = settings.numberOfNetworkThreads
    def numberOfIoThreads: Int = settings.numberOfIoThreads
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val BROKER_CLUSTER_INFO_JSON_FORMAT: RootJsonFormat[BrokerClusterInfo] = jsonFormat6(
    BrokerClusterInfo)

  /**
    * used to generate the payload and url for POST/PUT request.
    * this request is extended by collie also so it is public than sealed.
    */
  trait Request extends ClusterRequest {
    @Optional("Ignoring zookeeper cluster key enable server to match a zk for you")
    def zookeeperClusterKey(zookeeperClusterKey: ObjectKey): Request.this.type =
      setting(ZOOKEEPER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(Objects.requireNonNull(zookeeperClusterKey)))
    @Optional("the default port is random")
    def clientPort(clientPort: Int): Request.this.type =
      setting(CLIENT_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(clientPort)))
    @Optional("the default port is random")
    def jmxPort(jmxPort: Int): Request.this.type =
      setting(JMX_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(jmxPort)))
    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): Request.this.type = setting(TAGS_KEY, JsObject(tags))

    /**
      * broker information creation.
      *  Here we open the access for reusing the creation to other module
      *
      * @return the payload of creation
      */
    final def creation: Creation =
      // auto-complete the creation via our refiner
      BROKER_CREATION_JSON_FORMAT.read(BROKER_CREATION_JSON_FORMAT.write(new Creation(noJsNull(settings.toMap))))

    /**
      * for testing only
      * @return the payload of update
      */
    private[v0] final def updating: Updating =
      // auto-complete the update via our refiner
      BROKER_UPDATING_JSON_FORMAT.read(BROKER_UPDATING_JSON_FORMAT.write(new Updating(noJsNull(settings.toMap))))
  }

  /**
    * similar to Request but it has execution methods.
    */
  sealed trait ExecutableRequest extends Request {
    def create()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo]
    def update()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo]
  }

  final class Access private[BrokerApi]
      extends ClusterAccess[Creation, Updating, BrokerClusterInfo](BROKER_PREFIX_PATH) {

    override def query: Query[BrokerClusterInfo] = new Query[BrokerClusterInfo] {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext): Future[Seq[BrokerClusterInfo]] = list(request)
    }

    def request: ExecutableRequest = new ExecutableRequest {

      override def create()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo] = post(creation)

      override def update()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo] =
        put(ObjectKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }
  }

  def access: Access = new Access
}
