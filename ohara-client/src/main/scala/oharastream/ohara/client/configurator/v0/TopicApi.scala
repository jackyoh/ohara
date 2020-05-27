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

import oharastream.ohara.client.Enum
import oharastream.ohara.client.configurator.{Data, QueryRequest}
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.setting.SettingDef.{Reference, Type}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka
import oharastream.ohara.kafka.{PartitionInfo, PartitionNode}
import org.apache.kafka.common.config.TopicConfig
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object TopicApi {
  val KIND: String = "topic"

  @VisibleForTesting
  private[ohara] val TOPICS_PREFIX_PATH: String = "topics"

  /**
    * the config with this group is mapped to kafka's custom config. Kafka divide configs into two parts.
    * 1) required configs (number of partitions and number of replications)
    * 2) custom configs (those config must be able to convert to string)
    *
    * Furthermore, kafka forbids us to put required configs to custom configs. Hence, we have to mark the custom config
    * in order to filter the custom from settings (see Creation).
    */
  private[this] val EXTRA_GROUP       = "extra"
  private[this] val CORE_DEFINITIONS  = mutable.Map[String, SettingDef]()
  private[this] val EXTRA_DEFINITIONS = mutable.Map[String, SettingDef]()
  private[this] def createExtraDef(f: SettingDef.Builder => SettingDef): SettingDef = {
    val settingDef = f(SettingDef.builder().orderInGroup(EXTRA_DEFINITIONS.size).group(EXTRA_GROUP))
    assert(!CORE_DEFINITIONS.contains(settingDef.key()), s"duplicate key:${settingDef.key()} is illegal")
    assert(!EXTRA_DEFINITIONS.contains(settingDef.key()), s"duplicate key:${settingDef.key()} is illegal")
    EXTRA_DEFINITIONS += (settingDef.key() -> settingDef)
    settingDef
  }
  private[this] def createCoreDef(f: SettingDef.Builder => SettingDef): SettingDef = {
    val settingDef = f(SettingDef.builder().orderInGroup(CORE_DEFINITIONS.size).group("core"))
    assert(!CORE_DEFINITIONS.contains(settingDef.key()), s"duplicate key:${settingDef.key()} is illegal")
    assert(!EXTRA_DEFINITIONS.contains(settingDef.key()), s"duplicate key:${settingDef.key()} is illegal")
    CORE_DEFINITIONS += (settingDef.key() -> settingDef)
    settingDef
  }
  val GROUP_DEFINITION: SettingDef =
    createCoreDef(_.key(GROUP_KEY).documentation("group of this worker cluster").optional(GROUP_DEFAULT).build())
  val NAME_DEFINITION: SettingDef =
    createCoreDef(_.key(NAME_KEY).documentation("name of this worker cluster").stringWithRandomDefault().build())
  val TAGS_DEFINITION: SettingDef =
    createCoreDef(_.key(TAGS_KEY).documentation("the tags to this cluster").optional(Type.TAGS).build())
  private[this] val BROKER_CLUSTER_KEY_KEY = "brokerClusterKey"
  val BROKER_CLUSTER_KEY_DEFINITION: SettingDef = createCoreDef(
    _.key(BROKER_CLUSTER_KEY_KEY)
      .documentation("broker cluster used to store data for this worker cluster")
      .required(Type.OBJECT_KEY)
      .reference(Reference.BROKER_CLUSTER)
      .build()
  )
  private[this] val NUMBER_OF_PARTITIONS_KEY          = "numberOfPartitions"
  private[this] val NUMBER_OF_PARTITIONS_DEFAULT: Int = 1
  val NUMBER_OF_PARTITIONS_DEFINITION: SettingDef = createCoreDef(
    _.key(NUMBER_OF_PARTITIONS_KEY)
      .documentation("the number of partitions")
      .positiveNumber(NUMBER_OF_PARTITIONS_DEFAULT)
      .build()
  )
  private[this] val NUMBER_OF_REPLICATIONS_KEY            = "numberOfReplications"
  private[this] val NUMBER_OF_REPLICATIONS_DEFAULT: Short = 1
  val NUMBER_OF_REPLICATIONS_DEFINITION: SettingDef = createCoreDef(
    _.key(NUMBER_OF_REPLICATIONS_KEY)
      .documentation("the number of replications")
      .positiveNumber(NUMBER_OF_REPLICATIONS_DEFAULT)
      .build()
  )

  private[this] val SEGMENT_BYTES_KEY           = TopicConfig.SEGMENT_BYTES_CONFIG
  private[this] val SEGMENT_BYTES_DEFAULT: Long = 1 * 1024 * 1024 * 1024L
  val SEGMENT_BYTES_DEFINITION: SettingDef = createExtraDef(
    _.key(SEGMENT_BYTES_KEY)
      .documentation(TopicConfig.SEGMENT_BYTES_DOC)
      .positiveNumber(SEGMENT_BYTES_DEFAULT)
      .build()
  )

  private[this] val SEGMENT_MS_KEY = TopicConfig.SEGMENT_MS_CONFIG
  // ONE WEEK
  private[this] val SEGMENT_MS_DEFAULT: Long = 7 * 24 * 60 * 60 * 1000L
  val SEGMENT_MS_DEFINITION: SettingDef = createExtraDef(
    _.key(SEGMENT_MS_KEY)
      .documentation(TopicConfig.SEGMENT_MS_DOC)
      .positiveNumber(SEGMENT_MS_DEFAULT)
      .build()
  )

  /**
    * list the custom configs of topic. It is useful to developers who long for controlling the topic totally.
    */
  def DEFINITIONS: Seq[SettingDef] = (CORE_DEFINITIONS.values ++ EXTRA_DEFINITIONS.values).toSeq

  final class Updating private[TopicApi] (val settings: Map[String, JsValue]) {
    def brokerClusterKey: Option[ObjectKey] = noJsNull(settings).get(BROKER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])
    private[TopicApi] def numberOfPartitions: Option[Int] =
      noJsNull(settings).get(NUMBER_OF_PARTITIONS_KEY).map(_.convertTo[Int])

    private[TopicApi] def numberOfReplications: Option[Short] =
      noJsNull(settings).get(NUMBER_OF_REPLICATIONS_KEY).map(_.convertTo[Short])

    private[TopicApi] def group: Option[String] = noJsNull(settings).get(GROUP_KEY).map(_.convertTo[String])

    private[TopicApi] def name: Option[String] = noJsNull(settings).get(NAME_KEY).map(_.convertTo[String])

    private[TopicApi] def tags: Option[Map[String, JsValue]] = noJsNull(settings).get(TAGS_KEY).map(_.asJsObject.fields)
  }

  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] =
    JsonRefinerBuilder[Updating]
      .format(new RootJsonFormat[Updating] {
        override def read(json: JsValue): Updating = new Updating(noJsNull(json.asJsObject.fields))
        override def write(obj: Updating): JsValue = JsObject(obj.settings)
      })
      .build

  final class Creation private[TopicApi] (val settings: Map[String, JsValue])
      extends oharastream.ohara.client.configurator.v0.BasicCreation {
    private[this] implicit def update(settings: Map[String, JsValue]): Updating = new Updating(noJsNull(settings))

    override def key: TopicKey = TopicKey.of(group, name)

    def brokerClusterKey: ObjectKey = settings.brokerClusterKey.get

    def numberOfPartitions: Int     = settings.numberOfPartitions.get
    def numberOfReplications: Short = settings.numberOfReplications.get

    override def group: String = settings.group.get

    override def name: String = settings.name.get

    override def tags: Map[String, JsValue] = settings.tags.get
  }

  implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    limitsOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def read(json: JsValue): Creation = new Creation(noJsNull(json.asJsObject.fields))
        override def write(obj: Creation): JsValue = JsObject(obj.settings)
      })
      // TODO: topic definitions may be changed by different Broker images so this check is dangerous
      .definitions(DEFINITIONS)
      .build

  import MetricsApi._

  abstract sealed class TopicState(val name: String) extends Serializable
  object TopicState extends Enum[TopicState] {
    case object RUNNING extends TopicState("RUNNING")
  }

  implicit val TOPIC_STATE_FORMAT: RootJsonFormat[TopicState] = new RootJsonFormat[TopicState] {
    override def read(json: JsValue): TopicState = TopicState.forName(json.convertTo[String].toUpperCase)
    override def write(obj: TopicState): JsValue = JsString(obj.name)
  }

  implicit val PARTITION_NODE_FORMAT: RootJsonFormat[PartitionNode] = new RootJsonFormat[PartitionNode] {
    private[this] case class _Node(id: Int, host: String, port: Int)
    private[this] val format = jsonFormat3(_Node)
    override def read(json: JsValue): PartitionNode = {
      val node = format.read(json)
      new PartitionNode(node.id, node.host, node.port)
    }
    override def write(obj: PartitionNode): JsValue =
      format.write(
        _Node(
          id = obj.id,
          host = obj.host,
          port = obj.port
        )
      )
  }

  implicit val PARTITION_INFO_FORMAT: RootJsonFormat[PartitionInfo] = new RootJsonFormat[PartitionInfo] {
    private[this] case class _PartitionInfo(
      id: Int,
      leader: PartitionNode,
      replicas: Seq[PartitionNode],
      inSyncReplicas: Seq[PartitionNode],
      beginningOffset: Long,
      endOffset: Long
    )
    private[this] val format = jsonFormat6(_PartitionInfo)
    override def read(json: JsValue): kafka.PartitionInfo = {
      val partitionInfo = format.read(json)
      new kafka.PartitionInfo(
        partitionInfo.id,
        partitionInfo.leader,
        partitionInfo.replicas.asJava,
        partitionInfo.inSyncReplicas.asJava,
        partitionInfo.beginningOffset,
        partitionInfo.endOffset
      )
    }
    override def write(obj: kafka.PartitionInfo): JsValue =
      format.write(
        _PartitionInfo(
          id = obj.id,
          leader = obj.leader,
          replicas = obj.replicas.asScala.toSeq,
          inSyncReplicas = obj.inSyncReplicas.asScala.toSeq,
          beginningOffset = obj.beginningOffset,
          endOffset = obj.endOffset
        )
      )
  }

  case class TopicInfo(
    settings: Map[String, JsValue],
    partitionInfos: Seq[PartitionInfo],
    nodeMetrics: Map[String, Metrics],
    state: Option[TopicState],
    lastModified: Long
  ) extends Data
      with Metricsable {
    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(settings)

    override def key: TopicKey = TopicKey.of(group, name)
    override def kind: String  = KIND

    /**
      * kafka topic does not support to group topic so we salt the group with name.
      * @return topic name for kafka
      */
    def topicNameOnKafka: String = key.topicNameOnKafka

    override def group: String = settings.group

    override def name: String = settings.name

    override def tags: Map[String, JsValue] = settings.tags

    def brokerClusterKey: ObjectKey = settings.brokerClusterKey
    def numberOfPartitions: Int     = settings.numberOfPartitions

    def numberOfReplications: Short = settings.numberOfReplications

    /**
      * @return the custom configs. the core configs are not included
      */
    def configs: Map[String, JsValue] = noJsNull(settings).filter {
      case (key, _) =>
        DEFINITIONS.filter(_.group() == EXTRA_GROUP).exists(_.key() == key)
    }

    override protected def raw: Map[String, JsValue] = TOPIC_INFO_FORMAT.write(this).asJsObject.fields
  }

  implicit val TOPIC_INFO_FORMAT: JsonRefiner[TopicInfo] = JsonRefinerBuilder[TopicInfo]
    .format(new RootJsonFormat[TopicInfo] {
      private[this] val format                    = jsonFormat5(TopicInfo)
      override def read(json: JsValue): TopicInfo = format.read(extractSetting(json.asJsObject))
      override def write(obj: TopicInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
    })
    .build

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  sealed trait Request {
    private[this] val settings: mutable.Map[String, JsValue] = mutable.Map()

    /**
      * set the group and name via key
      * @param topicKey topic key
      * @return this request
      */
    def key(topicKey: TopicKey): Request = {
      group(topicKey.group())
      name(topicKey.name())
    }

    @Optional("default group is \"default\"")
    def group(group: String): Request = setting(GROUP_KEY, JsString(CommonUtils.requireNonEmpty(group)))

    @Optional("default name is a random string. But it is required in updating")
    def name(name: String): Request =
      setting(NAME_KEY, JsString(CommonUtils.requireNonEmpty(name)))

    @Optional("server will match a broker cluster for you if the bk name is ignored")
    def brokerClusterKey(brokerClusterKey: ObjectKey): Request =
      setting(BROKER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(brokerClusterKey))

    @Optional("default value is DEFAULT_NUMBER_OF_PARTITIONS")
    def numberOfPartitions(numberOfPartitions: Int): Request =
      setting(NUMBER_OF_PARTITIONS_KEY, JsNumber(CommonUtils.requirePositiveInt(numberOfPartitions)))

    @Optional("default value is DEFAULT_NUMBER_OF_REPLICATIONS")
    def numberOfReplications(numberOfReplications: Short): Request =
      setting(NUMBER_OF_REPLICATIONS_KEY, JsNumber(CommonUtils.requirePositiveShort(numberOfReplications)))

    @Optional("default value is empty array")
    def tags(tags: Map[String, JsValue]): Request =
      setting(TAGS_KEY, JsObject(Objects.requireNonNull(tags)))

    def setting(key: String, value: JsValue): Request = settings(Map(key -> value))

    def settings(settings: Map[String, JsValue]): Request = {
      this.settings ++= settings
      this
    }

    /**
      * Creation instance includes many useful parsers for custom settings so we open it to code with a view to reusing
      * those convenient parsers.
      * @return the payload of creation
      */
    final def creation: Creation =
      // rewrite the creation via format since the format will auto-complete the creation
      // this make the creaion is consistent to creation sent to server
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(noJsNull(settings.toMap))))

    private[v0] final def updating: Updating =
      // rewrite the update via format since the format will auto-complete the creation
      // this make the update is consistent to creation sent to server
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(noJsNull(settings.toMap))))

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[TopicInfo]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[TopicInfo]
  }

  sealed trait Query extends BasicQuery[TopicInfo] {
    import spray.json._
    def state(value: TopicState): Query = setting("state", value.name)

    def brokerClusterKey(key: ObjectKey): Query = setting(BROKER_CLUSTER_KEY_KEY, ObjectKey.toJsonString(key).parseJson)

    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[v0]
      extends oharastream.ohara.client.configurator.v0.Access[Creation, Updating, TopicInfo](TOPICS_PREFIX_PATH) {
    def start(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, START_COMMAND)
    def stop(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit]  = put(key, STOP_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[TopicInfo]] = list(request)
    }

    def request: Request = new Request {
      override def create()(implicit executionContext: ExecutionContext): Future[TopicInfo] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[TopicInfo] =
        put(TopicKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }
  }

  def access: Access = new Access
}
