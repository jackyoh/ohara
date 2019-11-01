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
import java.util.concurrent.atomic.AtomicInteger

import com.island.ohara.client.Enum
import com.island.ohara.client.configurator.{Data, QueryRequest}
import com.island.ohara.common.annotations.{Optional, VisibleForTesting}
import com.island.ohara.common.setting.SettingDef.{Reference, Type}
import com.island.ohara.common.setting.{ObjectKey, SettingDef, TopicKey}
import com.island.ohara.common.util.CommonUtils
import org.apache.kafka.common.config.TopicConfig
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object TopicApi {
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
  private[this] val EXTRA_GROUP = "extra"

  private[this] val CORE_COUNTER = new AtomicInteger(0)
  private[this] val EXTRA_COUNTER = new AtomicInteger(0)
  private[this] def coreDefinitionBuilder =
    SettingDef.builder().orderInGroup(CORE_COUNTER.incrementAndGet()).group("core")
  private[this] def extraDefinitionBuilder =
    SettingDef.builder().orderInGroup(EXTRA_COUNTER.incrementAndGet()).group(EXTRA_GROUP)
  val GROUP_DEFINITION: SettingDef =
    coreDefinitionBuilder.key(GROUP_KEY).documentation("group of this worker cluster").optional(GROUP_DEFAULT).build()
  val NAME_DEFINITION: SettingDef =
    coreDefinitionBuilder.key(NAME_KEY).documentation("name of this worker cluster").optional().build()
  val TAGS_DEFINITION: SettingDef =
    coreDefinitionBuilder
      .key(TAGS_KEY)
      .documentation("the tags to this cluster")
      .optional()
      .valueType(Type.TAGS)
      .build()
  private[this] val BROKER_CLUSTER_KEY_KEY = "brokerClusterKey"
  val BROKER_CLUSTER_KEY_DEFINITION: SettingDef = coreDefinitionBuilder
    .key(BROKER_CLUSTER_KEY_KEY)
    .documentation("broker cluster used to store data for this worker cluster")
    .valueType(Type.OBJECT_KEY)
    .reference(Reference.BROKER_CLUSTER)
    .build()
  private[this] val NUMBER_OF_PARTITIONS_KEY = "numberOfPartitions"
  private[this] val NUMBER_OF_PARTITIONS_DEFAULT: Int = 1
  val NUMBER_OF_PARTITIONS_DEFINITION: SettingDef = coreDefinitionBuilder
    .key(NUMBER_OF_PARTITIONS_KEY)
    .documentation("the number of partitions")
    .valueType(Type.INT)
    .optional(NUMBER_OF_PARTITIONS_DEFAULT)
    .build()
  private[this] val NUMBER_OF_REPLICATIONS_KEY = "numberOfReplications"
  private[this] val NUMBER_OF_REPLICATIONS_DEFAULT: Short = 1
  val NUMBER_OF_REPLICATIONS_DEFINITION: SettingDef = coreDefinitionBuilder
    .key(NUMBER_OF_REPLICATIONS_KEY)
    .documentation("the number of replications")
    .valueType(Type.SHORT)
    .optional(NUMBER_OF_REPLICATIONS_DEFAULT)
    .build()

  private[this] val SEGMENT_BYTES_KEY = TopicConfig.SEGMENT_BYTES_CONFIG
  private[this] val SEGMENT_BYTES_DEFAULT: Long = 1 * 1024 * 1024 * 1024L
  val SEGMENT_BYTES_DEFINITION: SettingDef = extraDefinitionBuilder
    .key(SEGMENT_BYTES_KEY)
    .documentation(TopicConfig.SEGMENT_BYTES_DOC)
    // ONE WEEK
    .valueType(Type.LONG)
    .optional(SEGMENT_BYTES_DEFAULT)
    .build()

  private[this] val SEGMENT_MS_KEY = TopicConfig.SEGMENT_MS_CONFIG
  private[this] val SEGMENT_MS_DEFAULT: Long = 7 * 24 * 60 * 60 * 1000L
  val SEGMENT_MS_DEFINITION: SettingDef = extraDefinitionBuilder
    .key(SEGMENT_MS_KEY)
    .documentation(TopicConfig.SEGMENT_MS_DOC)
    .valueType(Type.LONG)
    // ONE WEEK
    .optional(SEGMENT_MS_DEFAULT)
    .build()

  /**
    * list the custom configs of topic. It is useful to developers who long for controlling the topic totally.
    */
  val DEFINITIONS: Seq[SettingDef] = Seq(
    GROUP_DEFINITION,
    NAME_DEFINITION,
    TAGS_DEFINITION,
    BROKER_CLUSTER_KEY_DEFINITION,
    NUMBER_OF_PARTITIONS_DEFINITION,
    NUMBER_OF_REPLICATIONS_DEFINITION,
    SEGMENT_BYTES_DEFINITION,
    SEGMENT_MS_DEFINITION
  )

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

  implicit val TOPIC_UPDATING_FORMAT: RootJsonFormat[Updating] =
    JsonRefiner[Updating]
      .format(new RootJsonFormat[Updating] {
        override def read(json: JsValue): Updating = new Updating(noJsNull(json.asJsObject.fields))
        override def write(obj: Updating): JsValue = JsObject(obj.settings)
      })
      .rejectEmptyString()
      .requirePositiveNumber(NUMBER_OF_PARTITIONS_KEY)
      .requirePositiveNumber(NUMBER_OF_REPLICATIONS_KEY)
      .refine

  final class Creation private[TopicApi] (val settings: Map[String, JsValue])
      extends com.island.ohara.client.configurator.v0.BasicCreation {

    private[this] implicit def update(settings: Map[String, JsValue]): Updating = new Updating(noJsNull(settings))

    def key: TopicKey = TopicKey.of(group, name)

    def brokerClusterKey: ObjectKey = settings.brokerClusterKey.get

    def numberOfPartitions: Int = settings.numberOfPartitions.get
    def numberOfReplications: Short = settings.numberOfReplications.get

    override def group: String = settings.group.get

    override def name: String = settings.name.get

    override def tags: Map[String, JsValue] = settings.tags.get
  }

  implicit val TOPIC_CREATION_FORMAT: OharaJsonFormat[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    basicRulesOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def read(json: JsValue): Creation = new Creation(noJsNull(json.asJsObject.fields))
        override def write(obj: Creation): JsValue = JsObject(obj.settings)
      })
      .requireKey(BROKER_CLUSTER_KEY_KEY)
      .nullToInt(NUMBER_OF_PARTITIONS_KEY, NUMBER_OF_PARTITIONS_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_PARTITIONS_KEY)
      .nullToInt(NUMBER_OF_REPLICATIONS_KEY, NUMBER_OF_REPLICATIONS_DEFAULT)
      .requirePositiveNumber(NUMBER_OF_REPLICATIONS_KEY)
      .rejectEmptyString()
      .nullToEmptyObject(TAGS_KEY)
      .requirePositiveNumber(SEGMENT_BYTES_KEY)
      .nullToLong(SEGMENT_BYTES_KEY, SEGMENT_BYTES_DEFAULT)
      .requirePositiveNumber(SEGMENT_MS_KEY)
      .nullToLong(SEGMENT_MS_KEY, SEGMENT_MS_DEFAULT)
      .refine

  import MetricsApi._

  abstract sealed class TopicState(val name: String) extends Serializable
  object TopicState extends Enum[TopicState] {
    case object NONE extends TopicState("NONE")
    case object RUNNING extends TopicState("RUNNING")
  }

  implicit val TOPIC_STATE_FORMAT: RootJsonFormat[TopicState] = new RootJsonFormat[TopicState] {
    override def read(json: JsValue): TopicState = TopicState.forName(json.convertTo[String].toUpperCase)
    override def write(obj: TopicState): JsValue = JsString(obj.name)
  }

  final case class PartitionInfo(index: Int,
                                 leaderNode: String,
                                 replicaNodes: Set[String],
                                 inSyncReplicaNodes: Set[String],
                                 beginningOffset: Long,
                                 endOffset: Long)

  implicit val TOPIC_PARTITION_FORMAT: RootJsonFormat[PartitionInfo] = jsonFormat6(PartitionInfo)

  case class TopicInfo(settings: Map[String, JsValue],
                       partitionInfos: Seq[PartitionInfo],
                       metrics: Metrics,
                       state: Option[TopicState],
                       lastModified: Long)
      extends Data {

    override protected def matched(key: String, value: String): Boolean = key match {
      case "state" => matchOptionString(state.map(_.name), value)
      case _       => matchSetting(settings, key, value)
    }

    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(settings)

    override def key: TopicKey = TopicKey.of(group, name)
    override def kind: String = "topic"

    /**
      * kafka topic does not support to group topic so we salt the group with name.
      * @return topic name for kafka
      */
    def topicNameOnKafka: String = key.topicNameOnKafka

    override def group: String = settings.group

    override def name: String = settings.name

    override def tags: Map[String, JsValue] = settings.tags

    def brokerClusterKey: ObjectKey = settings.brokerClusterKey
    def numberOfPartitions: Int = settings.numberOfPartitions

    def numberOfReplications: Short = settings.numberOfReplications

    /**
      * @return the custom configs. the core configs are not included
      */
    def configs: Map[String, String] = noJsNull(settings)
      .filter {
        case (key, value) =>
          DEFINITIONS.filter(_.group() == EXTRA_GROUP).exists(_.key() == key) &&
            value.isInstanceOf[JsString]
      }
      .map {
        case (key, value) => key -> value.convertTo[String]
      }
  }

  implicit val TOPIC_INFO_FORMAT: RootJsonFormat[TopicInfo] = jsonFormat5(TopicInfo)

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

    @Optional("default configs is empty array")
    def configs(configs: Map[String, String]): Request = {
      configs.foreach {
        case (key, value) => setting(key, JsString(value))
      }
      this
    }

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
      TOPIC_CREATION_FORMAT.read(TOPIC_CREATION_FORMAT.write(new Creation(noJsNull(settings.toMap))))

    private[v0] final def updating: Updating =
      // rewrite the update via format since the format will auto-complete the creation
      // this make the update is consistent to creation sent to server
      TOPIC_UPDATING_FORMAT.read(TOPIC_UPDATING_FORMAT.write(new Updating(noJsNull(settings.toMap))))

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
    def state(value: TopicState): Query = set("state", value.name)

    def brokerClusterKey(key: ObjectKey): Query = setting(BROKER_CLUSTER_KEY_KEY, ObjectKey.toJsonString(key).parseJson)

    def setting(key: String, value: JsValue): Query = set(key, value match {
      case JsString(s) => s
      case _           => value.toString
    })

    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[v0]
      extends com.island.ohara.client.configurator.v0.Access[Creation, Updating, TopicInfo](TOPICS_PREFIX_PATH) {
    def start(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, START_COMMAND)
    def stop(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, STOP_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext): Future[Seq[TopicInfo]] = list(request)
    }

    def request: Request = new Request {

      override def create()(implicit executionContext: ExecutionContext): Future[TopicInfo] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[TopicInfo] =
        put(TopicKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }

  }

  def access: Access = new Access
}
