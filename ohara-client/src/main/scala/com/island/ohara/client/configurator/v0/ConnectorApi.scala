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

import com.island.ohara.client.Enum
import com.island.ohara.client.configurator.Data
import com.island.ohara.common.annotations.{Optional, VisibleForTesting}
import com.island.ohara.common.data.Column
import com.island.ohara.common.setting.{ConnectorKey, ObjectKey, PropGroups, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.kafka.connector.json._
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat, _}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object ConnectorApi {

  val CONNECTORS_PREFIX_PATH: String = "connectors"

  /**
    * The default value of group for this API.
    */
  val GROUP_DEFAULT: String = com.island.ohara.client.configurator.v0.GROUP_DEFAULT
  private[this] val WORKER_CLUSTER_NAME_KEY: String = ConnectorDefUtils.WORKER_CLUSTER_NAME_DEFINITION.key()
  private[this] val NUMBER_OF_TASKS_KEY: String = ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key()
  private[this] val TOPIC_KEYS_KEY: String = ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key()
  private[this] val TOPIC_NAMES_KEY: String = ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()
  private[v0] val CONNECTOR_CLASS_KEY: String = ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key()
  @VisibleForTesting
  private[v0] val COLUMNS_KEY: String = ConnectorDefUtils.COLUMNS_DEFINITION.key()
  @VisibleForTesting
  private[v0] val CONNECTOR_KEY_KEY: String = ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key()
  private[this] val GROUP_KEY: String = ConnectorDefUtils.CONNECTOR_GROUP_DEFINITION.key()
  private[this] val NAME_KEY: String = ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key()
  private[this] val DEFAULT_NUMBER_OF_TASKS: Int = 1

  /**
    * The name is a part of "Restful APIs" so "DON'T" change it arbitrarily
    */
  // Make this class to be serializable since it's stored in configurator
  abstract sealed class State(val name: String) extends Serializable
  object State extends Enum[State] {
    case object UNASSIGNED extends State("UNASSIGNED")
    case object RUNNING extends State("RUNNING")
    case object PAUSED extends State("PAUSED")
    case object FAILED extends State("FAILED")
    case object DESTROYED extends State("DESTROYED")
  }

  implicit val CONNECTOR_STATE_FORMAT: RootJsonFormat[State] =
    new RootJsonFormat[State] {
      override def write(obj: State): JsValue = JsString(obj.name)
      override def read(json: JsValue): State =
        State.forName(json.convertTo[String])
    }

  final case class Creation(settings: Map[String, JsValue]) extends CreationRequest {

    private[this] implicit def update(settings: Map[String, JsValue]): Update = Update(settings)

    /**
      * Convert all json value to plain string. It keeps the json format but all stuff are in string.
      */
    def plain: Map[String, String] = noJsNull(settings).map {
      case (k, v) =>
        k -> (v match {
          case JsString(value) => value
          case _               => v.toString()
        })
    }
    def className: String = settings.className.get
    def columns: Seq[Column] = settings.columns.get
    def numberOfTasks: Int = settings.numberOfTasks.get
    def workerClusterName: Option[String] = settings.workerClusterName
    def topicKeys: Set[TopicKey] = settings.topicKeys.get

    override def group: String = settings.group.get

    override def name: String = settings.name.get

    def key: ConnectorKey = ConnectorKey.of(group, name)

    override def tags: Map[String, JsValue] = settings.tags.get
  }

  implicit val CONNECTOR_CREATION_FORMAT: OharaJsonFormat[Creation] = JsonRefiner[Creation]
    .format(new RootJsonFormat[Creation] {
      override def write(obj: Creation): JsValue = JsObject(noJsNull(obj.settings))
      override def read(json: JsValue): Creation = Creation(json.asJsObject.fields)
    })
    // set the default number of tasks
    .nullToInt(NUMBER_OF_TASKS_KEY, DEFAULT_NUMBER_OF_TASKS)
    .rejectEmptyString()
    .nullToString(GROUP_KEY, () => GROUP_DEFAULT)
    .nullToString(NAME_KEY, () => CommonUtils.randomString(10))
    .nullToEmptyObject(TAGS_KEY)
    .nullToEmptyArray(COLUMNS_KEY)
    .nullToEmptyArray(TOPIC_KEYS_KEY)
    // TOPIC_NAME_KEYS is used internal, and its value is always replaced by topic key. Hence, we produce a quick failure
    // to users to save their life :)
    .rejectKeyword(TOPIC_NAMES_KEY)
    // CONNECTOR_KEY_KEY is internal keyword
    .rejectKeyword(CONNECTOR_KEY_KEY)
    .requireKey(CONNECTOR_CLASS_KEY)
    .valueChecker(
      COLUMNS_KEY, {
        case v: JsArray if v.elements.nonEmpty =>
          try {
            val columns = PropGroups.ofJson(v.toString()).toColumns.asScala
            // name can't be empty
            if (columns.exists(_.name().isEmpty))
              throw DeserializationException(msg = s"name can't be empty", fieldNames = List("name"))
            // newName can't be empty
            if (columns.exists(_.newName().isEmpty))
              throw DeserializationException(msg = s"newName can't be empty", fieldNames = List("newName"))
            // order can't be negative number
            if (columns.exists(_.order() < 0))
              throw DeserializationException(msg = s"order can't be negative number", fieldNames = List("order"))
            // order can't be duplicate
            if (columns.map(_.order).toSet.size != columns.size)
              throw DeserializationException(msg = s"duplicate order:${columns.map(_.order)}",
                                             fieldNames = List("order"))
          } catch {
            case e: DeserializationException => throw e
            case other: Throwable =>
              throw DeserializationException(
                msg = s"the string to $COLUMNS_KEY is not correct format",
                cause = other,
                fieldNames = List(COLUMNS_KEY)
              )
          }
        case _ => // do nothing
      }
    )
    .refine

  final case class Update(settings: Map[String, JsValue]) {
    private[ConnectorApi] def group: Option[String] = noJsNull(settings).get(GROUP_KEY).map(_.convertTo[String])
    private[ConnectorApi] def name: Option[String] = noJsNull(settings).get(NAME_KEY).map(_.convertTo[String])
    def className: Option[String] = noJsNull(settings).get(CONNECTOR_CLASS_KEY).map(_.convertTo[String])

    def columns: Option[Seq[Column]] =
      noJsNull(settings).get(COLUMNS_KEY).map(s => PropGroups.ofJson(s.toString).toColumns.asScala)
    def numberOfTasks: Option[Int] = noJsNull(settings).get(NUMBER_OF_TASKS_KEY).map(_.convertTo[Int])
    def workerClusterName: Option[String] = noJsNull(settings).get(WORKER_CLUSTER_NAME_KEY).map(_.convertTo[String])

    def topicKeys: Option[Set[TopicKey]] =
      noJsNull(settings).get(TOPIC_KEYS_KEY).map(_.convertTo[Set[TopicKey]])

    def tags: Option[Map[String, JsValue]] = noJsNull(settings).get(TAGS_KEY).map(_.asJsObject.fields)
  }

  implicit val CONNECTOR_UPDATE_FORMAT: RootJsonFormat[Update] = JsonRefiner[Update]
    .format(new RootJsonFormat[Update] {
      override def write(obj: Update): JsValue = JsObject(noJsNull(obj.settings))
      override def read(json: JsValue): Update = Update(json.asJsObject.fields)
    })
    // TOPIC_NAME_KEYS is used internal, and its value is always replaced by topic key. Hence, we produce a quick failure
    // to users to save their life :)
    .rejectKeyword(TOPIC_NAMES_KEY)
    // CONNECTOR_KEY_KEY is internal keyword
    .rejectKeyword(CONNECTOR_KEY_KEY)
    .rejectEmptyString()
    .valueChecker(
      COLUMNS_KEY, {
        case v: JsArray => CONNECTOR_CREATION_FORMAT.check(COLUMNS_KEY, v)
        case _          => // do nothing
      }
    )
    .refine

  import MetricsApi._

  case class Status(state: State, nodeName: String, error: Option[String])
  implicit val STATUS_FORMAT: RootJsonFormat[Status] = jsonFormat3(Status)

  /**
    * this is what we store in configurator
    */
  final case class ConnectorDescription(settings: Map[String, JsValue],
                                        status: Option[Status],
                                        tasksStatus: Seq[Status],
                                        metrics: Metrics,
                                        lastModified: Long)
      extends Data {

    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = Creation(settings)

    override def key: ConnectorKey = settings.key

    override def group: String = settings.group

    /**
      * Convert all json value to plain string. It keeps the json format but all stuff are in string.
      */
    def plain: Map[String, String] = settings.plain

    override def name: String = settings.name
    override def kind: String = "connector"
    def className: String = settings.className

    def columns: Seq[Column] = settings.columns
    def numberOfTasks: Int = settings.numberOfTasks
    def workerClusterName: String = settings.workerClusterName.get
    def topicKeys: Set[TopicKey] = settings.topicKeys
    override def tags: Map[String, JsValue] = settings.tags
  }

  implicit val CONNECTOR_DESCRIPTION_FORMAT: RootJsonFormat[ConnectorDescription] =
    new RootJsonFormat[ConnectorDescription] {
      private[this] val format = jsonFormat5(ConnectorDescription)
      override def read(json: JsValue): ConnectorDescription = format.read(json)

      override def write(obj: ConnectorDescription): JsValue =
        JsObject(
          noJsNull(
            format.write(obj).asJsObject.fields
            // TODO: the group should be equal to workerClusterName ... by chia
              + (GROUP_KEY -> JsString(GROUP_DEFAULT))
              + (NAME_KEY -> obj.settings.getOrElse(NAME_KEY, JsNull))
              + ("state" -> obj.status.map(_.state.name).map(JsString(_)).getOrElse(JsNull))
              + ("error" -> obj.status.flatMap(_.error).map(JsString(_)).getOrElse(JsNull))
          ))
    }

  /**
    * used to generate the payload and url for POST/PUT request.
    * This basic class is used to collect settings of connector. It is also used by validation so we extract the same behavior from Request.
    * We use private[v0] instead of "sealed" since it is extendable to ValidationApi.
    */
  abstract class BasicRequest private[v0] {
    protected[this] var settings: mutable.Map[String, JsValue] = mutable.Map[String, JsValue]()

    def key(key: ConnectorKey): BasicRequest.this.type = {
      group(key.group())
      name(key.name())
    }
    def group(group: String): BasicRequest.this.type =
      setting(GROUP_KEY, JsString(CommonUtils.requireNonEmpty(group)))
    def name(name: String): BasicRequest.this.type =
      setting(NAME_KEY, JsString(CommonUtils.requireNonEmpty(name)))

    def className(className: String): BasicRequest.this.type =
      setting(CONNECTOR_CLASS_KEY, JsString(CommonUtils.requireNonEmpty(className)))

    @Optional("Not all connectors demand this field. See connectors document for more details")
    def columns(columns: Seq[Column]): BasicRequest.this.type =
      setting(COLUMNS_KEY, PropGroups.ofColumns(columns.asJava).toJsonString.parseJson)

    @Optional(
      "You don't need to fill this field when update/create connector. But this filed is required in starting connector")
    def topicKey(topicKey: TopicKey): BasicRequest.this.type = topicKeys(Set(Objects.requireNonNull(topicKey)))

    @Optional(
      "You don't need to fill this field when update/create connector. But this filed is required in starting connector")
    def topicKeys(topicKeys: Set[TopicKey]): BasicRequest.this.type =
      setting(TOPIC_KEYS_KEY, TopicKey.toJsonString(topicKeys.asJava).parseJson)

    @Optional("default value is 1")
    def numberOfTasks(numberOfTasks: Int): BasicRequest.this.type =
      setting(NUMBER_OF_TASKS_KEY, JsNumber(CommonUtils.requirePositiveInt(numberOfTasks)))

    @Optional("server will match a worker cluster for you if the wk name is ignored")
    def workerClusterName(workerClusterName: String): BasicRequest.this.type =
      setting(WORKER_CLUSTER_NAME_KEY, JsString(CommonUtils.requireNonEmpty(workerClusterName)))

    @Optional("extra settings for this connectors")
    def setting(key: String, value: JsValue): BasicRequest.this.type = settings(
      Map(CommonUtils.requireNonEmpty(key) -> Objects.requireNonNull(value)))

    @Optional("extra settings for this connectors")
    def settings(settings: Map[String, JsValue]): BasicRequest.this.type = {
      import scala.collection.JavaConverters._
      this.settings ++= CommonUtils.requireNonEmpty(settings.asJava).asScala.toMap
      this
    }

    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): BasicRequest.this.type =
      setting(TAGS_KEY, JsObject(Objects.requireNonNull(tags)))

    /**
      * generate the payload for request. It removes the ignored fields and keeping all value in json representation.
      * This method is exposed to sub classes since this generation is not friendly and hence we should reuse it as much as possible.
      * Noted, it throw unchecked exception if you haven't filled all required fields
      * @return creation object
      */
    def creation: Creation = CONNECTOR_CREATION_FORMAT.read(CONNECTOR_CREATION_FORMAT.write(Creation(settings.toMap)))
  }

  /**
    * The do-action methods are moved from BasicRequest to this one. Hence, ValidationApi ConnectorRequest does not have those weired methods
    */
  sealed abstract class Request extends BasicRequest {

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[ConnectorDescription]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[ConnectorDescription]
  }

  class Access private[v0]
      extends com.island.ohara.client.configurator.v0.Access[ConnectorDescription](CONNECTORS_PREFIX_PATH) {

    /**
      * start to run a connector on worker cluster.
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def start(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, START_COMMAND)

    /**
      * stop and remove a running connector.
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def stop(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, STOP_COMMAND)

    /**
      * pause a running connector
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def pause(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, PAUSE_COMMAND)

    /**
      * resume a paused connector
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def resume(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, RESUME_COMMAND)

    def request: Request = new Request {
      private[v0] def update: Update =
        CONNECTOR_UPDATE_FORMAT.read(CONNECTOR_UPDATE_FORMAT.write(Update(settings.toMap)))

      override def create()(implicit executionContext: ExecutionContext): Future[ConnectorDescription] =
        exec.post[Creation, ConnectorDescription, ErrorApi.Error](url, creation)

      override def update()(implicit executionContext: ExecutionContext): Future[ConnectorDescription] = {
        exec.put[Update, ConnectorDescription, ErrorApi.Error](
          url(ObjectKey.of(update.group.getOrElse(GROUP_DEFAULT), update.name.get)),
          update)
      }
    }
  }

  def access: Access = new Access
}
