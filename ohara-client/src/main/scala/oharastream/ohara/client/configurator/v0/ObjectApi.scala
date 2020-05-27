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

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.setting.ObjectKey
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object ObjectApi {
  val KIND: String                = "object"
  val OBJECTS_PREFIX_PATH: String = "objects"

  final class Creation(val settings: Map[String, JsValue]) extends BasicCreation {
    private[this] implicit def update(settings: Map[String, JsValue]): Updating = new Updating(noJsNull(settings))

    override def group: String = settings.group.get

    override def name: String = settings.name.get

    override def tags: Map[String, JsValue] = settings.tags.get
  }
  private[ohara] implicit val CREATION_JSON_FORMAT: JsonRefiner[Creation] =
    rulesOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(obj.settings)

        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      })
      .nullToEmptyObject(TAGS_KEY)
      .build

  final class Updating(val settings: Map[String, JsValue]) {
    // We use the update parser to get the name and group
    private[ObjectApi] def name: Option[String] = noJsNull(settings).get(NAME_KEY).map(_.convertTo[String])

    private[ObjectApi] def group: Option[String] = noJsNull(settings).get(GROUP_KEY).map(_.convertTo[String])

    def tags: Option[Map[String, JsValue]] = noJsNull(settings).get(TAGS_KEY).map {
      case s: JsObject => s.fields
      case other: JsValue =>
        throw new IllegalArgumentException(s"the type of tags should be JsObject, actual type is ${other.getClass}")
    }
  }

  implicit val UPDATING_JSON_FORMAT: RootJsonFormat[Updating] = new RootJsonFormat[Updating] {
    override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)

    override def write(obj: Updating): JsValue = JsObject(noJsNull(obj.settings))
  }

  final class ObjectInfo private[v0] (val settings: Map[String, JsValue]) extends Data with Serializable {
    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(noJsNull(settings))

    override def group: String = settings.group

    override def name: String = settings.name

    override def lastModified: Long = settings(LAST_MODIFIED_KEY).convertTo[Long]

    override def kind: String = KIND

    override def tags: Map[String, JsValue] = settings.tags

    override def equals(obj: Any): Boolean = obj match {
      case other: ObjectInfo => other.settings == settings
      case _                 => false
    }

    override def hashCode(): Int = settings.hashCode()

    override def toString: String = settings.toString()

    override protected def raw: Map[String, JsValue] = settings
  }

  object ObjectInfo {
    def apply(settings: Map[String, JsValue], lastModified: Long): ObjectInfo =
      new ObjectInfo(settings + (LAST_MODIFIED_KEY -> JsNumber(lastModified)))
  }

  implicit val OBJECT_JSON_FORMAT: JsonRefiner[ObjectInfo] = JsonRefinerBuilder[ObjectInfo]
    .format(new RootJsonFormat[ObjectInfo] {
      override def write(obj: ObjectInfo): JsValue = JsObject(obj.settings)
      override def read(json: JsValue): ObjectInfo = new ObjectInfo(json.asJsObject.fields)
    })
    .build

  trait Request {
    private[this] val settings: mutable.Map[String, JsValue] = mutable.Map[String, JsValue]()

    def name(name: String): Request =
      settings(Map(NAME_KEY -> JsString(name)))

    def group(group: String): Request =
      settings(Map(GROUP_KEY -> JsString(group)))

    def key(key: ObjectKey): Request =
      settings(Map(NAME_KEY -> JsString(key.name()), GROUP_KEY -> JsString(key.group())))

    def settings(settings: Map[String, JsValue]): Request = {
      this.settings ++= settings
      this
    }

    def creation: Creation =
      CREATION_JSON_FORMAT.read(CREATION_JSON_FORMAT.write(new Creation(settings.toMap)))

    def updating: Updating =
      UPDATING_JSON_FORMAT.read(UPDATING_JSON_FORMAT.write(new Updating(settings.toMap)))

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[ObjectInfo]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[ObjectInfo]
  }

  class Access private[v0]
      extends oharastream.ohara.client.configurator.v0.Access[Creation, Updating, ObjectInfo](OBJECTS_PREFIX_PATH) {
    def request: Request = new Request {
      override def create()(implicit executionContext: ExecutionContext): Future[ObjectInfo] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[ObjectInfo] =
        put(ObjectKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }
  }

  def access: Access = new Access
}
