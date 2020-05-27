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

import oharastream.ohara.client.configurator.{Data, QueryRequest}
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}

object PipelineApi {
  val KIND: String                  = "pipeline"
  val PIPELINES_PREFIX_PATH: String = "pipelines"

  /**
    * action key. it is used to auto-remove the existent objs from endpoints,
    */
  val REFRESH_COMMAND: String = "refresh"

  final case class Endpoint(group: String, name: String, kind: String) {
    def key: ObjectKey = ObjectKey.of(group, name)
  }
  implicit val ENDPOINT_JSON_FORMAT: JsonRefiner[Endpoint] =
    JsonRefinerBuilder[Endpoint]
      .format(jsonFormat3(Endpoint))
      .nullToString(GROUP_KEY, GROUP_DEFAULT)
      .rejectEmptyString()
      .build

  final case class Updating(
    endpoints: Option[Set[Endpoint]],
    tags: Option[Map[String, JsValue]]
  )

  implicit val UPDATING_JSON_FORMAT: RootJsonFormat[Updating] =
    JsonRefinerBuilder[Updating].format(jsonFormat2(Updating)).rejectEmptyString().build

  final case class Creation(
    group: String,
    name: String,
    endpoints: Set[Endpoint],
    tags: Map[String, JsValue]
  ) extends oharastream.ohara.client.configurator.v0.BasicCreation

  implicit val CREATION_JSON_FORMAT: JsonRefiner[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    rulesOfKey[Creation]
      .format(jsonFormat4(Creation))
      .rejectEmptyString()
      .nullToEmptyObject(TAGS_KEY)
      .nullToEmptyArray("endpoints")
      .build

  import MetricsApi._

  /**
    * Metricsable offers many helper methods so we let ObjectAbstract extend it.
    */
  final case class ObjectAbstract(
    group: String,
    name: String,
    kind: String,
    className: Option[String],
    state: Option[String],
    error: Option[String],
    nodeMetrics: Map[String, Metrics],
    lastModified: Long,
    tags: Map[String, JsValue]
  ) extends Metricsable {
    def key: ObjectKey = ObjectKey.of(group, name)
  }

  implicit val OBJECT_ABSTRACT_JSON_FORMAT: RootJsonFormat[ObjectAbstract] = new RootJsonFormat[ObjectAbstract] {
    private[this] val format                         = jsonFormat9(ObjectAbstract)
    override def read(json: JsValue): ObjectAbstract = format.read(json)
    override def write(obj: ObjectAbstract): JsValue = format.write(obj)
  }

  final case class Pipeline(
    group: String,
    name: String,
    endpoints: Set[Endpoint],
    objects: Set[ObjectAbstract],
    jarKeys: Set[ObjectKey],
    lastModified: Long,
    tags: Map[String, JsValue]
  ) extends Data {
    override def kind: String = KIND

    override protected def raw: Map[String, JsValue] = PIPELINE_JSON_FORMAT.write(this).asJsObject.fields
  }

  implicit val PIPELINE_JSON_FORMAT: JsonRefiner[Pipeline] =
    JsonRefinerBuilder[Pipeline].format(jsonFormat7(Pipeline)).build

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  trait Request {
    /**
      * set the group and name via key
      * @param objectKey object key
      * @return this request
      */
    def key(objectKey: ObjectKey): Request = {
      group(objectKey.group())
      name(objectKey.name())
    }

    @Optional("default def is a GROUP_DEFAULT")
    def group(group: String): Request

    @Optional("default name is a random string. But it is required in updating")
    def name(name: String): Request

    @Optional("default value is empty")
    def endpoint(data: Data): Request = endpoints(Seq(data))

    @Optional("default value is empty")
    def endpoints(data: Seq[Data]): Request
    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): Request

    private[v0] def creation: Creation

    private[v0] def updating: Updating

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[Pipeline]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[Pipeline]
  }

  sealed trait Query extends BasicQuery[Pipeline] {
    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[v0]
      extends oharastream.ohara.client.configurator.v0.Access[Creation, Updating, Pipeline](PIPELINES_PREFIX_PATH) {
    def refresh(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, REFRESH_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[Pipeline]] = list(request)
    }

    def request: Request = new Request {
      private[this] var group: String = GROUP_DEFAULT
      private[this] var name: String  = _
      // TODO: remove this helper (https://github.com/oharastream/ohara/issues/3530)
      private[this] var endpoints: Set[Endpoint]   = _
      private[this] var tags: Map[String, JsValue] = _

      override def group(group: String): Request = {
        this.group = CommonUtils.requireNonEmpty(group)
        this
      }

      override def name(name: String): Request = {
        this.name = CommonUtils.requireNonEmpty(name)
        this
      }

      override def endpoints(data: Seq[Data]): Request = {
        val newEndpoints = data.map(d => Endpoint(group = d.group, name = d.name, kind = d.kind)).toSet
        if (this.endpoints == null) this.endpoints = newEndpoints
        else this.endpoints ++= newEndpoints
        this
      }

      override def tags(tags: Map[String, JsValue]): Request = {
        if (this.tags == null) this.tags = Objects.requireNonNull(tags)
        else this.tags ++= Objects.requireNonNull(tags)
        this
      }

      override private[v0] def creation: Creation =
        // auto-complete the creation via our refiner
        CREATION_JSON_FORMAT.read(
          CREATION_JSON_FORMAT.write(
            Creation(
              group = CommonUtils.requireNonEmpty(group),
              name = if (CommonUtils.isEmpty(name)) CommonUtils.randomString(10) else name,
              endpoints = if (endpoints == null) Set.empty else endpoints,
              tags = if (tags == null) Map.empty else tags
            )
          )
        )

      override private[v0] def updating: Updating =
        // auto-complete the updating via our refiner
        UPDATING_JSON_FORMAT.read(
          UPDATING_JSON_FORMAT.write(
            Updating(
              endpoints = Option(endpoints),
              tags = Option(tags)
            )
          )
        )

      override def create()(implicit executionContext: ExecutionContext): Future[Pipeline] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[Pipeline] =
        put(ObjectKey.of(group, name), updating)
    }
  }

  def access: Access = new Access
}
