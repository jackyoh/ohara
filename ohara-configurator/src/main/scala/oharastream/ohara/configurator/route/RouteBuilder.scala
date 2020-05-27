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

package oharastream.ohara.configurator.route

import java.util.Objects

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{
  as,
  complete,
  delete,
  entity,
  get,
  parameter,
  parameterMap,
  path,
  pathEnd,
  pathPrefix,
  post,
  put,
  _
}
import akka.http.scaladsl.server.Route
import oharastream.ohara.client.configurator.v0.{BasicCreation, JsonRefiner}
import oharastream.ohara.client.configurator.{Data, QueryRequest}
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.DataStore
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Ohara offers ~100 APIs to construct the unparalleled streaming platform. However, the routes generated by many APIs
  * almost kill us in maintaining and updating. In order to save our developers from understanding the complicated routes
  * , this class extract the basic rules to be base for most routes.
  * @tparam Creation creation request
  * @tparam Updating updating request
  * @tparam Res response
  */
trait RouteBuilder[Creation <: BasicCreation, Updating, Res <: Data]
    extends oharastream.ohara.common.pattern.Builder[server.Route] {
  private[this] var root: String                                             = _
  private[this] var customPost: Option[() => Route]                          = None
  private[this] var hookOfCreation: Option[HookOfCreation[Creation, Res]]    = None
  private[this] var hookAfterCreation: Option[HookAfterCreation[Res]]        = None
  private[this] var hookAfterUpdating: Option[HookAfterUpdating[Res]]        = None
  private[this] var hookOfUpdating: Option[HookOfUpdating[Updating, Res]]    = None
  private[this] var hookOfGet: HookOfGet[Res]                                = (res: Res) => Future.successful(res)
  private[this] var hookOfList: HookOfList[Res]                              = (res: Seq[Res]) => Future.successful(res)
  private[this] var hookBeforeDelete: HookBeforeDelete                       = (_: ObjectKey) => Future.unit
  private[this] val hookOfPutActions                                         = mutable.Map[String, HookOfAction[Res]]()
  private[this] var hookOfFinalPutAction: Option[HookOfAction[Res]]          = None
  private[this] val hookOfDeleteActions                                      = mutable.Map[String, HookOfAction[ObjectKey]]()
  private[this] var hookOfFinalDeleteAction: Option[HookOfAction[ObjectKey]] = None

  def root(root: String): RouteBuilder[Creation, Updating, Res] = {
    this.root = CommonUtils.requireNonEmpty(root)
    this
  }

  @Optional("default route is redirected to error response")
  def customPost(customPost: () => Route): RouteBuilder[Creation, Updating, Res] = {
    if (hookOfCreation.isDefined)
      throw new IllegalArgumentException("you can't define both of customPost and hookOfCreation")
    this.customPost = Some(customPost)
    this
  }

  @Optional("default route is redirected to error response")
  def hookOfCreation(hookOfCreation: HookOfCreation[Creation, Res]): RouteBuilder[Creation, Updating, Res] = {
    if (customPost.isDefined)
      throw new IllegalArgumentException("you can't define both of customPost and hookOfCreation")
    this.hookOfCreation = Some(hookOfCreation)
    this
  }

  @Optional("nothing happen after creating response")
  def hookAfterCreation(hookAfterCreation: HookAfterCreation[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookAfterCreation = Some(hookAfterCreation)
    this
  }

  @Optional("nothing happen after updating response")
  def hookAfterUpdating(hookAfterUpdating: HookAfterUpdating[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookAfterUpdating = Some(hookAfterUpdating)
    this
  }

  @Optional("default route is redirected to error response")
  def hookOfUpdating(hookOfUpdating: HookOfUpdating[Updating, Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfUpdating = Some(hookOfUpdating)
    this
  }

  @Optional("add custom hook to process the returned result")
  def hookOfGet(hookOfGet: HookOfGet[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfGet = Objects.requireNonNull(hookOfGet)
    this
  }

  @Optional("add custom hook to process the returned result")
  def hookOfList(hookOfList: HookOfList[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfList = Objects.requireNonNull(hookOfList)
    this
  }

  @Optional("add custom hook to process the result before deleting it")
  def hookBeforeDelete(hookBeforeDelete: HookBeforeDelete): RouteBuilder[Creation, Updating, Res] = {
    this.hookBeforeDelete = Objects.requireNonNull(hookBeforeDelete)
    this
  }

  @Optional("add custom hook to response specific PUT action")
  def hookOfPutAction(action: String, hookOfAction: HookOfAction[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfPutActions += (CommonUtils.requireNonEmpty(action) -> Objects.requireNonNull(hookOfAction))
    this
  }

  @Optional("add custom hook to response remaining PUT action")
  def hookOfFinalPutAction(hookOfFinalPutAction: HookOfAction[Res]): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfFinalPutAction = Some(hookOfFinalPutAction)
    this
  }

  @Optional("add custom hook to response specific DELETE action")
  def hookOfDeleteAction(
    action: String,
    hookOfAction: HookOfAction[ObjectKey]
  ): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfDeleteActions += (CommonUtils.requireNonEmpty(action) -> Objects.requireNonNull(hookOfAction))
    this
  }

  @Optional("add custom hook to response remaining DELETE action")
  def hookOfFinalDeleteAction(
    hookOfFinalDeleteAction: HookOfAction[ObjectKey]
  ): RouteBuilder[Creation, Updating, Res] = {
    this.hookOfFinalDeleteAction = Some(hookOfFinalDeleteAction)
    this
  }

  override def build(): Route = doBuild(
    root = CommonUtils.requireNonEmpty(root),
    customPost = customPost,
    hookOfCreation = hookOfCreation,
    hookAfterCreation = hookAfterCreation,
    hookAfterUpdating = hookAfterUpdating,
    hookOfUpdating = hookOfUpdating,
    hookOfGet = Objects.requireNonNull(hookOfGet),
    hookOfList = Objects.requireNonNull(hookOfList),
    hookBeforeDelete = Objects.requireNonNull(hookBeforeDelete),
    hookOfPutActions = hookOfPutActions.toMap,
    hookOfFinalPutAction = hookOfFinalPutAction,
    hookOfDeleteActions = hookOfDeleteActions.toMap,
    hookOfFinalDeleteAction = hookOfFinalDeleteAction
  )

  protected def doBuild(
    root: String,
    customPost: Option[() => Route],
    hookOfCreation: Option[HookOfCreation[Creation, Res]],
    hookAfterCreation: Option[HookAfterCreation[Res]],
    hookAfterUpdating: Option[HookAfterUpdating[Res]],
    hookOfUpdating: Option[HookOfUpdating[Updating, Res]],
    hookOfList: HookOfList[Res],
    hookOfGet: HookOfGet[Res],
    hookBeforeDelete: HookBeforeDelete,
    hookOfPutActions: Map[String, HookOfAction[Res]],
    hookOfFinalPutAction: Option[HookOfAction[Res]],
    hookOfDeleteActions: Map[String, HookOfAction[ObjectKey]],
    hookOfFinalDeleteAction: Option[HookOfAction[ObjectKey]]
  ): Route
}

object RouteBuilder {
  def apply[Creation <: BasicCreation, Updating, Res <: Data: ClassTag]()(
    implicit store: DataStore,
    // normally, update request does not carry the name field,
    // Hence, the check of name have to be executed by format of creation
    // since it must have name field.
    rm: JsonRefiner[Creation],
    rm1: RootJsonFormat[Updating],
    rm2: JsonRefiner[Res],
    executionContext: ExecutionContext
  ): RouteBuilder[Creation, Updating, Res] =
    (
      root: String,
      customPost: Option[() => Route],
      hookOfCreation: Option[HookOfCreation[Creation, Res]],
      hookAfterCreation: Option[HookAfterCreation[Res]],
      hookAfterUpdating: Option[HookAfterUpdating[Res]],
      hookOfUpdating: Option[HookOfUpdating[Updating, Res]],
      hookOfList: HookOfList[Res],
      hookOfGet: HookOfGet[Res],
      hookBeforeDelete: HookBeforeDelete,
      hookOfPutActions: Map[String, HookOfAction[Res]],
      hookOfFinalPutAction: Option[HookOfAction[Res]],
      hookOfDeleteActions: Map[String, HookOfAction[ObjectKey]],
      hookOfFinalDeleteAction: Option[HookOfAction[ObjectKey]]
    ) =>
      pathPrefix(root) {
        pathEnd {
          customPost
            .map(_.apply())
            .getOrElse(post(entity(as[Creation]) { creation =>
              hookOfCreation
                .map(
                  hook =>
                    complete(
                      hook(creation)
                        .flatMap(res => store.addIfAbsent(res))
                        .flatMap(res => {
                          val newRes = rm2.response(res)
                          hookAfterCreation.map(hook => hook(newRes)).getOrElse(Future.successful(newRes))
                        })
                    )
                )
                .getOrElse(routeToOfficialUrl(s"/$root"))
            })) ~
            get {
              parameterMap { params =>
                complete(
                  store
                    .values[Res]()
                    .flatMap(hookOfList(_))
                    .map { x =>
                      x.map(res => rm2.response(res))
                    }
                    .map(_.filter(_.matched(QueryRequest(params.filter {
                      // the empty stuff causes false always since there is nothing matched to "empty"
                      // hence, we remove them from parameters for careless users :)
                      case (key, value) => key.nonEmpty && value.nonEmpty
                    }))))
                )
              }
            }
        } ~ path(Segment) { name =>
          parameter(GROUP_KEY ? GROUP_DEFAULT) { group =>
            val key =
              ObjectKey.of(
                rm.check(GROUP_KEY, JsString(group)).value,
                rm.check(NAME_KEY, JsString(name)).value
              )
            get(complete(store.value[Res](key).flatMap(hookOfGet(_)).map(rm2.response(_)))) ~
              delete(
                complete(
                  hookBeforeDelete(key).map(_ => key).flatMap(store.remove[Res](_).map(_ => StatusCodes.NoContent))
                )
              ) ~
              put(
                entity(as[Updating])(
                  update =>
                    hookOfUpdating
                      .map(
                        hook =>
                          complete(
                            store
                              .get[Res](key)
                              .flatMap(previous => hook(key = key, updating = update, previous = previous))
                              .flatMap(store.add)
                              .flatMap(
                                res => {
                                  val newRes = rm2.response(res)
                                  hookAfterUpdating.map(hook => hook(newRes)).getOrElse(Future.successful(newRes))
                                }
                              )
                          )
                      )
                      .getOrElse(routeToOfficialUrl(s"/$root/${key.name}?${key.group()}"))
                )
              )
          }
        }
      } ~ pathPrefix(root / Segment / Segment) {
        case (name, subName) =>
          parameterMap { params =>
            val key =
              ObjectKey.of(params.getOrElse(GROUP_KEY, oharastream.ohara.client.configurator.v0.GROUP_DEFAULT), name)
            put {
              hookOfPutActions.get(subName).orElse(hookOfFinalPutAction) match {
                case None => routeToOfficialUrl(s"/$root/$subName")
                case Some(f) =>
                  complete {
                    store.value[Res](key).flatMap(res => f(res, subName, params)).map(_ => StatusCodes.Accepted)
                  }
              }
            } ~ delete {
              hookOfDeleteActions
                .get(subName)
                .orElse(hookOfFinalDeleteAction)
                .map(_(key, subName, params))
                .map(_.map(_ => StatusCodes.Accepted))
                .map(complete(_))
                .getOrElse(routeToOfficialUrl(s"/$root/$subName"))
            }
          }
      }
}
