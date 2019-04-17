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
import java.io.File
import java.nio.charset.CodingErrorAction

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.directives.FileInfo
import akka.util.ByteString
import com.island.ohara.common.util.{CommonUtils, VersionUtils}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Codec, Source}

object StreamApi {

  /**
    * StreamApp List Page max acceptable upload file size (1 MB currently)
    */
  final val MAX_FILE_SIZE = 1 * 1024 * 1024L

  /**
    * StreamApp List Page file "key name" for form-data
    */
  final val INPUT_KEY = "streamapp"

  final val CONTENT_TYPE = MediaTypes.`application/java-archive`

  final val TMP_ROOT = System.getProperty("java.io.tmpdir")

  /**
    * the only entry for ohara streamApp
    */
  final val MAIN_ENTRY = "com.island.ohara.streams.StreamApp"

  /**
    *  limit the length of docker container name (&lt; 60).
    */
  final val LIMIT_OF_DOCKER_NAME_LENGTH: Int = 60

  final val PREFIX_KEY = "ost"

  final val DIVIDER: String = "-"

  final val JARURL_KEY: String = "STREAMAPP_JARURL"
  final val APPID_KEY: String = "STREAMAPP_APPID"
  final val SERVERS_KEY: String = "STREAMAPP_SERVERS"
  final val FROM_TOPIC_KEY: String = "STREAMAPP_FROMTOPIC"
  final val TO_TOPIC_KEY: String = "STREAMAPP_TOTOPIC"

  /**
    * StreamApp Docker Image name
    */
  final val STREAMAPP_IMAGE: String = s"oharastream/streamapp:${VersionUtils.VERSION}"

  val STREAM_PREFIX_PATH: String = "stream"
  val STREAM_LIST_PREFIX_PATH: String = "jars"
  val STREAM_PROPERTY_PREFIX_PATH: String = "property"
  val START_COMMAND: String = "start"
  val STOP_COMMAND: String = "stop"
  val STATUS_COMMAND: String = "status"

  /**
    * format unique applicationId for the streamApp.
    * It can be used in setting container's hostname and name
    * @param streamId the streamApp id
    * @return a formatted string. form: ${prefix}-${streamId}
    */
  def formatAppId(streamId: String): String =
    assertLength(
      Seq(
        PREFIX_KEY,
        streamId
      ).mkString(DIVIDER))

  /**
    * format the cluster name by unique id
    *
    * @param id the streamApp unique id
    * @return cluster name
    */
  def formatClusterName(id: String): String =
    CommonUtils.assertOnlyNumberAndChar(id.replaceAll("-", ""))

  /**
    * create temp file(with suffix .tmp) inside temp folder
    *
    * @param fileInfo the request file
    * @return the tmp file
    */
  def saveTmpFile(fileInfo: FileInfo): File = CommonUtils.createTempFile(fileInfo.fileName)

  private[this] def assertLength(s: String): String = if (s.length > LIMIT_OF_DOCKER_NAME_LENGTH)
    throw new IllegalArgumentException(s"limit of length is $LIMIT_OF_DOCKER_NAME_LENGTH. actual: ${s.length}")
  else s

  /**
    * the streamApp description that is kept in ohara Stores
    *
    * @param workerClusterName the worker cluster name
    * @param id the streamApp unique id
    * @param name streamApp name in pipeline
    * @param instances numbers of streamApp running container
    * @param jarInfo uploaded jar information
    * @param from the candidate topics for streamApp consume from
    * @param to the candidate topics for streamApp produce to
    * @param lastModified this data change time
    */
  final case class StreamAppDescription(workerClusterName: String,
                                        id: String,
                                        name: String,
                                        instances: Int,
                                        jarInfo: JarApi.JarInfo,
                                        from: Seq[String],
                                        to: Seq[String],
                                        state: Option[String],
                                        lastModified: Long)
      extends Data {
    override def kind: String = "streamApp"
    override def toString: String =
      s"""
          workerClusterName: $workerClusterName,
          id: $id,
          name: $name,
          instances: $instances,
          jarInfo: $jarInfo,
          fromTopics: $from,
          toTopics: $to
      """.stripMargin
  }
  implicit val STREAM_ACTION_RESPONSE_JSON_FORMAT: RootJsonFormat[StreamAppDescription] = jsonFormat9(
    StreamAppDescription)

  /**
    * The Stream Cluster Information
    *
    * @param name cluster name
    * @param imageName image name
    * @param ports port list (useless in stream)
    * @param nodeNames actual running nodes
    * @param state the state of this warehouse (see '''ContainerState''')
    */
  final case class StreamClusterInfo(
    name: String,
    imageName: String,
    // We don't care the ports since streamApp communicates by broker
    ports: Seq[Int] = Seq.empty,
    nodeNames: Seq[String] = Seq.empty,
    state: Option[String] = None
  ) extends ClusterInfo

  // StreamApp List Request Body
  final case class StreamListRequest(jarName: String)
  implicit val STREAM_LIST_REQUEST_JSON_FORMAT: RootJsonFormat[StreamListRequest] = jsonFormat1(StreamListRequest)

  // StreamApp List Response Body
  final case class StreamListResponse(id: String, name: String, jarName: String, lastModified: Long)
  implicit val STREAM_JAR_JSON_FORMAT: RootJsonFormat[StreamListResponse] =
    jsonFormat4(StreamListResponse)

  // StreamApp Property Request Body
  final case class StreamPropertyRequest(name: String, from: Seq[String], to: Seq[String], instances: Int)
  implicit val STREAM_PROPERTY_REQUEST_JSON_FORMAT: RootJsonFormat[StreamPropertyRequest] = jsonFormat4(
    StreamPropertyRequest)

  // StreamApp Property Response Body
  final case class StreamPropertyResponse(id: String,
                                          jarName: String,
                                          name: String,
                                          from: Seq[String],
                                          to: Seq[String],
                                          instances: Int,
                                          lastModified: Long)
  implicit val STREAM_PROPERTY_RESPONSE_JSON_FORMAT: RootJsonFormat[StreamPropertyResponse] = jsonFormat7(
    StreamPropertyResponse
  )

  sealed abstract class ActionAccess extends BasicAccess(s"$STREAM_PREFIX_PATH") {

    /**
      *  start a streamApp
      *
      * @param id streamApp component id
      * @param executionContext execution context
      * @return status of streamApp ("RUNNING" if success, "EXITED" if fail)
      */
    def start(id: String)(implicit executionContext: ExecutionContext): Future[StreamAppDescription]

    /**
      * stop a streamApp
      *
      * @param id streamApp component id
      * @param executionContext execution context
      * @return status of streamApp (None if stop successful, or throw exception)
      */
    def stop(id: String)(implicit executionContext: ExecutionContext): Future[StreamAppDescription]
  }
  def accessOfAction(): ActionAccess = new ActionAccess {
    private[this] def url(id: String, action: String): String =
      s"http://${_hostname}:${_port}/${_version}/${_prefixPath}/$id/$action"
    override def start(id: String)(implicit executionContext: ExecutionContext): Future[StreamAppDescription] =
      exec.put[StreamAppDescription, ErrorApi.Error](url(id, START_COMMAND))
    override def stop(id: String)(implicit executionContext: ExecutionContext): Future[StreamAppDescription] =
      exec.put[StreamAppDescription, ErrorApi.Error](url(id, STOP_COMMAND))
  }

  sealed abstract class ListAccess extends BasicAccess(s"$STREAM_PREFIX_PATH/$STREAM_LIST_PREFIX_PATH") {

    /**
      * list jar information of uploading streamApp jars
      *
      * @param wkName query parameter to filter by assigned worker cluster name
      * @param executionContext execution context
      * @return jar list in the worker cluster name, or all jars if not specify
      */
    def list(wkName: Option[String])(implicit executionContext: ExecutionContext): Future[Seq[StreamListResponse]]

    /**
      * upload streamApp jars to worker cluster,
      * will try to find pre-defined worker cluster if not assigned worker cluster name
      *
      * @param filePaths jar path list
      * @param wkName uploaded worker cluster name
      * @param executionContext execution context
      * @return upload jars to assigned worker cluster, or pre-defined worker cluster
      */
    def upload(filePaths: Seq[String], wkName: Option[String])(
      implicit executionContext: ExecutionContext): Future[Seq[StreamListResponse]] =
      upload(
        filePaths: Seq[String],
        wkName,
        INPUT_KEY,
        CONTENT_TYPE
      )
    def upload(filePaths: Seq[String], wkName: Option[String], inputKey: String, contentType: ContentType)(
      implicit executionContext: ExecutionContext): Future[Seq[StreamListResponse]]

    /**
      * delete streamApp jar by id
      *
      * @param id streamApp id
      * @param executionContext execution context
      * @return the deleted jar
      */
    def delete(id: String)(implicit executionContext: ExecutionContext): Future[StreamListResponse]

    /**
      * update jar information
      *
      * @param id streamApp id
      * @param request update request
      * @param executionContext execution context
      * @return the updated jar
      */
    def update(id: String, request: StreamListRequest)(
      implicit executionContext: ExecutionContext): Future[StreamListResponse]
  }
  // To avoid different charset handle, replace the malformedInput and unMappable char
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  def accessOfList(): ListAccess = new ListAccess {
    private[this] def request(
      target: String,
      inputKey: String,
      contentType: ContentType,
      filePaths: Seq[String],
      wkName: Option[String])(implicit executionContext: ExecutionContext): Future[HttpRequest] = {
      var stricts = filePaths.map(filePath => {
        Multipart.FormData.BodyPart.Strict(
          inputKey,
          HttpEntity(
            contentType,
            ByteString(Source.fromFile(filePath).mkString)
          ),
          Map("filename" -> new File(filePath).getName)
        )
      })
      if (wkName.isDefined) stricts :+= Multipart.FormData.BodyPart.Strict(Parameters.CLUSTER_NAME, wkName.get)

      Marshal(Multipart.FormData(stricts: _*))
        .to[RequestEntity]
        .map(
          entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity)
        )
    }

    override def list(wkName: Option[String])(
      implicit executionContext: ExecutionContext): Future[Seq[StreamListResponse]] =
      exec.get[Seq[StreamListResponse], ErrorApi.Error](
        Parameters.appendTargetCluster(s"http://${_hostname}:${_port}/${_version}/${_prefixPath}",
                                       wkName.getOrElse("")))

    override def upload(filePaths: Seq[String], wkName: Option[String], inputKey: String, contentType: ContentType)(
      implicit executionContext: ExecutionContext): Future[Seq[StreamListResponse]] = {
      request(s"http://${_hostname}:${_port}/${_version}/${_prefixPath}", inputKey, contentType, filePaths, wkName)
        .flatMap(exec.request[Seq[StreamListResponse], ErrorApi.Error])
    }
    override def delete(id: String)(implicit executionContext: ExecutionContext): Future[StreamListResponse] =
      exec.delete[StreamListResponse, ErrorApi.Error](s"http://${_hostname}:${_port}/${_version}/${_prefixPath}/$id")
    override def update(id: String, request: StreamListRequest)(
      implicit executionContext: ExecutionContext): Future[StreamListResponse] =
      exec.put[StreamListRequest, StreamListResponse, ErrorApi.Error](
        s"http://${_hostname}:${_port}/${_version}/${_prefixPath}/$id",
        request
      )
  }

  sealed trait PropertyAccess {
    def hostname(hostname: String)(implicit executionContext: ExecutionContext): PropertyAccess
    def port(port: Int)(implicit executionContext: ExecutionContext): PropertyAccess
    def get(id: String)(implicit executionContext: ExecutionContext): Future[StreamPropertyResponse]
    def update(id: String, request: StreamPropertyRequest)(
      implicit executionContext: ExecutionContext): Future[StreamPropertyResponse]
  }

  def accessOfProperty(): PropertyAccess = new PropertyAccess {
    private[this] val access: Access[StreamPropertyRequest, StreamPropertyResponse] =
      new Access[StreamPropertyRequest, StreamPropertyResponse](
        s"$STREAM_PREFIX_PATH/$STREAM_PROPERTY_PREFIX_PATH"
      )

    override def hostname(hostname: String)(implicit executionContext: ExecutionContext): PropertyAccess = {
      access.hostname(hostname)
      this
    }
    override def port(port: Int)(implicit executionContext: ExecutionContext): PropertyAccess = {
      access.port(port)
      this
    }

    override def get(id: String)(implicit executionContext: ExecutionContext): Future[StreamPropertyResponse] =
      access.get(id)
    override def update(
      id: String,
      request: StreamPropertyRequest
    )(implicit executionContext: ExecutionContext): Future[StreamPropertyResponse] =
      access.update(id, request)
  }
}
