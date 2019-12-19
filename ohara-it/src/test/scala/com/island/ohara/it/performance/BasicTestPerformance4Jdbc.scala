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

package com.island.ohara.it.performance

import java.io.File
import java.sql.Timestamp

import com.island.ohara.common.util.Releasable
import org.junit.{After, Before, Test}
import com.island.ohara.client.configurator.v0.FileInfoApi
import com.island.ohara.client.configurator.v0.InspectApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.setting.{ConnectorKey, ObjectKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.jdbc.source.JDBCSourceConnector

import scala.concurrent.ExecutionContext.Implicits.global
import spray.json.{JsNumber, JsString}

abstract class BasicTestPerformance4Jdbc extends BasicTestPerformance {
  /*private[this] val DB_URL_KEY: String = "ohara.it.performance.jdbc.url"
  private[this] val url: String =
    sys.env.getOrElse(DB_URL_KEY, throw new AssumptionViolatedException(s"$DB_URL_KEY does not exists!!!"))

  private[this] val DB_USER_NAME_KEY: String = "ohara.it.performance.jdbc.username"
  private[this] val user: String =
    sys.env.getOrElse(DB_USER_NAME_KEY, throw new AssumptionViolatedException(s"$DB_USER_NAME_KEY does not exists!!!"))

  private[this] val DB_PASSWORD_KEY: String = "ohara.it.performance.jdb.password"
  private[this] val password: String =
    sys.env.getOrElse(DB_PASSWORD_KEY, throw new AssumptionViolatedException(s"$DB_PASSWORD_KEY does not exists!!!"))
   */
  private[this] val url: String      = "jdbc:oracle:thin:@//ohara-jenkins-it-02:1521/xe.localdomain"
  private[this] val user: String     = "ohara"
  private[this] val password: String = "island123"

  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath: String  = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")

  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.jdbc.needDeleteTable"
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "false").toBoolean

  private[this] val connectorKey: ConnectorKey  = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey          = TopicKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val timestampColumnName: String = "COLUMN0"

  protected def productName: String
  protected def tableName: String
  protected def isColumnNameUpperCase: Boolean = true
  private[this] var client: DatabaseClient     = _

  @Before
  final def setup(): Unit = {
    client = DatabaseClient.builder.url(url).user(user).password(password).build
  }

  @Test
  def test(): Unit = {
    createTopic(topicKey)
    setupInputData()
    try {
      setupConnector(
        connectorKey = connectorKey,
        topicKey = topicKey,
        className = classOf[JDBCSourceConnector].getName(),
        settings = Map(
          com.island.ohara.connector.jdbc.source.DB_URL                -> JsString(url),
          com.island.ohara.connector.jdbc.source.DB_USERNAME           -> JsString(user),
          com.island.ohara.connector.jdbc.source.DB_PASSWORD           -> JsString(password),
          com.island.ohara.connector.jdbc.source.DB_TABLENAME          -> JsString(tableName),
          com.island.ohara.connector.jdbc.source.TIMESTAMP_COLUMN_NAME -> JsString(timestampColumnName),
          com.island.ohara.connector.jdbc.source.DB_SCHEMA_PATTERN     -> JsString(user),
          com.island.ohara.connector.jdbc.source.JDBC_FETCHDATA_SIZE   -> JsNumber(10000),
          com.island.ohara.connector.jdbc.source.JDBC_FLUSHDATA_SIZE   -> JsNumber(10000)
        )
      )
      sleepUntilEnd()
    } finally if (needDeleteData) client.dropTable(tableName)
  }

  override protected def sharedJars(): Set[ObjectKey] = {
    val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    val localFiles                 = new File(jarFolderPath)
    localFiles.list
      .map(fileName => {
        val jar = new File(CommonUtils.path(jarFolderPath, fileName))
        result(jarApi.request.file(jar).upload()).key
      })
      .toSet
  }

  override protected def preCreateStorage(cellNames: Seq[String]): String = {
    val client = DatabaseClient.builder.url(url).user(user).password(password).build
    try {
      val columnInfos = cellNames.zipWithIndex
        .map {
          case (columnName, index) =>
            if (index == 0) timestampColumnName
            else columnName
        }
        .map(columnName => if (!isColumnNameUpperCase) columnName.toLowerCase else columnName.toUpperCase)
        .zipWithIndex
        .map {
          case (columnName, index) =>
            if (index == 0) RdbColumn(columnName, "TIMESTAMP", true)
            else if (index == 1) RdbColumn(columnName, "VARCHAR(45)", true)
            else RdbColumn(columnName, "VARCHAR(45)", false)
        }
      client.createTable(tableName, columnInfos)
      tableName
    } finally Releasable.close(client)
  }

  override protected def writeToStorage(cellNames: Seq[String], rows: Seq[Seq[String]]): Unit = {
    val client = DatabaseClient.builder.url(url).user(user).password(password).build
    try {
      val sql = s"INSERT INTO $tableName VALUES " + cellNames
        .map(_ => "?")
        .mkString("(", ",", ")")

      val preparedStatement = client.connection.prepareStatement(sql)
      try {
        rows.foreach { row =>
          row.zipWithIndex.foreach {
            case (value, index) =>
              if (index == 0) preparedStatement.setTimestamp(index + 1, new Timestamp(value.toLong))
              else preparedStatement.setString(index + 1, value)
          }
          preparedStatement.executeUpdate()
        }
      } finally Releasable.close(preparedStatement)
    } finally Releasable.close(client)
  }

  @After
  def close(): Unit = {
    Releasable.close(client)
  }
}
