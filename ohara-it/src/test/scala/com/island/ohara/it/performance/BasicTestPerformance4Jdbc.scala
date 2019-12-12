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

import com.island.ohara.common.util.Releasable
import org.junit.Before
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.client.configurator.v0.FileInfoApi
import com.island.ohara.client.configurator.v0.InspectApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.setting.{ConnectorKey, ObjectKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.jdbc.source.JDBCSourceConnector
import org.junit.{After, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import spray.json.{JsNumber, JsString}

abstract class BasicTestPerformance4Jdbc extends BasicTestPerformance {
  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath: String  = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")

  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.jdbc.needDeleteTable"
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val numberOfProducerThread     = 4

  protected def url: String
  protected def user: String
  protected def password: String
  protected def jarName: String
  protected def productName: String
  protected def tableName: String
  protected def columnNamePrefix: String
  protected def timestampColumnName: String
  protected def insertTimestampValue: String
  private[this] var client: DatabaseClient = _

  @Before
  final def setup(): Unit = {
    client = DatabaseClient.builder.url(url).user(user).password(password).build
  }

  @Test
  def test(): Unit = {
    createTopic(topicKey)
    log.info(s"$productName databse table name is $tableName for JDBC performance test ...1")
    setupTableData(tableName)
    log.info(s"$productName databse table name is $tableName for JDBC performance test ...2")
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
    val jar                        = new File(CommonUtils.path(jarFolderPath, jarName))
    Set(result(jarApi.request.file(jar).upload()).key)
  }

  private[this] def setupTableData(tableName: String): (Long, Long) = {
    val columnSize = 5
    val columnInfos = (0 until columnSize).map(
      c =>
        if (c == 0) RdbColumn(timestampColumnName, "TIMESTAMP", false)
        else if (c == 1) RdbColumn(s"${columnNamePrefix}1", "VARCHAR(45)", true)
        else RdbColumn(s"${columnNamePrefix}${c}", "VARCHAR(45)", false)
    )
    client.createTable(tableName, columnInfos)

    val pool        = Executors.newFixedThreadPool(numberOfProducerThread)
    val closed      = new AtomicBoolean(false)
    val count       = new LongAdder()
    val sizeInBytes = new LongAdder()
    try {
      (0 until numberOfProducerThread).foreach { x =>
        pool.execute(() => {
          val client = DatabaseClient.builder.url(url).user(user).password(password).build
          try while (!closed.get() && sizeInBytes.longValue() <= sizeOfInputData) {
            val sql = s"INSERT INTO $tableName VALUES ($insertTimestampValue" + (1 until columnSize)
              .map(_ => "?")
              .mkString(",", ",", ")")
            val preparedStatement = client.connection.prepareStatement(sql)
            try {
              (1 until columnSize).foreach(i => {
                val value = s"${i}-${CommonUtils.randomString()}"
                preparedStatement.setString(i, value)
                sizeInBytes.add(value.length)
              })
              count.increment()
              preparedStatement.executeUpdate()
            } finally Releasable.close(preparedStatement)
          } finally Releasable.close(client)
        })
      }
    } finally {
      pool.shutdown()
      pool.awaitTermination(durationOfPerformance.toMillis * 10, TimeUnit.MILLISECONDS)
      closed.set(true)
    }
    (count.longValue(), sizeInBytes.longValue())
  }

  @After
  def close(): Unit = {
    Releasable.close(client)
  }
}
