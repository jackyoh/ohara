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
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.common.util.Releasable
import com.island.ohara.client.configurator.v0.FileInfoApi
import com.island.ohara.client.configurator.v0.InspectApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.setting.{ConnectorKey, ObjectKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.jdbc.source.JDBCSourceConnector
import com.island.ohara.it.category.PerformanceGroup
import org.junit.{After, AssumptionViolatedException, Test}
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Jdbc extends BasicTestPerformance {
  private[this] val JAR_FOLDER_KEY: String   = "ohara.it.jar.folder"
  private[this] val DB_URL_KEY: String       = "ohara.it.oracle.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.oracle.db.username"
  private[this] val DB_PASSWORD_KEY: String  = "ohara.it.oracle.db.password"

  private[this] val jarFolderPath: String = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")

  private[this] val url: String =
    sys.env.getOrElse(DB_URL_KEY, throw new AssumptionViolatedException(s"$DB_URL_KEY does not exists!!!"))
  private[this] val user: String =
    sys.env.getOrElse(DB_USER_NAME_KEY, throw new AssumptionViolatedException(s"$DB_USER_NAME_KEY does not exists!!!"))
  private[this] val password: String =
    sys.env.getOrElse(DB_PASSWORD_KEY, throw new AssumptionViolatedException(s"$DB_PASSWORD_KEY does not exists!!!"))

  private[this] val timestampColumnName: String = "COLUMN0"
  private[this] val client                      = DatabaseClient.builder.url(url).user(user).password(password).build

  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  private[this] val numberOfProducerThread = 4

  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.jdbc.needDeleteTable"
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  @Test
  def test(): Unit = {
    createTopic(topicKey)
    val (tableName, _, _) = setupTableData()
    log.info(s"Oracle databse table name is ${tableName} for JDBC performance test")
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
    val jar                        = new File(CommonUtils.path(jarFolderPath, "ojdbc8.jar"))
    Set(result(jarApi.request.file(jar).upload()).key)
  }

  private[this] def setupTableData(): (String, Long, Long) = {
    val tableName: String = s"TABLE${CommonUtils.randomString().toUpperCase()}"
    val columnSize        = 5
    val columnInfos = (0 until columnSize).map(
      c =>
        if (c == 0) RdbColumn(timestampColumnName, "TIMESTAMP", false)
        else if (c == 1) RdbColumn("COLUMN1", "VARCHAR(45)", true)
        else RdbColumn(s"COLUMN${c}", "VARCHAR(45)", false)
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
            val sql = s"INSERT INTO $tableName VALUES (TO_TIMESTAMP('2018-09-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" + (1 until columnSize)
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
    (tableName, count.longValue(), sizeInBytes.longValue())
  }

  @After
  def close(): Unit = {
    Releasable.close(client)
  }
}
