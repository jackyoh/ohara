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

package oharastream.ohara.connector.jdbc.source

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.client.kafka.WorkerJson.ConnectorCreationResponse
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class TestJDBCSourceConnectorError extends With3Brokers3Workers {
  private[this] val db: Database = Database.local()
  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val incrementColumnName = "increment"
  private[this] val timestampColumnName = "c0"
  private[this] val queryColumn         = "c1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  @BeforeEach
  def setup(): Unit = {
    val columns = Seq(
      RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
      RdbColumn(timestampColumnName, "TIMESTAMP(6)", false),
      RdbColumn(queryColumn, "VARCHAR(45)", false)
    )
    client.createTable(tableName, columns)
    val statement = client.connection.createStatement()
    try {
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-12 14:20:56.648', 'aaa')"
      )
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-12 14:20:56.656', 'bbb')"
      )
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-12 14:20:57.062', 'ccc')"
      )
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-12 14:20:58.388', 'ddd')"
      )
    } finally Releasable.close(statement)
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(createConnector(connectorAdmin, connectorKey, topicKey))
    val consumer =
      Consumer
        .builder()
        .topicKey(topicKey)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()
    try {
      val statement = client.connection.createStatement()
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-17 14:20:59.0', 'eee')"
      )
      statement.execute(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('2020-09-17 14:21:04.0', 'fff')"
      )
      val resultData = consumer.poll(java.time.Duration.ofSeconds(20), 6).asScala
      println(s"RESULT SIZE: ${resultData.size}")
    } finally Releasable.close(consumer)
  }

  private[this] def createConnector(
    connectorAdmin: ConnectorAdmin,
    connectorKey: ConnectorKey,
    topicKey: TopicKey
  ): Future[ConnectorCreationResponse] = {
    connectorAdmin
      .connectorCreator()
      .connectorKey(connectorKey)
      .connectorClass(classOf[JDBCSourceConnector])
      .topicKey(topicKey)
      .numberOfTasks(3)
      .settings(sourceConnectorProps.toMap)
      .create()
  }

  private[this] val sourceConnectorProps = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL_KEY                -> db.url,
        DB_USERNAME_KEY           -> db.user,
        DB_PASSWORD_KEY           -> db.password,
        DB_TABLENAME_KEY          -> tableName,
        TIMESTAMP_COLUMN_NAME_KEY -> timestampColumnName,
        INCREMENT_COLUMN_NAME_KEY -> incrementColumnName
      ).asJava
    )
  )
  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))
}
