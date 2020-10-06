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

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Column, DataType, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class TestJDBCSourceConnectorRow extends With3Brokers3Workers {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val incrementColumnName = "column0"
  private[this] val timestampColumnName = "column1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  @BeforeEach
  def setup(): Unit = {
    client.createTable(
      tableName,
      Seq(
        RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
        RdbColumn(timestampColumnName, "TIMESTAMP", false),
        RdbColumn("column2", "longblob", false),
        RdbColumn("column3", "int", false)
      )
    )
    val statement =
      client.connection.prepareStatement(
        s"INSERT INTO $tableName($timestampColumnName, column2, column3) VALUES(?, ?, ?)"
      )
    try {
      (1 to 10).foreach { _ =>
        statement.setTimestamp(1, new Timestamp(CommonUtils.current() - 432000000))
        statement.setBytes(2, "HELLO WORLD".getBytes)
        statement.setInt(3, 5)
        statement.executeUpdate()
      }
    } finally Releasable.close(statement)
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val columns = Seq(
      Column.builder().name(incrementColumnName).dataType(DataType.INT).newName("COLUMN0").order(0).build(),
      Column.builder().name(timestampColumnName).dataType(DataType.OBJECT).newName("COLUMN1").order(1).build(),
      //Column.builder().name("column2").dataType(DataType.BYTES).newName("COLUMN2").order(2).build(),
      Column.builder().name("column3").dataType(DataType.LONG).newName("COLUMN3").order(3).build()
    )

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .columns(columns)
        .create()
    )

    val record = pollData(topicKey, Duration(30, TimeUnit.SECONDS), 10)
    println(s"RECORD SIZE IS: ${record.size}")
    record.head.key.get.cell("COLUMN3").value.isInstanceOf[Long] shouldBe true
  }

  private[this] def pollData(
    topicKey: TopicKey,
    timeout: scala.concurrent.duration.Duration,
    size: Int
  ): Seq[Record[Row, Array[Byte]]] = {
    val consumer = Consumer
      .builder()
      .topicKey(topicKey)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try consumer.poll(java.time.Duration.ofNanos(timeout.toNanos), size).asScala.toSeq
    finally consumer.close()
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(10, TimeUnit.SECONDS))

  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL_KEY                -> db.url,
        DB_USERNAME_KEY           -> db.user,
        DB_PASSWORD_KEY           -> db.password,
        DB_TABLENAME_KEY          -> tableName,
        TIMESTAMP_COLUMN_NAME_KEY -> timestampColumnName
      ).asJava
    )
  )
  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
