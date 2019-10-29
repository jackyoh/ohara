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

package com.island.ohara.connector.jdbc.source

import java.sql.Statement

import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.data.{Cell, Row, Serializer}
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.kafka.Consumer
import com.island.ohara.kafka.Consumer.Record
import com.island.ohara.kafka.connector.TaskSetting
import com.island.ohara.testing.With3Brokers3Workers
import com.island.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Test the JDBC Source Connector
  */
class TestJDBCSourceConnector extends With3Brokers3Workers with Matchers {
  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val workerClient = WorkerClient(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn("column1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("column2", "varchar(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:03.123456', 'a61', 'a62', 6)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:04.123', 'a71', 'a72', 7)")
    statement.executeUpdate(s"INSERT INTO $tableName(column1) VALUES('2018-09-01 00:00:05')")

    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 3 MINUTE, 'a41', 'a42', 4)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)")
  }

  @Test
  def testJDBCSourceConnector(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      workerClient
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(props.toMap)
        .create())
    try {

      val record = pollData(topicKey, 30 seconds, 6)

      val row0: Row = record.head.key.get
      row0.size shouldBe 4
      row0.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:00.0").toString
      row0.cell(1) shouldBe Cell.of("column2", "a11")
      row0.cell(2) shouldBe Cell.of("column3", "a12")
      row0.cell(3).toString shouldBe Cell.of("column4", "1").toString

      val row1: Row = record(1).key.get
      row1.size shouldBe 4
      row1.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:01.0").toString
      row1.cell(1) shouldBe Cell.of("column2", "a21")
      row1.cell(2) shouldBe Cell.of("column3", "a22")
      row1.cell(3).toString shouldBe Cell.of("column4", "2").toString

      val row2: Row = record(2).key.get
      row2.size shouldBe 4
      row2.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:02.0").toString
      row2.cell(1) shouldBe Cell.of("column2", "a31")
      row2.cell(2) shouldBe Cell.of("column3", "a32")
      row2.cell(3).toString shouldBe Cell.of("column4", "3").toString

      val row3: Row = record(3).key.get
      row3.size shouldBe 4
      row3.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:03.123456").toString
      row3.cell(1) shouldBe Cell.of("column2", "a61")
      row3.cell(2) shouldBe Cell.of("column3", "a62")
      row3.cell(3).toString shouldBe Cell.of("column4", "6").toString

      val row4: Row = record(4).key.get
      row4.size shouldBe 4
      row4.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:04.123").toString
      row4.cell(1) shouldBe Cell.of("column2", "a71")
      row4.cell(2) shouldBe Cell.of("column3", "a72")
      row4.cell(3).toString shouldBe Cell.of("column4", "7").toString

      val row5: Row = record(5).key.get
      row5.size shouldBe 4
      row5.cell(0).toString shouldBe Cell.of("column1", "2018-09-01 00:00:05.0").toString
      row5.cell(1) shouldBe Cell.of("column2", "null")
      row5.cell(2) shouldBe Cell.of("column3", "null")
      row5.cell(3).toString shouldBe Cell.of("column4", "0").toString
      record.size shouldBe 6
    } finally result(workerClient.delete(connectorKey))
  }

  @Test
  def testTimestampColumnNameEmpty(): Unit = {
    val jdbcSourceConnector: JDBCSourceConnector = new JDBCSourceConnector()

    intercept[NoSuchElementException] {
      jdbcSourceConnector.checkTimestampColumnName("")
    }
  }

  @Test
  def testTimestampColumnNameNull(): Unit = {
    val jdbcSourceConnector: JDBCSourceConnector = new JDBCSourceConnector()

    intercept[NoSuchElementException] {
      jdbcSourceConnector.checkTimestampColumnName(null)
    }
  }

  @Test
  def testTimestampColumnName(): Unit = {
    val jdbcSourceConnector: JDBCSourceConnector = new JDBCSourceConnector()
    jdbcSourceConnector.checkTimestampColumnName("column1")
    jdbcSourceConnector.checkTimestampColumnName("Column1col1")
    jdbcSourceConnector.checkTimestampColumnName("col1")
    jdbcSourceConnector.checkTimestampColumnName("col-1")

    //Input error column name
    intercept[IllegalArgumentException] {
      jdbcSourceConnector.checkTimestampColumnName("1COLUMN1")
    }

    intercept[IllegalArgumentException] {
      jdbcSourceConnector.checkTimestampColumnName("100col")
    }
  }

  private[this] def pollData(topicKey: TopicKey,
                             timeout: scala.concurrent.duration.Duration,
                             size: Int): Seq[Record[Row, Array[Byte]]] = {
    val consumer = Consumer
      .builder()
      .topicName(topicKey.topicNameOnKafka)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try consumer.poll(java.time.Duration.ofNanos(timeout.toNanos), size).asScala
    finally consumer.close()
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)

  @After
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }

  import scala.collection.JavaConverters._
  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(DB_URL -> db.url,
          DB_USERNAME -> db.user,
          DB_PASSWORD -> db.password,
          DB_TABLENAME -> tableName,
          TIMESTAMP_COLUMN_NAME -> timestampColumnName).asJava))
}
