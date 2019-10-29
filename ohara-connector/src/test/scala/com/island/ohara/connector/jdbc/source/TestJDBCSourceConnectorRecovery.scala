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
import com.island.ohara.common.data.{Row, Serializer}
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.kafka.Consumer
import com.island.ohara.kafka.connector.TaskSetting
import com.island.ohara.testing.With3Brokers3Workers
import com.island.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TestJDBCSourceConnectorRecovery extends With3Brokers3Workers with Matchers {
  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val workerClient = WorkerClient(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
    val column2 = RdbColumn("column2", "VARCHAR(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", true)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    (1 to 1000).foreach(x => {
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,column2,column3,column4) VALUES('2018-09-01 00:00:00', 'a${x}-1', 'a${x}-2', ${x})")
    })
  }

  @Test
  def testRecovery(): Unit = {
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
        .create()
    )
    try {
      val consumer = Consumer
        .builder()
        .topicName(topicKey.topicNameOnKafka)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()

      try {
        val poll1 = consumer.poll(java.time.Duration.ofSeconds(30), 1).asScala

        poll1.size < 1000 shouldBe true

        //Pause JDBC Source Connector
        result(workerClient.pause(connectorKey))

        val row0: Row = poll1.head.key.get
        row0.cell(0).name shouldBe "column1"
        row0.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"

        //Confirm topic data is zero
        val poll2 = consumer.poll(java.time.Duration.ofSeconds(1), 0).asScala
        poll2.isEmpty shouldBe true

        //Insert Data before resuming connector
        val statement: Statement = db.connection.createStatement()

        statement.executeUpdate(
          s"INSERT INTO $tableName($timestampColumnName,column2,column3,column4) VALUES('2018-09-01 01:00:00', 'a1001-1', 'a1001-2', 1001)")

        //Resume JDBC Source Connector
        result(workerClient.resume(connectorKey))

        consumer.seekToBeginning() //Reset consumer

        val poll3 = consumer.poll(java.time.Duration.ofSeconds(60), 1001).asScala
        poll3.size shouldBe 1001

        poll3.head.key.get.cell(1).name shouldBe "column2"
        poll3.head.key.get.cell(1).value shouldBe "a1-1"

        poll3(500).key.get.cell(1).name shouldBe "column2"
        poll3(500).key.get.cell(1).value shouldBe "a501-1"

        poll3(1000).key.get.cell(1).name shouldBe "column2"
        poll3.last.key.get.cell(1).name shouldBe "column2"
        poll3.last.key.get.cell(1).value shouldBe "a1001-1"

        //Delete JDBC Source Connector
        result(workerClient.delete(connectorKey))

        val poll4 = consumer.poll(java.time.Duration.ofSeconds(1), 0).asScala
        poll4.isEmpty shouldBe true

        //Create JDBC Source Connector
        result(
          workerClient
            .connectorCreator()
            .connectorKey(connectorKey)
            .connectorClass(classOf[JDBCSourceConnector])
            .topicKey(topicKey)
            .numberOfTasks(1)
            .settings(props.toMap)
            .create()
        )
        statement.executeUpdate(
          s"INSERT INTO $tableName($timestampColumnName,column2,column3,column4) VALUES('2018-09-02 00:00:01', 'a1002-1', 'a1002-2', 1002)")

        //Get all topic data for test
        consumer.seekToBeginning() //Reset consumer
        val poll5 = consumer.poll(java.time.Duration.ofSeconds(30), 1002).asScala
        poll5.size shouldBe 1002
        poll5.last.key.get.cell(1).name shouldBe "column2"
        poll5.last.key.get.cell(1).value shouldBe "a1002-1"

        poll5.last.key.get.cell(2).name shouldBe "column3"
        poll5.last.key.get.cell(2).value shouldBe "a1002-2"

        poll5(1000).key.get.cell(2).name shouldBe "column3"
        poll5(1000).key.get.cell(2).value shouldBe "a1001-2"

        poll5(1001).key.get.cell(2).value shouldBe "a1002-2"

        consumer.seekToBeginning() //Reset consumer
        val poll6 = consumer.poll(java.time.Duration.ofSeconds(30), 1002).asScala
        poll6.size shouldBe 1002
      } finally consumer.close
    } finally result(workerClient.delete(connectorKey))
  }

  @After
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }

  import scala.collection.JavaConverters._
  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(Map(
      DB_URL -> db.url,
      DB_USERNAME -> db.user,
      DB_PASSWORD -> db.password,
      DB_TABLENAME -> tableName,
      TIMESTAMP_COLUMN_NAME -> timestampColumnName,
      JDBC_FETCHDATA_SIZE -> "1",
      JDBC_FLUSHDATA_SIZE -> "1"
    ).asJava))
  private[this] def result[T](f: Future[T]): T = Await.result(f, 30 seconds)
}
