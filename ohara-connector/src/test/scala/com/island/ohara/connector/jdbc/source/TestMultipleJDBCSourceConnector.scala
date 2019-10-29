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
import com.island.ohara.common.data.Serializer
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.kafka.Consumer
import com.island.ohara.kafka.connector.TaskSetting
import com.island.ohara.testing.With3Brokers3Workers
import com.island.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class TestMultipleJDBCSourceConnector extends With3Brokers3Workers with Matchers {
  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val workerClient = WorkerClient(testUtil.workersConnProps)
  private[this] val connectorKey1 = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
  private[this] val connectorKey2 = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")

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
  def testRunningTwoConnector(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      workerClient
        .connectorCreator()
        .connectorKey(connectorKey1)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(props.toMap)
        .create())

    val consumer =
      Consumer
        .builder()
        .topicName(topicKey.topicNameOnKafka)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()
    try {
      val record1 = consumer.poll(java.time.Duration.ofSeconds(30), 6).asScala
      record1.size shouldBe 6

      result(
        workerClient
          .connectorCreator()
          .connectorKey(connectorKey2)
          .connectorClass(classOf[JDBCSourceConnector])
          .topicKey(topicKey)
          .numberOfTasks(1)
          .settings(props.toMap)
          .create())

      consumer.seekToBeginning()
      val record2 = consumer.poll(java.time.Duration.ofSeconds(30), 12).asScala
      record2.size shouldBe 12

      val statement: Statement = db.connection.createStatement()
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1, column2, column3, column4) VALUES('2018-09-02 00:00:05', 'a81', 'a82', 8)")

      consumer.seekToBeginning()
      val record3 = consumer.poll(java.time.Duration.ofSeconds(30), 14).asScala
      record3.size shouldBe 14

      val expectResult: Seq[String] = Seq(
        "2018-09-01 00:00:00.0,a11,a12,1",
        "2018-09-01 00:00:01.0,a21,a22,2",
        "2018-09-01 00:00:02.0,a31,a32,3",
        "2018-09-01 00:00:03.123456,a61,a62,6",
        "2018-09-01 00:00:04.123,a71,a72,7",
        "2018-09-01 00:00:05.0,null,null,0",
        "2018-09-01 00:00:00.0,a11,a12,1",
        "2018-09-01 00:00:01.0,a21,a22,2",
        "2018-09-01 00:00:02.0,a31,a32,3",
        "2018-09-01 00:00:03.123456,a61,a62,6",
        "2018-09-01 00:00:04.123,a71,a72,7",
        "2018-09-01 00:00:05.0,null,null,0",
        "2018-09-02 00:00:05.0,a81,a82,8",
        "2018-09-02 00:00:05.0,a81,a82,8"
      )
      val resultData: Seq[String] = record3.map(x => x.key.get).map(x => x.cells().asScala.map(_.value).mkString(","))

      (0 to expectResult.size - 1).foreach(i => resultData(i) shouldBe expectResult(i))
    } finally consumer.close()
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)

  @After
  def tearDown(): Unit = {
    result(workerClient.delete(connectorKey2))
    result(workerClient.delete(connectorKey1))
    Releasable.close(client)
    Releasable.close(db)
  }

  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(DB_URL -> db.url,
          DB_USERNAME -> db.user,
          DB_PASSWORD -> db.password,
          DB_TABLENAME -> tableName,
          TIMESTAMP_COLUMN_NAME -> timestampColumnName).asJava))

}
