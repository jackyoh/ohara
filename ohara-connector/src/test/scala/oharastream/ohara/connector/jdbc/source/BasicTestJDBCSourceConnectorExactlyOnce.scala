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

import java.sql.{Statement, Timestamp}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.client.kafka.WorkerJson.ConnectorCreationResponse
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Test}
import org.junit.runners.Parameterized.Parameters

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import org.scalatest.matchers.should.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(value = classOf[Parameterized])
abstract class BasicTestJDBCSourceConnectorExactlyOnce(inputDataTime: Long) extends With3Brokers3Workers {
  protected[this] val db: Database        = Database.local()
  protected[this] val tableName           = "table1"
  protected[this] val timestampColumnName = "timestamp"
  protected[this] val incrementColumnName = "increment"
  protected[this] def jdbcSourceConnectorProps: JDBCSourceConnectorConfig

  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build

  private[this] val queryColumn = "c1"
  private[this] val columnSize  = 3
  private[this] val columns = Seq(RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)) ++
    (1 to columnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", false)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val tableTotalCount: LongAdder = new LongAdder()
  private[this] val connectorAdmin             = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def createTable(): Unit = {
    client.createTable(
      tableName,
      Seq(RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true)) ++ columns
    )
  }

  private[this] val inputDataThread: Releasable = {
    val pool            = Executors.newSingleThreadExecutor()
    val startTime: Long = CommonUtils.current()
    pool.execute(() => {
      if (client.tables().map(_.name).find(_ == tableName).isEmpty) createTable()

      val sql =
        s"INSERT INTO $tableName($timestampColumnName, c1, c2, c3) VALUES (${columns.map(_ => "?").mkString(",")})"
      val preparedStatement = client.connection.prepareStatement(sql)
      try {
        while ((CommonUtils.current() - startTime) <= inputDataTime) {
          // 432000000 is 5 days ago
          val timestampData = new Timestamp(CommonUtils.current() - 432000000 + tableTotalCount.intValue())
          preparedStatement.setTimestamp(1, timestampData)
          rowData().asScala.zipWithIndex.foreach {
            case (result, index) => {
              preparedStatement.setString(index + 2, result.value().toString)
            }
          }
          preparedStatement.execute()
          tableTotalCount.add(1)
        }
      } finally Releasable.close(preparedStatement)
    })
    () => {
      pool.shutdown()
      pool.awaitTermination(inputDataTime, TimeUnit.SECONDS)
    }
  }
  @Test
  def testConnectorStartPauseResume(): Unit = {
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
    val statement = client.connection.createStatement()

    try {
      val records = consumer.poll(java.time.Duration.ofSeconds(5), tableTotalCount.intValue()).asScala
      tableTotalCount.intValue() >= records.size shouldBe true

      result(connectorAdmin.pause(connectorKey))
      result(connectorAdmin.resume(connectorKey))

      consumer.seekToBeginning()
      TimeUnit.MILLISECONDS.sleep(inputDataTime) // Finally to wait all data write the database table
      val resultRecords = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
      resultRecords.size shouldBe tableTotalCount.intValue()

      // Check the topic data is equals the database table
      val resultSet              = statement.executeQuery(s"select * from $tableName order by $queryColumn")
      val tableData: Seq[String] = Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(3)).toSeq
      val topicData: Seq[String] = resultRecords
        .map(record => record.key.get.cell(queryColumn).value().toString)
        .sorted[String]
        .toSeq
      checkData(tableData, topicData)
    } finally {
      result(connectorAdmin.delete(connectorKey)) // Avoid table not forund from the JDBC source connector
      Releasable.close(statement)
      Releasable.close(consumer)
    }
  }

  @Test
  def testConnectorStartDelete(): Unit = {
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
    val statement = client.connection.createStatement()
    try {
      val records1 = consumer.poll(java.time.Duration.ofSeconds(5), tableTotalCount.intValue()).asScala
      tableTotalCount.intValue() >= records1.size shouldBe true

      result(connectorAdmin.delete(connectorKey))
      result(createConnector(connectorAdmin, connectorKey, topicKey))
      TimeUnit.SECONDS.sleep(5)
      val records2 = consumer.poll(java.time.Duration.ofSeconds(5), tableTotalCount.intValue()).asScala
      tableTotalCount.intValue() >= records2.size shouldBe true

      result(connectorAdmin.delete(connectorKey))
      result(createConnector(connectorAdmin, connectorKey, topicKey))

      consumer.seekToBeginning()
      TimeUnit.MILLISECONDS.sleep(inputDataTime) // Finally to wait all data write the database table
      val resultRecords = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
      resultRecords.size shouldBe tableTotalCount.intValue()

      // Check the topic data is equals the database table
      val resultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")

      val tableData: Seq[String] = Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(3)).toSeq
      val topicData: Seq[String] = resultRecords
        .map(record => record.key.get.cell(queryColumn).value().toString)
        .sorted[String]
        .toSeq
      checkData(tableData, topicData)
    } finally {
      result(connectorAdmin.delete(connectorKey)) // Avoid table not forund from the JDBC source connector
      Releasable.close(statement)
      Releasable.close(consumer)
    }
  }

  @Test
  def testTableInsertDelete(): Unit = {
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
    val statement = client.connection.createStatement()
    try {
      val resultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")
      val queryResult: String = Iterator
        .continually(resultSet)
        .takeWhile(_.next())
        .map(_.getString(queryColumn))
        .toSeq
        .head
      TimeUnit.MILLISECONDS.sleep(inputDataTime) // Wait thread all data write to the table
      statement.executeUpdate(s"DELETE FROM $tableName WHERE $queryColumn='${queryResult}'")
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES(NOW(), '${queryResult}')"
      )
      TimeUnit.MILLISECONDS.sleep(10)
      val result = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue() + 1).asScala
      tableTotalCount.intValue() + 1 shouldBe result.size

      val topicData: Seq[String] = result
        .map(record => record.key.get.cell(queryColumn).value().toString)
        .sorted[String]
        .toSeq
      val updateResultSet = statement.executeQuery(s"select * from $tableName order by $timestampColumnName")
      val resultTableData: Seq[String] =
        Iterator.continually(updateResultSet).takeWhile(_.next()).map(_.getString(queryColumn)).toSeq
      checkData(resultTableData, topicData)
    } finally {
      result(connectorAdmin.delete(connectorKey)) // Avoid table not forund from the JDBC source connector
      Releasable.close(statement)
      Releasable.close(consumer)
    }
  }

  @Test
  def testTableUpdate(): Unit = {
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
    val statement = client.connection.createStatement()
    try {
      TimeUnit.MILLISECONDS.sleep(inputDataTime) // Wait thread all data write to the table
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES(NOW(), 'hello1')"
      )
      TimeUnit.SECONDS.sleep(5)
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES(NOW(), 'hello2')"
      )
      statement.executeUpdate(s"UPDATE $tableName SET $timestampColumnName = NOW() WHERE $queryColumn = 'hello2'")

      val expectedRow = tableTotalCount.intValue() + 2
      val result      = consumer.poll(java.time.Duration.ofSeconds(30), expectedRow).asScala
      result.size shouldBe expectedRow // Because update and insert the different timestamp
    } finally {
      result(connectorAdmin.delete(connectorKey)) // Avoid table not forund from the JDBC source connector
      Releasable.close(statement)
      Releasable.close(consumer)
    }
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
      .settings(jdbcSourceConnectorProps.toMap)
      .create()
  }

  private[this] def checkData(tableData: Seq[String], topicData: Seq[String]): Unit = {
    tableData.foreach(topicData.contains(_) shouldBe true)
  }

  private[this] def rowData(): Row = {
    Row.of(
      (1 to columnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  @After
  def after(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
    }
    Releasable.close(inputDataThread)
    Releasable.close(client)
    Releasable.close(db)
  }
}

object BasicTestJDBCSourceConnectorExactlyOnce {
  @Parameters(name = "{index} test input data time is {0} MILLISECONDS")
  def parameters(): java.util.Collection[Long] = {
    Seq(3000L, 30000L).asJava
  }
}
