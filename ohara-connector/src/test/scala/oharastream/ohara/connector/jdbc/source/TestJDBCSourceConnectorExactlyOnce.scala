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
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.concurrent.duration._
import org.junit.{After, Test}
import org.scalatest.Matchers._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

@RunWith(value = classOf[Parameterized])
class TestJDBCSourceConnectorExactlyOnce(inputDataTime: Long) extends With3Brokers3Workers {
  private[this] val durationTime: Long  = inputDataTime
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "c0"
  private[this] val columnSize          = 3
  private[this] val columns = Seq(RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)) ++
    (1 to columnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", true)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val tableTotalCount = new LongAdder()
  private[this] val connectorAdmin  = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def createTable(): Unit = {
    client.createTable(tableName, columns)
  }

  private[this] val inputDataThread: Releasable = {
    val pool            = Executors.newSingleThreadExecutor()
    val startTime: Long = CommonUtils.current()
    pool.execute(() => {
      if (client.tables().map(_.name).find(_ == tableName).isEmpty) createTable()

      val sql               = s"INSERT INTO $tableName VALUES (${columns.map(_ => "?").mkString(",")})"
      val preparedStatement = client.connection.prepareStatement(sql)
      try {
        while ((CommonUtils.current() - startTime) <= durationTime) {
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
      pool.awaitTermination(durationTime, TimeUnit.SECONDS)
    }
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(jdbcSourceConnectorProps.toMap)
        .create()
    )

    TimeUnit.SECONDS.sleep(5)
    val consumer =
      Consumer
        .builder()
        .topicName(topicKey.topicNameOnKafka)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()

    val result1 = consumer.poll(java.time.Duration.ofSeconds(10), tableTotalCount.intValue()).asScala
    tableTotalCount.intValue() >= result1.size shouldBe true

    result(connectorAdmin.pause(connectorKey))
    result(connectorAdmin.resume(connectorKey))
    TimeUnit.SECONDS.sleep(5)

    consumer.seekToBeginning()
    val result2 = consumer.poll(java.time.Duration.ofSeconds(10), tableTotalCount.intValue()).asScala
    result2.size >= result1.size shouldBe true

    result(connectorAdmin.delete(connectorKey))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(jdbcSourceConnectorProps.toMap)
        .create()
    )
    TimeUnit.SECONDS.sleep(2)
    result(connectorAdmin.delete(connectorKey))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(jdbcSourceConnectorProps.toMap)
        .create()
    )

    // Check the table and topic data size
    TimeUnit.MILLISECONDS.sleep(durationTime)
    consumer.seekToBeginning() //Reset consumer
    val result3 = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
    tableTotalCount.intValue() shouldBe result3.size

    // Check the topic data is equals the database table
    val statement = client.connection.createStatement()
    try {
      val queryColumn = "c1"
      val resultSet   = statement.executeQuery(s"select * from $tableName order by $queryColumn")

      val tableData: Seq[String] = Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(2)).toSeq
      val topicData: Seq[String] = result3
        .map(record => record.key.get.cell(queryColumn).value().toString)
        .sorted[String]

      tableData.zipWithIndex.foreach {
        case (record, index) => {
          record shouldBe topicData(index)
        }
      }

      val result = statement.executeQuery(s"select * from $tableName where $queryColumn='${tableData.head}'")
      val queryResult = Iterator
        .continually(result)
        .takeWhile(_.next())
        .map(record => {
          val timestamp = record.getString(timestampColumnName)
          val column1   = record.getString(queryColumn)
          (timestamp, column1)
        })
        .toSeq
        .head
      statement.executeUpdate(s"DELETE FROM $tableName WHERE $queryColumn='${queryResult._2}'")
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES('${queryResult._1}', 'hello')"
      )
      consumer.seekToBeginning() //Reset consumer
      val result4 = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
      tableTotalCount.intValue() shouldBe result4.size

      statement.executeUpdate(s"UPDATE $tableName SET $timestampColumnName=NOW() WHERE $queryColumn='hello'")
      statement.executeUpdate(s"INSERT INTO $tableName($timestampColumnName, $queryColumn) VALUES(NOW(), 'aaaaaa')")
      TimeUnit.SECONDS.sleep(5)
      consumer.seekToBeginning() //Reset consumer
      val result5 = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
      result5.size shouldBe tableTotalCount.intValue() + 2 // Because update and insert the now timestamp
    } finally Releasable.close(statement)
  }

  private[this] def rowData(): Row = {
    Row.of(
      (1 to columnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  private[this] val jdbcSourceConnectorProps = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL                -> db.url,
        DB_USERNAME           -> db.user,
        DB_PASSWORD           -> db.password,
        DB_TABLENAME          -> tableName,
        TIMESTAMP_COLUMN_NAME -> timestampColumnName
      ).asJava
    )
  )

  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)

  @After
  def after(): Unit = {
    Releasable.close(inputDataThread)
    Releasable.close(client)
    Releasable.close(db)
  }
}

object TestJDBCSourceConnectorExactlyOnce {
  @Parameters(name = "{index} test input data time is {0} MILLISECONDS")
  def parameters(): java.util.Collection[Long] = {
    Seq(3000L, 30000L).asJava
  }
}
