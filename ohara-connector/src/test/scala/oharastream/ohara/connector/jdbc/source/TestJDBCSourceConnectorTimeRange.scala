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
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.client.kafka.WorkerJson.ConnectorCreationResponse
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

@RunWith(value = classOf[Parameterized])
class TestJDBCSourceConnectorTimeRange(parameter: TimeRangeParameter) extends With3Brokers3Workers {
  private[this] var startTimestamp     = parameter.startTimestamp
  private[this] val stopTimestamp      = parameter.stopTimestamp
  private[this] val incrementTimestamp = parameter.increment

  protected[this] val db: Database        = Database.local()
  protected[this] val tableName           = "table1"
  protected[this] val timestampColumnName = "c0"

  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val beginIndex      = 1
  private[this] val totalColumnSize = 3
  private[this] val columns = Seq(
    RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
  ) ++
    (beginIndex to totalColumnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", true)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    client.createTable(tableName, columns)
    insertData(startTimestamp.getTime().to(stopTimestamp.getTime()).by(incrementTimestamp).map { value =>
      new Timestamp(value)
    })
  }

  @Test
  def testConnector(): Unit = {
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
      val records1 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala
      records1.size shouldBe tableCurrentTimeResultCount()

      TimeUnit.SECONDS.sleep(10)

      insertData((1 to 100).map { _ =>
        new Timestamp(CommonUtils.current())
      })

      consumer.seekToBeginning()
      val records2 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala
      records2.size shouldBe tableCurrentTimeResultCount()

      TimeUnit.SECONDS.sleep(10)
      insertData(Seq(new Timestamp(CommonUtils.current())))
      consumer.seekToBeginning()
      val records3 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala

      tableData(
        records3
          .map { record =>
            record.key().get().cell(timestampColumnName).value().toString()
          }
          .sorted[String]
          .toSeq
      )
    } finally Releasable.close(consumer)
  }

  private[this] def tableCurrentTimeResultCount(): Int = {
    val preparedStatement =
      client.connection.prepareStatement(s"SELECT count(*) FROM $tableName WHERE $timestampColumnName <= ?")
    preparedStatement.setTimestamp(1, new Timestamp(CommonUtils.current()))

    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getInt(1)
        else 0
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def tableData(topicRecords: Seq[String]): Unit = {
    val preparedStatement = client.connection.prepareStatement(
      s"SELECT * FROM $tableName WHERE $timestampColumnName <= ? ORDER BY $timestampColumnName"
    )
    preparedStatement.setTimestamp(1, new Timestamp(CommonUtils.current()))

    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        Iterator
          .continually(resultSet)
          .takeWhile(_.next())
          .zipWithIndex
          .foreach {
            case (result, index) =>
              result.getTimestamp(timestampColumnName).toString() shouldBe topicRecords(index)
          }
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def insertData(timestamps: Seq[Timestamp]): Unit = {
    val sql               = s"INSERT INTO $tableName($timestampColumnName, c1, c2, c3) VALUES (?, ?, ?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      timestamps.foreach { timestamp =>
        preparedStatement.setTimestamp(1, timestamp)
        rowData().asScala.zipWithIndex.foreach {
          case (result, index) => {
            preparedStatement.setString(index + 2, result.value().toString)
          }
        }
        preparedStatement.execute()
      }
    } finally Releasable.close(preparedStatement)
  }

  @After
  def after(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
    }
    Releasable.close(client)
    Releasable.close(db)
  }

  private[this] def rowData(): Row = {
    Row.of(
      (beginIndex to totalColumnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

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
      .numberOfTasks(parameter.taskNumber)
      .settings(jdbcSourceConnectorProps.toMap)
      .create()
  }

  protected[this] def jdbcSourceConnectorProps: JDBCSourceConnectorConfig = {
    JDBCSourceConnectorConfig(
      TaskSetting.of(
        Map(
          DB_URL                -> db.url,
          DB_USERNAME           -> db.user,
          DB_PASSWORD           -> db.password,
          DB_TABLENAME          -> tableName,
          TIMESTAMP_COLUMN_NAME -> timestampColumnName,
          TASK_TOTAL_KEY        -> "0",
          TASK_HASH_KEY         -> "0"
        ).asJava
      )
    )
  }
}

object TestJDBCSourceConnectorTimeRange {
  @Parameters(name = "{index} test, injected_args: {0}")
  def parameters(): java.util.Collection[TimeRangeParameter] = {
    val FIVE_DAY_TIMESTAMP = 432000000 // 5 * 24 * 60 * 60 * 1000 => 5 day
    val ONE_DAY_TIMESTAMP  = 86400000  // 1 * 24 * 60 * 60 * 1000 => 1 day
    val ONE_HOUR_TIMESTAMP = 3600000   // 60 * 60 * 1000 => 1 hour

    (1 to 3)
      .map { taskNumber =>
        Seq(
          // For test the from 5 day ago to 1 day ago data
          TimeRangeParameter(
            new Timestamp(CommonUtils.current() - FIVE_DAY_TIMESTAMP),
            new Timestamp(CommonUtils.current() - ONE_DAY_TIMESTAMP),
            ONE_HOUR_TIMESTAMP,
            s"Number of tasks: $taskNumber, Less current timestamp",
            taskNumber
          ),
          // For test the from 5 day ago to current time
          TimeRangeParameter(
            new Timestamp(CommonUtils.current() - FIVE_DAY_TIMESTAMP),
            new Timestamp(CommonUtils.current()),
            ONE_HOUR_TIMESTAMP,
            s"Number of tasks: $taskNumber, Equals current timestamp",
            taskNumber
          ),
          // For the the from 5 day ago to 5 day later
          TimeRangeParameter(
            new Timestamp(CommonUtils.current() - FIVE_DAY_TIMESTAMP),
            new Timestamp(CommonUtils.current() + FIVE_DAY_TIMESTAMP),
            ONE_HOUR_TIMESTAMP,
            s"Number of tasks: $taskNumber, more than the current timestamp",
            taskNumber
          )
        )
      }
      .flatten
      .asJava
  }
}

case class TimeRangeParameter(
  startTimestamp: Timestamp,
  stopTimestamp: Timestamp,
  increment: Int,
  describe: String,
  taskNumber: Int
) {
  override def toString(): String = describe
}
