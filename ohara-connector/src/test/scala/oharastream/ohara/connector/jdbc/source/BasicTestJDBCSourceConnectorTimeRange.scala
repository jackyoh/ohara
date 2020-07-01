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
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@RunWith(value = classOf[Parameterized])
abstract class BasicTestJDBCSourceConnectorTimeRange(timestampInfo: TimestampInfo) extends With3Brokers3Workers {
  private[this] var startTimestamp              = timestampInfo.startTimestamp
  private[this] val stopTimestamp               = timestampInfo.stopTimestamp
  private[this] val incrementTimestamp          = timestampInfo.increment
  private[this] val currentTimestamp: Timestamp = new Timestamp(CommonUtils.current())

  protected[this] val db: Database        = Database.local()
  protected[this] val tableName           = "table1"
  protected[this] val incrementColumnName = "c0"
  protected[this] val timestampColumnName = "c1"

  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val beginIndex      = 2
  private[this] val totalColumnSize = 4
  private[this] val columns = Seq(
    RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
    RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
  ) ++
    (beginIndex to totalColumnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", false)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    client.createTable(tableName, columns)
    val sql               = s"INSERT INTO $tableName($timestampColumnName, c2, c3, c4) VALUES (?, ?, ?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      while (startTimestamp.getTime() < stopTimestamp.getTime()) {
        startTimestamp = new Timestamp(startTimestamp.getTime() + incrementTimestamp)
        preparedStatement.setTimestamp(1, startTimestamp)

        rowData().asScala.zipWithIndex.foreach {
          case (result, index) => {
            preparedStatement.setString(index + 2, result.value().toString)
          }
        }
        preparedStatement.execute()
      }
    } finally Releasable.close(preparedStatement)
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
      val tableCurrentTimestampCount = tableCurrentTimeResultCount()
      val records                    = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimestampCount).asScala
      records.size shouldBe tableCurrentTimestampCount
      checkData(
        records
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
    preparedStatement.setTimestamp(1, currentTimestamp)
    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getInt(1)
        else 0
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def checkData(topicRecords: Seq[String]): Unit = {
    val preparedStatement = client.connection.prepareStatement(
      s"SELECT * FROM $tableName WHERE $timestampColumnName <= ? ORDER BY $timestampColumnName"
    )
    preparedStatement.setTimestamp(1, currentTimestamp)

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
      .numberOfTasks(3)
      .settings(jdbcSourceConnectorProps.toMap)
      .create()
  }

  protected[this] def jdbcSourceConnectorProps: JDBCSourceConnectorConfig
}

object BasicTestJDBCSourceConnectorTimeRange {
  @Parameters(name = "{index} test, test case is {0}")
  def parameters(): java.util.Collection[TimestampInfo] = {
    val ltCurrentTimestamp = TimestampInfo(
      new Timestamp(CommonUtils.current() - 432000000),
      new Timestamp(CommonUtils.current() - 86400000),
      3600000,
      "Less current timestamp"
    )

    val eqCurrentTimestamp = TimestampInfo(
      new Timestamp(CommonUtils.current() - 432000000),
      new Timestamp(CommonUtils.current()),
      3600000,
      "Equals current timestamp"
    )

    val gtCurrentTimestamp = TimestampInfo(
      new Timestamp(CommonUtils.current() - 432000000),
      new Timestamp(CommonUtils.current() + 432000000),
      3600000,
      "more than the current timestamp"
    )
    Seq(ltCurrentTimestamp, eqCurrentTimestamp, gtCurrentTimestamp).asJava
  }
}

case class TimestampInfo(
  startTimestamp: Timestamp,
  stopTimestamp: Timestamp,
  increment: Int,
  describe: String
) {
  override def toString(): String = describe
}
