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

import java.sql.{Date, Statement, Time, Timestamp}
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestJDBCSourceConnectorDataType extends With3Brokers3Workers {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  @BeforeEach
  def setup(): Unit = {
    val connection = client.connection
    val statement  = connection.createStatement()

    statement.executeUpdate(
      s"create table $tableName($timestampColumnName timestamp(6)," +
        "column2 longblob," +
        "column3 bit," +
        "column4 tinyint," +
        "column5 boolean," +
        "column6 BIGINT," +
        "column7 float," +
        "column8 double," +
        "column9 decimal," +
        "column10 date," +
        "column11 time," +
        "column12 ENUM('A','B')," +
        "column13 LONGTEXT)"
    )

    val sql   = s"INSERT INTO table1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val pstmt = connection.prepareStatement(sql)

    val binaryData = "some string data ...".getBytes()
    pstmt.setString(1, "2018-10-01 00:00:00")
    pstmt.setBytes(2, binaryData)
    pstmt.setByte(3, 1)
    pstmt.setInt(4, 100)
    pstmt.setBoolean(5, false)
    pstmt.setLong(6, 1000)
    pstmt.setFloat(7, 200)
    pstmt.setDouble(8, 2000)
    pstmt.setBigDecimal(9, java.math.BigDecimal.valueOf(10000))
    pstmt.setDate(10, Date.valueOf("2018-10-01"))
    pstmt.setTime(11, Time.valueOf("11:00:00"))
    pstmt.setString(12, "B")
    pstmt.setString(13, "aaaaaaaaaa")
    pstmt.executeUpdate()
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
        .numberOfTasks(3)
        .settings(props.toMap)
        .create()
    )

    try {
      val record = pollData(topicKey, Duration(30, TimeUnit.SECONDS), 1)
      val row0   = record.head.key.get

      record.size shouldBe 1

      // Test timestamp type
      row0.cell(0).value.isInstanceOf[Timestamp] shouldBe true
      row0.cell(0).value.toString shouldBe "2018-10-01 00:00:00.0"

      // Test byte array type
      row0.cell(1).value.isInstanceOf[Array[Byte]] shouldBe true
      new String(row0.cell(1).value.asInstanceOf[Array[Byte]]) shouldBe "some string data ..."

      // Test bit type
      row0.cell(2).value.isInstanceOf[Boolean] shouldBe true
      row0.cell(2).value shouldBe true

      // Test tinyint type
      row0.cell(3).value.isInstanceOf[Integer] shouldBe true
      row0.cell(3).value shouldBe 100

      // Test boolean type
      row0.cell(4).value.isInstanceOf[Boolean] shouldBe true
      row0.cell(4).value shouldBe false

      // Test long type
      row0.cell(5).value.isInstanceOf[Long] shouldBe true
      row0.cell(5).value shouldBe 1000

      // Test float type
      row0.cell(6).value.isInstanceOf[Float] shouldBe true
      row0.cell(6).value shouldBe 200.0

      // Test double type
      row0.cell(7).value.isInstanceOf[Double] shouldBe true
      row0.cell(7).value shouldBe 2000.0

      // Test bigdecimal type
      row0.cell(8).value.isInstanceOf[java.math.BigDecimal] shouldBe true
      row0.cell(8).value shouldBe java.math.BigDecimal.valueOf(10000)

      // Test date type
      row0.cell(9).value.isInstanceOf[Date] shouldBe true
      row0.cell(9).value shouldBe Date.valueOf("2018-10-01")

      // Test time type
      row0.cell(10).value.isInstanceOf[Time] shouldBe true
      row0.cell(10).value shouldBe Time.valueOf("11:00:00")

      // Test enum type
      row0.cell(11).value.isInstanceOf[String] shouldBe true
      row0.cell(11).value.toString shouldBe "B"

      // Test longtext type
      row0.cell(12).value.isInstanceOf[String] shouldBe true
      row0.cell(12).value.toString shouldBe "aaaaaaaaaa"
    } finally result(connectorAdmin.delete(connectorKey))
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

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  @AfterEach
  def tearDown(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
    }
    Releasable.close(client)
    Releasable.close(db)
  }

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
}
