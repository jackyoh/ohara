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

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector.{RowSourceContext, TaskSetting}
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestTimestampIncrementQueryMode extends OharaTest {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "TABLE1"
  private[this] val incrementColumnName = "COLUMN0"
  private[this] val timestampColumnName = "COLUMN1"

  @BeforeEach
  def setup(): Unit = {
    client.connection.setAutoCommit(false)
    val columns = Seq(
      RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
      RdbColumn(timestampColumnName, "TIMESTAMP(6)", false),
      RdbColumn("COLUMN2", "varchar(45)", false),
      RdbColumn("COLUMN3", "VARCHAR(45)", false),
      RdbColumn("COLUMN4", "integer", false)
    )
    client.createTable(tableName, columns)

    val statement = client.connection.createStatement
    try {
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:03.12', 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:04.123456', 'a51', 'a52', 5)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES(NOW() + INTERVAL 3 DAY, 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)"
      )
    } finally Releasable.close(statement)
  }

  @Test
  def testIsCompletedTrue(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    mockQueryMode(key, 5).isCompleted(key, startTimestamp, stopTimestamp) shouldBe true
  }

  @Test
  def testIsCompletedFalse(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    (1 to 4).foreach { i =>
      mockQueryMode(key, i).isCompleted(key, startTimestamp, stopTimestamp) shouldBe false
    }
  }

  @Test
  def testIsCompletedException(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    an[IllegalArgumentException] should be thrownBy
      mockQueryMode(key, 6).isCompleted(key, startTimestamp, stopTimestamp)
  }

  private[this] def mockQueryMode(key: String, value: Int): TimestampIncrementQueryMode = {
    val rowSourceContext          = Mockito.mock(classOf[RowSourceContext])
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> value.toString)
    when(
      rowSourceContext.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    val queryMode = TimestampIncrementQueryMode.builder
      .config(JDBCSourceConnectorConfig(taskSetting()))
      .client(client)
      .rowSourceContext(rowSourceContext)
      //.dbProduct("mysql")
      //.schema(Seq.empty)
      //.topics(Seq.empty)
      .build()
    queryMode.offsetCache.loadIfNeed(rowSourceContext, key)
    queryMode
  }

  private[this] def taskSetting(): TaskSetting = {
    val taskSetting: TaskSetting = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.stringOption(INCREMENT_COLUMN_NAME_KEY)).thenReturn(java.util.Optional.of(incrementColumnName))
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    when(taskSetting.columns).thenReturn(
      Seq(
        Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
        Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
        Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
      ).asJava
    )
    when(taskSetting.topicKeys()).thenReturn(Set(TopicKey.of("g", "topic1")).asJava)
    taskSetting
  }

  @AfterEach
  def afterTest(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
