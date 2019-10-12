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

import java.sql.{Statement, Timestamp}

import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.data.{Column, DataType}
import com.island.ohara.common.rule.OharaTest
import com.island.ohara.common.util.Releasable
import com.island.ohara.kafka.connector.{RowSourceRecord, TaskSetting}
import com.island.ohara.testing.service.Database
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.{After, Before, Test}
import org.mockito.Mockito.when
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class TestJDBCSourceTaskRecovery extends OharaTest with Matchers with MockitoSugar {
  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "TABLE1"
  private[this] val timestampColumnName = "COLUMN1"
  private[this] val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
  private[this] val taskContext: SourceTaskContext = mock[SourceTaskContext]
  private[this] val taskSetting: TaskSetting = mock[TaskSetting]
  private[this] val offsetStorageReader: OffsetStorageReader = mock[OffsetStorageReader]

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
    val column2 = RdbColumn("COLUMN2", "varchar(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)
    val column4 = RdbColumn("COLUMN4", "integer", true)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a21', 'a22', 2)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a31', 'a32', 3)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a41', 'a42', 4)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:02', 'a51', 'a52', 5)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:03.12', 'a61', 'a62', 6)")
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-02 00:00:04', 'a71', 'a72', 7)")

    // Mock JDBC Source Task
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])

    when(taskSetting.stringValue(DB_URL)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(MODE)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(JDBC_FETCHDATA_SIZE)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(JDBC_FLUSHDATA_SIZE)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))

    val columns: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
      Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
    )

    when(taskSetting.columns).thenReturn(columns.asJava)
    when(taskSetting.topicNames()).thenReturn(Seq("topic1").asJava)

  }

  @Test
  def testNormal(): Unit = {
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala

    rows.head.row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    rows.head.row.cell(1).value shouldBe "a11"
    rows.head.row.cell(2).value shouldBe 1

    rows(1).row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    rows(1).row.cell(1).value shouldBe "a21"
    rows(1).row.cell(2).value shouldBe 2

    rows.last.row.cell(0).value.toString shouldBe "2018-09-02 00:00:04.0"
    rows.last.row.cell(1).value shouldBe "a71"
    rows.last.row.cell(2).value shouldBe 7

    //Test offset value for JDBC Source Connector
    rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe s"${new Timestamp(0).toString},1"
    })

    rows(1).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe s"${new Timestamp(0).toString},2"
    })

    rows(2).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe s"${new Timestamp(0).toString},3"
    })

    rows(3).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe s"${new Timestamp(0).toString},4"
    })

    rows(4).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe "2018-09-01 00:00:00.0,1"
    })

    rows(5).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe "2018-09-01 00:00:02.0,1"
    })
  }

  @Test
  def testRestartJDBCSourceConnector_1(): Unit = {
    val maps: Map[String, Object] = Map("db.table.offset" -> s"${new Timestamp(0).toString},1")
    when(offsetStorageReader.offset(Map("db.table.name" -> tableName).asJava)).thenReturn(maps.asJava)
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.size shouldBe 6
  }

  @Test
  def testRestartJDBCSourceConnector_2(): Unit = {
    val maps: Map[String, Object] = Map("db.table.offset" -> s"${new Timestamp(0).toString},2")
    when(offsetStorageReader.offset(Map("db.table.name" -> tableName).asJava)).thenReturn(maps.asJava)
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.size shouldBe 5
  }

  @Test
  def testRestartJDBCSourceConnector_3(): Unit = {
    val maps: Map[String, Object] = Map("db.table.offset" -> s"${new Timestamp(0).toString},4")
    when(offsetStorageReader.offset(Map("db.table.name" -> tableName).asJava)).thenReturn(maps.asJava)
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.size shouldBe 3
  }

  @Test
  def testRestartJDBCSourceConnector_4(): Unit = {
    val maps: Map[String, Object] = Map("db.table.offset" -> "2018-09-01 00:00:02.0,1")
    when(offsetStorageReader.offset(Map("db.table.name" -> tableName).asJava)).thenReturn(maps.asJava)
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.size shouldBe 1
    rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe "2018-09-01 00:00:02.0,2"
    })
  }

  @Test
  def testInsertData(): Unit = {
    val statement: Statement = db.connection.createStatement()
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-03 00:00:04', 'a81', 'a82', 8)")

    val maps: Map[String, Object] = Map("db.table.offset" -> "2018-09-01 00:00:02.0,1")
    when(offsetStorageReader.offset(Map("db.table.name" -> tableName).asJava)).thenReturn(maps.asJava)
    jdbcSourceTask._start(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala

    rows.size shouldBe 2
    rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe "2018-09-01 00:00:02.0,2"
    })
    rows.head.row.cell(0).value.toString shouldBe "2018-09-02 00:00:04.0"
    rows.head.row.cell(2).name shouldBe "COLUMN4"
    rows.head.row.cell(2).value shouldBe 7

    rows.last.row.cell(1).name shouldBe "COLUMN2"
    rows.last.row.cell(1).value shouldBe "a81"
  }

  @After
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
