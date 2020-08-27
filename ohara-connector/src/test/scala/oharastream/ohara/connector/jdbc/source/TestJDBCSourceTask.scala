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

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType, Row}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.{RowSourceRecord, TaskSetting}
import oharastream.ohara.testing.service.Database
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.{After, Before, Test}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestJDBCSourceTask extends OharaTest {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "TABLE1"
  private[this] val timestampColumnName = "COLUMN1"

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn("COLUMN1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("COLUMN2", "varchar(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)
    val column4 = RdbColumn("COLUMN4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

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
  }

  @Test
  def testPoll(): Unit = {
    val jdbcSourceTask: JDBCSourceTask           = new JDBCSourceTask()
    val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
    val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])

    val taskSetting: TaskSetting = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))

    val columns: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
      Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
    )

    when(taskSetting.columns).thenReturn(columns.asJava)
    when(taskSetting.topicKeys()).thenReturn(Set(TopicKey.of("g", "topic1")).asJava)
    jdbcSourceTask.run(taskSetting)

    val rows1: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows1.head.row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    rows1.head.row.cell(1).value shouldBe "a11"
    rows1.head.row.cell(2).value shouldBe 1

    rows1.head.row.cell(0).name shouldBe "COLUMN1"
    rows1(1).row.cell(1).name shouldBe "COLUMN2"
    rows1(2).row.cell(2).name shouldBe "COLUMN4"

    //Test row 1 offset
    rows1.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "1"
    })
    //Test row 2 offset
    rows1(1).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "2"
    })
    //Test row 4 offset
    rows1(3).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "4"
    })
    //Test row 5 offset
    rows1(4).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "5"
    })
    rows1.size shouldBe 5

    val statement: Statement = db.connection.createStatement()
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 23:00:00.0', 'a81', 'a82', 8)"
    )
    jdbcSourceTask.stop()

    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "5")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows2: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows2.size shouldBe 1
    rows2.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "6"
    })
  }

  @Test
  def testRowTimestamp(): Unit = {
    val jdbcSourceTask                         = new JDBCSourceTask()
    val schema: Seq[Column]                    = Seq(Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Timestamp]] = Seq(ColumnInfo("COLUMN1", "timestamp", new Timestamp(0)))
    val row0: Row                              = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN1").value.toString shouldBe "1970-01-01 08:00:00.0"
  }

  @Test
  def testRowInt(): Unit = {
    val jdbcSourceTask                   = new JDBCSourceTask()
    val schema: Seq[Column]              = Seq(Column.builder().name("COLUMN1").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row                        = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN1").value shouldBe 100
  }

  @Test
  def testCellOrder(): Unit = {
    val jdbcSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(
      Column.builder().name("c1").dataType(DataType.INT).order(1).build(),
      Column.builder().name("c0").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] =
      Seq(ColumnInfo("c1", "int", Integer.valueOf(100)), ColumnInfo("c0", "int", Integer.valueOf(50)))
    val cells = jdbcSourceTask.row(schema, columnInfo).cells().asScala
    cells.head.name shouldBe "c0"
    cells.head.value shouldBe 50
    cells(1).name shouldBe "c1"
    cells(1).value shouldBe 100
  }

  @Test
  def testRowNewName(): Unit = {
    val jdbcSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").newName("COLUMN100").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row                        = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN100").value shouldBe 100
  }

  @Test
  def testPartitionKeyError_1(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val taskSetting: TaskSetting       = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    jdbcSourceTask.run(taskSetting)

    // FirstTimestamp data from the rdb table. This is fake data
    val firstTimestamp: Timestamp = Timestamp.valueOf("2020-06-10 15:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2020-06-09 15:00:00")
    an[IllegalArgumentException] should be thrownBy
      jdbcSourceTask.partitionKey("table1", firstTimestamp, stopTimestamp)
  }

  @Test
  def testPartitionKeyError_2(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val taskSetting: TaskSetting       = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    jdbcSourceTask.run(taskSetting)

    // Partition Range timestamp is over the current timestamp
    val firstTimestamp: Timestamp = Timestamp.valueOf("2020-06-10 15:00:00")
    val stopTimestamp: Timestamp  = new Timestamp(CommonUtils.current() + 86400000)
    an[IllegalArgumentException] should be thrownBy
      jdbcSourceTask.partitionKey("table1", firstTimestamp, stopTimestamp)
  }

  @Test
  def testPartitionKeyNormal(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val taskSetting: TaskSetting       = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    jdbcSourceTask.run(taskSetting)

    val firstTimestamp: Timestamp = Timestamp.valueOf("2020-06-10 15:00:00")

    // Test the current timestamp is not stop timestamp
    val stopTimestamp: Timestamp = Timestamp.valueOf("2020-06-12 12:00:00")
    val result                   = jdbcSourceTask.partitionKey("table1", firstTimestamp, stopTimestamp)
    result shouldBe "table1:2020-06-11 15:00:00.0~2020-06-12 15:00:00.0"
  }

  @Test
  def testIsCompletedFalse(): Unit = {
    val jdbcSourceTask                           = new JDBCSourceTask()
    val taskSetting: TaskSetting                 = Mockito.mock(classOf[TaskSetting])
    val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
    val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])
    val maps: Map[String, Object]                = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "2")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    jdbcSourceTask.run(taskSetting)

    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    val isCompleted               = jdbcSourceTask.isCompleted(startTimestamp, stopTimestamp)
    isCompleted shouldBe false
  }

  @Test
  def testIsCompletedTrue(): Unit = {
    val jdbcSourceTask                           = new JDBCSourceTask()
    val taskSetting: TaskSetting                 = Mockito.mock(classOf[TaskSetting])
    val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
    val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])
    val maps: Map[String, Object]                = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "5")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    jdbcSourceTask.run(taskSetting)

    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    jdbcSourceTask.pollRecords()
    val isCompleted = jdbcSourceTask.isCompleted(startTimestamp, stopTimestamp)
    isCompleted shouldBe true
  }

  @Test
  def testNeedToRun(): Unit = {
    val jdbcSourceTask           = new JDBCSourceTask()
    val taskSetting: TaskSetting = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(1))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(3))
    jdbcSourceTask.run(taskSetting)

    val needToRun = jdbcSourceTask.needToRun(Timestamp.valueOf("2018-09-02 00:00:00"))
    needToRun shouldBe true
  }

  @After
  def afterTest(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
