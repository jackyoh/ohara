package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.common.data.{Column, DataType, Row}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestTimestampQueryMode extends OharaTest {
  @Test
  def testRowTimestamp(): Unit = {
    val schema: Seq[Column]                    = Seq(Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Timestamp]] = Seq(ColumnInfo("COLUMN1", "timestamp", new Timestamp(0)))
    val row0: Row                              = TimestampQueryMode.builder.row(schema, columnInfo)
    row0.cell("COLUMN1").value.toString shouldBe "1970-01-01 08:00:00.0"
  }

  @Test
  def testRowInt(): Unit = {
    val schema: Seq[Column]              = Seq(Column.builder().name("COLUMN1").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row                        = TimestampQueryMode.builder.row(schema, columnInfo)
    row0.cell("COLUMN1").value shouldBe 100
  }

  @Test
  def testCellOrder(): Unit = {
    val schema: Seq[Column] = Seq(
      Column.builder().name("c1").dataType(DataType.INT).order(1).build(),
      Column.builder().name("c0").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] =
      Seq(ColumnInfo("c1", "int", Integer.valueOf(100)), ColumnInfo("c0", "int", Integer.valueOf(50)))
    val cells = TimestampQueryMode.builder.row(schema, columnInfo).cells().asScala
    cells.head.name shouldBe "c0"
    cells.head.value shouldBe 50
    cells(1).name shouldBe "c1"
    cells(1).value shouldBe 100
  }

  @Test
  def testRowNewName(): Unit = {
    val schema: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").newName("COLUMN100").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row                        = TimestampQueryMode.builder.row(schema, columnInfo)
    row0.cell("COLUMN100").value shouldBe 100
  }

  /*@Test
  def testIsCompletedFalse(): Unit = {
    val task                                     = new JDBCSourceTask()
    val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
    val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])
    val maps: Map[String, Object]                = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "2")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    task.initialize(taskContext.asInstanceOf[SourceTaskContext])
    task.run(taskSetting())

    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    val isCompleted               = task.isCompleted(startTimestamp, stopTimestamp)
    isCompleted shouldBe false
  }*/

  /*@Test
  def testIsCompletedTrue(): Unit = {
    val task                                     = new JDBCSourceTask()
    val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
    val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])
    val maps: Map[String, Object]                = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "5")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    task.initialize(taskContext.asInstanceOf[SourceTaskContext])
    task.run(taskSetting())

    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    task.pollRecords()

    val isCompleted = task.isCompleted(startTimestamp, stopTimestamp)
    isCompleted shouldBe true
  }*/
}
