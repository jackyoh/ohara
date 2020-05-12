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

import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration

class JDBCSourceTask extends RowSourceTask {
  private[this] lazy val logger                                          = Logger(getClass.getName)
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var dbTableDataProvider: DBTableDataProvider             = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] var topics: Seq[String]                                  = _
  private[this] var inMemoryOffsets: Offsets                             = _
  private[this] var topicOffsets: Offsets                                = _
  private[this] var lastPoll: Long                                       = -1
  private[this] var needRecovery: Boolean                                = false

  /**
    * Start the Task. This should handle any configuration parsing and one-time setup from the task.
    *
    * @param settings initial configuration
    */
  override protected[source] def run(settings: TaskSetting): Unit = {
    logger.info("Starting JDBC Source Connector")
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)

    dbTableDataProvider = new DBTableDataProvider(jdbcSourceConnectorConfig)

    schema = settings.columns.asScala.toSeq
    topics = settings.topicNames().asScala.toSeq
    val tableName = jdbcSourceConnectorConfig.dbTableName
    inMemoryOffsets = new Offsets(rowContext, tableName)
    topicOffsets = new Offsets(rowContext, tableName)
    needRecovery = true
  }

  /**
    * Poll this SourceTask for new records. This method should block if no data is currently available.
    *
    * @return a array from RowSourceRecord
    */
  override protected[source] def pollRecords(): java.util.List[RowSourceRecord] =
    try {
      val current                 = CommonUtils.current()
      val frequenceTime: Duration = jdbcSourceConnectorConfig.jdbcFrequenceTime

      if (isRunningQuery(current, lastPoll, frequenceTime)) {
        val tableName: String           = jdbcSourceConnectorConfig.dbTableName
        val timestampColumnName: String = jdbcSourceConnectorConfig.timestampColumnName
        val flushDataSize: Int          = jdbcSourceConnectorConfig.jdbcFlushDataSize
        var inMemoryOffset              = inMemoryOffsets.readOffset()
        var topicOffset                 = topicOffsets.readOffset()
        val resultSet: QueryResultIterator =
          dbTableDataProvider
            .executeQuery(Timestamp.valueOf(parseOffsetInfo(inMemoryOffset).timestamp))

        try if (needRecovery) {
          //Recovery and update offset in the memory
          val recoveryQueryRecordCount = parseOffsetInfo(topicOffset).queryRecordCount
          resultSet.slice(0, recoveryQueryRecordCount).zipWithIndex.foreach {
            case (columns, index) =>
              val timestampColumnValue = dbTimestampColumnValue(columns, timestampColumnName)
              inMemoryOffset = offsetStringResult(OffsetInfo(timestampColumnValue, index + 1))
          }
        } finally needRecovery = false

        lastPoll = current
        Option(resultSet.slice(0, flushDataSize).flatMap { columns =>
          val newSchema =
            if (schema.isEmpty)
              columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
            else schema

          val timestampColumnValue = dbTimestampColumnValue(columns, timestampColumnName)

          if (!timestampColumnValue.equals(parseOffsetInfo(inMemoryOffset).timestamp)) {
            val previousTimestamp = parseOffsetInfo(inMemoryOffset).timestamp
            topicOffset = offsetStringResult(OffsetInfo(previousTimestamp, 1))
            inMemoryOffset = offsetStringResult(OffsetInfo(timestampColumnValue, 1))
          } else {
            val queryRecordCount = parseOffsetInfo(inMemoryOffset).queryRecordCount + 1
            inMemoryOffset = offsetStringResult(OffsetInfo(timestampColumnValue, queryRecordCount))
            topicOffset = offsetStringResult(OffsetInfo(parseOffsetInfo(topicOffset).timestamp, queryRecordCount))
          }
          inMemoryOffsets.updateOffset(inMemoryOffset)
          logger.debug(s"Topic offset is $topicOffset and Memory offset is $inMemoryOffset")

          topics.map(
            RowSourceRecord
              .builder()
              .sourcePartition(JDBCSourceTask.partition(tableName).asJava)
              //Writer Offset
              .sourceOffset(JDBCSourceTask.offset(topicOffset).asJava)
              //Create Ohara Row
              .row(row(newSchema, columns))
              .topicName(_)
              .build()
          )
        }).map { rowSourceRecords =>
            if (rowSourceRecords.isEmpty) {
              inMemoryOffsets.updateOffset(inMemoryOffset)
              dbTableDataProvider.releaseResultSet(true)
              Seq.empty
            } else rowSourceRecords
          }
          .get
          .toList
          .asJava
      } else {
        Seq.empty.asJava
      }
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        Seq.empty.asJava
    }

  /**
    * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
    * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
    * fully stopped. Note that this method necessarily may be invoked from a different thread than pollRecords() and commitOffsets()
    */
  override protected def terminate(): Unit = Releasable.close(dbTableDataProvider)

  private[source] def row(schema: Seq[Column], columns: Seq[ColumnInfo[_]]): Row = {
    Row.of(
      schema
        .sortBy(_.order)
        .map(s => (s, values(s.name, columns)))
        .map {
          case (s, value) =>
            Cell.of(
              s.newName,
              s.dataType match {
                case DataType.BOOLEAN                 => value.asInstanceOf[Boolean]
                case DataType.SHORT                   => value.asInstanceOf[Short]
                case DataType.INT                     => value.asInstanceOf[Int]
                case DataType.LONG                    => value.asInstanceOf[Long]
                case DataType.FLOAT                   => value.asInstanceOf[Float]
                case DataType.DOUBLE                  => value.asInstanceOf[Double]
                case DataType.BYTE                    => value.asInstanceOf[Byte]
                case DataType.STRING                  => value.asInstanceOf[String]
                case DataType.BYTES | DataType.OBJECT => value
                case _                                => throw new IllegalArgumentException("Unsupported type...")
              }
            )
        }: _*
    )
  }

  private[this] def values(schemaColumnName: String, dbColumnInfos: Seq[ColumnInfo[_]]): Any = {
    dbColumnInfos
      .find(_.columnName == schemaColumnName)
      .map(_.value)
      .getOrElse(throw new RuntimeException(s"Database Table not have the $schemaColumnName column"))
  }

  private[source] def dbTimestampColumnValue(dbColumnInfo: Seq[ColumnInfo[_]], timestampColumnName: String): String =
    dbColumnInfo
      .find(_.columnName == timestampColumnName)
      .map(_.value.asInstanceOf[Timestamp].toString)
      .getOrElse(
        throw new RuntimeException(s"$timestampColumnName not in ${jdbcSourceConnectorConfig.dbTableName} table.")
      )

  private[source] def isRunningQuery(currentTime: Long, lastPoll: Long, frequenceTime: Duration): Boolean =
    (currentTime - lastPoll) > frequenceTime.toMillis

  /**
    * Offset format is ${TimestampOffset},${QueryRecordCount}
    * Example: 2018-09-01 00:00:00,0
    * @param offsetInfo
    * @return offset value
    */
  private[this] def offsetStringResult(offsetInfo: OffsetInfo): String = {
    offsetInfo.timestamp + "," + offsetInfo.queryRecordCount
  }

  private[this] def parseOffsetInfo(offsetString: String): OffsetInfo = {
    val offset = offsetString.split(",")
    OffsetInfo(offset.head, offset.last.toInt)
  }

  private class Offsets(context: RowSourceContext, tableName: String) {
    private[this] val offsets: Map[String, _] = context.offset(JDBCSourceTask.partition(tableName).asJava).asScala.toMap
    private[this] var cache: Map[String, String] =
      if (offsets.isEmpty) Map(tableName -> offsetStringResult(OffsetInfo(new Timestamp(0).toString, 0)))
      else Map(tableName                 -> offsets(JDBCSourceTask.DB_TABLE_OFFSET_KEY).asInstanceOf[String])

    private[source] def updateOffset(timestamp: String): Unit = {
      this.cache = Map(tableName -> timestamp)
    }

    private[source] def readOffset(): String = this.cache(tableName)
  }

  private[this] case class OffsetInfo(timestamp: String, queryRecordCount: Int)
}

object JDBCSourceTask {
  private[source] val DB_TABLE_NAME_KEY   = "db.table.name"
  private[source] val DB_TABLE_OFFSET_KEY = "db.table.offset"

  def partition(tableName: String): Map[String, _] = Map(DB_TABLE_NAME_KEY   -> tableName)
  def offset(timestamp: String): Map[String, _]    = Map(DB_TABLE_OFFSET_KEY -> timestamp)
}
