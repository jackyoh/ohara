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

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import com.typesafe.scalalogging.Logger
import oharastream.ohara.client.configurator.v0.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import oharastream.ohara.kafka.connector.{RowSourceContext, RowSourceRecord, RowSourceTask, TaskSetting}

import scala.jdk.CollectionConverters._

class JDBCSourceTask1 extends RowSourceTask {
  private[this] lazy val logger = Logger(getClass.getName)

  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] var topics: Seq[String]                                  = _

  private[this] var client: DatabaseClient                             = _
  private[this] var connection: Connection                             = _
  private[this] var timestampRangeCache: BlockingQueue[TimestampRange] = _
  private[this] var inMemoryOffsets: Offsets                           = _
  private[this] val dbProduct: String                                  = client.connection.getMetaData.getDatabaseProductName

  override protected def run(settings: TaskSetting): Unit = {
    logger.info("Starting JDBC Source Connector")
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    connection = client.connection
    connection.setAutoCommit(false)
    timestampRangeCache = new ArrayBlockingQueue[TimestampRange](1024)

    inMemoryOffsets = new Offsets(rowContext, jdbcSourceConnectorConfig.dbTableName)
    schema = settings.columns.asScala.toSeq
    topics = settings.topicNames().asScala.toSeq
  }

  override protected def pollRecords(): util.List[RowSourceRecord] = {
    val inMemoryOffset = inMemoryOffsets.readOffset()
    if (timestampRangeCache.isEmpty) {
      val splitTimestampUnit = 1000 * 60 * 60 * 24 // one day
      val offsetTimestamp    = Timestamp.valueOf(inMemoryOffset).getTime()
      val currentTimestamp   = CommonUtils.current() // TODO from Database
      splitTimeStamp(splitTimestampUnit, offsetTimestamp, currentTimestamp).foreach { timestamp =>
        timestampRangeCache.offer(timestamp)
      }
    }
    val timestampRange: TimestampRange = timestampRangeCache.poll(5, TimeUnit.SECONDS)

    val tableName           = jdbcSourceConnectorConfig.dbTableName
    val timeStampColumnName = jdbcSourceConnectorConfig.timestampColumnName
    val sql =
      s"SELECT * FROM $tableName WHERE $timeStampColumnName > ? AND $timeStampColumnName <= ? ORDER BY $timeStampColumnName"
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    try {
      preparedStatement.setTimestamp(1, timestampRange.startTimestamp, DateTimeUtils.CALENDAR)
      preparedStatement.setTimestamp(2, timestampRange.stopTimestamp, DateTimeUtils.CALENDAR)

      val resultSet = preparedStatement.executeQuery()
      try {
        val rdbColumnInfo                              = columns(jdbcSourceConnectorConfig.dbTableName)
        val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
        val resultSetIterator                          = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)
        resultSetIterator
          .flatMap { result =>
            val newSchema: Seq[Column] =
              if (schema.isEmpty)
                result.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
              else schema

            val timestampColumnValue = dbTimestampColumnValue(result, timeStampColumnName)
            inMemoryOffsets.updateOffset(timestampColumnValue)

            topics.map(
              RowSourceRecord
                .builder()
                .sourcePartition(JDBCSourceTask.partition(tableName).asJava)
                //Writer Offset
                .sourceOffset(JDBCSourceTask.offset(timestampColumnValue).asJava)
                //Create Ohara Row
                .row(row(newSchema, result))
                .topicName(_)
                .build()
            )
          }
          .toList
          .asJava
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  override protected def terminate(): Unit = {
    Releasable.close(connection)
    Releasable.close(client)
  }

  private[this] def splitTimeStamp(
    splitTimestampUnit: Long,
    offsetTimestamp: Long,
    currentTimestamp: Long
  ): Seq[TimestampRange] = {
    val durationTimestamp   = currentTimestamp - offsetTimestamp
    val splitTimestampTotal = durationTimestamp / splitTimestampUnit

    (1 to splitTimestampTotal.toInt).map { i =>
      val start = offsetTimestamp + (splitTimestampUnit * (i - 1))
      val stop  = offsetTimestamp + (splitTimestampUnit * i)
      TimestampRange(new Timestamp(start), new Timestamp(stop))
    } ++ {
      val finalStartTimestamp = offsetTimestamp + (splitTimestampUnit * splitTimestampTotal)
      if (finalStartTimestamp < currentTimestamp)
        Seq(TimestampRange(new Timestamp(finalStartTimestamp), new Timestamp(currentTimestamp)))
      else Seq.empty
    }
  }

  private class Offsets(context: RowSourceContext, tableName: String) {
    private[this] val offsets: Map[String, _] =
      context.offset(JDBCSourceTask1.partition(tableName).asJava).asScala.toMap
    private[this] var cache: Map[String, String] =
      if (offsets.isEmpty) Map(tableName -> new Timestamp(0).toString)
      else Map(tableName                 -> offsets(JDBCSourceTask1.DB_TABLE_OFFSET_KEY).asInstanceOf[String])

    private[source] def updateOffset(timestamp: String): Unit = {
      val olderTimestamp = Timestamp.valueOf(this.cache(tableName)).getTime
      val newTimestamp   = Timestamp.valueOf(timestamp).getTime
      if (olderTimestamp < newTimestamp) this.cache = Map(tableName -> timestamp)
    }

    private[source] def readOffset(): String = this.cache(tableName)
  }

  private[source] def columns(tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }

  private[source] def dbTimestampColumnValue(dbColumnInfo: Seq[ColumnInfo[_]], timestampColumnName: String): String =
    dbColumnInfo
      .find(_.columnName == timestampColumnName)
      .map(_.value.asInstanceOf[Timestamp].toString)
      .getOrElse(
        throw new RuntimeException(s"$timestampColumnName not in ${jdbcSourceConnectorConfig.dbTableName} table.")
      )

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

  private[this] case class TimestampRange(startTimestamp: Timestamp, stopTimestamp: Timestamp)
}

object JDBCSourceTask1 {
  private[source] val DB_TABLE_NAME_KEY   = "db.table.name"
  private[source] val DB_TABLE_OFFSET_KEY = "db.table.offset"

  def partition(tableName: String): Map[String, _] = Map(DB_TABLE_NAME_KEY   -> tableName)
  def offset(timestamp: String): Map[String, _]    = Map(DB_TABLE_OFFSET_KEY -> timestamp)
}
