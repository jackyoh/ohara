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
import java.util.Objects

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.kafka.connector.RowSourceRecord
import oharastream.ohara.common.pattern.Builder
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}

import scala.jdk.CollectionConverters._

trait DataModeHandler {
  protected[source] def queryTableCount(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Long

  protected[source] def queryData(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp,
    tablePartition: String
  ): Seq[RowSourceRecord]

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

  private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }

  private[this] def values(schemaColumnName: String, dbColumnInfos: Seq[ColumnInfo[_]]): Any = {
    dbColumnInfos
      .find(_.columnName == schemaColumnName)
      .map(_.value)
      .getOrElse(throw new RuntimeException(s"Database Table not have the $schemaColumnName column"))
  }
}

object DataModeHandler {
  def timestampMode: TimestampDataModeHandler = new TimestampDataModeHandler
  class TimestampDataModeHandler private[DataModeHandler] extends Builder[DataModeHandler] {
    private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
    private[this] var schema: Seq[Column]                                  = _
    private[this] var topics: Seq[TopicKey]                                = _
    private[this] var offsetCache: JDBCOffsetCache                         = _
    private[this] var dbProduct: String                                    = _

    def jdbcSourceConnectorConfig(jdbcSourceConnectorConfig: JDBCSourceConnectorConfig): TimestampDataModeHandler = {
      this.jdbcSourceConnectorConfig = Objects.requireNonNull(jdbcSourceConnectorConfig)
      this
    }

    def schema(schema: Seq[Column]): TimestampDataModeHandler = {
      this.schema = Objects.requireNonNull(schema)
      this
    }

    def topics(topics: Seq[TopicKey]): TimestampDataModeHandler = {
      this.topics = Objects.requireNonNull(topics)
      this
    }

    def offsetCache(offsetCache: JDBCOffsetCache): TimestampDataModeHandler = {
      this.offsetCache = Objects.requireNonNull(offsetCache)
      this
    }

    def dbProduct(dbProduct: String): TimestampDataModeHandler = {
      this.dbProduct = Objects.requireNonNull(dbProduct)
      this
    }

    override def build(): DataModeHandler = new DataModeHandler() {
      override protected[source] def queryTableCount(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Long = {
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
        val sql =
          s"SELECT count(*) FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ?"

        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try {
            if (resultSet.next()) resultSet.getInt(1)
            else 0
          } finally Releasable.close(resultSet)
        } finally Releasable.close(statement)
      }

      override protected[source] def queryData(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp,
        tablePartition: String
      ): Seq[RowSourceRecord] = {
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName

        val sql =
          s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $timestampColumnName"

        val prepareStatement = client.connection.prepareStatement(sql)
        try {
          prepareStatement.setFetchSize(jdbcSourceConnectorConfig.jdbcFetchDataSize)
          prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = prepareStatement.executeQuery()
          try {
            val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
            val rdbColumnInfo                              = columns(client, tableName)
            val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

            val offset: JDBCOffsetInfo = offsetCache.readOffset(tablePartition)

            results.zipWithIndex
              .filter {
                case (_, index) =>
                  index >= offset.index
              }
              .take(jdbcSourceConnectorConfig.jdbcFlushDataSize)
              .flatMap {
                case (columns, rowIndex) =>
                  val newSchema =
                    if (schema.isEmpty)
                      columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
                    else schema
                  val offset = JDBCOffsetInfo(rowIndex + 1)
                  offsetCache.update(tablePartition, offset)

                  topics.map(
                    RowSourceRecord
                      .builder()
                      .sourcePartition(Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> tablePartition).asJava)
                      //Writer Offset
                      .sourceOffset(
                        Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> offset.toString).asJava
                      )
                      //Create Ohara Row
                      .row(row(newSchema, columns))
                      .topicKey(_)
                      .build()
                  )
              }
              .toSeq
          } finally Releasable.close(resultSet)
        } finally Releasable.close(prepareStatement)
      }
    }
  }

  def incrementTimestampMode: IncrementTimestampDataModeHandler = new IncrementTimestampDataModeHandler
  class IncrementTimestampDataModeHandler private[DataModeHandler] extends Builder[DataModeHandler] {
    private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
    private[this] var schema: Seq[Column]                                  = _
    private[this] var topics: Seq[TopicKey]                                = _
    private[this] var offsetCache: JDBCOffsetCache                         = _
    private[this] var dbProduct: String                                    = _

    def jdbcSourceConnectorConfig(
      jdbcSourceConnectorConfig: JDBCSourceConnectorConfig
    ): IncrementTimestampDataModeHandler = {
      this.jdbcSourceConnectorConfig = Objects.requireNonNull(jdbcSourceConnectorConfig)
      this
    }

    def schema(schema: Seq[Column]): IncrementTimestampDataModeHandler = {
      this.schema = Objects.requireNonNull(schema)
      this
    }

    def topics(topics: Seq[TopicKey]): IncrementTimestampDataModeHandler = {
      this.topics = Objects.requireNonNull(topics)
      this
    }

    def offsetCache(offsetCache: JDBCOffsetCache): IncrementTimestampDataModeHandler = {
      this.offsetCache = Objects.requireNonNull(offsetCache)
      this
    }

    def dbProduct(dbProduct: String): IncrementTimestampDataModeHandler = {
      this.dbProduct = Objects.requireNonNull(dbProduct)
      this
    }

    override def build(): DataModeHandler = new DataModeHandler {
      override protected[source] def queryTableCount(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Long = {
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        val incrementColumnName = jdbcSourceConnectorConfig.incrementColumnName
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
        val sql =
          s"SELECT $incrementColumnName FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $incrementColumnName DESC"
        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try {
            if (resultSet.next()) resultSet.getLong(1)
            else 0
          } finally Releasable.close(resultSet)
        } finally Releasable.close(statement)
      }

      override protected[source] def queryData(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp,
        tablePartition: String
      ): Seq[RowSourceRecord] = {
        val tableName                   = jdbcSourceConnectorConfig.dbTableName
        val timestampColumnName         = jdbcSourceConnectorConfig.timestampColumnName
        val incrementColumnName         = jdbcSourceConnectorConfig.incrementColumnName
        val topicOffset: JDBCOffsetInfo = offsetCache.readOffset(tablePartition)

        val sql =
          s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? and $incrementColumnName > ? ORDER BY $timestampColumnName, $incrementColumnName"
        val prepareStatement = client.connection.prepareStatement(sql)
        try {
          prepareStatement.setFetchSize(jdbcSourceConnectorConfig.jdbcFetchDataSize)
          prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setLong(3, topicOffset.index)

          val resultSet = prepareStatement.executeQuery()
          try {
            val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
            val rdbColumnInfo                              = columns(client, tableName)
            val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

            results
              .take(jdbcSourceConnectorConfig.jdbcFlushDataSize)
              .flatMap { columns =>
                val newSchema =
                  if (schema.isEmpty)
                    columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
                  else schema
                val idValue = columns
                  .find(_.columnName == incrementColumnName)
                  .map(_.value.toString())
                  .getOrElse(throw new IllegalArgumentException(s"The $incrementColumnName column is not found"))
                  .toLong
                val topicOffset: JDBCOffsetInfo = offsetCache.readOffset(tablePartition)

                if (idValue > topicOffset.index) {
                  val offset = JDBCOffsetInfo(idValue)

                  offsetCache.update(tablePartition, offset)
                  topics.map(
                    RowSourceRecord
                      .builder()
                      .sourcePartition(Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> tablePartition).asJava)
                      //Writer Offset
                      .sourceOffset(
                        Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> offset.toString).asJava
                      )
                      //Create Ohara Row
                      .row(row(newSchema, columns))
                      .topicKey(_)
                      .build()
                  )
                } else Seq.empty
              }
              .toSeq
          } finally Releasable.close(resultSet)
        } finally Releasable.close(prepareStatement)
      }
    }
  }
}
