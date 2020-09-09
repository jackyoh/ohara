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

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import oharastream.ohara.kafka.connector.{RowSourceContext, RowSourceRecord}

trait TimestampQueryMode extends BaseQueryMode {
  def config: JDBCSourceConnectorConfig
  def client: DatabaseClient
  def firstTimestampValue: Timestamp
  def dbProduct: String
  def rowSourceContext: RowSourceContext
  def topics: Seq[TopicKey]
  def schema: Seq[Column]
  def offsetCache: JDBCOffsetCache
}

object TimestampQueryMode {
  def builder: Builder = new Builder()

  class Builder private[source] extends oharastream.ohara.common.pattern.Builder[TimestampQueryMode] {
    private[this] var config: JDBCSourceConnectorConfig  = _
    private[this] var client: DatabaseClient             = _
    private[this] var firstTimestampValue: Timestamp     = _
    private[this] var dbProduct: String                  = _
    private[this] var rowSourceContext: RowSourceContext = _
    private[this] var topics: Seq[TopicKey]              = _
    private[this] var schema: Seq[Column]                = _

    def config(config: JDBCSourceConnectorConfig): Builder = {
      if (config == null) throw new IllegalArgumentException("JDBCSourceConnectorConfig object can't set the null")
      else this.config = config
      this
    }

    def client(client: DatabaseClient): Builder = {
      if (client == null) throw new IllegalArgumentException("DatabaseClient object can't set the null")
      else this.client = client
      this
    }

    def firstTimestampValue(firstTimestampValue: Timestamp): Builder = {
      this.firstTimestampValue = firstTimestampValue
      this
    }

    def dbProduct(dbProduct: String): Builder = {
      this.dbProduct = CommonUtils.requireNonEmpty(dbProduct)
      this
    }

    def rowSourceContext(rowSourceContext: RowSourceContext): Builder = {
      this.rowSourceContext = rowSourceContext
      this
    }

    def topics(topics: Seq[TopicKey]): Builder = {
      this.topics = topics
      this
    }

    def schema(schema: Seq[Column]): Builder = {
      this.schema = schema
      this
    }

    override def build(): TimestampQueryMode = new TimestampQueryMode {
      override val offsetCache: JDBCOffsetCache       = new JDBCOffsetCache()
      override val config: JDBCSourceConnectorConfig  = Builder.this.config
      override val client: DatabaseClient             = Builder.this.client
      override val firstTimestampValue: Timestamp     = Builder.this.firstTimestampValue
      override val dbProduct: String                  = Builder.this.dbProduct
      override val rowSourceContext: RowSourceContext = Builder.this.rowSourceContext
      override val topics: Seq[TopicKey]              = Builder.this.topics
      override val schema: Seq[Column]                = Builder.this.schema

      override protected[source] def queryData(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = {
        val tableName           = config.dbTableName
        val timestampColumnName = config.timestampColumnName
        offsetCache.loadIfNeed(rowSourceContext, key)

        val sql =
          s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? AND $timestampColumnName < ? ORDER BY $timestampColumnName"
        val prepareStatement = client.connection.prepareStatement(sql)
        try {
          prepareStatement.setFetchSize(config.fetchDataSize)
          prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = prepareStatement.executeQuery()
          try {
            val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
            val rdbColumnInfo                              = columns(client, tableName)
            val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)
            val offset                                     = offsetCache.readOffset(key)
            results.zipWithIndex
              .filter {
                case (_, index) =>
                  index >= offset
              }
              .take(config.flushDataSize)
              .flatMap {
                case (columns, rowIndex) =>
                  val newSchema =
                    if (schema.isEmpty)
                      columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
                    else schema
                  val offset = rowIndex + 1
                  offsetCache.update(key, offset)
                  topics.map(
                    RowSourceRecord
                      .builder()
                      .sourcePartition(java.util.Map.of(JDBCOffsetCache.TABLE_PARTITION_KEY, key))
                      //Writer Offset
                      .sourceOffset(
                        java.util.Map.of(JDBCOffsetCache.TABLE_OFFSET_KEY, offset.toString)
                      )
                      //Create Ohara Row
                      .row(row(newSchema, columns))
                      .topicKey(_)
                      .build()
                  )
              }
              .toSeq
          } finally Releasable.close(resultSet)
        } finally {
          Releasable.close(prepareStatement)
          // Use the JDBC fetchSize function, should setting setAutoCommit function to false.
          // Confirm this connection ResultSet to update, need to call connection commit function.
          // Release any database locks currently held by this Connection object
          client.connection.commit()
        }
      }

      override protected[source] def isCompleted(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Boolean = {
        val dbCount     = count(startTimestamp, stopTimestamp)
        val offsetIndex = offsetCache.readOffset(key)
        if (dbCount < offsetIndex)
          throw new IllegalArgumentException(
            s"The $startTimestamp~$stopTimestamp data offset value over the table count. Please confirm your data"
          )
        else offsetIndex == dbCount
      }

      private[this] def count(startTimestamp: Timestamp, stopTimestamp: Timestamp) = {
        val sql =
          s"SELECT COUNT(*) FROM ${config.dbTableName} WHERE ${config.timestampColumnName} >= ? AND ${config.timestampColumnName} < ?"

        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try {
            if (resultSet.next()) resultSet.getInt(1)
            else 0
          } finally Releasable.close(resultSet)
        } finally {
          Releasable.close(statement)
          // Use the JDBC fetchSize function, should setting setAutoCommit function to false.
          // Confirm this connection ResultSet to update, need to call connection commit function.
          // Release any database locks currently held by this Connection object
          client.connection.commit()
        }
      }
      private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] = {
        val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
        rdbTables.head.columns
      }
    }
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
    private[this] def values(schemaColumnName: String, dbColumnInfo: Seq[ColumnInfo[_]]): Any = {
      dbColumnInfo
        .find(_.columnName == schemaColumnName)
        .map(_.value)
        .getOrElse(throw new RuntimeException(s"Database table not have the $schemaColumnName column"))
    }
  }
}
