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
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import oharastream.ohara.kafka.connector.{RowSourceContext, RowSourceRecord}

abstract class TimestampQueryMode extends BaseQueryMode {
  protected[this] def config: JDBCSourceConnectorConfig
  protected[this] def client: DatabaseClient
  protected[this] def offsetCache: JDBCOffsetCache
  protected[this] def dbProduct: String
  protected[this] def topics: Seq[TopicKey]
  protected[this] def rowContext: RowSourceContext
  protected[this] def schema: Seq[Column]
}

object TimestampQueryMode {
  def builder: Builder = new Builder()
  class Builder private[source] extends oharastream.ohara.common.pattern.Builder[BaseQueryMode] {
    private[this] var config: JDBCSourceConnectorConfig = _
    private[this] var client: DatabaseClient            = _
    private[this] var offsetCache: JDBCOffsetCache      = _
    private[this] var dbProduct: String                 = _
    private[this] var topics: Seq[TopicKey]             = _
    private[this] var rowContext: RowSourceContext      = _
    private[this] var schema: Seq[Column]               = _

    def config(config: JDBCSourceConnectorConfig): Builder = {
      this.config = config
      this
    }

    def client(client: DatabaseClient): Builder = {
      this.client = client
      this
    }

    def offsetCache(offsetCache: JDBCOffsetCache): Builder = {
      this.offsetCache = offsetCache
      this
    }

    def dbProduct(dbProduct: String): Builder = {
      this.dbProduct = dbProduct
      this
    }

    def topics(topics: Seq[TopicKey]): Builder = {
      this.topics = topics
      this
    }

    def rowContext(rowContext: RowSourceContext): Builder = {
      this.rowContext = rowContext
      this
    }

    def schema(schema: Seq[Column]): Builder = {
      this.schema = schema
      this
    }

    override def build: TimestampQueryMode = new TimestampQueryMode {
      override protected[this] val config: JDBCSourceConnectorConfig = Builder.this.config
      override protected[this] val client: DatabaseClient            = Builder.this.client
      override protected[this] val offsetCache: JDBCOffsetCache      = Builder.this.offsetCache
      override protected[this] val dbProduct: String                 = Builder.this.dbProduct
      override protected[this] val topics: Seq[TopicKey]             = Builder.this.topics
      override protected[this] val rowContext: RowSourceContext      = Builder.this.rowContext
      override protected[this] val schema: Seq[Column]               = Builder.this.schema

      override protected[source] def queryData(
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp,
        key: String
      ): Seq[RowSourceRecord] = {
        val tableName           = config.dbTableName
        val timestampColumnName = config.timestampColumnName
        offsetCache.loadIfNeed(rowContext, key)

        val sql =
          s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? AND $timestampColumnName < ? ORDER BY $timestampColumnName"
        val prepareStatement = client.connection.prepareStatement(sql)
        try {
          prepareStatement.setFetchSize(config.fetchDataSize)
          prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          println(s"StartTimestamp: $startTimestamp       StopTimestamp: $stopTimestamp")
          val resultSet = prepareStatement.executeQuery()
          try {
            val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
            val rdbColumnInfo                              = columns(client, tableName)
            val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

            val offset = offsetCache.readOffset(key)

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
                      //Create ohara Row
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
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp,
        key: String
      ): Boolean = {
        val dbCount     = count(startTimestamp, stopTimestamp)
        val offsetIndex = offsetCache.readOffset(key)
        if (dbCount < offsetIndex)
          throw new IllegalArgumentException(
            s"The $startTimestamp~$stopTimestamp data offset index error ($dbCount < $offsetIndex). Please confirm your data"
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
  }
}
