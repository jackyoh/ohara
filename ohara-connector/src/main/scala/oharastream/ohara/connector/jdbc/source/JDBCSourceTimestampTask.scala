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

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import oharastream.ohara.kafka.connector.RowSourceRecord

import scala.jdk.CollectionConverters._

class JDBCSourceTimestampTask extends BasicJDBCSourceTask {
  private[this] var topics: Seq[TopicKey]                                = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var timestampColumnName: String                          = _
  private[this] var offsetCache: JDBCOffsetCache                         = _

  override protected[this] def runJDBCSourceTask(
    jdbcSourceConnectorConfig: JDBCSourceConnectorConfig,
    topics: Seq[TopicKey],
    schema: Seq[Column]
  ): Unit = {
    this.jdbcSourceConnectorConfig = jdbcSourceConnectorConfig
    timestampColumnName =
      jdbcSourceConnectorConfig.incrementTimestampColumnName.split(SPLIT_INCREMENT_TIMESTAMP_COLUMN_COMMA).last
    this.offsetCache = new JDBCOffsetCache()
    this.topics = topics
    this.schema = schema
  }

  protected[this] def queryData(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord] = {
    val tableName      = jdbcSourceConnectorConfig.dbTableName
    val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, tablePartition)

    val sql =
      s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $timestampColumnName"

    val prepareStatement = client.connection.prepareStatement(sql)
    try {
      prepareStatement.setFetchSize(jdbcSourceConnectorConfig.jdbcFetchDataSize)
      prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
      prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
      val resultSet = prepareStatement.executeQuery()
      try {
        val tableTimestampPartition                    = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
        val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
        val rdbColumnInfo                              = columns(jdbcSourceConnectorConfig.dbTableName)
        val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

        val offset: JDBCOffsetInfo =
          offsetCache.readOffset(tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp))

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
              offsetCache.update(tableTimestampPartition, offset)

              topics.map(
                RowSourceRecord
                  .builder()
                  .sourcePartition(Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> tableTimestampPartition).asJava)
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

  /**
    * The start timestamp and stop timestamp range can't change.
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  protected[this] def isCompleted(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Boolean = {
    val tableName = jdbcSourceConnectorConfig.dbTableName
    val sql =
      s"SELECT count(*) FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ?"

    val statement = client.connection.prepareStatement(sql)
    try {
      statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
      statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
      val resultSet = statement.executeQuery()
      try {
        val dbCount =
          if (resultSet.next()) resultSet.getInt(1)
          else 0
        val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
        val offsetIndex    = offsetCache.readOffset(tablePartition).index
        if (dbCount < offsetIndex) {
          throw new IllegalArgumentException(
            s"The $startTimestamp~$stopTimestamp data offset index error ($dbCount < $offsetIndex). Please confirm your data"
          )
        } else offsetIndex == dbCount
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  override protected def tableFirstTimestampValueSQL(): String =
    s"SELECT $timestampColumnName FROM ${jdbcSourceConnectorConfig.dbTableName} ORDER BY $timestampColumnName"
}
