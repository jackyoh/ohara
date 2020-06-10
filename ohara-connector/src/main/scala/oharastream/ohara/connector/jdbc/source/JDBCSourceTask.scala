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
import java.util

import oharastream.ohara.client.configurator.v0.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

import scala.jdk.CollectionConverters._

class JDBCSourceTask extends RowSourceTask {
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] var dbProduct: String                                    = _
  private[this] var offsetCache: JDBCOffsetCache                         = _
  private[this] var topics: Seq[TopicKey]                                = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int                       = 86400000 // 1 day

  override protected[source] def run(settings: TaskSetting): Unit = {
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    dbProduct = client.connection.getMetaData.getDatabaseProductName
    offsetCache = new JDBCOffsetCache()
    topics = settings.topicKeys().asScala.toSeq
    schema = settings.columns.asScala.toSeq
  }

  override protected[source] def pollRecords(): util.List[RowSourceRecord] = {
    val tableName           = jdbcSourceConnectorConfig.dbTableName
    val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
    var startTimestamp      = tableFirstTimestampValue(tableName, timestampColumnName)
    var stopTimestamp       = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    val tablePartition      = tableTimestampPartitionKey(tableName, startTimestamp, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, tablePartition)

    while (!needToRun(startTimestamp, stopTimestamp)
           || isCompleted(startTimestamp, stopTimestamp)) {
      startTimestamp = stopTimestamp
      stopTimestamp = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (overCurrentTimestamp(stopTimestamp) && isCompleted(startTimestamp, stopTimestamp))
        return Seq.empty.asJava
    }
    queryData(startTimestamp, stopTimestamp).asJava
  }

  private[this] def tableFirstTimestampValue(tableName: String, timestampColumnName: String): Timestamp = {
    val statement = client.connection.createStatement()
    try {
      val resultSet =
        statement.executeQuery(s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName")
      try {
        if (!resultSet.next()) new Timestamp(CommonUtils.current())
        else resultSet.getTimestamp(timestampColumnName)
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  private[this] def overCurrentTimestamp(stopTimestamp: Timestamp): Boolean = {
    val query = dbProduct.toLowerCase match {
      case ORACLE_DB_NAME => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _              => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try {
        val currentTimestamp = if (rs.next()) rs.getTimestamp(1) else new Timestamp(0)
        stopTimestamp.getTime > currentTimestamp.getTime
      } finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  private[this] def queryData(startTimestamp: Timestamp, stopTimestamp: Timestamp): Seq[RowSourceRecord] = {
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
        val tableTimestampPartition                    = tableTimestampPartitionKey(tableName, startTimestamp, stopTimestamp)
        val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
        val rdbColumnInfo                              = columns(jdbcSourceConnectorConfig.dbTableName)
        val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

        val offset: JDBCOffsetInfo =
          offsetCache.readOffset(tableTimestampPartitionKey(tableName, startTimestamp, stopTimestamp))

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

  private[this] def tableTimestampPartitionKey(
    tableName: String,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): String = {
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }

  private[this] def isCompleted(startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean = {
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
        val dbCount =
          if (resultSet.next()) resultSet.getInt(1)
          else 0
        val tablePartition = tableTimestampPartitionKey(tableName, startTimestamp, stopTimestamp)
        offsetCache.readOffset(tablePartition).index == dbCount
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  private[this] def needToRun(startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean = {
    val tableName         = jdbcSourceConnectorConfig.dbTableName
    val taskTotal         = jdbcSourceConnectorConfig.taskTotal
    val taskHash          = jdbcSourceConnectorConfig.taskHash
    val partitionHashCode = tableTimestampPartitionKey(tableName, startTimestamp, stopTimestamp).hashCode()
    Math.abs(partitionHashCode) % taskTotal == taskHash
  }

  private[source] def columns(tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
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

  private[this] def values(schemaColumnName: String, dbColumnInfos: Seq[ColumnInfo[_]]): Any = {
    dbColumnInfos
      .find(_.columnName == schemaColumnName)
      .map(_.value)
      .getOrElse(throw new RuntimeException(s"Database Table not have the $schemaColumnName column"))
  }

  override protected def terminate(): Unit = Releasable.close(client)
}
