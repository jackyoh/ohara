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
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import oharastream.ohara.kafka.connector._

import scala.jdk.CollectionConverters._

class JDBCSourceTask extends RowSourceTask {
  private[this] val TIMESTAMP_PARTITION_RANGE: Int = 86400000 // 1 day

  protected[this] var dbProduct: String              = _
  protected[this] var firstTimestampValue: Timestamp = _

  private[this] var config: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient            = _

  private[this] var offsetCache: JDBCOffsetCache = _
  private[this] var topics: Seq[TopicKey]        = _
  private[this] var schema: Seq[Column]          = _
  private[this] var queryMode: BaseQueryMode     = _

  override protected[source] def run(settings: TaskSetting): Unit = {
    config = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(config.dbURL)
      .user(config.dbUserName)
      .password(config.dbPassword)
      .build

    // setAutoCommit must be set to false when setting the fetch size
    client.connection.setAutoCommit(false)
    dbProduct = client.connection.getMetaData.getDatabaseProductName
    topics = settings.topicKeys().asScala.toSeq
    schema = settings.columns.asScala.toSeq
    offsetCache = new JDBCOffsetCache()
    firstTimestampValue = tableFirstTimestampValue(config.timestampColumnName)
    queryMode = TimestampQueryMode.builder
      .client(client)
      .config(config)
      .offsetCache(offsetCache)
      .firstTimestampValue(firstTimestampValue)
      .build()
  }

  override protected[source] def pollRecords(): java.util.List[RowSourceRecord] = {
    val timestampRange = calcTimestampRange(firstTimestampValue, firstTimestampValue)
    var startTimestamp = timestampRange._1
    var stopTimestamp  = replaceToCurrentTimestamp(timestampRange._2)

    // Generate the start timestamp and stop timestamp to run multi task for the query
    while (!needToRun(startTimestamp) ||
           queryMode.isCompleted(
             partitionKey(config.dbTableName, startTimestamp, stopTimestamp),
             startTimestamp,
             stopTimestamp
           )) {
      val currentTimestamp = current()
      val timestampRange   = calcTimestampRange(firstTimestampValue, stopTimestamp)

      if (timestampRange._2.getTime <= currentTimestamp.getTime) {
        startTimestamp = timestampRange._1
        stopTimestamp = timestampRange._2
      } else if (needToRun(currentTimestamp)) return queryData(stopTimestamp, currentTimestamp).asJava
      else return Seq.empty.asJava
    }
    queryData(startTimestamp, stopTimestamp).asJava
  }

  override protected[source] def terminate(): Unit = Releasable.close(client)

  private[this] def tableFirstTimestampValue(
    timestampColumnName: String
  ): Timestamp = {
    val sql = dbProduct.toUpperCase match {
      case ORACLE.name =>
        s"SELECT $timestampColumnName FROM ${config.dbTableName} ORDER BY $timestampColumnName FETCH FIRST 1 ROWS ONLY"
      case _ =>
        s"SELECT $timestampColumnName FROM ${config.dbTableName} ORDER BY $timestampColumnName LIMIT 1"
    }

    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getTimestamp(timestampColumnName)
        else new Timestamp(CommonUtils.current())
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def replaceToCurrentTimestamp(timestamp: Timestamp): Timestamp = {
    val currentTimestamp = current()
    if (timestamp.getTime > currentTimestamp.getTime) currentTimestamp
    else timestamp
  }

  private[source] def needToRun(timestamp: Timestamp): Boolean = {
    val partitionHashCode = partitionKey(config.dbTableName, firstTimestampValue, timestamp).hashCode()
    Math.abs(partitionHashCode) % config.taskTotal == config.taskHash
  }

  private[this] def queryData(startTimestamp: Timestamp, stopTimestamp: Timestamp): Seq[RowSourceRecord] = {
    val tableName           = config.dbTableName
    val timestampColumnName = config.timestampColumnName
    val key                 = partitionKey(tableName, firstTimestampValue, startTimestamp)
    offsetCache.loadIfNeed(rowContext, key)

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

  protected[source] def calcTimestampRange(
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): (Timestamp, Timestamp) = {
    if (timestamp.getTime < firstTimestampValue.getTime)
      throw new IllegalArgumentException("The timestamp less than the first data timestamp")
    val page                        = (timestamp.getTime - firstTimestampValue.getTime) / TIMESTAMP_PARTITION_RANGE
    val startTimestamp              = new Timestamp((page * TIMESTAMP_PARTITION_RANGE) + firstTimestampValue.getTime)
    val stopTimestamp               = new Timestamp(startTimestamp.getTime + TIMESTAMP_PARTITION_RANGE)
    val currentTimestamp: Timestamp = current()
    if (startTimestamp.getTime > currentTimestamp.getTime && stopTimestamp.getTime > currentTimestamp.getTime)
      throw new IllegalArgumentException("The timestamp over the current timestamp")
    (startTimestamp, stopTimestamp)
  }

  private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }

  private[this] def current(): Timestamp = {
    val query = dbProduct.toUpperCase match {
      case ORACLE.name => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _           => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try {
        if (rs.next()) rs.getTimestamp(1) else new Timestamp(0)
      } finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  private[source] def partitionKey(tableName: String, firstTimestampValue: Timestamp, timestamp: Timestamp): String = {
    val timestampRange = calcTimestampRange(firstTimestampValue, timestamp)
    s"$tableName:${timestampRange._1.toString}~${timestampRange._2.toString}"
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
}
