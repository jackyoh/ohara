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
  protected[this] var dbProduct: String              = _
  protected[this] var firstTimestampValue: Timestamp = _

  private[this] var config: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient            = _
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int    = 86400000 // 1 day
  private[this] var offsetCache: JDBCOffsetCache      = _
  private[this] var topics: Seq[TopicKey]             = _
  private[this] var schema: Seq[Column]               = _

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
    val timestampColumnName =
      config.timestampColumnName

    this.offsetCache = new JDBCOffsetCache()
    firstTimestampValue = tableFirstTimestampValue(timestampColumnName)
  }

  override protected[source] def pollRecords(): java.util.List[RowSourceRecord] = {
    var startTimestamp = firstTimestampValue
    var stopTimestamp  = replaceToCurrentTimestamp(new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE))

    // Generate the start timestap and stop timestamp to run multi task for the query
    while (!needToRun(stopTimestamp) ||
           isCompleted(startTimestamp, stopTimestamp)) {
      val currentTimestamp = current()
      val addTimestamp     = new Timestamp(stopTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (addTimestamp.getTime() > currentTimestamp.getTime()) {
        if (needToRun(currentTimestamp)) {
          return queryData(stopTimestamp, currentTimestamp).asJava
        } else return Seq.empty.asJava
      } else {
        startTimestamp = stopTimestamp
        stopTimestamp = addTimestamp
      }
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
        s"SELECT $timestampColumnName FROM ${config.dbTableName} ORDER BY $timestampColumnName limit 1"
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
    if (timestamp.getTime() > currentTimestamp.getTime()) currentTimestamp
    else timestamp
  }

  private[source] def needToRun(stopTimestamp: Timestamp): Boolean = {
    val partitionHashCode =
      partitionKey(config.dbTableName, firstTimestampValue, stopTimestamp).hashCode()
    Math.abs(partitionHashCode) % config.taskTotal == config.taskHash
  }

  private[source] def partitionKey(
    tableName: String,
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): String = {
    var startTimestamp: Timestamp   = firstTimestampValue
    var stopTimestamp: Timestamp    = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    val currentTimestamp: Timestamp = current()

    // TODO Refactor this function to remove while loop to calc partition key
    while (!(timestamp.getTime() >= startTimestamp.getTime() && timestamp.getTime() <= stopTimestamp.getTime())) {
      startTimestamp = stopTimestamp
      stopTimestamp = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (timestamp.getTime() < firstTimestampValue.getTime())
        throw new IllegalArgumentException("The timestamp over the first data timestamp")

      if (startTimestamp.getTime() > currentTimestamp.getTime() && stopTimestamp.getTime() > current()
            .getTime()) {
        throw new IllegalArgumentException("The timestamp over the current timestamp")
      }
    }
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }

  private[this] def queryData(startTimestamp: Timestamp, stopTimestamp: Timestamp): Seq[RowSourceRecord] = {
    val tableName           = config.dbTableName
    val timestampColumnName = config.timestampColumnName
    val key                 = partitionKey(tableName, firstTimestampValue, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, key)

    val sql =
      s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $timestampColumnName"
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
      this.client.connection.commit()
    }
  }

  /**
    * The start timestamp and stop timestamp range can't change.
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  private[source] def isCompleted(
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Boolean = {
    val dbCount = count(startTimestamp, stopTimestamp)
    val key     = partitionKey(config.dbTableName, firstTimestampValue, stopTimestamp)

    val offsetIndex = offsetCache.readOffset(key)
    if (dbCount < offsetIndex) {
      throw new IllegalArgumentException(
        s"The $startTimestamp~$stopTimestamp data offset index error ($dbCount < $offsetIndex). Please confirm your data"
      )
    } else offsetIndex == dbCount
  }

  private[this] def count(startTimestamp: Timestamp, stopTimestamp: Timestamp) = {
    val sql =
      s"SELECT count(*) FROM ${config.dbTableName} WHERE ${config.timestampColumnName} >= ? and ${config.timestampColumnName} < ?"

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
      this.client.connection.commit()
    }
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
