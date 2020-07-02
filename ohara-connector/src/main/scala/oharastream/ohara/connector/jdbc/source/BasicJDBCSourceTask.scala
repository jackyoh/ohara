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

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

import scala.jdk.CollectionConverters._

abstract class BasicJDBCSourceTask extends RowSourceTask {
  protected[this] var dbProduct: String              = _
  protected[this] var firstTimestampValue: Timestamp = _

  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int                       = 86400000 // 1 day

  override protected[source] def run(settings: TaskSetting): Unit = {
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    dbProduct = client.connection.getMetaData.getDatabaseProductName
    val topics = settings.topicKeys().asScala.toSeq
    val schema = settings.columns.asScala.toSeq
    val timestampColumnName =
      jdbcSourceConnectorConfig.timestampColumnName

    runJDBCSourceTask(jdbcSourceConnectorConfig, topics, schema)
    firstTimestampValue = tableFirstTimestampValue(timestampColumnName)
  }

  override protected[source] def pollRecords(): util.List[RowSourceRecord] = {
    var startTimestamp = firstTimestampValue
    var stopTimestamp  = replaceToCurrentTimestamp(new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE))

    // Generate the start timestap and stop timestamp to runn multi task for the query
    while (!needToRun(stopTimestamp) ||
           isCompleted(client, startTimestamp, stopTimestamp)) {
      val currentTimestamp = current()
      val addTimestamp     = new Timestamp(stopTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (addTimestamp.getTime() > currentTimestamp.getTime()) {
        if (needToRun(currentTimestamp)) return queryData(client, stopTimestamp, currentTimestamp).asJava
        else return Seq.empty.asJava
      } else {
        startTimestamp = stopTimestamp
        stopTimestamp = addTimestamp
      }
    }
    queryData(client, startTimestamp, stopTimestamp).asJava
  }

  private[this] def replaceToCurrentTimestamp(timestamp: Timestamp): Timestamp = {
    val currentTimestamp = current()
    if (timestamp.getTime() > currentTimestamp.getTime()) currentTimestamp
    else timestamp
  }

  protected[this] def current(): Timestamp = {
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

  protected[this] def tableFirstTimestampValue(
    timestampColumnName: String
  ): Timestamp = {
    val statement = client.connection.createStatement()
    try {
      val resultSet =
        statement.executeQuery(tableFirstTimestampValueSQL())
      try {
        if (!resultSet.next()) new Timestamp(CommonUtils.current())
        else resultSet.getTimestamp(timestampColumnName)
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  protected[this] def columns(tableName: String): Seq[RdbColumn] = {
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

  private[source] def tableTimestampPartitionKey(
    tableName: String,
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): String = {
    var startTimestamp: Timestamp   = firstTimestampValue
    var stopTimestamp: Timestamp    = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    val currentTimestamp: Timestamp = current()
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

  private[this] def needToRun(stopTimestamp: Timestamp): Boolean = {
    val tableName         = jdbcSourceConnectorConfig.dbTableName
    val taskTotal         = jdbcSourceConnectorConfig.taskTotal
    val taskHash          = jdbcSourceConnectorConfig.taskHash
    val partitionHashCode = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp).hashCode()
    Math.abs(partitionHashCode) % taskTotal == taskHash
  }

  override protected def terminate(): Unit = Releasable.close(client)

  protected[this] def runJDBCSourceTask(
    jdbcSourceConnectorConfig: JDBCSourceConnectorConfig,
    topics: Seq[TopicKey],
    schema: Seq[Column]
  ): Unit

  protected[this] def tableFirstTimestampValueSQL(): String

  protected[this] def isCompleted(client: DatabaseClient, startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean

  protected[this] def queryData(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord]
}
