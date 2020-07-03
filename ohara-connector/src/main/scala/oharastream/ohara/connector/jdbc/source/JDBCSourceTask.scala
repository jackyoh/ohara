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

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}
import scala.jdk.CollectionConverters._

class JDBCSourceTask extends RowSourceTask {
  protected[this] var dbProduct: String              = _
  protected[this] var firstTimestampValue: Timestamp = _

  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] var dataModeHandler: DataModeHandler                     = _
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int                       = 86400000 // 1 day
  private[this] var offsetCache: JDBCOffsetCache                         = _

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

    this.offsetCache = new JDBCOffsetCache()

    dataModeHandler =
      if (jdbcSourceConnectorConfig.incrementColumnName.isEmpty)
        DataModeHandler.timestampMode
          .jdbcSourceConnectorConfig(jdbcSourceConnectorConfig)
          .dbProduct(dbProduct)
          .schema(schema)
          .topics(topics)
          .offsetCache(offsetCache)
          .build()
      else
        DataModeHandler.incrementTimestampMode
          .jdbcSourceConnectorConfig(jdbcSourceConnectorConfig)
          .dbProduct(dbProduct)
          .schema(schema)
          .topics(topics)
          .offsetCache(offsetCache)
          .build()

    firstTimestampValue = tableFirstTimestampValue(timestampColumnName)
  }

  override protected[source] def pollRecords(): util.List[RowSourceRecord] = {
    val tableName      = jdbcSourceConnectorConfig.dbTableName
    var startTimestamp = firstTimestampValue
    var stopTimestamp  = replaceToCurrentTimestamp(new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE))

    // Generate the start timestap and stop timestamp to runn multi task for the query
    while (!needToRun(stopTimestamp) ||
           isCompleted(client, startTimestamp, stopTimestamp)) {
      val currentTimestamp = current()
      val addTimestamp     = new Timestamp(stopTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (addTimestamp.getTime() > currentTimestamp.getTime()) {
        if (needToRun(currentTimestamp)) {
          val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, currentTimestamp)
          return dataModeHandler.queryData(client, stopTimestamp, currentTimestamp, tablePartition).asJava
        } else return Seq.empty.asJava
      } else {
        startTimestamp = stopTimestamp
        stopTimestamp = addTimestamp
      }
    }
    val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, tablePartition)
    dataModeHandler.queryData(client, startTimestamp, stopTimestamp, tablePartition).asJava
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
      val tableName = jdbcSourceConnectorConfig.dbTableName
      val sql       = s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName"
      val resultSet =
        statement.executeQuery(sql)
      try {
        if (!resultSet.next()) new Timestamp(CommonUtils.current())
        else resultSet.getTimestamp(timestampColumnName)
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
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
    val tableName      = jdbcSourceConnectorConfig.dbTableName
    val dbCount        = dataModeHandler.queryTableCount(client, startTimestamp, stopTimestamp)
    val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
    val offsetIndex    = offsetCache.readOffset(tablePartition).index
    if (dbCount < offsetIndex) {
      throw new IllegalArgumentException(
        s"The $startTimestamp~$stopTimestamp data offset index error ($dbCount < $offsetIndex). Please confirm your data"
      )
    } else offsetIndex == dbCount
  }

  override protected def terminate(): Unit = Releasable.close(client)
}
