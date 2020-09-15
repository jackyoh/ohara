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
import oharastream.ohara.common.data.Column
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.kafka.connector._
import scala.jdk.CollectionConverters._

class JDBCSourceTask extends RowSourceTask {
  private[this] val TIMESTAMP_PARTITION_RANGE: Int = 86400000 // 1 day

  protected[this] var dbProduct: String              = _
  protected[this] var firstTimestampValue: Timestamp = _

  private[this] var config: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient            = _

  private[this] var topics: Seq[TopicKey]    = _
  private[this] var schema: Seq[Column]      = _
  private[this] var queryMode: BaseQueryMode = _

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
    firstTimestampValue = tableFirstTimestampValue(config.timestampColumnName)

    queryMode = config.incrementColumnName
      .map { _ =>
        TimestampIncrementQueryMode.builder
          .client(client)
          .config(config)
          .dbProduct(dbProduct)
          .rowSourceContext(rowContext)
          .topics(topics)
          .schema(schema)
          .build()
      }
      .getOrElse(
        TimestampQueryMode.builder
          .client(client)
          .config(config)
          .firstTimestampValue(firstTimestampValue)
          .dbProduct(dbProduct)
          .rowSourceContext(rowContext)
          .topics(topics)
          .schema(schema)
          .build()
      )
  }

  override protected[source] def pollRecords(): java.util.List[RowSourceRecord] = {
    val timestampRange = calcTimestampRange(firstTimestampValue, firstTimestampValue)
    var startTimestamp = timestampRange._1
    var stopTimestamp  = replaceToCurrentTimestamp(timestampRange._2)

    // Generate the start timestamp and stop timestamp to run multi task for the query
    while (!needToRun(startTimestamp) ||
           queryMode.isCompleted(
             partitionKey(config.dbTableName, firstTimestampValue, startTimestamp),
             startTimestamp,
             stopTimestamp
           )) {
      val currentTimestamp = current()
      val timestampRange   = calcTimestampRange(firstTimestampValue, stopTimestamp)

      if (timestampRange._2.getTime <= currentTimestamp.getTime) {
        startTimestamp = timestampRange._1
        stopTimestamp = timestampRange._2
      } /*else if (needToRun(currentTimestamp)) {
        return queryMode
          .queryData(
            partitionKey(config.dbTableName, firstTimestampValue, stopTimestamp),
            stopTimestamp,
            currentTimestamp
          )
          .asJava
      }*/
      else return Seq.empty.asJava
    }
    queryMode
      .queryData(partitionKey(config.dbTableName, firstTimestampValue, startTimestamp), startTimestamp, stopTimestamp)
      .asJava
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

  private[source] def calcTimestampRange(
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): (Timestamp, Timestamp) = {
    if (timestamp.getTime < firstTimestampValue.getTime)
      throw new IllegalArgumentException("The timestamp less than the first data timestamp")
    val page             = (timestamp.getTime - firstTimestampValue.getTime) / TIMESTAMP_PARTITION_RANGE
    val startTimestamp   = new Timestamp((page * TIMESTAMP_PARTITION_RANGE) + firstTimestampValue.getTime)
    val stopTimestamp    = new Timestamp(startTimestamp.getTime + TIMESTAMP_PARTITION_RANGE)
    val currentTimestamp = current()
    if (startTimestamp.getTime > currentTimestamp.getTime && stopTimestamp.getTime > currentTimestamp.getTime)
      throw new IllegalArgumentException("The timestamp over the current timestamp")
    (startTimestamp, stopTimestamp)
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
}
