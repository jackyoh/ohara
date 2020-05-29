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
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}
import scala.jdk.CollectionConverters._

class MultiNodeJDBCSourceTask extends RowSourceTask {
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] var dbProduct: String                                    = _
  private[this] var offsetCache: JDBCOffsetCache                         = _

  override protected def run(settings: TaskSetting): Unit = {
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    dbProduct = client.connection.getMetaData.getDatabaseProductName
    offsetCache = new JDBCOffsetCache()
  }

  override protected def pollRecords(): util.List[RowSourceRecord] = {
    val tableName           = jdbcSourceConnectorConfig.dbTableName
    val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName

    val firstTimestampValue = tableFirstTimestampValue(tableName, timestampColumnName)

    if (firstTimestampValue.nonEmpty) {
      var startTimestamp = firstTimestampValue.get
      var stopTimestamp  = new Timestamp(startTimestamp.getTime() + 86400000)

      while (partitionIsCompleted(startTimestamp, stopTimestamp)) { // TODO Check hash code
        startTimestamp = stopTimestamp
        stopTimestamp = new Timestamp(startTimestamp.getTime() + 86400000)

        if (overCurrentTimestamp(startTimestamp, stopTimestamp)) return Seq.empty.asJava
      }
      // TODO for query data
      println(s"Start timestamp is: ${startTimestamp}    Stop timestamp is: ${stopTimestamp}")

      val timestampTableParation = s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
      offsetCache.update(timestampTableParation, JDBCOffsetInfo(0, true)) // TODO Get database data
    }
    Seq.empty.asJava
  }

  private[this] def tableFirstTimestampValue(tableName: String, timestampColumnName: String): Option[Timestamp] = {
    val statement = client.connection.createStatement()
    try {
      val resultSet =
        statement.executeQuery(s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName")
      try {
        if (!resultSet.next()) Option.empty
        else Option(resultSet.getTimestamp(timestampColumnName))
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  private[this] def partitionIsCompleted(startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean = {
    val timestampTableParation =
      tableTimestampParationKey(jdbcSourceConnectorConfig.dbTableName, startTimestamp, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, timestampTableParation)
    val offset: Option[JDBCOffsetInfo] = offsetCache.readOffset(timestampTableParation)
    offset
      .map(
        _.isCompleted
      )
      .getOrElse(false)
  }

  private[this] def overCurrentTimestamp(startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean = {
    val query = dbProduct.toLowerCase match {
      case ORACLE_DB_NAME => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _              => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try {
        val currentTimestamp = if (rs.next()) rs.getTimestamp(1) else new Timestamp(0)
        (startTimestamp.getTime > currentTimestamp.getTime && stopTimestamp.getTime > currentTimestamp.getTime)
      } finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  private[this] def tableTimestampParationKey(
    tableName: String,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): String = {
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }

  override protected def terminate(): Unit = {
    // Nothing
  }
}
