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
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import oharastream.ohara.kafka.connector.RowSourceRecord

trait TimestampQueryMode extends BaseQueryMode {
  def config: JDBCSourceConnectorConfig
  def offsetCache: JDBCOffsetCache
  def client: DatabaseClient
  def firstTimestampValue: Timestamp
}

object TimestampQueryMode {
  def builder: Builder = new Builder()

  class Builder private[source] extends oharastream.ohara.common.pattern.Builder[TimestampQueryMode] {
    private[this] var config: JDBCSourceConnectorConfig = _
    private[this] var offsetCache: JDBCOffsetCache      = _
    private[this] var client: DatabaseClient            = _
    private[this] var firstTimestampValue: Timestamp    = _

    def config(config: JDBCSourceConnectorConfig): Builder = {
      this.config = config
      this
    }

    def offsetCache(offsetCache: JDBCOffsetCache): Builder = {
      this.offsetCache = offsetCache
      this
    }

    def client(client: DatabaseClient): Builder = {
      this.client = client
      this
    }

    def firstTimestampValue(firstTimestampValue: Timestamp): Builder = {
      this.firstTimestampValue = firstTimestampValue
      this
    }

    override def build(): TimestampQueryMode = new TimestampQueryMode {
      override val config: JDBCSourceConnectorConfig = Builder.this.config
      override def offsetCache: JDBCOffsetCache      = Builder.this.offsetCache
      override def client: DatabaseClient            = Builder.this.client
      override def firstTimestampValue: Timestamp    = Builder.this.firstTimestampValue

      override protected[source] def queryData(
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = {
        null
      }

      override protected[source] def isCompleted(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Boolean = {
        val dbCount = count(startTimestamp, stopTimestamp)
        //val key     = partitionKey(config.dbTableName, firstTimestampValue, startTimestamp)

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
    }
  }
}
