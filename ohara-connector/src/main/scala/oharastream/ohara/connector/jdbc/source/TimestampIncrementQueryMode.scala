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
import oharastream.ohara.kafka.connector.{RowSourceContext, RowSourceRecord}

trait TimestampIncrementQueryMode extends BaseQueryMode {
  def config: JDBCSourceConnectorConfig
  def offsetCache: JDBCOffsetCache
  def rowSourceContext: RowSourceContext
  def client: DatabaseClient
}

object TimestampIncrementQueryMode {
  def builder: Builder = new Builder()

  class Builder private[source] extends oharastream.ohara.common.pattern.Builder[TimestampIncrementQueryMode] {
    private[this] var config: JDBCSourceConnectorConfig  = _
    private[this] var rowSourceContext: RowSourceContext = _
    private[this] var client: DatabaseClient             = _

    def config(config: JDBCSourceConnectorConfig): Builder = {
      this.config = config
      this
    }

    def rowSourceContext(rowSourceContext: RowSourceContext): Builder = {
      this.rowSourceContext = rowSourceContext
      this
    }

    def client(client: DatabaseClient): Builder = {
      this.client = client
      this
    }

    override def build(): TimestampIncrementQueryMode = new TimestampIncrementQueryMode() {
      override val offsetCache: JDBCOffsetCache       = new JDBCOffsetCache()
      override val config: JDBCSourceConnectorConfig  = Builder.this.config
      override val rowSourceContext: RowSourceContext = Builder.this.rowSourceContext
      override val client: DatabaseClient             = Builder.this.client

      override protected[source] def queryData(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = {
        Seq.empty
      }

      override protected[source] def isCompleted(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Boolean = {
        val value       = incrementValue(startTimestamp, stopTimestamp)
        val offsetIndex = offsetCache.readOffset(key)
        if (value < offsetIndex)
          throw new IllegalArgumentException("Then table increment value less than topic offset value")
        else value == offsetIndex
      }

      private[this] def incrementValue(startTimestamp: Timestamp, stopTimestamp: Timestamp): Int = {
        val incrementColumnName =
          config.incrementColumnName.getOrElse(throw new IllegalArgumentException("The increment column not setting"))
        val sql =
          s"SELECT $incrementColumnName FROM ${config.dbTableName} WHERE ${config.timestampColumnName} >= ? AND ${config.timestampColumnName} < ? ORDER BY $incrementColumnName DESC"

        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try if (resultSet.next()) resultSet.getInt(1)
          else 0
          finally Releasable.close(resultSet)
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
