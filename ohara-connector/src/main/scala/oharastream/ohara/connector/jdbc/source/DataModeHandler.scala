package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.Column
import oharastream.ohara.kafka.connector.RowSourceRecord
import oharastream.ohara.common.pattern.Builder
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.util.DateTimeUtils

trait DataModeHandler {
  protected[source] def firstTimestampValueSQL(): String

  protected[source] def queryTableCount(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Long

  protected[source] def queryData(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord]
}

object DataModeHandler {
  def timestampMode: TimestampDataModeHandler = new TimestampDataModeHandler
  class TimestampDataModeHandler private[DataModeHandler] extends Builder[DataModeHandler] {
    private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
    private[this] var schema: Seq[Column]                                  = _
    //private[this] var topics: Seq[TopicKey]                                = _

    def jdbcSourceConnectorConfig(jdbcSourceConnectorConfig: JDBCSourceConnectorConfig): TimestampDataModeHandler = {
      this.jdbcSourceConnectorConfig = jdbcSourceConnectorConfig
      this
    }

    def schema(schema: Seq[Column]): TimestampDataModeHandler = {
      this.schema = schema
      this
    }

    /*def topics(topics: Seq[TopicKey]): TimestampDataModeHandler = {
      this.topics
      this
    }*/

    override def build(): DataModeHandler = new DataModeHandler() {
      override protected[source] def queryTableCount(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Long = {
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
            if (resultSet.next()) resultSet.getInt(1)
            else 0
          } finally Releasable.close(resultSet)
        } finally Releasable.close(statement)
      }

      override protected[source] def queryData(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = ???

      override protected[source] def firstTimestampValueSQL(): String = {
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName"
      }
    }
  }

  def incrementTimestampMode: IncrementTimestampDataModeHandler = new IncrementTimestampDataModeHandler
  class IncrementTimestampDataModeHandler private[DataModeHandler] extends Builder[DataModeHandler] {
    private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
    private[this] var schema: Seq[Column]                                  = _
    // private[this] var topics: Seq[TopicKey]                                = _

    def jdbcSourceConnectorConfig(
      jdbcSourceConnectorConfig: JDBCSourceConnectorConfig
    ): IncrementTimestampDataModeHandler = {
      this.jdbcSourceConnectorConfig = jdbcSourceConnectorConfig
      this
    }

    def schema(schema: Seq[Column]): IncrementTimestampDataModeHandler = {
      this.schema = schema
      this
    }

    /*def topics(topics: Seq[TopicKey]): IncrementTimestampDataModeHandler = {
      this.topics
      this
    }*/

    override def build(): DataModeHandler = new DataModeHandler {
      override protected[source] def queryTableCount(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Long = {
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        val incrementColumnName = jdbcSourceConnectorConfig.incrementColumnName
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
        val sql =
          s"SELECT $incrementColumnName FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $incrementColumnName DESC"
        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try {
            if (resultSet.next()) resultSet.getLong(1)
            else 0
          } finally Releasable.close(resultSet)
        } finally Releasable.close(statement)
      }

      override protected[source] def queryData(
        client: DatabaseClient,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = ???

      override protected[source] def firstTimestampValueSQL(): String = {
        val tableName           = jdbcSourceConnectorConfig.dbTableName
        val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
        val incrementColumnName = jdbcSourceConnectorConfig.incrementColumnName
        s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName, $incrementColumnName"
      }
    }
  }
}
