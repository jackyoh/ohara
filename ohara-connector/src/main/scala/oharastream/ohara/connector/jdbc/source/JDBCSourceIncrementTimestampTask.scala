package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import oharastream.ohara.kafka.connector.RowSourceRecord

import scala.jdk.CollectionConverters._

class JDBCSourceIncrementTimestampTask extends BasicJDBCSourceTask {
  private[this] var topics: Seq[TopicKey]                                = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var timestampColumnName: String                          = _
  private[this] var incrementColumnName: String                          = _
  private[this] var offsetCache: JDBCOffsetCache                         = _

  override protected[this] def runJDBCSourceTask(
    jdbcSourceConnectorConfig: JDBCSourceConnectorConfig,
    topics: Seq[TopicKey],
    schema: Seq[Column]
  ): Unit = {
    val incrementTimestampColumnNames =
      jdbcSourceConnectorConfig.incrementTimestampColumnName.split(SPLIT_INCREMENT_TIMESTAMP_COLUMN_COMMA)
    this.jdbcSourceConnectorConfig = jdbcSourceConnectorConfig
    this.incrementColumnName = incrementTimestampColumnNames.head
    this.timestampColumnName = incrementTimestampColumnNames.last
    this.topics = topics
    this.schema = schema
    this.offsetCache = new JDBCOffsetCache()
  }

  protected[this] def queryData(
    client: DatabaseClient,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord] = {
    val tableName      = jdbcSourceConnectorConfig.dbTableName
    val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, tablePartition)

    val topicOffset: JDBCOffsetInfo =
      offsetCache.readOffset(tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp))

    val sql =
      s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? and $incrementColumnName > ? ORDER BY $timestampColumnName, $incrementColumnName"
    val prepareStatement = client.connection.prepareStatement(sql)
    try {
      prepareStatement.setFetchSize(jdbcSourceConnectorConfig.jdbcFetchDataSize)
      prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
      prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
      prepareStatement.setLong(3, topicOffset.index)

      val resultSet = prepareStatement.executeQuery()
      try {
        val tableTimestampPartition                    = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
        val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
        val rdbColumnInfo                              = columns(jdbcSourceConnectorConfig.dbTableName)
        val results                                    = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)

        results
          .take(jdbcSourceConnectorConfig.jdbcFlushDataSize)
          .flatMap { columns =>
            val newSchema =
              if (schema.isEmpty)
                columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
              else schema
            val idValue = columns
              .find(_.columnName == incrementColumnName)
              .map(_.value.toString())
              .getOrElse(throw new IllegalArgumentException(s"The $incrementColumnName column is not found"))
              .toLong
            val topicOffset: JDBCOffsetInfo =
              offsetCache.readOffset(tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp))

            if (idValue > topicOffset.index) {
              val offset = JDBCOffsetInfo(idValue)

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
            } else Seq.empty
          }
          .toSeq
      } finally Releasable.close(resultSet)
    } finally Releasable.close(prepareStatement)
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
    val tableName = jdbcSourceConnectorConfig.dbTableName
    val sql =
      s"SELECT $incrementColumnName FROM $tableName WHERE $timestampColumnName >= ? and $timestampColumnName < ? ORDER BY $incrementColumnName DESC"
    val statement = client.connection.prepareStatement(sql)
    try {
      statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
      statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
      val resultSet = statement.executeQuery()
      try {
        val id =
          if (resultSet.next()) resultSet.getLong(1)
          else 0
        val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
        val offsetIndex    = offsetCache.readOffset(tablePartition).index
        if (offsetIndex > id)
          throw new IllegalArgumentException(
            s"The $startTimestamp~$stopTimestamp data offset index error ($offsetIndex > $id). Please confirm your data"
          )
        else offsetIndex == id
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
  }

  override protected def tableFirstTimestampValueSQL(): String =
    s"SELECT $timestampColumnName FROM ${jdbcSourceConnectorConfig.dbTableName} ORDER BY $timestampColumnName, $incrementColumnName"
}
