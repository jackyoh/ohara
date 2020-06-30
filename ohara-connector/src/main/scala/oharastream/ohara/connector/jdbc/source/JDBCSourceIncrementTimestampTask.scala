package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp
import java.util

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}
import scala.jdk.CollectionConverters._

class JDBCSourceIncrementTimestampTask extends RowSourceTask {
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] var dbProduct: String                                    = _
  private[this] var offsetCache: JDBCOffsetCache                         = _
  private[this] var topics: Seq[TopicKey]                                = _
  private[this] var schema: Seq[Column]                                  = _
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int                       = 86400000 // 1 day
  private[this] var firstTimestampValue: Timestamp                       = _
  private[this] var incrementColumnName: String                          = _
  private[this] var timestampColumnName: String                          = _

  override protected[source] def run(settings: TaskSetting): Unit = {
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    dbProduct = client.connection.getMetaData.getDatabaseProductName
    offsetCache = new JDBCOffsetCache()
    topics = settings.topicKeys().asScala.toSeq
    schema = settings.columns.asScala.toSeq
    val tableName = jdbcSourceConnectorConfig.dbTableName
    val incrementTimestampColumnNames =
      jdbcSourceConnectorConfig.incrementTimestampColumnName.split(SPLIT_INCREMENT_TIMESTAMP_COLUMN_COMMA)
    val incrementColumnName = incrementTimestampColumnNames.head
    val timestampColumnName = incrementTimestampColumnNames.last
    firstTimestampValue = tableFirstTimestampValue(tableName, timestampColumnName, incrementColumnName)
  }

  override protected[source] def pollRecords(): util.List[RowSourceRecord] = {
    val tableName      = jdbcSourceConnectorConfig.dbTableName
    var startTimestamp = firstTimestampValue
    var stopTimestamp  = replaceToCurrentTimestamp(new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE))
    val tablePartition = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp)
    offsetCache.loadIfNeed(rowContext, tablePartition)

    // Generate the start timestap and stop timestamp to runn multi task for the query
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

  private[this] def tableFirstTimestampValue(
    tableName: String,
    timestampColumnName: String,
    incrementColumnName: String
  ): Timestamp = {
    val statement = client.connection.createStatement()
    try {
      val resultSet =
        statement.executeQuery(
          s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName, $incrementColumnName"
        )
      try {
        if (!resultSet.next()) new Timestamp(CommonUtils.current())
        else resultSet.getTimestamp(timestampColumnName)
      } finally Releasable.close(resultSet)
    } finally Releasable.close(statement)
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

  private[this] def replaceToCurrentTimestamp(timestamp: Timestamp): Timestamp = {
    val currentTimestamp = current()
    if (timestamp.getTime() > currentTimestamp.getTime()) current
    else timestamp
  }

  private[this] def queryData(startTimestamp: Timestamp, stopTimestamp: Timestamp): Seq[RowSourceRecord] = {
    val tableName = jdbcSourceConnectorConfig.dbTableName
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
              .map(_.value.toString)
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

      if (startTimestamp.getTime() > currentTimestamp.getTime() && stopTimestamp.getTime() > current.getTime()) {
        throw new IllegalArgumentException("The timestamp over the current timestamp")
      }
    }
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }

  /**
    * The start timestamp and stop timestamp range can't change.
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  private[this] def isCompleted(startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean = {
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

  private[this] def needToRun(stopTimestamp: Timestamp): Boolean = {
    val tableName         = jdbcSourceConnectorConfig.dbTableName
    val taskTotal         = jdbcSourceConnectorConfig.taskTotal
    val taskHash          = jdbcSourceConnectorConfig.taskHash
    val partitionHashCode = tableTimestampPartitionKey(tableName, firstTimestampValue, stopTimestamp).hashCode()
    Math.abs(partitionHashCode) % taskTotal == taskHash
  }

  private[source] def columns(tableName: String): Seq[RdbColumn] = {
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

  override protected def terminate(): Unit = Releasable.close(client)
}
