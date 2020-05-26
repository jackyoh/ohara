package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

import scala.jdk.CollectionConverters._

class MultiNodeJDBCSourceTask extends RowSourceTask {
  private[this] var jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = _
  private[this] var client: DatabaseClient                               = _
  private[this] var testCount: Int                                       = 0
  private[this] var dbProduct: String = _

  override protected def run(settings: TaskSetting): Unit = {
    jdbcSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    dbProduct = client.connection.getMetaData.getDatabaseProductName
  }

  override protected def pollRecords(): util.List[RowSourceRecord] = {
    val tableName           = jdbcSourceConnectorConfig.dbTableName
    val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName
    TimeUnit.SECONDS.sleep(1)

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
    }
    Seq.empty.asJava
  }

  override protected def terminate(): Unit = {
    // Nothing
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
    startTimestamp.getTime()
    stopTimestamp.getTime()

    testCount = testCount + 1
    !(testCount % 10 == 0)

    //false
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
}
