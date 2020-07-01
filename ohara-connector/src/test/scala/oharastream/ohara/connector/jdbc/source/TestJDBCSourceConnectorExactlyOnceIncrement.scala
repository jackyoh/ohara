package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.kafka.connector.TaskSetting
import scala.jdk.CollectionConverters._

class TestJDBCSourceConnectorExactlyOnceIncrement(inputDataTime: Long)
    extends BasicTestJDBCSourceConnectorExactlyOnce(inputDataTime: Long) {
  override protected[this] val jdbcSourceConnectorProps: JDBCSourceConnectorConfig = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL                          -> db.url,
        DB_USERNAME                     -> db.user,
        DB_PASSWORD                     -> db.password,
        DB_TABLENAME                    -> tableName,
        INCREMENT_TIMESTAMP_COLUMN_NAME -> timestampColumnName,
        TASK_TOTAL_KEY                  -> "0",
        TASK_HASH_KEY                   -> "0"
      ).asJava
    )
  )
}