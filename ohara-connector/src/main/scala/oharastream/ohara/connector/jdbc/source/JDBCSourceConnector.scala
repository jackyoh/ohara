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

import java.util.concurrent.atomic.AtomicInteger

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector._
import org.slf4j.{Logger, LoggerFactory}
import scala.jdk.CollectionConverters._

/**
  * This class for JDBC Source connector plugin
  */
class JDBCSourceConnector extends RowSourceConnector {
  private[this] var settings: TaskSetting = _

  /**
    * Start this Connector. This method will only be called on a clean Connector, i.e. it has
    * either just been instantiated and initialized or stop() has been invoked.
    *
    * @param settings configuration settings
    */
  override protected def run(settings: TaskSetting): Unit = {
    this.settings = settings

    val jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    val tableName                                            = jdbcSourceConnectorConfig.dbTableName
    val timestampColumnName                                  = jdbcSourceConnectorConfig.timestampColumnName

    val client = DatabaseClient.builder
      .url(jdbcSourceConnectorConfig.dbURL)
      .user(jdbcSourceConnectorConfig.dbUserName)
      .password(jdbcSourceConnectorConfig.dbPassword)
      .build
    try {
      checkTimestampColumnName(timestampColumnName)

      if (client.tableQuery.tableName(tableName).execute().isEmpty)
        throw new NoSuchElementException(s"$tableName table is not found.")
    } catch {
      case e: Exception => throw new RuntimeException(e)
    } finally Releasable.close(client)
  }

  /**
    * Returns the RowSourceTask implementation for this Connector.
    *
    * @return a JDBCSourceTask class
    */
  override protected def taskClass(): Class[_ <: RowSourceTask] = classOf[JDBCSourceTask]

  /**
    * Return the settings for source task.
    *
    * @return a seq from settings
    */
  override protected def taskSettings(maxTasks: Int): java.util.List[TaskSetting] =
    Seq
      .fill(maxTasks)(settings)
      .zipWithIndex
      .map {
        case (setting, index) =>
          setting.append(java.util.Map.of(TASK_TOTAL_KEY, maxTasks.toString, TASK_HASH_KEY, index.toString))
      }
      .asJava

  /**
    * stop this connector
    */
  override protected def terminate(): Unit = {
    // Nothing
  }

  protected[jdbc] def checkTimestampColumnName(timestampColumnName: String): Unit = {
    if (timestampColumnName == null)
      throw new NoSuchElementException(s"Timestamp column is null, Please input timestamp type column name.")

    if (timestampColumnName.isEmpty)
      throw new NoSuchElementException(s"Timestamp column is empty, Please input timestamp type column name.")

    if (!timestampColumnName.matches("^[a-zA-Z]{1}.*"))
      throw new IllegalArgumentException("Your column name input error, Please checkout your column name.")
  }

  /**
    * used to set the order of definitions.
    */
  private[this] val counter = new AtomicInteger(0)
  override protected def customSettingDefinitions(): java.util.Map[String, SettingDef] =
    Map(
      DB_URL -> SettingDef
        .builder()
        .displayName("jdbc url")
        .documentation("Connection database url")
        .required(SettingDef.Type.STRING)
        .key(DB_URL)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      DB_USERNAME -> SettingDef
        .builder()
        .displayName("user name")
        .documentation("Connection database user name")
        .required(SettingDef.Type.STRING)
        .key(DB_USERNAME)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      DB_PASSWORD -> SettingDef
        .builder()
        .displayName("password")
        .documentation("Connection database user password")
        .required(SettingDef.Type.PASSWORD)
        .key(DB_PASSWORD)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      DB_TABLENAME -> SettingDef
        .builder()
        .displayName("table name")
        .documentation("write to topic from database table name")
        .required(SettingDef.Type.JDBC_TABLE)
        .key(DB_TABLENAME)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      DB_CATALOG_PATTERN -> SettingDef
        .builder()
        .displayName("catalog pattern")
        .documentation("database metadata catalog")
        .optional(SettingDef.Type.STRING)
        .key(DB_CATALOG_PATTERN)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      DB_SCHEMA_PATTERN -> SettingDef
        .builder()
        .displayName("schema pattern")
        .documentation("database metadata schema pattern")
        .optional(SettingDef.Type.STRING)
        .key(DB_SCHEMA_PATTERN)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      MODE -> SettingDef
        .builder()
        .displayName("mode")
        .documentation("Only support timestamp column")
        .key(MODE)
        .optional(MODE_DEFAULT)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      TIMESTAMP_COLUMN_NAME -> SettingDef
        .builder()
        .displayName("timestamp column name")
        .documentation("Use a timestamp column to detect new and modified rows")
        .required(SettingDef.Type.STRING)
        .key(TIMESTAMP_COLUMN_NAME)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      JDBC_FETCHDATA_SIZE -> SettingDef
        .builder()
        .displayName("JDBC fetch size")
        .documentation("Setting JDBC fetch data size for ResultSet")
        .key(JDBC_FETCHDATA_SIZE)
        .optional(JDBC_FETCHDATA_SIZE_DEFAULT)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      JDBC_FLUSHDATA_SIZE -> SettingDef
        .builder()
        .displayName("JDBC flush size")
        .documentation("Setting Data flush to topic size")
        .key(JDBC_FLUSHDATA_SIZE)
        .optional(JDBC_FLUSHDATA_SIZE_DEFAULT)
        .orderInGroup(counter.getAndIncrement())
        .build(),
      JDBC_FREQUENCE_TIME -> SettingDef
        .builder()
        .displayName("Fetch data frequence")
        .documentation("Setting fetch data frequency from database")
        .key(JDBC_FREQUENCE_TIME)
        .optional(java.time.Duration.ofMillis(JDBC_FREQUENCE_TIME_DEFAULT.toMillis))
        .orderInGroup(counter.getAndIncrement())
        .build()
    ).asJava
}

object JDBCSourceConnector {
  val LOG: Logger = LoggerFactory.getLogger(classOf[JDBCSourceConnector])
}
