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

package com.island.ohara.connector.jdbc.source

import java.util.concurrent.atomic.AtomicInteger

import com.island.ohara.common.setting.SettingDef
import com.island.ohara.kafka.connector._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * This class for JDBC Source connector plugin
  */
class JDBCSourceConnector extends RowSourceConnector {

  private[this] var settings: TaskSetting = _

  /**
    * Start this Connector. This method will only be called on a clean Connector, i.e. it has
    * either just been instantiated and initialized or _stop() has been invoked.
    *
    * @param settings configuration settings
    */
  override protected def _start(settings: TaskSetting): Unit = {
    this.settings = settings

    val jdbcSourceConnectorConfig: JDBCSourceConnectorConfig = JDBCSourceConnectorConfig(settings)
    val tableName = jdbcSourceConnectorConfig.dbTableName
    val timestampColumnName = jdbcSourceConnectorConfig.timestampColumnName

    val dbTableDataProvider: DBTableDataProvider = new DBTableDataProvider(jdbcSourceConnectorConfig)
    try {
      checkTimestampColumnName(timestampColumnName)

      if (!dbTableDataProvider.isTableExists(tableName))
        throw new NoSuchElementException(s"$tableName table is not found.")

    } finally dbTableDataProvider.close()
  }

  /**
    * Returns the RowSourceTask implementation for this Connector.
    *
    * @return a JDBCSourceTask class
    */
  override protected def _taskClass(): Class[_ <: RowSourceTask] = {
    classOf[JDBCSourceTask]
  }

  /**
    * Return the settings for source task.
    *
    * @return a seq from settings
    */
  override protected def _taskSettings(maxTasks: Int): java.util.List[TaskSetting] = {
    //TODO
    Seq(settings).asJava
  }

  /**
    * stop this connector
    */
  override protected def _stop(): Unit = {
    //TODO
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
  override protected def _definitions(): java.util.List[SettingDef] = Seq(
    SettingDef
      .builder()
      .displayName("jdbc url")
      .documentation("Connection database url")
      .valueType(SettingDef.Type.STRING)
      .key(DB_URL)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("user name")
      .documentation("Connection database user name")
      .valueType(SettingDef.Type.STRING)
      .key(DB_USERNAME)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("password")
      .documentation("Connection database user password")
      .valueType(SettingDef.Type.PASSWORD)
      .key(DB_PASSWORD)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("table name")
      .documentation("write to topic from database table name")
      .valueType(SettingDef.Type.JDBC_TABLE)
      .key(DB_TABLENAME)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("catalog pattern")
      .documentation("database metadata catalog")
      .valueType(SettingDef.Type.STRING)
      .key(DB_CATALOG_PATTERN)
      .optional()
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("schema pattern")
      .documentation("database metadata schema pattern")
      .valueType(SettingDef.Type.STRING)
      .key(DB_SCHEMA_PATTERN)
      .optional()
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("mode")
      .documentation("Only support timestamp column")
      .valueType(SettingDef.Type.STRING)
      .key(MODE)
      .optional(MODE_DEFAULT)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("timestamp column name")
      .documentation("Use a timestamp column to detect new and modified rows")
      .valueType(SettingDef.Type.STRING)
      .key(TIMESTAMP_COLUMN_NAME)
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("JDBC fetch size")
      .documentation("Setting JDBC fetch data size for ResultSet")
      .valueType(SettingDef.Type.INT)
      .key(JDBC_FETCHDATA_SIZE)
      .optional(String.valueOf(JDBC_FETCHDATA_SIZE_DEFAULT))
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("JDBC flush size")
      .documentation("Setting Data flush to topic size")
      .valueType(SettingDef.Type.INT)
      .key(JDBC_FLUSHDATA_SIZE)
      .optional(String.valueOf(JDBC_FLUSHDATA_SIZE_DEFAULT))
      .orderInGroup(counter.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("Fetch data frequence")
      .documentation("Setting fetch data frequency from database")
      .valueType(SettingDef.Type.DURATION)
      .key(JDBC_FREQUENCE_TIME)
      .optional(String.valueOf(JDBC_FREQUENCE_TIME_DEFAULT))
      .orderInGroup(counter.getAndIncrement())
      .build()
  ).asJava

  override protected def _version: ConnectorVersion = ConnectorVersion.DEFAULT
}

object JDBCSourceConnector {
  val LOG: Logger = LoggerFactory.getLogger(classOf[JDBCSourceConnector])
}
