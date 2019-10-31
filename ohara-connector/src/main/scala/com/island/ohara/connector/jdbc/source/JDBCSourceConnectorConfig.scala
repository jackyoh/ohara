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

import com.island.ohara.common.util.CommonUtils
import com.island.ohara.kafka.connector.TaskSetting

import scala.concurrent.duration.Duration

/**
  * This class is getting property value
  */
case class JDBCSourceConnectorConfig(dbURL: String,
                                     dbUserName: String,
                                     dbPassword: String,
                                     dbTableName: String,
                                     dbCatalogPattern: Option[String],
                                     dbSchemaPattern: Option[String],
                                     mode: String,
                                     jdbcFetchDataSize: Int,
                                     jdbcFlushDataSize: Int,
                                     jdbcFrequenceTime: Duration,
                                     timestampColumnName: String) {
  def toMap: Map[String, String] = Map(
    DB_URL -> dbURL,
    DB_USERNAME -> dbUserName,
    DB_PASSWORD -> dbPassword,
    DB_TABLENAME -> dbTableName,
    MODE -> mode,
    JDBC_FETCHDATA_SIZE -> jdbcFetchDataSize.toString,
    JDBC_FLUSHDATA_SIZE -> jdbcFlushDataSize.toString,
    JDBC_FREQUENCE_TIME -> toJavaDuration(jdbcFrequenceTime).toString,
    TIMESTAMP_COLUMN_NAME -> timestampColumnName
  ) ++ dbCatalogPattern.map(s => Map(DB_CATALOG_PATTERN -> s)).getOrElse(Map.empty) ++ dbSchemaPattern
    .map(s => Map(DB_SCHEMA_PATTERN -> s))
    .getOrElse(Map.empty)
}

object JDBCSourceConnectorConfig {
  def apply(settings: TaskSetting): JDBCSourceConnectorConfig = {
    JDBCSourceConnectorConfig(
      dbURL = settings.stringValue(DB_URL),
      dbUserName = settings.stringValue(DB_USERNAME),
      dbPassword = settings.stringValue(DB_PASSWORD),
      dbTableName = settings.stringValue(DB_TABLENAME),
      dbCatalogPattern = Option(settings.stringOption(DB_CATALOG_PATTERN).orElse(null)).filterNot(CommonUtils.isEmpty),
      dbSchemaPattern = Option(settings.stringOption(DB_SCHEMA_PATTERN).orElse(null)).filterNot(CommonUtils.isEmpty),
      mode = settings.stringOption(MODE).orElse(MODE_DEFAULT),
      jdbcFetchDataSize = settings.intOption(JDBC_FETCHDATA_SIZE).orElse(JDBC_FETCHDATA_SIZE_DEFAULT),
      jdbcFlushDataSize = settings.intOption(JDBC_FLUSHDATA_SIZE).orElse(JDBC_FLUSHDATA_SIZE_DEFAULT),
      jdbcFrequenceTime = Option(settings.durationOption(JDBC_FREQUENCE_TIME).orElse(null))
        .fold(JDBC_FREQUENCE_TIME_DEFAULT)(toScalaDuration),
      timestampColumnName = settings.stringValue(TIMESTAMP_COLUMN_NAME)
    )
  }
}
