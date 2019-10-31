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

package com.island.ohara.connector.jdbc

import scala.concurrent.duration.Duration

package object source {
  val DB_URL: String = "source.db.url"
  val DB_USERNAME: String = "source.db.username"
  val DB_PASSWORD: String = "source.db.password"
  val DB_TABLENAME: String = "source.table.name"
  val DB_CATALOG_PATTERN: String = "source.schema.catalog"
  val DB_SCHEMA_PATTERN: String = "source.schema.pattern"
  val JDBC_FETCHDATA_SIZE: String = "source.jdbc.fetch.size"
  val JDBC_FLUSHDATA_SIZE: String = "source.jdbc.flush.size"
  val JDBC_FREQUENCE_TIME: String = "source.jdbc.frequence.time"
  val MODE: String = "mode"
  val MODE_DEFAULT = "timestamp"
  val JDBC_FETCHDATA_SIZE_DEFAULT: Int = 1000
  val JDBC_FLUSHDATA_SIZE_DEFAULT: Int = 1000
  val JDBC_FREQUENCE_TIME_DEFAULT: Duration = Duration("0 second")
  val TIMESTAMP_COLUMN_NAME: String = "source.timestamp.column.name"
  val ORACLE_DB_NAME = "oracle"

  def toJavaDuration(d: Duration): java.time.Duration = java.time.Duration.ofMillis(d.toMillis)
  def toScalaDuration(d: java.time.Duration): Duration =
    Duration(d.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
}
