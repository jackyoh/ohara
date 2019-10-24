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

package com.island.ohara.it.connector

import com.island.ohara.common.util.CommonUtils

abstract class BasicTestPostgresqlJDBCSourceConnector extends BasicTestConnectorCollie {
  private[this] val DB_URL_KEY: String = "ohara.it.postgresql.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.postgresql.db.username"
  private[this] val DB_PASSWORD_KEY: String = "ohara.it.postgresql.db.password"

  override protected def dbUrl(): Option[String] = Option(DB_URL_KEY)

  override protected def dbUserName(): Option[String] = Option(DB_USER_NAME_KEY)

  override protected def dbPassword(): Option[String] = Option(DB_PASSWORD_KEY)

  override protected def dbName(): String = "postgresql"

  override protected def jdbcDriverJarFileName(): String = "postgresql-42.2.6.jar"

  override protected val tableName: String = s"table${CommonUtils.randomString(5)}"

  override protected val columnPrefixName: String = "column"

  override protected val insertDataSQL: String = s"INSERT INTO $tableName VALUES('2018-09-01 00:00:00',?,?,?)"

  override protected val BINARY_TYPE_NAME: String = "BYTEA"
}
