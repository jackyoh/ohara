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

abstract class BasicTestOracleJDBCSourceConnector extends BasicTestConnectorCollie {
  private[this] val DB_URL_KEY: String = "ohara.it.oracle.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.oracle.db.username"
  private[this] val DB_PASSWORD_KEY: String = "ohara.it.oracle.db.password"

  override protected def dbUrl(): Option[String] = Option(DB_URL_KEY)

  override protected def dbUserName(): Option[String] = Option(DB_USER_NAME_KEY)

  override protected def dbPassword(): Option[String] = Option(DB_PASSWORD_KEY)

  override protected def dbName(): String = "oracle"

  override protected def jdbcDriverJarFileName(): String = "ojdbc8.jar"

  override protected val tableName: String = s"TABLE${CommonUtils.randomString(5)}".toUpperCase

  override protected val columnPrefixName: String = "COLUMN"

  override protected val insertDataSQL: String =
    s"INSERT INTO $tableName VALUES(TO_TIMESTAMP('2018-09-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),?,?,?)"

  override protected val BINARY_TYPE_NAME: String = "RAW"
}
