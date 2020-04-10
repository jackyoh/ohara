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

package oharastream.ohara.it.connector.jdbc
import oharastream.ohara.it.category.ConnectorGroup
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.ContainerPlatform
import org.junit.AssumptionViolatedException
import org.junit.experimental.categories.Category

@Category(Array(classOf[ConnectorGroup]))
class TestOracleJDBCSourceConnector(platform: ContainerPlatform)
    extends BasicTestConnectorCollie(platform: ContainerPlatform) {
  private[this] val DB_URL_KEY: String       = "ohara.it.oracle.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.oracle.db.username"
  private[this] val DB_PASSWORD_KEY: String  = "ohara.it.oracle.db.password"

  override protected def dbUrl(): String =
    sys.env.getOrElse(DB_URL_KEY, throw new AssumptionViolatedException(s"$DB_URL_KEY does not exists!!!"))

  override protected def dbUserName(): String =
    sys.env.getOrElse(DB_USER_NAME_KEY, throw new AssumptionViolatedException(s"$DB_USER_NAME_KEY does not exists!!!"))

  override protected def dbPassword(): String =
    sys.env.getOrElse(DB_PASSWORD_KEY, throw new AssumptionViolatedException(s"$DB_PASSWORD_KEY does not exists!!!"))

  override protected def dbName(): String = "oracle"

  override protected val tableName: String = s"TABLE${CommonUtils.randomString(5)}".toUpperCase

  override protected def jdbcDriverJarFileName(): String = "ojdbc8.jar"

  override protected val columnPrefixName: String = "COLUMN"

  override protected val BINARY_TYPE_NAME: String = "RAW(30)"
}
