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

package com.island.ohara.it.performance

import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
//import org.junit.AssumptionViolatedException
import org.junit.experimental.categories.Category

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Oracle extends BasicTestPerformance4Jdbc {
  /*private[this] val DB_URL_KEY: String       = "ohara.it.oracle.db.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.oracle.db.username"
  private[this] val DB_PASSWORD_KEY: String  = "ohara.it.oracle.db.password"
   */

  /*override protected[performance] def url: String =
    sys.env.getOrElse(DB_URL_KEY, throw new AssumptionViolatedException(s"$DB_URL_KEY does not exists!!!"))

  override protected[performance] def user: String =
    sys.env.getOrElse(DB_USER_NAME_KEY, throw new AssumptionViolatedException(s"$DB_USER_NAME_KEY does not exists!!!"))

  override protected[performance] def password: String =
    sys.env.getOrElse(DB_PASSWORD_KEY, throw new AssumptionViolatedException(s"$DB_PASSWORD_KEY does not exists!!!"))
   */

  override protected val url: String      = "jdbc:oracle:thin:@//ohara-jenkins-it-02:1521/xe.localdomain"
  override protected val user: String     = "ohara"
  override protected val password: String = "island123"

  override protected val timestampColumnName: String = "COLUMN0"

  override protected val tableName: String =
    s"TABLE${CommonUtils.randomString().toUpperCase()}"

  override protected val jarName: String = "ojdbc8.jar"

  override protected val productName: String = "Oracle"

  override protected val columnNamePrefix: String = "COLUMN"

  override protected val insertTimestampValue: String =
    "TO_TIMESTAMP('2018-09-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
}
