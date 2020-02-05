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

import com.island.ohara.common.setting.ConnectorKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.jdbc.source.JDBCSourceConnector
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Oracle extends BasicTestPerformance4Jdbc {
  override protected val tableName: String =
    s"TABLE${CommonUtils.randomString().toUpperCase()}"

  @Test
  def test(): Unit = {
    createTable()
    setupTableData(sizeOfInputData)
    createTopic()
    try {
      setupConnector(
        connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
        className = classOf[JDBCSourceConnector].getName(),
        settings = Map(
          com.island.ohara.connector.jdbc.source.DB_URL                -> JsString(url),
          com.island.ohara.connector.jdbc.source.DB_USERNAME           -> JsString(user),
          com.island.ohara.connector.jdbc.source.DB_PASSWORD           -> JsString(password),
          com.island.ohara.connector.jdbc.source.DB_TABLENAME          -> JsString(tableName),
          com.island.ohara.connector.jdbc.source.TIMESTAMP_COLUMN_NAME -> JsString(timestampColumnName),
          com.island.ohara.connector.jdbc.source.DB_SCHEMA_PATTERN     -> JsString(user),
          com.island.ohara.connector.jdbc.source.JDBC_FETCHDATA_SIZE   -> JsNumber(10000),
          com.island.ohara.connector.jdbc.source.JDBC_FLUSHDATA_SIZE   -> JsNumber(10000)
        )
      )
      sleepUntilEnd()
    } finally if (needDeleteData) client.dropTable(tableName)
  }

  override protected def afterFrequencySleep(reports: Seq[PerformanceReport]): Unit = {
    setupTableData(sizeOfDurationInputData)
  }
}
