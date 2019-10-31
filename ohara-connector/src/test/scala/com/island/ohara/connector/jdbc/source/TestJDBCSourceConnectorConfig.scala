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
import com.island.ohara.common.rule.OharaTest
import com.island.ohara.kafka.connector.TaskSetting
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
class TestJDBCSourceConnectorConfig extends OharaTest with Matchers {

  private[this] def jdbcConfig(settings: Map[String, String]): JDBCSourceConnectorConfig =
    JDBCSourceConnectorConfig(TaskSetting.of(settings.asJava))

  @Test
  def testSettingProperty(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL -> "jdbc:mysql://localhost/test",
        DB_USERNAME -> "root",
        DB_PASSWORD -> "123456",
        DB_TABLENAME -> "TABLE1",
        DB_SCHEMA_PATTERN -> "schema1",
        TIMESTAMP_COLUMN_NAME -> "CDC_TIMESTAMP"
      )

    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.dbURL shouldBe "jdbc:mysql://localhost/test"
    jdbcSourceConnectorConfig.dbUserName shouldBe "root"
    jdbcSourceConnectorConfig.dbPassword shouldBe "123456"
    jdbcSourceConnectorConfig.dbTableName shouldBe "TABLE1"
    jdbcSourceConnectorConfig.mode shouldBe "timestamp"
    jdbcSourceConnectorConfig.dbSchemaPattern.get shouldBe "schema1"
    jdbcSourceConnectorConfig.timestampColumnName shouldBe "CDC_TIMESTAMP"
    jdbcSourceConnectorConfig.jdbcFetchDataSize shouldBe JDBC_FETCHDATA_SIZE_DEFAULT
  }

  @Test
  def testFetchDataSize(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL -> "jdbc:mysql://localhost/test",
        DB_USERNAME -> "root",
        DB_PASSWORD -> "123456",
        DB_TABLENAME -> "TABLE1",
        DB_SCHEMA_PATTERN -> "schema1",
        JDBC_FETCHDATA_SIZE -> "500",
        TIMESTAMP_COLUMN_NAME -> "CDC_TIMESTAMP"
      )

    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.jdbcFetchDataSize shouldBe 500
  }

  @Test
  def testFrequenceTimeDefault(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL -> "jdbc:mysql://localhost/test",
        DB_USERNAME -> "root",
        DB_PASSWORD -> "123456",
        DB_TABLENAME -> "TABLE1",
        DB_SCHEMA_PATTERN -> "schema1",
        TIMESTAMP_COLUMN_NAME -> "CDC_TIMESTAMP"
      )
    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.jdbcFrequenceTime.toMillis shouldBe 0
  }

  @Test
  def testFrequenceTime(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL -> "jdbc:mysql://localhost/test",
        DB_USERNAME -> "root",
        DB_PASSWORD -> "123456",
        DB_TABLENAME -> "TABLE1",
        DB_SCHEMA_PATTERN -> "schema1",
        JDBC_FREQUENCE_TIME -> "10 second",
        TIMESTAMP_COLUMN_NAME -> "CDC_TIMESTAMP"
      )
    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.jdbcFrequenceTime.toMillis shouldBe 10000
  }

  @Test
  def testException(): Unit = {
    intercept[NoSuchElementException] {
      jdbcConfig(Map())
    }.getMessage shouldBe s"$DB_URL doesn't exist"

    intercept[NoSuchElementException] {
      jdbcConfig(Map(DB_URL -> "jdbc:mysql://localhost:3306"))
    }.getMessage shouldBe s"$DB_USERNAME doesn't exist"

    intercept[NoSuchElementException] {
      jdbcConfig(Map(DB_URL -> "jdbc:mysql://localhost/test", DB_USERNAME -> "root"))
    }.getMessage shouldBe s"$DB_PASSWORD doesn't exist"
  }

  @Test
  def testCatalogAndSchema(): Unit = {
    val config = JDBCSourceConnectorConfig(
      dbURL = "123",
      dbUserName = "123",
      dbPassword = "123",
      dbTableName = "123",
      dbCatalogPattern = None,
      dbSchemaPattern = None,
      mode = "123",
      jdbcFetchDataSize = 1000,
      jdbcFlushDataSize = 1000,
      jdbcFrequenceTime = Duration("0 second"),
      timestampColumnName = "123"
    )

    config.toMap.contains(DB_CATALOG_PATTERN) shouldBe false
    config.toMap.contains(DB_SCHEMA_PATTERN) shouldBe false

    val configMap = Map[String, String](
      DB_URL -> "aa",
      DB_USERNAME -> "aa",
      DB_PASSWORD -> "aa",
      DB_TABLENAME -> "aa",
      DB_CATALOG_PATTERN -> "aa",
      DB_SCHEMA_PATTERN -> "aa",
      MODE -> "aa",
      TIMESTAMP_COLUMN_NAME -> "aa"
    )

    jdbcConfig(configMap).dbSchemaPattern.isEmpty shouldBe false
    jdbcConfig(configMap).dbCatalogPattern.isEmpty shouldBe false

    val configMap2 = Map[String, String](
      DB_URL -> "aa",
      DB_USERNAME -> "aa",
      DB_PASSWORD -> "aa",
      DB_TABLENAME -> "aa",
      MODE -> "aa",
      TIMESTAMP_COLUMN_NAME -> "aa"
    )

    jdbcConfig(configMap2).dbSchemaPattern.isEmpty shouldBe true
    jdbcConfig(configMap2).dbCatalogPattern.isEmpty shouldBe true

    jdbcConfig(configMap2) shouldBe jdbcConfig(configMap2)
    jdbcConfig(configMap2) shouldBe jdbcConfig(jdbcConfig(configMap2).toMap)
  }
}
