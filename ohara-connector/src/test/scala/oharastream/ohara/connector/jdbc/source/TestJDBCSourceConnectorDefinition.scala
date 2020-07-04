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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.SettingDef.{Necessary, Permission, Reference}
import oharastream.ohara.common.setting.{ConnectorKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestJDBCSourceConnectorDefinition extends WithBrokerWorker {
  private[this] val jdbcSource                 = new JDBCSourceConnector
  private[this] val connectorAdmin             = ConnectorAdmin(testUtil().workersConnProps())
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  @Test
  def checkDbURL(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_URL)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkDbUserName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_USERNAME)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkDbPassword(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_PASSWORD)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.PASSWORD
  }

  @Test
  def checkFetchDataSize(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(JDBC_FETCHDATA_SIZE)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultInt shouldBe JDBC_FETCHDATA_SIZE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.INT
  }

  @Test
  def checkFlushDataSize(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(JDBC_FLUSHDATA_SIZE)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultInt shouldBe JDBC_FLUSHDATA_SIZE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.INT
  }

  @Test
  def checkFrequenceTime(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(JDBC_FREQUENCE_TIME)
    definition.necessary should not be Necessary.REQUIRED
    definition.defaultDuration() shouldBe java.time.Duration.ofMillis(JDBC_FREQUENCE_TIME_DEFAULT.toMillis)
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.DURATION
  }

  @Test
  def checkTableName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_TABLENAME)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.JDBC_TABLE
  }

  @Test
  def checkCatalogPattern(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_CATALOG_PATTERN)
    definition.necessary() should not be Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkSchemaPattern(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_SCHEMA_PATTERN)
    definition.necessary() should not be Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkMode(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(MODE)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultString() shouldBe MODE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkTimeStampColumnName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(TIMESTAMP_COLUMN_NAME)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def testSource(): Unit = {
    val url: String                 = "jdbc:postgresql://localhost:5432/postgres"
    val userName: String            = "user1"
    val password: String            = "123456"
    val tableName: String           = "table1"
    val timeStampColumnName: String = "COLUMN1"
    val topicKey                    = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey                = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    val response = result(
      connectorAdmin
        .connectorValidator()
        .connectorKey(connectorKey)
        .numberOfTasks(3)
        .topicKey(topicKey)
        .settings(
          Map(
            DB_URL                -> url,
            DB_USERNAME           -> userName,
            DB_PASSWORD           -> password,
            DB_TABLENAME          -> tableName,
            TIMESTAMP_COLUMN_NAME -> timeStampColumnName,
            JDBC_FETCHDATA_SIZE   -> "1000",
            JDBC_FLUSHDATA_SIZE   -> "1000",
            JDBC_FREQUENCE_TIME   -> "1 second"
          )
        )
        .connectorClass(classOf[JDBCSourceConnector])
        .run()
    )
    response.settings().size should not be 0

    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.OPTIONAL

    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.COLUMNS_DEFINITION.key())
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_URL)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_USERNAME)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_PASSWORD)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_TABLENAME)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == JDBC_FREQUENCE_TIME)
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == TIMESTAMP_COLUMN_NAME)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_CATALOG_PATTERN)
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_SCHEMA_PATTERN)
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response.errorCount() shouldBe 0
  }
}
