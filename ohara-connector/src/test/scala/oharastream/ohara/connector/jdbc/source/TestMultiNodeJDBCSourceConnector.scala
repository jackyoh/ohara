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

import java.sql.Statement
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{Before, Test}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}

class TestMultiNodeJDBCSourceConnector extends With3Brokers3Workers {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn("column1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("column2", "varchar(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO $tableName(column1, column2, column3, column4) VALUES('2020-05-20 00:00:00', 'a11', 'a12', 1)"
    )

    /*statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)"
    )
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)"
    )
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)"
    )
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:03.123456', 'a61', 'a62', 6)"
    )
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:04.123', 'a71', 'a72', 7)"
    )
    statement.executeUpdate(s"INSERT INTO $tableName(column1) VALUES('2018-09-01 00:00:05')")

    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 3 MINUTE, 'a41', 'a42', 4)"
    )
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)"
    )*/
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[MultiNodeJDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .create()
    )
    TimeUnit.SECONDS.sleep(15)
    /*val record = pollData(topicKey, 0 seconds, 0)
    println(record)*/
  }
  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)

  /*private[this] def pollData(
                              topicKey: TopicKey,
                              timeout: scala.concurrent.duration.Duration,
                              size: Int
                            ): Seq[Record[Row, Array[Byte]]] = {
    val consumer = Consumer
      .builder()
      .topicName(topicKey.topicNameOnKafka)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try consumer.poll(java.time.Duration.ofNanos(timeout.toNanos), size).asScala.toSeq
    finally consumer.close()
  }*/

  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL                -> db.url,
        DB_USERNAME           -> db.user,
        DB_PASSWORD           -> db.password,
        DB_TABLENAME          -> tableName,
        TIMESTAMP_COLUMN_NAME -> timestampColumnName
      ).asJava
    )
  )
}
