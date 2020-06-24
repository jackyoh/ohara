package oharastream.ohara.connector.jdbc.source

import java.sql.{Statement, Timestamp}
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.client.kafka.WorkerJson.ConnectorCreationResponse
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{After, Before, Test}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.matchers.should.Matchers._

class TestJDBCSourceConnectorIncrementMode extends With3Brokers3Workers {
  private[this] var startTimestamp     = Timestamp.valueOf("2020-06-17 00:00:00")
  private[this] val stopTimestamp      = Timestamp.valueOf("2020-06-21 00:00:00")
  private[this] val incrementTimestamp = 1800000

  private[this] val db: Database = Database.local()
  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val incrementColumnName = "c0"
  private[this] val timestampColumnName = "c1"
  private[this] val columnSize          = 3
  private[this] val columns = Seq(
    RdbColumn(incrementColumnName, "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
    RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
  ) ++
    (2 to columnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", true)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    client.createTable(tableName, columns)
    val sql               = s"INSERT INTO $tableName(c1, c2, c3) VALUES (?, ?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      while (startTimestamp.getTime() < stopTimestamp.getTime()) {
        startTimestamp = new Timestamp(startTimestamp.getTime() + incrementTimestamp)
        preparedStatement.setTimestamp(1, startTimestamp)

        rowData().asScala.zipWithIndex.foreach {
          case (result, index) => {
            preparedStatement.setString(index + 2, result.value().toString)
          }
        }
        preparedStatement.execute()
      }
    } finally Releasable.close(preparedStatement)
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(createConnector(connectorAdmin, connectorKey, topicKey))
    val consumer =
      Consumer
        .builder()
        .topicKey(topicKey)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()
    try {
      val records = consumer.poll(java.time.Duration.ofSeconds(30), 96).asScala

      val preparedStatement = client.connection.prepareStatement(s"select count(*) from $tableName")
      try {
        val resultSet = preparedStatement.executeQuery()
        val dbCount =
          if (resultSet.next()) resultSet.getInt(1)
          else 0
        dbCount shouldBe records.size
      } finally Releasable.close(preparedStatement)
    } finally {
      result(connectorAdmin.delete(connectorKey))
      Releasable.close(consumer)
    }
  }

  @After
  def after(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
    }
    Releasable.close(client)
    Releasable.close(db)
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  private[this] def createConnector(
    connectorAdmin: ConnectorAdmin,
    connectorKey: ConnectorKey,
    topicKey: TopicKey
  ): Future[ConnectorCreationResponse] = {
    connectorAdmin
      .connectorCreator()
      .connectorKey(connectorKey)
      .connectorClass(classOf[JDBCSourceConnector])
      .topicKey(topicKey)
      .numberOfTasks(3)
      .settings(jdbcSourceConnectorProps.toMap)
      .create()
  }

  private[this] val jdbcSourceConnectorProps = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL                -> db.url,
        DB_USERNAME           -> db.user,
        DB_PASSWORD           -> db.password,
        DB_TABLENAME          -> tableName,
        TIMESTAMP_COLUMN_NAME -> timestampColumnName,
        INCREMENT_COLUMN_NAME -> "c0",
        TASK_TOTAL_KEY        -> "0",
        TASK_HASH_KEY         -> "0"
      ).asJava
    )
  )

  private[this] def rowData(): Row = {
    Row.of(
      (2 to columnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }
}
