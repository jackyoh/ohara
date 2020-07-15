package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{Before, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class TestConnectorPartitionKey extends With3Brokers3Workers {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
    val column2 = RdbColumn("column2", "varchar(45)", true)
    client.createTable(tableName, Seq(column1, column2))
    val sql               = s"INSERT INTO $tableName(column1, column2) VALUES(?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      val startTimestamp = Timestamp.valueOf("2020-07-10 00:00:00").getTime()
      val stopTimestamp  = Timestamp.valueOf("2020-07-12 00:00:00").getTime()
      var count          = 0
      startTimestamp.to(stopTimestamp).by(3600000).foreach { i =>
        preparedStatement.setTimestamp(1, new Timestamp(i))
        preparedStatement.setString(2, CommonUtils.randomString())
        preparedStatement.executeUpdate()
        count = count + 1
      }
      println(s"Total count is $count")
    } finally Releasable.close(preparedStatement)
  }

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .create()
    )
    try {
      val record = pollData(topicKey, Duration(10, TimeUnit.SECONDS), 1)
      println(s"Record size is ${record.size}")
    } finally result(connectorAdmin.delete(connectorKey))
  }

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

  private[this] def pollData(
    topicKey: TopicKey,
    timeout: scala.concurrent.duration.Duration,
    size: Int
  ): Seq[Record[Row, Array[Byte]]] = {
    val consumer = Consumer
      .builder()
      .topicKey(topicKey)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try consumer.poll(java.time.Duration.ofNanos(timeout.toNanos), size).asScala.toSeq
    finally consumer.close()
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(10, TimeUnit.SECONDS))
}
