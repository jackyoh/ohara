package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database

import scala.concurrent.duration._
import collection.JavaConverters._
import org.junit.{After, Test}
import org.scalatest.Matchers._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class TestJDBCSourceConnectorExactlyOnce extends With3Brokers3Workers {
  private[this] val durationTime: Long  = 30000
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "c0"
  private[this] val columnSize          = 3
  private[this] val columns = Seq(RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)) ++
    (1 to columnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", true)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val tableTotalCount = new LongAdder()
  private[this] val connectorAdmin  = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def createTable(): Unit = {
    client.createTable(tableName, columns)
  }

  private[this] val inputDataThread: Releasable = {
    val pool            = Executors.newSingleThreadExecutor()
    val startTime: Long = CommonUtils.current()
    pool.execute(() => {
      val sql               = s"INSERT INTO $tableName VALUES (${columns.map(_ => "?").mkString(",")})"
      val preparedStatement = client.connection.prepareStatement(sql)

      while ((CommonUtils.current() - startTime) <= durationTime) {
        // 432000000 is 5 days ago
        val timestampData = new Timestamp(CommonUtils.current() - 432000000 + tableTotalCount.intValue())
        preparedStatement.setTimestamp(1, timestampData)
        rowData().asScala.zipWithIndex.foreach {
          case (result, index) => {
            preparedStatement.setString(index + 2, result.value().toString)
          }
        }
        preparedStatement.execute()
        tableTotalCount.add(1)
      }
      preparedStatement.close()
    })
    () => {
      pool.shutdown()
      pool.awaitTermination(durationTime, TimeUnit.SECONDS)
    }
  }

  private[this] def rowData(): Row = {
    Row.of(
      (1 to columnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  @Test
  def test(): Unit = {
    createTable()

    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(jdbcSourceConnectorProps.toMap)
        .create()
    )

    TimeUnit.SECONDS.sleep(5)
    result(connectorAdmin.pause(connectorKey))

    TimeUnit.SECONDS.sleep(5)
    result(connectorAdmin.resume(connectorKey))
    result(connectorAdmin.delete(connectorKey))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(jdbcSourceConnectorProps.toMap)
        .create()
    )

    val consumer =
      Consumer
        .builder()
        .topicName(topicKey.topicNameOnKafka)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()

    // Check the table and topic data size
    TimeUnit.MILLISECONDS.sleep(durationTime + 15)
    val records = consumer.poll(java.time.Duration.ofSeconds(30), tableTotalCount.intValue()).asScala
    tableTotalCount.intValue() shouldBe records.size
  }

  private[this] val jdbcSourceConnectorProps = JDBCSourceConnectorConfig(
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

  private[this] def result[T](future: Future[T]): T = Await.result(future, 10 seconds)

  @After
  def after(): Unit = {
    Releasable.close(inputDataThread)
    Releasable.close(client)
    Releasable.close(db)
  }
}
