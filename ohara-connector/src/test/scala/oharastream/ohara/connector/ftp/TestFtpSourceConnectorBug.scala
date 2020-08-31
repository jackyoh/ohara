package oharastream.ohara.connector.ftp

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.Test
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestFtpSourceConnectorBug extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  @Test
  def test(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "source-connector-test")
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[FtpSource])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(
          Map(
            INPUT_FOLDER_KEY          -> "/input",
            COMPLETED_FOLDER_KEY      -> "/output",
            ERROR_FOLDER_KEY          -> "/error",
            FTP_HOSTNAME_KEY          -> "10.100.0.138",
            FTP_PORT_KEY              -> "21",
            FTP_USER_NAME_KEY         -> "ohara",
            FTP_PASSWORD_KEY          -> "island123",
            FILE_CACHE_SIZE_KEY       -> "3"
          )
        )
        .create()
    )

    val consumer = Consumer
      .builder()
      .topicKey(topicKey)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    val records = consumer.poll(java.time.Duration.ofNanos(Duration(20, TimeUnit.SECONDS).toNanos), 100001).asScala.toSeq
    println(s"Record size is ${records.size}")
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(30, TimeUnit.SECONDS))
}
