package oharastream.ohara.connector.ftp

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestFtpSourceConnectorBug extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)
  private[this] val fileSystem = FileSystem.ftpBuilder
    .hostname(testUtil.ftpServer.hostname)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()
  private[this] val inputFolder     = "/input"
  private[this] val completedFolder = "/complete"
  private[this] val outputFolder    = "/output"

  @Before
  def setup(): Unit = {
    createFolder(fileSystem, inputFolder)
    createFolder(fileSystem, completedFolder)
    createFolder(fileSystem, outputFolder)
    val header = "TX_ACCT_DT,ACCT_KEY,TX_TIME,TX_DT,TX_TYPE,CUR_CD,TX_AMT,DB_CR,AMT_BAL,CUST_KEY"
    val data = (1 to 100000)
      .map { i =>
        Seq(s"a-$i", s"b-$i", s"c-$i", s"d-$i", s"e-$i", s"f-$i", s"g-$i", s"h-$i", s"i-$i", s"j-$i")
      }
      .map(_.mkString(","))
    setupInput(fileSystem, inputFolder, header, data)
  }

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
            INPUT_FOLDER_KEY     -> inputFolder,
            COMPLETED_FOLDER_KEY -> completedFolder,
            ERROR_FOLDER_KEY     -> outputFolder,
            FTP_HOSTNAME_KEY     -> testUtil.ftpServer.hostname,
            FTP_PORT_KEY         -> testUtil.ftpServer.port.toString,
            FTP_USER_NAME_KEY    -> testUtil.ftpServer.user,
            FTP_PASSWORD_KEY     -> testUtil.ftpServer.password,
            FILE_CACHE_SIZE_KEY  -> "3"
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
    val records =
      consumer.poll(java.time.Duration.ofNanos(Duration(20, TimeUnit.SECONDS).toNanos), 100001).asScala.toSeq
    println(s"Record size is ${records.size}")
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(30, TimeUnit.SECONDS))

  private[this] def createFolder(fileSystem: FileSystem, path: String): Unit = {
    if (fileSystem.exists(path)) {
      fileSystem
        .listFileNames(path)
        .asScala
        .map(oharastream.ohara.common.util.CommonUtils.path(path, _))
        .foreach(fileSystem.delete)
      fileSystem.delete(path)
    }
    fileSystem.mkdirs(path)
  }

  private[this] def setupInput(fileSystem: FileSystem, inputFolder: String, header: String, data: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(
        fileSystem.create(
          oharastream.ohara.common.util.CommonUtils.path(inputFolder, s"${CommonUtils.randomString(8)}.csv")
        )
      )
    )
    try {
      writer.append(header)
      writer.newLine()
      data.foreach(line => {
        writer.append(line)
        writer.newLine()
      })
    } finally writer.close()
  }
}
