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

package oharastream.ohara.it.connector.jdbc

import java.io.File
import java.sql.{PreparedStatement, Statement, Timestamp}

import oharastream.ohara.client.configurator.v0.FileInfoApi.FileInfo
import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.v0.{BrokerApi, ContainerApi, FileInfoApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, ObjectKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.source.{JDBCSourceConnector, JDBCSourceConnectorConfig}
import oharastream.ohara.it.{PaltformModeInfo, WithRemoteConfigurator}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.connector.TaskSetting
import com.typesafe.scalalogging.Logger
import org.junit.{After, Before, Test}
import org.scalatest.Matchers._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

abstract class BasicTestConnectorCollie(paltform: PaltformModeInfo)
    extends WithRemoteConfigurator(paltform: PaltformModeInfo) {
  private[this] val log                    = Logger(classOf[BasicTestConnectorCollie])
  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath          = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")

  private[this] val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
  private[this] val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

  protected def tableName(): String
  protected def columnPrefixName(): String
  private[this] var timestampColumn: String = _

  private[this] var client: DatabaseClient = _

  private[this] var jdbcJarFileInfo: FileInfo = _

  protected def dbUrl(): Option[String]
  protected def dbUserName(): Option[String]
  protected def dbPassword(): Option[String]
  protected def dbName(): String
  protected def insertDataSQL(): String
  protected def BINARY_TYPE_NAME: String

  /**
    * This function for setting database JDBC jar file name.
    * from local upload to configurator server for connector worker container to download use.
    * @return JDBC driver file name
    */
  protected def jdbcDriverJarFileName(): String

  private[this] def zkApi = ZookeeperApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def bkApi = BrokerApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def wkApi = WorkerApi.access.hostname(configuratorHostname).port(configuratorPort)

  private[this] def containerApi = ContainerApi.access.hostname(configuratorHostname).port(configuratorPort)

  @Before
  final def setup(): Unit = {
    checkDataBaseInfo()           //Check db info
    uploadJDBCJarToConfigurator() //For upload JDBC jar

    // Create database client
    client = DatabaseClient.builder.url(dbUrl().get).user(dbUserName().get).password(dbPassword().get).build

    // Create table
    val columns = (1 to 4).map(x => s"${columnPrefixName()}$x")
    timestampColumn = columns(0)

    val column1 = RdbColumn(columns(0), "TIMESTAMP", false)
    val column2 = RdbColumn(columns(1), "varchar(45)", false)
    val column3 = RdbColumn(columns(2), "integer", true)
    val column4 = RdbColumn(columns(3), BINARY_TYPE_NAME, false)
    client.createTable(tableName(), Seq(column1, column2, column3, column4))

    // Insert data in the table
    val preParedstatement: PreparedStatement = client.connection.prepareStatement(insertDataSQL)
    (1 to 100).foreach(i => {
      preParedstatement.setString(1, s"a${i}")
      preParedstatement.setInt(2, i)
      preParedstatement.setBytes(3, s"binary-value${i}".getBytes)
      preParedstatement.executeUpdate()
    })
  }

  @Test
  def testJDBCSourceConnector(): Unit = {
    val zkCluster = result(
      zk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        clientPort = CommonUtils.availablePort(),
        electionPort = CommonUtils.availablePort(),
        peerPort = CommonUtils.availablePort(),
        nodeNames = Set(nodes.head.name)
      )
    )
    result(zk_start(zkCluster.key))
    assertCluster(() => result(zk_clusters()), () => result(zk_containers(zkCluster.key)), zkCluster.key)
    val bkCluster = result(
      bk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        clientPort = CommonUtils.availablePort(),
        jmxPort = CommonUtils.availablePort(),
        zookeeperClusterKey = zkCluster.key,
        nodeNames = Set(nodes.head.name)
      )
    )
    result(bk_start(bkCluster.key))
    assertCluster(() => result(bk_clusters()), () => result(bk_containers(bkCluster.key)), bkCluster.key)
    log.info("[WORKER] start to test worker")
    val nodeName = nodes.head.name
    log.info("[WORKER] verify:nonExists done")
    val clientPort = CommonUtils.availablePort()
    val jmxPort    = CommonUtils.availablePort()
    log.info("[WORKER] create ...")
    val wkCluster = result(
      wk_create(
        clusterKey = serviceNameHolder.generateClusterKey(),
        clientPort = clientPort,
        jmxPort = jmxPort,
        brokerClusterKey = bkCluster.key,
        nodeNames = Set(nodeName)
      )
    )
    log.info("[WORKER] create done")
    result(wk_start(wkCluster.key))
    log.info("[WORKER] start done")
    assertCluster(() => result(wk_clusters()), () => result(wk_containers(wkCluster.key)), wkCluster.key)
    log.info("[WORKER] verify:create done")
    result(wk_exist(wkCluster.key)) shouldBe true
    log.info("[WORKER] verify:exist done")
    // we can't assume the size since other tests may create zk cluster at the same time
    result(wk_clusters()).isEmpty shouldBe false
    testConnectors(wkCluster)

    runningJDBCSourceConnector(wkCluster)
    checkTopicData(bkCluster.connectionProps, topicKey.topicNameOnKafka())

    result(wk_stop(wkCluster.key))
    await(() => {
      // In configurator mode: clusters() will return the "stopped list" in normal case
      // In collie mode: clusters() will return the "cluster list except stop one" in normal case
      // we should consider these two cases by case...
      val clusters = result(wk_clusters())
      !clusters.map(_.key).contains(wkCluster.key) || clusters.find(_.key == wkCluster.key).get.state.isEmpty
    })
    // the cluster is stopped actually, delete the data
    wk_delete(wkCluster.key)
  }

  private[this] def runningJDBCSourceConnector(workerClusterInfo: WorkerClusterInfo): Unit =
    result(
      ConnectorAdmin(workerClusterInfo)
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(props().toMap)
        .create()
    )

  private[this] def checkTopicData(brokers: String, topicNameOnKafka: String): Unit = {
    val consumer =
      Consumer
        .builder()
        .topicName(topicNameOnKafka)
        .offsetFromBegin()
        .connectionProps(brokers)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()
    try {
      val record: Seq[Record[Row, Array[Byte]]] = consumer.poll(java.time.Duration.ofSeconds(50), 100).asScala
      record.size shouldBe 100

      record.head.key.get.cell(0).value.asInstanceOf[Timestamp].getTime shouldBe 1535760000000L
      record.head.key.get.cell(1).value shouldBe "a1"
      record.head.key.get.cell(2).value shouldBe 1
      new String(record.head.key.get.cell(3).value.asInstanceOf[Array[Byte]]) shouldBe "binary-value1"

      record.last.key.get.cell(0).value.asInstanceOf[Timestamp].getTime shouldBe 1535760000000L
      record.last.key.get.cell(1).value shouldBe "a100"
      record.last.key.get.cell(2).value shouldBe 100
      new String(record.last.key.get.cell(3).value.asInstanceOf[Array[Byte]]) shouldBe "binary-value100"
    } finally {
      consumer.close()
    }
  }

  private[this] def uploadJDBCJarToConfigurator(): Unit = {
    val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    val jar                        = new File(CommonUtils.path(jarFolderPath, jdbcDriverJarFileName()))
    jdbcJarFileInfo = result(jarApi.request.file(jar).upload())
  }

  private[this] def zk_clusters(): Future[Seq[ZookeeperApi.ZookeeperClusterInfo]] =
    zkApi.list()

  private[this] def zk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def zk_start(clusterKey: ObjectKey): Future[Unit] = zkApi.start(clusterKey)

  private[this] def zk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    electionPort: Int,
    peerPort: Int,
    nodeNames: Set[String]
  ): Future[ZookeeperApi.ZookeeperClusterInfo] =
    zkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .electionPort(electionPort)
      .peerPort(peerPort)
      .nodeNames(nodeNames)
      .create()

  private[this] def bk_start(clusterKey: ObjectKey): Future[Unit] = bkApi.start(clusterKey)

  private[this] def bk_clusters(): Future[Seq[BrokerApi.BrokerClusterInfo]] = bkApi.list()

  private[this] def bk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def bk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    jmxPort: Int,
    zookeeperClusterKey: ObjectKey,
    nodeNames: Set[String]
  ): Future[BrokerApi.BrokerClusterInfo] =
    bkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .zookeeperClusterKey(zookeeperClusterKey)
      .nodeNames(nodeNames)
      .create()

  private[this] def wk_start(clusterKey: ObjectKey): Future[Unit] = wkApi.start(clusterKey)

  private[this] def wk_stop(clusterKey: ObjectKey): Future[Unit] =
    wkApi.forceStop(clusterKey).map(_ => Unit)

  private[this] def wk_containers(clusterKey: ObjectKey): Future[Seq[ContainerApi.ContainerInfo]] =
    containerApi.get(clusterKey).map(_.flatMap(_.containers))

  private[this] def wk_exist(clusterKey: ObjectKey): Future[Boolean] = wkApi.list().map(_.exists(_.key == clusterKey))

  private[this] def wk_delete(clusterKey: ObjectKey): Future[Unit] = wkApi.delete(clusterKey)

  private[this] def wk_clusters(): Future[Seq[WorkerApi.WorkerClusterInfo]] = wkApi.list()

  private[this] def wk_create(
    clusterKey: ObjectKey,
    clientPort: Int,
    jmxPort: Int,
    brokerClusterKey: ObjectKey,
    nodeNames: Set[String]
  ): Future[WorkerApi.WorkerClusterInfo] =
    wkApi.request
      .key(clusterKey)
      .clientPort(clientPort)
      .jmxPort(jmxPort)
      .brokerClusterKey(brokerClusterKey)
      .nodeNames(nodeNames)
      .sharedJarKeys(Set(jdbcJarFileInfo.key))
      .create()

  private[this] def checkDataBaseInfo(): Unit = {
    if (dbUrl().isEmpty || dbUserName().isEmpty || dbPassword().isEmpty)
      skipTest(s"Skip the JDBC source connector test, Please setting dbURL, dbUserName and dbPassword")

    if (jarFolderPath.isEmpty)
      skipTest(s"Please setting jdbc jar folder path.")
  }

  private[this] def testConnectors(cluster: WorkerClusterInfo): Unit =
    await(
      () =>
        try {
          log.info(s"worker node head: ${cluster.nodeNames.head}:${cluster.clientPort}")
          result(ConnectorAdmin(cluster).connectorDefinitions()).nonEmpty
        } catch {
          case e: Throwable =>
            log.info(s"[WORKER] worker cluster:${cluster.name} is starting ... retry", e)
            false
        }
    )

  private[this] def props(): JDBCSourceConnectorConfig =
    JDBCSourceConnectorConfig(
      TaskSetting.of(
        Map(
          "source.db.url"                -> dbUrl().get,
          "source.db.username"           -> dbUserName().get,
          "source.db.password"           -> dbPassword().get,
          "source.table.name"            -> tableName,
          "source.timestamp.column.name" -> timestampColumn,
          "source.schema.pattern"        -> "TUSER"
        ).asJava
      )
    )

  @After
  def afterTest(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table ${tableName()}")
    }
    Releasable.close(client)
  }
}
