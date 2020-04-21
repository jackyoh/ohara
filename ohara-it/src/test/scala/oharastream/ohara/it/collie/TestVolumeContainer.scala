package oharastream.ohara.it.collie

import java.time.Duration

import oharastream.ohara.agent.ArgumentsBuilder
import oharastream.ohara.client.configurator.v0.{BrokerApi, ZookeeperApi}
import oharastream.ohara.client.configurator.v0.ZookeeperApi.{CLIENT_PORT_DEFINITION, DATA_DIR_DEFINITION}
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import oharastream.ohara.kafka.{Consumer, Producer}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._

@RunWith(value = classOf[Parameterized])
class TestVolumeContainer(platform: ContainerPlatform) extends IntegrationTest {
  private[this] val containerClient = platform.setupContainerClient()

  @Test
  def test(): Unit = {
    val zkConfigPath       = s"${ZookeeperApi.ZOOKEEPER_HOME_FOLDER}/conf/zoo.cfg"
    val zkClientPort       = CommonUtils.availablePort()
    val zkDataDir          = s"${ZookeeperApi.ZOOKEEPER_HOME_FOLDER}/data"
    val myIdPath: String   = s"$zkDataDir/myid"
    val zkVolumeName       = "volume1"
    val zkNodePath: String = s"/tmp/ccc"
    println(s"zkNodePath is ${zkNodePath}")
    result(
      containerClient.volumeCreator
        .name(zkVolumeName)
        .nodeName(platform.nodeNames.head)
        .path(zkNodePath)
        .create()
    )

    val zkArguments = ArgumentsBuilder()
      .mainConfigFile(zkConfigPath)
      .file(zkConfigPath)
      .append(CLIENT_PORT_DEFINITION.key(), zkClientPort)
      .append(DATA_DIR_DEFINITION.key(), zkDataDir)
      .done
      .file(myIdPath)
      .append(0)
      .done
      .build

    result(
      containerClient.containerCreator
        .nodeName(platform.nodeNames.head)
        .name("zookeeper")
        .portMappings(Map(zkClientPort -> zkClientPort))
        .imageName(ZookeeperApi.IMAGE_NAME_DEFAULT)
        .mountVolumes(Map(zkVolumeName -> zkDataDir))
        .arguments(zkArguments)
        .create()
    )

    val bkConfigPath: String = s"${BrokerApi.BROKER_HOME_FOLDER}/config/broker.config"
    val logDir: String       = s"${BrokerApi.BROKER_HOME_FOLDER}/logs"
    val bkClientPort         = CommonUtils.availablePort()
    val bkNodePath: String   = s"/tmp/ccc"
    val bkVolumeName         = "volume2"
    println(s"bkNodePath is ${bkNodePath}")
    result(
      containerClient.volumeCreator
        .name(bkVolumeName)
        .nodeName(platform.nodeNames.head)
        .path(bkNodePath)
        .create()
    )

    val bkArguments = ArgumentsBuilder()
      .mainConfigFile(bkConfigPath)
      .file(bkConfigPath)
      .append("zookeeper.connect", s"${platform.nodeNames.head}:${zkClientPort}")
      .append(BrokerApi.LOG_DIRS_DEFINITION.key(), logDir)
      .append(BrokerApi.NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_DEFINITION.key(), 1)
      .append(s"listeners=PLAINTEXT://:${bkClientPort}")
      .append(s"advertised.listeners=PLAINTEXT://${platform.nodeNames.head}:${bkClientPort}")
      .done
      .build
    result(
      containerClient.containerCreator
        .nodeName(platform.nodeNames.head)
        .name("broker")
        .portMappings(Map(bkClientPort -> bkClientPort))
        .imageName(BrokerApi.IMAGE_NAME_DEFAULT)
        .mountVolumes(Map(bkVolumeName -> logDir))
        .arguments(bkArguments)
        .create()
    )

    val topicName = CommonUtils.randomString()
    val producer = Producer
      .builder()
      .connectionProps(s"${platform.nodeNames.head}:${bkClientPort}")
      .allAcks()
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()
    val numberOfRecords = 5
    (0 until numberOfRecords).foreach(
      index => producer.sender().key(index.toString).value(index.toString).topicName(topicName).send()
    )
    producer.flush()
    producer.close()

    val consumer = Consumer
      .builder()
      .connectionProps(s"${platform.nodeNames.head}:${bkClientPort}")
      .offsetFromBegin()
      .topicName(topicName)
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()
    val records = consumer.poll(Duration.ofSeconds(30), numberOfRecords)
    println(s"Record size is ${records.size()}")
  }
}

object TestVolumeContainer {
  @Parameters(name = "{index} mode = {0}")
  def parameters: java.util.Collection[ContainerPlatform] = {
    val modes = ContainerPlatform.all
    if (modes.isEmpty) java.util.Collections.singletonList(ContainerPlatform.empty)
    else modes.asJava
  }
}
