package com.island.ohara.configurator

import java.time.Duration

import com.island.ohara.client.ConnectorClient
import com.island.ohara.client.configurator.v0.ConnectorApi.ConnectorConfigurationRequest
import com.island.ohara.client.configurator.v0.TopicApi.TopicCreationRequest
import com.island.ohara.client.configurator.v0.{ConnectorApi, TopicApi}
import com.island.ohara.common.data.ConnectorState
import com.island.ohara.common.util.{CommonUtil, ReleaseOnce}
import com.island.ohara.integration.WithBrokerWorker
import com.island.ohara.kafka.KafkaClient
import com.island.ohara.kafka.connector.{RowSourceConnector, RowSourceRecord, RowSourceTask, TaskConfig}
import org.junit.{After, Test}
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TestControlSource extends WithBrokerWorker with Matchers {

  private[this] val configurator = Configurator
    .builder()
    .hostname("localhost")
    .port(0)
    .kafkaClient(KafkaClient.of(testUtil.brokersConnProps))
    .connectClient(ConnectorClient(testUtil.workersConnProps))
    .build()

  private[this] val access = ConnectorApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, 10 seconds)

  @Test
  def testNormalCase(): Unit = {
    val topicName = methodName
    val topic = Await.result(TopicApi
                               .access()
                               .hostname(configurator.hostname)
                               .port(configurator.port)
                               .add(TopicCreationRequest(topicName, 1, 1)),
                             10 seconds)
    val request = ConnectorConfigurationRequest(name = methodName,
                                                className = classOf[DumbSource].getName,
                                                schema = Seq.empty,
                                                topics = Seq(topic.id),
                                                numberOfTasks = 1,
                                                configs = Map.empty)

    val source = result(access.add(request))

    // test idempotent start
    (0 until 3).foreach(_ =>
      Await.result(access.start(source.id), 10 seconds).state.get shouldBe ConnectorState.RUNNING)
    val connectorClient = ConnectorClient(testUtil.workersConnProps)
    try {
      CommonUtil.await(() =>
                         try connectorClient.exist(source.id)
                         catch {
                           case _: Throwable => false
                       },
                       Duration.ofSeconds(30))
      CommonUtil.await(() => connectorClient.status(source.id).connector.state == ConnectorState.RUNNING,
                       Duration.ofSeconds(20))
      result(access.get(source.id)).state.get shouldBe ConnectorState.RUNNING

      // test idempotent pause
      (0 until 3).foreach(_ =>
        Await.result(access.pause(source.id), 10 seconds).state.get shouldBe ConnectorState.PAUSED)

      CommonUtil
        .await(() => connectorClient.status(source.id).connector.state == ConnectorState.PAUSED, Duration.ofSeconds(20))
      result(access.get(source.id)).state.get shouldBe ConnectorState.PAUSED

      // test idempotent resume
      (0 until 3).foreach(_ =>
        Await.result(access.resume(source.id), 10 seconds).state.get shouldBe ConnectorState.RUNNING)
      CommonUtil.await(() => connectorClient.status(source.id).connector.state == ConnectorState.RUNNING,
                       Duration.ofSeconds(20))
      result(access.get(source.id)).state.get shouldBe ConnectorState.RUNNING

      // test idempotent stop. the connector should be removed
      (0 until 3).foreach(_ => Await.result(access.stop(source.id), 10 seconds))

      CommonUtil.await(() => connectorClient.nonExist(source.id), Duration.ofSeconds(20))
      result(access.get(source.id)).state shouldBe None
    } finally {
      if (connectorClient.exist(source.id)) connectorClient.delete(source.id)
      ReleaseOnce.close(connectorClient)
    }
  }

  @Test
  def testUpdateRunningSource(): Unit = {
    val topicName = methodName
    val topic = Await.result(TopicApi
                               .access()
                               .hostname(configurator.hostname)
                               .port(configurator.port)
                               .add(TopicCreationRequest(topicName, 1, 1)),
                             10 seconds)
    val request = ConnectorConfigurationRequest(name = methodName,
                                                className = classOf[DumbSource].getName,
                                                schema = Seq.empty,
                                                topics = Seq(topic.id),
                                                numberOfTasks = 1,
                                                configs = Map.empty)

    val source = result(access.add(request))
    // test start
    Await.result(access.start(source.id), 10 seconds)
    val connectorClient = ConnectorClient(testUtil.workersConnProps)
    try {
      CommonUtil.await(() =>
                         try connectorClient.exist(source.id)
                         catch {
                           case _: Throwable => false
                       },
                       Duration.ofSeconds(30))
      CommonUtil.await(() => connectorClient.status(source.id).connector.state == ConnectorState.RUNNING,
                       Duration.ofSeconds(20))

      an[IllegalArgumentException] should be thrownBy result(access.update(source.id, request.copy(numberOfTasks = 2)))
      an[IllegalArgumentException] should be thrownBy result(access.delete(source.id))

      // test stop. the connector should be removed
      Await.result(access.stop(source.id), 10 seconds)
      CommonUtil.await(() => connectorClient.nonExist(source.id), Duration.ofSeconds(20))
      result(access.get(source.id)).state shouldBe None
    } finally {
      if (connectorClient.exist(source.id)) connectorClient.delete(source.id)
      ReleaseOnce.close(connectorClient)
    }
  }

  @After
  def tearDown(): Unit = ReleaseOnce.close(configurator)
}

class DumbSource extends RowSourceConnector {
  private[this] var config: TaskConfig = _
  override protected def _taskClass(): Class[_ <: RowSourceTask] = classOf[DumbSourceTask]
  override protected def _taskConfigs(maxTasks: Int): java.util.List[TaskConfig] = Seq.fill(maxTasks)(config).asJava
  override protected def _start(config: TaskConfig): Unit = {
    this.config = config
  }
  override protected def _stop(): Unit = {}
}

class DumbSourceTask extends RowSourceTask {
  override protected def _start(config: TaskConfig): Unit = {}

  override protected def _stop(): Unit = {}

  override protected def _poll(): java.util.List[RowSourceRecord] = Seq.empty.asJava
}
