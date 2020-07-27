package oharastream.ohara.it.collie

import java.time.Duration

import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.{Consumer, Producer}
import org.junit.Test
import org.scalatest.matchers.should.Matchers._

class TestVolume extends OharaTest {
  private[this] val connectionProps = "ohara-jenkins-it-00:34573"
  private[this] val topicKey        = TopicKey.of("default", "topic2")

  @Test
  def testProducer(): Unit = {
    val producer = Producer
      .builder()
      .connectionProps(connectionProps)
      .allAcks()
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()
    try {
      val numberOfRecords = 100
      (0 until numberOfRecords).foreach(
        index => producer.sender().key(index.toString).value(CommonUtils.randomString(10)).topicKey(topicKey).send()
      )
      producer.flush()
    } finally Releasable.close(producer)
  }

  @Test
  def testConsumer(): Unit = {
    val consumer = Consumer
      .builder()
      .connectionProps(connectionProps)
      .offsetFromBegin()
      .topicKey(topicKey)
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()
    try {
      val numberOfRecords = 100
      val records = consumer.poll(Duration.ofSeconds(30), numberOfRecords)
      records.forEach { record =>
        println(record.value().get)
      }
      records.size() shouldBe numberOfRecords
    } finally consumer.close()
  }
}
