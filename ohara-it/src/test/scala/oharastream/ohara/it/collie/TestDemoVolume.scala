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

package oharastream.ohara.it.collie

import java.time.Duration

import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.kafka.{Consumer, Producer}
import org.scalatest.matchers.should.Matchers._
import scala.jdk.CollectionConverters._
import org.junit.Test

class TestDemoVolume extends OharaTest {
  private[this] val topicKey: TopicKey      = TopicKey.of("group", "topic1")
  private[this] val brokerPort: Int = 40540
  private[this] val connectionProps: String = s"ohara-jenkins-it-00:$brokerPort"

  @Test
  def testProduce(): Unit = {
    writeData(100)
  }

  @Test
  def testConsumer(): Unit = {
    readData() shouldBe 100
  }

  private[this] def writeData(numberOfRecords: Int): Unit = {
    val producer = Producer
      .builder()
      .connectionProps(connectionProps)
      .allAcks()
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()

    try {
      (0 until numberOfRecords).foreach(
        index => producer.sender().key(index.toString).value(s"value-${index.toString}").topicKey(topicKey).send()
      )
      producer.flush()
    } finally producer.close()
  }

  private[this] def readData(): Int = {
    val consumer = Consumer
      .builder()
      .connectionProps(connectionProps)
      .offsetFromBegin()
      .topicKey(topicKey)
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING)
      .build()
    try {
      val records = consumer.poll(Duration.ofSeconds(30), 100)
      records.asScala.foreach { record =>
        println(record.value().get)
      }
      records.size
    } finally consumer.close()
  }
}
