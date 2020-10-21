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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.{Consumer, Producer}
import org.junit.jupiter.api.Test
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration

class TestProducer extends OharaTest {
  @Test
  def test(): Unit = {
    val topicKey: TopicKey = TopicKey.of("default", CommonUtils.randomString(5))
    val brokers: String    = "k8s-master:37650,k8s-slave00:37650"

    val producer = Producer
      .builder()
      .keySerializer(Serializer.ROW)
      .connectionProps(brokers)
      .build()
    val consumer = Consumer
      .builder()
      .topicKey(topicKey)
      .offsetFromBegin()
      .connectionProps(brokers)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try {
      val count = new LongAdder()
      producer
        .sender()
        .topicKey(topicKey)
        .key(
          Row.of(
            (0 until 10).map(index => {
              Cell.of(s"c$index", CommonUtils.randomString())
            }): _*
          )
        )
        .send()
        .whenComplete {
          case (meta, _) =>
            if (meta != null) {
              count.add(1)
            }
        }
      val result = consumer.poll(java.time.Duration.ofNanos(Duration(30, TimeUnit.SECONDS).toNanos), 1).asScala.toSeq
      println(s"RESULT SIZE IS ${result.size}")
    } finally Releasable.close(producer)
  }
}
