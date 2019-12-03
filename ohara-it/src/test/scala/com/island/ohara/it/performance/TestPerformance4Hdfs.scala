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

package com.island.ohara.it.performance

import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.hdfs.sink.HDFSSink
import com.island.ohara.it.category.PerformanceGroup
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}
import org.junit.{AssumptionViolatedException, Before, Test}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Hdfs extends BasicTestPerformance {
  private[this] val HDFS_URL_KEY: String  = "ohara.it.hdfs.url"
  private[this] val TOPIC_DIR_KEY: String = "ohara.it.hdfs.topicDir"

  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  @Before
  def setup(): Unit = {
    sys.env.getOrElse(
      HDFS_URL_KEY,
      throw new AssumptionViolatedException(s"$HDFS_URL_KEY does not exists!!!")
    )
    sys.env.getOrElse(
      TOPIC_DIR_KEY,
      throw new AssumptionViolatedException(s"$TOPIC_DIR_KEY does not exists!!!")
    )
  }

  @Test
  def test(): Unit = {
    setupTopic(topicKey)
    setupConnector(
      connectorKey = connectorKey,
      topicKey = topicKey,
      className = classOf[HDFSSink].getName(),
      settings = Map(
        com.island.ohara.connector.hdfs.sink.HDFS_URL_KEY   -> JsString("hdfs://ohara-jenkins-it-02:9000"),
        com.island.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY -> JsNumber(2000),
        com.island.ohara.connector.hdfs.sink.TOPICS_DIR_KEY -> JsString("/topics")
      )
    )
    sleepUntilEnd()
  }
}
