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

import com.island.ohara.client.configurator.v0.TopicApi.TopicInfo
import com.island.ohara.connector.smb.SmbSink
import com.island.ohara.it.category.PerformanceGroup
import com.island.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.JsString

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4SambaSink extends BasicTestPerformance4Samba {
  private[this] val outputDir: String    = "output"
  private[this] var topicInfo: TopicInfo = _

  @Test
  def test(): Unit = {
    topicInfo = createTopic()
    produce(topicInfo)
    setupConnector(
      className = classOf[SmbSink].getName(),
      settings = sambaSettings
        + (CsvConnectorDefinitions.OUTPUT_FOLDER_KEY -> JsString(createSambaFolder(outputDir)))
    )
    sleepUntilEnd()
  }

  override def afterStoppingConnector(): Unit = {
    if (needDeleteData) removeSambaFolder(s"${outputDir}/${topicInfo.topicNameOnKafka}")
  }
}
