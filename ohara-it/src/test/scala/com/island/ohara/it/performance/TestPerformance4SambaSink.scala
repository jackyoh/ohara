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

import com.island.ohara.common.setting.ConnectorKey
import com.island.ohara.connector.smb.SmbSink
import com.island.ohara.common.setting.TopicKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.JsString

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4SambaSink extends BasicTestPerformance4Samba {
  private[this] val dataDir: String            = "output"
  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  @Test
  def test(): Unit = {
    produce(createTopic(topicKey))
    setupConnector(
      connectorKey = connectorKey,
      topicKey = topicKey,
      className = classOf[SmbSink].getName(),
      settings = sambaSettings
        + ("output.folder" -> JsString(createSambaFolder(dataDir)))
    )
    sleepUntilEnd()
  }

  override def afterMetrics(): Unit = {
    stopConnector(connectorKey)
    deleteFolder(s"${dataDir}/${topicKey.topicNameOnKafka}")
  }
}
