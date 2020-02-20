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

package oharastream.ohara.it.performance

import oharastream.ohara.client.configurator.v0.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.v0.TopicApi.TopicInfo
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.ftp.FtpSink
import oharastream.ohara.it.category.PerformanceGroup
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import spray.json.JsString
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4FtpSink extends BasicTestPerformance4Ftp {
  private[this] val dataDir: String = "/tmp"

  @Test
  def test(): Unit = {
    createTopic()
    produce(timeoutOfInputData)
    loopInputDataThread(produce)
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[FtpSink].getName(),
      settings = ftpSettings
        + (CsvConnectorDefinitions.OUTPUT_FOLDER_KEY -> JsString(createFtpFolder(dataDir)))
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (cleanupTestData)
      topicInfos.foreach { topicInfo =>
        val path = s"${dataDir}/${topicInfo.topicNameOnKafka}"
        if (exists(path)) recursiveRemoveFolder(path)
      }
}
