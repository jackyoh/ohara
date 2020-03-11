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
import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.hdfs.sink.HDFSSink
import oharastream.ohara.it.category.PerformanceGroup
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}
import org.junit.{AssumptionViolatedException, Test}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4HdfsSink extends BasicTestPerformance {
  private[this] val NEED_DELETE_DATA_KEY: String = PerformanceTestingUtils.DATA_CLEANUP_KEY
  private[this] val dataDir: String              = "/tmp"
  private[this] val hdfsURL: String = sys.env.getOrElse(
    PerformanceTestingUtils.HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.HDFS_URL_KEY} does not exists!!!")
  )

  private[this] val hdfsFileFlushSize: Int = sys.env
    .getOrElse(
      PerformanceTestingUtils.HDFS_FILE_FLUSH_SIZE_KEY,
      PerformanceTestingUtils.HDFS_FILE_FLUSH_SIZE_DEFAULT
    )
    .toInt

  private[this] val needDeleteData: Boolean = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  @Test
  def test(): Unit = {
    createTopic()
    produce(timeoutOfInputData)
    loopInputDataThread(produce)
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[HDFSSink].getName(),
      settings = Map(
        oharastream.ohara.connector.hdfs.sink.HDFS_URL_KEY   -> JsString(hdfsURL),
        oharastream.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY -> JsNumber(hdfsFileFlushSize),
        oharastream.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(
          PerformanceHDFSUtils.createFolder(hdfsURL, dataDir)
        )
      )
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (needDeleteData) {
      topicInfos.foreach { topicInfo =>
        val path = s"${dataDir}/${topicInfo.topicNameOnKafka}"
        PerformanceHDFSUtils.deleteFolder(hdfsURL, path)
      }
    }
}
