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
import oharastream.ohara.connector.ftp.FtpSource
import oharastream.ohara.connector.hdfs.sink.HDFSSink
import oharastream.ohara.it.category.PerformanceGroup
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4FtpSourceToHDFSSink extends BasicTestPerformance4Ftp {
  private[this] val ftpCompletedPath = "/completed"
  private[this] val ftpErrorPath     = "/error"
  private[this] val (path, _, _)     = setupInputData(timeoutOfInputData)

  @Test
  def test(): Unit = {
    createTopic()
    loopInputDataThread(setupInputData)
    //Running FTP Source Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[FtpSource].getName,
      settings = ftpSettings
        + (CsvConnectorDefinitions.INPUT_FOLDER_KEY     -> JsString(path))
        + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(createFtpFolder(ftpCompletedPath)))
        + (CsvConnectorDefinitions.ERROR_FOLDER_KEY     -> JsString(createFtpFolder(ftpErrorPath)))
    )

    //Running HDFS Sink Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[HDFSSink].getName(),
      settings = Map(
        oharastream.ohara.connector.hdfs.sink.HDFS_URL_KEY   -> JsString(PerformanceHDFSUtils.hdfsURL),
        oharastream.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY -> JsNumber(numberOfCsvFileToFlush),
        oharastream.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(
          PerformanceHDFSUtils.createFolder(PerformanceHDFSUtils.hdfsURL, PerformanceHDFSUtils.dataDir)
        )
      )
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (cleanupTestData) {
      //Delete file for the FTP
      removeFtpFolder(path)
      removeFtpFolder(ftpCompletedPath)
      removeFtpFolder(ftpErrorPath)

      //Delete file from the HDFS
      topicInfos.foreach { topicInfo =>
        val path = s"${PerformanceHDFSUtils.dataDir}/${topicInfo.topicNameOnKafka}"
        PerformanceHDFSUtils.deleteFolder(PerformanceHDFSUtils.hdfsURL, path)
      }
    }
}
