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

import com.island.ohara.client.configurator.v0.ConnectorApi.ConnectorInfo
import com.island.ohara.client.configurator.v0.TopicApi.TopicInfo
import com.island.ohara.client.filesystem.FileSystem
import com.island.ohara.common.setting.ConnectorKey
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.connector.ftp.FtpSource
import com.island.ohara.connector.hdfs.sink.HDFSSink
import com.island.ohara.it.category.PerformanceGroup
import com.island.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.{AssumptionViolatedException, Test}
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4FtpSourceToHDFSSink extends BasicTestPerformance4Ftp {
  private[this] val hdfsURL: String = sys.env.getOrElse(
    PerformanceTestingUtils.HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.HDFS_URL_KEY} does not exists!!!")
  )
  private[this] val dataDir: String = "/tmp"
  private[this] val completedPath   = "/completed"
  private[this] val errorPath       = "/error"
  private[this] val (path, _, _)    = setupInputData()

  @Test
  def test(): Unit = {
    createTopic()
    // Running FTP Source Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[FtpSource].getName,
      settings = ftpSettings
        + (CsvConnectorDefinitions.INPUT_FOLDER_KEY     -> JsString(path))
        + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(createFtpFolder(completedPath)))
        + (CsvConnectorDefinitions.ERROR_FOLDER_KEY     -> JsString(createFtpFolder(errorPath)))
    )

    //Running HDFS Sink Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[HDFSSink].getName(),
      settings = Map(
        com.island.ohara.connector.hdfs.sink.HDFS_URL_KEY      -> JsString(hdfsURL),
        com.island.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY    -> JsNumber(2000),
        com.island.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(dataDir)
      )
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (cleanupTestData) {
      // Delete file for the FTP
      removeFtpFolder(path)
      removeFtpFolder(completedPath)
      removeFtpFolder(errorPath)

      // Delete file for the HDFS
      val fileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build
      try topicInfos.foreach { topicInfo =>
        val path = s"${dataDir}/${topicInfo.topicNameOnKafka}"
        if (fileSystem.exists(path)) fileSystem.delete(path, true)
      } finally Releasable.close(fileSystem)
    }
}
