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

import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.ftp.FtpSource
import oharastream.ohara.it.category.PerformanceGroup
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.JsString

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4FtpSource extends BasicTestPerformance4Ftp {
  @Test
  def test(): Unit = {
    createTopic()
    val completedPath = "/completed"
    val errorPath     = "/error"
    val (path, _, _)  = setupInputData(timeoutOfInputData)
    try {
      loopInputData()
      setupConnector(
        connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
        className = classOf[FtpSource].getName,
        settings = ftpSettings
          + (CsvConnectorDefinitions.INPUT_FOLDER_KEY     -> JsString(path))
          + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(createFtpFolder(completedPath)))
          + (CsvConnectorDefinitions.ERROR_FOLDER_KEY     -> JsString(createFtpFolder(errorPath)))
      )
      sleepUntilEnd()
    } finally if (cleanupTestData) {
      removeFtpFolder(path)
      removeFtpFolder(completedPath)
      removeFtpFolder(errorPath)
    }
  }
}
