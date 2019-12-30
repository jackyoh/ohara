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

import com.island.ohara.connector.ftp.FtpSource
import com.island.ohara.it.category.PerformanceGroup
import com.island.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.JsString

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4FtpSource extends BasicTestPerformance4Ftp {
  @Test
  def test(): Unit = {
    createTopic()
    val (path, _, _) = setupInputData()
    try {
      setupConnector(
        className = classOf[FtpSource].getName,
        settings = ftpSettings
          + (CsvConnectorDefinitions.INPUT_FOLDER_KEY     -> JsString(path))
          + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(createFtpFolder("/completed")))
          + (CsvConnectorDefinitions.ERROR_FOLDER_KEY     -> JsString(createFtpFolder("/error")))
      )
      sleepUntilEnd()
    } finally if (cleanupTestData) removeFtpFolder(path)
  }
}
