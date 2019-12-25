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

import com.island.ohara.client.filesystem.FileSystem
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.connector.hdfs.sink.HDFSSink
import com.island.ohara.it.category.PerformanceGroup
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}
import org.junit.{After, AssumptionViolatedException, Test}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4HdfsSink extends BasicTestPerformance {
  private[this] val HDFS_URL_KEY: String         = "ohara.it.performance.hdfs.url"
  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.hdfs.needDeleteData"

  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  private[this] val dataDir: String = "/tmp"
  private[this] val hdfsURL: String = sys.env.getOrElse(
    HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"$HDFS_URL_KEY does not exists!!!")
  )

  private[this] val needDeleteData: Boolean = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  @Test
  def test(): Unit = {
    produce(createTopic(topicKey))
    setupConnector(
      connectorKey = connectorKey,
      topicKey = topicKey,
      className = classOf[HDFSSink].getName(),
      settings = Map(
        com.island.ohara.connector.hdfs.sink.HDFS_URL_KEY      -> JsString(hdfsURL),
        com.island.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY    -> JsNumber(2000),
        com.island.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(dataDir)
      )
    )
    sleepUntilEnd()
  }

  @After
  def deleteData(): Unit =
    if (needDeleteData) {
      val fileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build
      try fileSystem.delete(s"${dataDir}/${topicKey.topicNameOnKafka}", true)
      finally Releasable.close(fileSystem)
    }
}
