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
import oharastream.ohara.connector.jdbc.source.JDBCSourceConnector
import oharastream.ohara.it.category.PerformanceGroup
import org.junit.experimental.categories.Category
import org.junit.{AssumptionViolatedException, Test}
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4JDBCSourceToHDFSSink extends BasicTestPerformance4Jdbc {
  private[this] val dataDir: String = "/tmp"
  private[this] val hdfsURL: String = sys.env.getOrElse(
    PerformanceTestingUtils.HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.HDFS_URL_KEY} does not exists!!!")
  )

  override protected val tableName: String = s"TABLE${CommonUtils.randomString().toUpperCase()}"

  @Test
  def test(): Unit = {
    createTable()
    setupInputData(timeoutOfInputData)
    loopInputData()
    createTopic()

    //Running JDBC Source Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[JDBCSourceConnector].getName(),
      settings = Map(
        oharastream.ohara.connector.jdbc.source.DB_URL                -> JsString(url),
        oharastream.ohara.connector.jdbc.source.DB_USERNAME           -> JsString(user),
        oharastream.ohara.connector.jdbc.source.DB_PASSWORD           -> JsString(password),
        oharastream.ohara.connector.jdbc.source.DB_TABLENAME          -> JsString(tableName),
        oharastream.ohara.connector.jdbc.source.TIMESTAMP_COLUMN_NAME -> JsString(timestampColumnName),
        oharastream.ohara.connector.jdbc.source.DB_SCHEMA_PATTERN     -> JsString(user),
        oharastream.ohara.connector.jdbc.source.JDBC_FETCHDATA_SIZE   -> JsNumber(10000),
        oharastream.ohara.connector.jdbc.source.JDBC_FLUSHDATA_SIZE   -> JsNumber(10000)
      )
    )

    //Running HDFS Sink Connector
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[HDFSSink].getName(),
      settings = Map(
        oharastream.ohara.connector.hdfs.sink.HDFS_URL_KEY      -> JsString(hdfsURL),
        oharastream.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY    -> JsNumber(2000),
        oharastream.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(dataDir)
      )
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (needDeleteData) {
      //Drop table for the database
      client.dropTable(tableName)

      //Delete file for the HDFS
      val fileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build
      try topicInfos.foreach { topicInfo =>
        val path = s"${dataDir}/${topicInfo.topicNameOnKafka}"
        if (fileSystem.exists(path)) fileSystem.delete(path, true)
      } finally Releasable.close(fileSystem)
    }
}
