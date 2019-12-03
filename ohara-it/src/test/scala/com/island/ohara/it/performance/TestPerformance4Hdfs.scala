package com.island.ohara.it.performance

import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.hdfs.sink.HDFSSink
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Hdfs extends BasicTestPerformance {
  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  @Test
  def test(): Unit = {
    setupTopic(topicKey)
    setupConnector(
      connectorKey = connectorKey,
      topicKey = topicKey,
      className = classOf[HDFSSink].getName(),
      settings = Map(
        com.island.ohara.connector.hdfs.sink.HDFS_URL_KEY   -> JsString("hdfs://ohara-jenkins-it-02:9000"),
        com.island.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY -> JsNumber(2000)
      )
    )
    sleepUntilEnd()
  }
}
