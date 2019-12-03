package com.island.ohara.it.performance

import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Hdfs extends BasicTestPerformance{
  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  @Test
  def test(): Unit = {
    setupTopic(topicKey)

  }
}
