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

import com.island.ohara.common.setting.TopicKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.perf.PerfSource
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4PerfSource extends BasicTestPerformance {
  private[this] val topicKey: TopicKey = TopicKey.of("benchmark", CommonUtils.randomString(5))

  @Test
  def test(): Unit = {
    createTopic(topicKey)
    setupConnector(
      topicKey = topicKey,
      className = classOf[PerfSource].getName,
      settings = Map.empty
    )
    sleepUntilEnd()
  }
}
