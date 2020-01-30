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

import com.island.ohara.common.setting.ConnectorKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.jio.JsonOut
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.JsNumber

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4JsonOut extends BasicTestPerformance {
  @Test
  def test(): Unit = {
    produce(createTopic())
    setupConnector(
      connectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5)),
      className = classOf[JsonOut].getName,
      settings = Map(com.island.ohara.connector.jio.BINDING_PORT_KEY -> JsNumber(workerClusterInfo.freePorts.head))
    )
    sleepUntilEnd()
  }
}
