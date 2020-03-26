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

package oharastream.ohara.it.client

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.v0.LogApi
import oharastream.ohara.it.{PaltformModeInfo, WithRemoteConfigurator}
import oharastream.ohara.it.category.ClientGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[ClientGroup]))
class TestQueryConfiguratorLog(paltform: PaltformModeInfo) extends WithRemoteConfigurator(paltform: PaltformModeInfo) {
  @Test
  def test(): Unit = {
    val log = result(LogApi.access.hostname(configuratorHostname).port(configuratorPort).log4Configurator())
    log.logs.size shouldBe 1
    log.logs.head.hostname.length should not be 0
    log.logs.head.value.length should not be 0

    val logOf1Second = result(LogApi.access.hostname(configuratorHostname).port(configuratorPort).log4Configurator(1)).logs.head.value
    TimeUnit.SECONDS.sleep(6)
    val logOf6Second = result(LogApi.access.hostname(configuratorHostname).port(configuratorPort).log4Configurator(6)).logs.head.value
    withClue(s"logOf1Second:$logOf1Second\nlogOf6Second:$logOf6Second") {
      // it counts on timer so the "=" is legal :)
      logOf1Second.length should be <= logOf6Second.length
    }
  }
}
