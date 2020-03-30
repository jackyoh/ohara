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

package oharastream.ohara.it.connector.jio

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.connector.jio.BasicTestsOfJsonOut
import oharastream.ohara.it.PaltformModeInfo
import oharastream.ohara.it.category.ConnectorGroup
import org.junit.experimental.categories.Category

@Category(Array(classOf[ConnectorGroup]))
class TestJsonOutOnDocker(paltform: PaltformModeInfo)
    extends BasicIntegrationTestsOfJsonIo(paltform: PaltformModeInfo)
    with BasicTestsOfJsonOut {
  override protected def connectorAdmin: ConnectorAdmin = _connectorAdmin

  override protected def brokersConnProps: String = _brokersConnProps
}
