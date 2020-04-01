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

package oharastream.ohara.it.connector.smb

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.connector.CsvSourceTestBase
import oharastream.ohara.connector.smb._
import oharastream.ohara.it.category.ConnectorGroup
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions._
import oharastream.ohara.kafka.connector.csv.CsvSourceConnector
import org.junit.experimental.categories.Category

@Category(Array(classOf[ConnectorGroup]))
class TestSmbSource extends CsvSourceTestBase {
  private[this] val itProps: ITSmbProps = ITSmbProps(sys.env)

  override protected val fileSystem: FileSystem = FileSystem.smbBuilder
    .hostname(itProps.hostname)
    .port(itProps.port)
    .user(itProps.username)
    .password(itProps.password)
    .shareName(itProps.shareName)
    .build()

  override protected val connectorClass: Class[_ <: CsvSourceConnector] = classOf[SmbSource]

  override protected val setupProps: Map[String, String] = Map(
    SMB_HOSTNAME_KEY     -> itProps.hostname,
    SMB_PORT_KEY         -> itProps.port.toString,
    SMB_USER_KEY         -> itProps.username,
    SMB_PASSWORD_KEY     -> itProps.password,
    SMB_SHARE_NAME_KEY   -> itProps.shareName,
    INPUT_FOLDER_KEY     -> "input",
    COMPLETED_FOLDER_KEY -> "completed",
    ERROR_FOLDER_KEY     -> "error"
  )
}
