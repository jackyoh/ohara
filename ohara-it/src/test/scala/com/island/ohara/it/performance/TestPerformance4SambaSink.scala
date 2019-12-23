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
import com.island.ohara.common.util.Releasable
import org.junit.AssumptionViolatedException
import com.island.ohara.common.setting.ConnectorKey
import com.island.ohara.connector.smb.SmbSink
import com.island.ohara.common.setting.TopicKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category
import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4SambaSink extends BasicTestPerformance {
  private[this] val SAMBA_HOSTNAME_KEY: String = "ohara.it.performance.samba.hostname"
  private[this] val sambaHostname: String = sys.env.getOrElse(
    SAMBA_HOSTNAME_KEY,
    throw new AssumptionViolatedException(s"$SAMBA_HOSTNAME_KEY does not exists!!!")
  )

  private[this] val SAMBA_USER_KEY: String = "ohara.it.performance.samba.user"
  private[this] val sambaUsername: String = sys.env.getOrElse(
    SAMBA_USER_KEY,
    throw new AssumptionViolatedException(s"$SAMBA_USER_KEY does not exists!!!")
  )

  private[this] val SAMBA_PASSWORD_KEY: String = "ohara.it.performance.samba.password"
  private[this] val sambaPassword: String = sys.env.getOrElse(
    SAMBA_PASSWORD_KEY,
    throw new AssumptionViolatedException(s"$SAMBA_PASSWORD_KEY does not exists!!!")
  )

  private[this] val SAMBA_PORT_KEY: String = "ohara.it.performance.samba.port"
  private[this] val sambaPort: Int = sys.env
    .getOrElse(
      SAMBA_PORT_KEY,
      "445"
    )
    .toInt

  private[this] val dataDir: String            = "output"
  private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))
  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.samba.needDeleteData"
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  @Test
  def test(): Unit = {
    val fileSystem =
      FileSystem.smbBuilder
        .hostname(sambaHostname)
        .port(sambaPort)
        .user(sambaUsername)
        .password(sambaPassword)
        .shareName(sambaUsername)
        .build()
    try {
      if (!fileSystem.exists(dataDir)) fileSystem.mkdirs(dataDir)
      produce(createTopic(topicKey))
      setupConnector(
        connectorKey = connectorKey,
        topicKey = topicKey,
        className = classOf[SmbSink].getName(),
        settings = Map(
          com.island.ohara.connector.smb.SMB_HOSTNAME_KEY   -> JsString(sambaHostname),
          com.island.ohara.connector.smb.SMB_PORT_KEY       -> JsNumber(sambaPort),
          com.island.ohara.connector.smb.SMB_USER_KEY       -> JsString(sambaUsername),
          com.island.ohara.connector.smb.SMB_PASSWORD_KEY   -> JsString(sambaPassword),
          com.island.ohara.connector.smb.SMB_SHARE_NAME_KEY -> JsString(sambaUsername),
          "output.folder"                                   -> JsString(dataDir)
        )
      )
      sleepUntilEnd()
    } finally Releasable.close(fileSystem)
  }

  override def afterMetrics(): Unit = {
    stopConnector(connectorKey)
    if (needDeleteData) {
      val fileSystem =
        FileSystem.smbBuilder
          .hostname(sambaHostname)
          .port(sambaPort)
          .user(sambaUsername)
          .password(sambaPassword)
          .shareName(sambaUsername)
          .build()
      try fileSystem.delete(s"${dataDir}/${topicKey.topicNameOnKafka}", true)
      finally Releasable.close(fileSystem)
    }
  }
}
