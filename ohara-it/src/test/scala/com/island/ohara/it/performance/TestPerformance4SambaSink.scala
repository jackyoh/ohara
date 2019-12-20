package com.island.ohara.it.performance

//import com.island.ohara.client.filesystem.FileSystem
/*import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.connector.smb.SmbSink*/
import com.island.ohara.common.setting.TopicKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
import org.junit.{After, Test}
import org.junit.experimental.categories.Category
//import spray.json.{JsNumber, JsString}

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4SambaSink extends BasicTestPerformance {
  /*private[this] val SAMBA_HOSTNAME_KEY: String = "ohara.it.performance.samba.hostname"
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
    .toInt*/

  //private[this] val dataDir: String            = "/tmp"
  //private[this] val connectorKey: ConnectorKey = ConnectorKey.of("benchmark", CommonUtils.randomString(5))

  private[this] val topicKey: TopicKey         = TopicKey.of("benchmark", CommonUtils.randomString(5))

  /*private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.samba.needDeleteData"
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean
  */

  @Test
  def test(): Unit = {
    produce(createTopic(topicKey))
    /*setupConnector(
      connectorKey = connectorKey,
      topicKey = topicKey,
      className = classOf[SmbSink].getName(),
      settings = Map(
        com.island.ohara.connector.smb.SMB_HOSTNAME_KEY   -> JsString(sambaHostname),
        com.island.ohara.connector.smb.SMB_PORT_KEY       -> JsNumber(sambaPort),
        com.island.ohara.connector.smb.SMB_USER_KEY       -> JsString(sambaUsername),
        com.island.ohara.connector.smb.SMB_PASSWORD_KEY   -> JsString(sambaPassword),
        com.island.ohara.connector.smb.SMB_SHARE_NAME_KEY -> JsString(sambaUsername)
      )
    )
    sleepUntilEnd()*/
  }

  @After
  def deleteData(): Unit = {

    /*val fileSystem =
      FileSystem.smbBuilder
        .hostname(props.hostname)
        .port(props.port)
        .user(props.user)
        .password(props.password)
        .shareName(props.shareName)
        .build()*/

  }
}
