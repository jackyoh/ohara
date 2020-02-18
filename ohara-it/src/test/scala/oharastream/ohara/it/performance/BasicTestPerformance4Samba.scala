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

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.atomic.LongAdder

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.AssumptionViolatedException
import spray.json.{JsNumber, JsString, JsValue}

import collection.JavaConverters._
import scala.concurrent.duration._

abstract class BasicTestPerformance4Samba extends BasicTestPerformance {
  private[this] val sambaHostname: String = sys.env.getOrElse(
    PerformanceTestingUtils.SAMBA_HOSTNAME_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.SAMBA_HOSTNAME_KEY} does not exists!!!")
  )

  private[this] val sambaUsername: String = sys.env.getOrElse(
    PerformanceTestingUtils.SAMBA_USER_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.SAMBA_USER_KEY} does not exists!!!")
  )

  private[this] val sambaPassword: String = sys.env.getOrElse(
    PerformanceTestingUtils.SAMBA_PASSWORD_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.SAMBA_PASSWORD_KEY} does not exists!!!")
  )

  private[this] val sambaPort: Int = sys.env
    .getOrElse(
      PerformanceTestingUtils.SAMBA_PORT_KEY,
      throw new AssumptionViolatedException(s"${PerformanceTestingUtils.SAMBA_PORT_KEY} does not exists!!!")
    )
    .toInt

  private[this] val sambaShare: String = sys.env.getOrElse(
    PerformanceTestingUtils.SAMBA_SHARE_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.SAMBA_SHARE_KEY} does not exists!!!")
  )

  private[this] val csvInputFolderKey       = PerformanceTestingUtils.CSV_INPUT_KEY
  private[this] val csvOutputFolder: String = value(csvInputFolderKey).getOrElse("input")

  private[this] val NEED_DELETE_DATA_KEY: String = PerformanceTestingUtils.DATA_CLEANUP_KEY
  protected[this] val needDeleteData: Boolean    = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  private[this] val totalSizeInBytes = new LongAdder()
  private[this] val count            = new LongAdder()

  protected val sambaSettings: Map[String, JsValue] = Map(
    oharastream.ohara.connector.smb.SMB_HOSTNAME_KEY   -> JsString(sambaHostname),
    oharastream.ohara.connector.smb.SMB_PORT_KEY       -> JsNumber(sambaPort),
    oharastream.ohara.connector.smb.SMB_USER_KEY       -> JsString(sambaUsername),
    oharastream.ohara.connector.smb.SMB_PASSWORD_KEY   -> JsString(sambaPassword),
    oharastream.ohara.connector.smb.SMB_SHARE_NAME_KEY -> JsString(sambaShare)
  )

  protected def createSambaFolder(path: String): String = {
    val client = sambaClient()
    try if (!client.exists(path)) client.mkdirs(path)
    finally Releasable.close(client)
    path
  }

  protected def removeSambaFolder(path: String): Unit = {
    val client = sambaClient()
    try client.delete(path, true)
    finally Releasable.close(client)
  }

  protected def exists(path: String): Boolean = {
    val client = sambaClient()
    try client.exists(path)
    finally Releasable.close(client)
  }

  override protected def setupInputData(timeout: Duration): (String, Long, Long) = {
    val cellNames: Set[String] = rowData().cells().asScala.map(_.name).toSet
    val numberOfRowsToFlush    = 1000
    val client                 = sambaClient()

    val start = CommonUtils.current()
    try {
      if (!client.exists(csvOutputFolder)) createSambaFolder(csvOutputFolder)

      while (totalSizeInBytes.longValue() <= sizeOfInputData &&
             (CommonUtils.current() - start) <= timeout.toMillis) {
        val file   = s"$csvOutputFolder/${CommonUtils.randomString()}"
        val writer = new BufferedWriter(new OutputStreamWriter(client.create(file)))
        try {
          writer
            .append(cellNames.mkString(","))
            .append("\n")
          (0 until numberOfRowsToFlush).foreach { _ =>
            val content = rowData().cells().asScala.map(_.value).mkString(",")
            count.increment()
            totalSizeInBytes.add(content.length)
            writer
              .append(content)
              .append("\n")
          }
        } finally Releasable.close(writer)
      }
    } finally Releasable.close(client)

    (csvOutputFolder, count.longValue(), totalSizeInBytes.longValue())
  }

  private[this] def sambaClient(): FileSystem =
    FileSystem.smbBuilder
      .hostname(sambaHostname)
      .port(sambaPort)
      .user(sambaUsername)
      .password(sambaPassword)
      .shareName(sambaShare)
      .build()
}
