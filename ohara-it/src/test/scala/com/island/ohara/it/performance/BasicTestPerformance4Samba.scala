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

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.client.filesystem.FileSystem
import com.island.ohara.common.util.{CommonUtils, Releasable}
import org.junit.AssumptionViolatedException
import spray.json.{JsNumber, JsString, JsValue}
import collection.JavaConverters._

abstract class BasicTestPerformance4Samba extends BasicTestPerformance {
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
      throw new AssumptionViolatedException(s"$SAMBA_PORT_KEY does not exists!!!")
    )
    .toInt

  private[this] val SAMBA_SHARE_KEY: String = "ohara.it.performance.samba.sharename"
  private[this] val sambaShare: String = sys.env.getOrElse(
    SAMBA_SHARE_KEY,
    throw new AssumptionViolatedException(s"$SAMBA_SHARE_KEY does not exists!!!")
  )

  private[this] val csvInputFolderKey       = "ohara.it.performance.csv.input"
  private[this] val csvOutputFolder: String = value(csvInputFolderKey).getOrElse("input")

  private[this] val NEED_DELETE_DATA_KEY: String = "ohara.it.performance.samba.needDeleteData"
  protected[this] val needDeleteData: Boolean    = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  private[this] val numberOfProducerThread = 2

  protected val sambaSettings: Map[String, JsValue] = Map(
    com.island.ohara.connector.smb.SMB_HOSTNAME_KEY   -> JsString(sambaHostname),
    com.island.ohara.connector.smb.SMB_PORT_KEY       -> JsNumber(sambaPort),
    com.island.ohara.connector.smb.SMB_USER_KEY       -> JsString(sambaUsername),
    com.island.ohara.connector.smb.SMB_PASSWORD_KEY   -> JsString(sambaPassword),
    com.island.ohara.connector.smb.SMB_SHARE_NAME_KEY -> JsString(sambaShare)
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

  protected def setupInputData(): (String, Long, Long) = {
    val cellNames: Set[String] = rowData().cells().asScala.map(_.name).toSet

    val numberOfRowsToFlush = 1000
    val pool                = Executors.newFixedThreadPool(numberOfProducerThread)
    val closed              = new AtomicBoolean(false)
    val count               = new LongAdder()
    val sizeInBytes         = new LongAdder()

    val client = sambaClient()
    try if (client.exists(csvOutputFolder)) throw new IllegalArgumentException(s"$csvOutputFolder exists!!!")
    else createSambaFolder(csvOutputFolder)
    finally Releasable.close(client)

    try {
      (0 until numberOfProducerThread).foreach { _ =>
        pool.execute(() => {
          val client = sambaClient()
          try while (!closed.get() && sizeInBytes.longValue() <= sizeOfInputData) {
            val file   = s"$csvOutputFolder/${CommonUtils.randomString()}"
            val writer = new BufferedWriter(new OutputStreamWriter(client.create(file)))
            try {
              writer
                .append(cellNames.mkString(","))
                .append("\n")
              (0 until numberOfRowsToFlush).foreach { _ =>
                val content = rowData().cells().asScala.map(_.value).mkString(",")
                count.increment()
                sizeInBytes.add(content.length)
                writer
                  .append(content)
                  .append("\n")
              }
            } finally Releasable.close(writer)
          } finally Releasable.close(client)
        })
      }
    } finally {
      pool.shutdown()
      pool.awaitTermination(durationOfPerformance.toMillis * 10, TimeUnit.MILLISECONDS)
      closed.set(true)
    }
    (csvOutputFolder, count.longValue(), sizeInBytes.longValue())
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
