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
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import com.island.ohara.client.filesystem.ftp.FtpClient
import com.island.ohara.common.util.{CommonUtils, Releasable}
import org.junit.AssumptionViolatedException
import spray.json.{JsNumber, JsString, JsValue}

import scala.collection.JavaConverters._

abstract class BasicTestPerformance4Ftp extends BasicTestPerformance {
  private[this] val ftpHostname = value("ohara.it.performance.ftp.hostname")
    .getOrElse(throw new AssumptionViolatedException("ohara.it.performance.ftp.hostname is required"))

  /**
    * add the route for ftp hostname to avoid the hostname error from remote services...
    * @return routes routes added to all services
    */
  override def routes: Map[String, String] = Map(ftpHostname -> CommonUtils.address(ftpHostname))

  /**
    * generate the default settings according to the ftp server. It includes
    * 1) hostname
    * 2) port
    * 3) user name
    * 4) user password
    */
  protected val ftpSettings: Map[String, JsValue] = Map(
    // convert the hostname to IP address
    com.island.ohara.connector.ftp.FTP_HOSTNAME_KEY  -> JsString(ftpHostname),
    com.island.ohara.connector.ftp.FTP_PORT_KEY      -> JsNumber(21),
    com.island.ohara.connector.ftp.FTP_USER_NAME_KEY -> JsString("admin"),
    com.island.ohara.connector.ftp.FTP_PASSWORD_KEY  -> JsString("admin")
  )

  private[this] val csvInputFolderKey       = "ohara.it.performance.csv.input"
  private[this] val csvOutputFolder: String = value(csvInputFolderKey).getOrElse("/input")

  private[this] val cleanupTestDataKey   = "ohara.it.performance.cleanup"
  protected val cleanupTestData: Boolean = value(cleanupTestDataKey).forall(_.toBoolean)

  private[this] def ftpClient() =
    FtpClient
      .builder()
      .hostname(ftpHostname)
      .port(21)
      .user("admin")
      .password("admin")
      .build

  protected def setupInputData(): (String, Long, Long) = {
    val cellNames: Set[String] = (0 until 10).map(index => s"c$index").toSet

    /**
      * if the number of threads is bigger than the number of data ports, it produces the error since no available data
      * port for extra threads :(
      */
    val numberOfProducerThread = 2
    val numberOfRowsToFlush    = 1000
    val pool                   = Executors.newFixedThreadPool(numberOfProducerThread)
    val closed                 = new AtomicBoolean(false)
    val count                  = new LongAdder()
    val sizeInBytes            = new LongAdder()

    val client = ftpClient()
    try if (client.exist(csvOutputFolder)) throw new IllegalArgumentException(s"$csvOutputFolder exists!!!")
    else client.mkdir(csvOutputFolder)
    finally Releasable.close(client)

    try {
      (0 until numberOfProducerThread).foreach { _ =>
        pool.execute(() => {
          val client = ftpClient()
          try while (!closed.get() && sizeInBytes.longValue() <= sizeOfInputData) {
            val file   = s"$csvOutputFolder/${CommonUtils.randomString()}"
            val writer = new BufferedWriter(new OutputStreamWriter(client.create(file)))
            try {
              writer
                .append(cellNames.mkString(","))
                .append("\n")
              (0 until numberOfRowsToFlush).foreach { _ =>
                val content = cellNames.map(_ => CommonUtils.randomString()).mkString(",")
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

  protected def createFtpFolder(path: String): String = {
    val client = ftpClient()
    try client.mkdir(path)
    finally Releasable.close(client)
    path
  }

  /**
    * remove all data in the path.
    * @param path file path on the remote ftp server
    */
  protected def removeFtpFolder(path: String): Unit = {
    val count     = 2
    val executors = Executors.newFixedThreadPool(4)
    val client    = ftpClient()
    try {
      val files = {
        val fs    = client.listFileNames(path).map(name => s"$path/$name")
        val queue = new ArrayBlockingQueue[String](fs.size)
        queue.addAll(fs.asJava)
        queue
      }
      (0 until count).foreach { _ =>
        executors.execute(() => {
          val client = ftpClient()
          try {
            var file = files.poll()
            while (file != null) {
              client.delete(file)
              file = files.poll()
            }
          } finally Releasable.close(client)
        })
      }
    } finally try {
      executors.shutdown()
      // we delete the folder only if all threads are completed
      if (executors.awaitTermination(60, TimeUnit.SECONDS)) client.delete(path)
      else throw new IllegalArgumentException(s"failed to remove folder:$path due to timeout")
    } finally Releasable.close(client)
  }
}
