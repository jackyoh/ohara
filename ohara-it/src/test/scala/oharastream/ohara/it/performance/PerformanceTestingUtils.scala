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

import oharastream.ohara.client.filesystem.FileSystem
import org.junit.AssumptionViolatedException

private[performance] object PerformanceTestingUtils {
  val INPUTDATA_TIMEOUT_KEY: String    = "ohara.it.performance.input.data.timeout"
  val DURATION_KEY: String             = "ohara.it.performance.duration"
  val REPORT_OUTPUT_KEY: String        = "ohara.it.performance.report.output"
  val LOG_METERS_FREQUENCY_KEY: String = "ohara.it.performance.log.meters.frequency"
  val DATA_SIZE_KEY: String            = "ohara.it.performance.data.size"
  val PARTITION_SIZE_KEY: String       = "ohara.it.performance.topic.partitions"
  val TASK_SIZE_KEY: String            = "ohara.it.performance.connector.tasks"
  val ROW_FLUSH_NUMBER_KEY: String     = "ohara.it.performance.row.flush.number"
  val FILENAME_CACHE_SIZE_KEY: String  = "ohara.it.performance.filename.cache.size"

  // FTP Setting Key
  val FTP_HOSTNAME_KEY: String = "ohara.it.performance.ftp.hostname"
  val FTP_PORT_KEY: String     = "ohara.it.performance.ftp.port"
  val FTP_USER_KEY: String     = "ohara.it.performance.ftp.user"
  val FTP_PASSWORD_KEY: String = "ohara.it.performance.ftp.password"

  // Samba Setting Key
  val SAMBA_HOSTNAME_KEY: String = "ohara.it.performance.samba.hostname"
  val SAMBA_USER_KEY: String     = "ohara.it.performance.samba.user"
  val SAMBA_PASSWORD_KEY: String = "ohara.it.performance.samba.password"
  val SAMBA_PORT_KEY: String     = "ohara.it.performance.samba.port"
  val SAMBA_SHARE_KEY: String    = "ohara.it.performance.samba.sharename"

  // JDBC Setting Key
  val DB_URL_KEY: String       = "ohara.it.performance.jdbc.url"
  val DB_USER_NAME_KEY: String = "ohara.it.performance.jdbc.username"
  val DB_PASSWORD_KEY: String  = "ohara.it.performance.jdbc.password"
  val JAR_FOLDER_KEY: String   = "ohara.it.jar.folder"

  // HDFS Setting Key
  val HDFS_URL_KEY: String = "ohara.it.performance.hdfs.url"
  val dataDir: String      = "/tmp"

  val hdfsURL: String = sys.env.getOrElse(
    PerformanceTestingUtils.HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.HDFS_URL_KEY} does not exists!!!")
  )

  val CSV_FILE_FLUSH_SIZE_KEY: String = "ohara.it.performance.csv.file.flush.size"
  val CSV_INPUT_KEY: String           = "ohara.it.performance.csv.input"

  val DATA_CLEANUP_KEY: String = "ohara.it.performance.cleanup"

  def createFolder(fileSystem: FileSystem, path: String): String = {
    if (fileSystem.nonExists(path)) fileSystem.mkdirs(path)
    path
  }

  def deleteFolder(fileSystem: FileSystem, path: String): Unit = {
    if (fileSystem.exists(path)) fileSystem.delete(path, true)
  }

  def exists(fileSystem: FileSystem, path: String): Boolean = {
    fileSystem.exists(path)
  }
}
