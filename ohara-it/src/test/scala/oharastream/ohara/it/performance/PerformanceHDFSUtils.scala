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
import oharastream.ohara.common.util.Releasable
import org.junit.AssumptionViolatedException

object PerformanceHDFSUtils {
  val dataDir: String = "/tmp"

  val hdfsURL: String = sys.env.getOrElse(
    PerformanceTestingUtils.HDFS_URL_KEY,
    throw new AssumptionViolatedException(s"${PerformanceTestingUtils.HDFS_URL_KEY} does not exists!!!")
  )

  def createFolder(hdfsURL: String, path: String): String = {
    val fileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build
    try {
      if (fileSystem.nonExists(path)) fileSystem.mkdirs(path)
      path
    } finally Releasable.close(fileSystem)
  }

  def deleteFolder(hdfsURL: String, path: String): Unit = {
    val fileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build
    try if (fileSystem.exists(path)) fileSystem.delete(path, true)
    finally Releasable.close(fileSystem)
  }
}
