package oharastream.ohara.it.performance

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.util.Releasable

object PerformanceHDFSUtils {
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
