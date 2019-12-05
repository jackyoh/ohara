package com.island.ohara.it.performance

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.client.configurator.v0.FileInfoApi
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category

import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Jdbc extends BasicTestPerformance {
  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath          = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")

  private[this] val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
  private[this] val jar = new File(CommonUtils.path(jarFolderPath, "ojdbc8.jar"))
  private[this] val jdbcJarFileInfo = result(jarApi.request.file(jar).upload())

  @Test
  def test(): Unit = {

  }

  override protected[performance] def sharedJars(): Set[ObjectKey] = Set(jdbcJarFileInfo.key)

  private[this] def insertData(): Unit = {
    val pool = Executors.newFixedThreadPool(1)
    val closed = new AtomicBoolean(false)
    val count = new LongAdder()
    val sizeInBytes = new LongAdder()

    (0 until 4).foreach {_ =>
      pool.execute(() => {

      })

    }

  }
}
