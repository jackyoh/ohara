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

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.common.util.Releasable
//import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.island.ohara.client.configurator.v0.FileInfoApi
import com.island.ohara.client.configurator.v0.InspectApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.{CommonUtils}
import com.island.ohara.it.category.PerformanceGroup
import org.junit.{After, Test}
import org.junit.experimental.categories.Category

import scala.concurrent.ExecutionContext.Implicits.global

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Jdbc extends BasicTestPerformance {
  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath: String  = sys.env.getOrElse(JAR_FOLDER_KEY, "/jar")
  private[this] val url: String            = "jdbc:oracle:thin:@//ohara-jenkins-it-02:1521/xe.localdomain"
  private[this] val user: String           = "user3"
  private[this] val password: String       = "123456"
  private[this] val client                 = DatabaseClient.builder.url(url).user(user).password(password).build

  @Test
  def test(): Unit = {
    val (tableName, count, totalBytes) = setupTableData()
    


  }

  override protected def sharedJars(): Set[ObjectKey] = {
    val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    val jar                        = new File(CommonUtils.path(jarFolderPath, "ojdbc8.jar"))
    Set(result(jarApi.request.file(jar).upload()).key)
  }

  private[this] def setupTableData(): (String, Long, Long) = {
    val tableName: String = s"TABLE${CommonUtils.randomString().toUpperCase()}"
    log.info(s"Oracle databse table name is ${tableName} for JDBC performance test")
    val columnSize  = 5
    val columnInfos = (0 until columnSize).map(c =>
      if (c == 0) RdbColumn(s"COLUMN${c}", "VARCHAR(45)", true)
      else RdbColumn(s"COLUMN${c}", "VARCHAR(45)", false)
    )
    client.createTable(tableName, columnInfos)

    val numberOfProducerThread = 4
    val sizeOfInputData        = 1024L * 1024L * 100

    val pool   = Executors.newFixedThreadPool(numberOfProducerThread)
    val closed = new AtomicBoolean(false)
    val count       = new LongAdder()
    val sizeInBytes = new LongAdder()
    try {
      (0 until numberOfProducerThread).foreach { x =>
        pool.execute(() => {
          val client = DatabaseClient.builder.url(url).user(user).password(password).build
          try while (!closed.get() && sizeInBytes.longValue() <= sizeOfInputData) {
            val sql = s"INSERT INTO $tableName VALUES " + (0 until columnSize).map(_ => "?").mkString("(", ",", ")")
            val preparedStatement = client.connection.prepareStatement(sql)
            try {
              (1 to columnSize).foreach(i => {
                val value = s"${i}-${CommonUtils.randomString()}"
                preparedStatement.setString(i, value)
                sizeInBytes.add(value.length)
              })
              count.increment()
              preparedStatement.executeUpdate()
            } finally Releasable.close(preparedStatement)
          } finally Releasable.close(client)
        })
      }
    } finally {
      pool.shutdown()
      pool.awaitTermination(60, TimeUnit.SECONDS)
      closed.set(true)
    }
    (tableName, count.longValue(), sizeInBytes.longValue())
  }

  @After
  def close(): Unit = {
    Releasable.close(client)
  }
}
