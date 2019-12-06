package com.island.ohara.it.performance

import java.io.File
import java.util.concurrent.Executors

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
  private[this] val user: String           = "user1"
  private[this] val password: String       = "123456"
  private[this] val client                 = DatabaseClient.builder.url(url).user(user).password(password).build

  @Test
  def test(): Unit = {
    insertData()
  }

  override protected def sharedJars(): Set[ObjectKey] = {
    val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    val jar                        = new File(CommonUtils.path(jarFolderPath, "ojdbc8.jar"))
    Set(result(jarApi.request.file(jar).upload()).key)
  }

  private[this] def insertData(): Unit = {
    val tableName: String = s"TABLE${CommonUtils.randomString().toUpperCase()}"

    val column1 = RdbColumn("COLUMN1", "VARCHAR(45)", true)
    val column2 = RdbColumn("COLUMN2", "VARCHAR(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)

    println(s"Table name is ${tableName}")
    client.createTable(tableName, Seq(column1, column2, column3))

    val pool = Executors.newFixedThreadPool(1)
    //val closed = new AtomicBoolean(false)
    //val count = new LongAdder()
    //val sizeInBytes = new LongAdder()

    (0 to 1).foreach { _ =>

      pool.execute(() => {
        //val client1 = DatabaseClient.builder.url(url).user(user).password(password).build
          (0 to 100).foreach(i => {
            val sql               = s"INSERT INTO ${tableName} VALUES('a${i}1', 'a${i}2', 'a${i}3')"
            val preparedStatement = client.connection.prepareStatement(sql)
            preparedStatement.executeUpdate()
          })
      })
    }
  }

  @After
  def close(): Unit = {
    Releasable.close(client)
  }
}
