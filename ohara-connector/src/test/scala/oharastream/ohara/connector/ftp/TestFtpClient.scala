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

package oharastream.ohara.connector.ftp

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.Charset
import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.{Before, Test}
import scala.jdk.CollectionConverters._

class TestFtpClient extends With3Brokers3Workers {
  private[this] val fileSystem = FileSystem.ftpBuilder
    .hostname(testUtil.ftpServer.hostname)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()
  private[this] val inputFolder = "/input"
  private[this] val fileName    = "a1.csv"
  @Before
  def setup(): Unit = {
    createFolder(fileSystem, inputFolder)
    val header = "column1,column2,column3,column4,column5,column6,column7,column8,column9,column10"
    val data = (1 to 100000)
      .map { i =>
        Seq(s"a-$i", s"b-$i", s"c-$i", s"d-$i", s"e-$i", s"f-$i", s"g-$i", s"h-$i", s"i-$i", s"j-$i")
      }
      .map(_.mkString(","))
    setupInput(fileSystem, inputFolder, header, data)
  }

  @Test
  def test(): Unit = {
    val fs = FileSystem.ftpBuilder
      .hostname(testUtil.ftpServer.hostname)
      .port(testUtil.ftpServer.port)
      .user(testUtil.ftpServer.user)
      .password(testUtil.ftpServer.password)
      .build()
    val path   = s"$inputFolder/$fileName"
    val reader = new BufferedReader(new InputStreamReader(fs.open(path), Charset.forName("UTF-8")))
    try {
      // Iterator.continually(reader).takeWhile(_.ready()).foreach {_.readLine()}
      reader.readLine()
    } finally Releasable.close(reader)
  }

  private[this] def createFolder(fileSystem: FileSystem, path: String): Unit = {
    if (fileSystem.exists(path)) {
      fileSystem
        .listFileNames(path)
        .asScala
        .map(oharastream.ohara.common.util.CommonUtils.path(path, _))
        .foreach(fileSystem.delete)
      fileSystem.delete(path)
    }
    fileSystem.mkdirs(path)
  }

  private[this] def setupInput(fileSystem: FileSystem, inputFolder: String, header: String, data: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(
        fileSystem.create(
          oharastream.ohara.common.util.CommonUtils.path(inputFolder, s"$fileName")
        )
      )
    )
    try {
      writer.append(header)
      writer.newLine()
      data.foreach(line => {
        writer.append(line)
        writer.newLine()
      })
    } finally writer.close()
  }
}
