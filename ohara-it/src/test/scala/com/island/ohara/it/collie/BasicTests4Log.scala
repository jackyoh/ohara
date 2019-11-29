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

package com.island.ohara.it.collie

import java.util.concurrent.TimeUnit

import com.island.ohara.it.IntegrationTest
import org.junit.{Before, Test}
import org.scalatest.Matchers._

abstract class BasicTests4Log extends IntegrationTest {
  protected def createBusyBox(imageName: String, arguments: Seq[String])
  protected def log(sinceSeconds: Option[Long]): String

  @Before
  def before(): Unit =
    createBusyBox("busybox", Seq("sh", "-c", "while true; do $(echo date); sleep 1; done"))

  @Test
  def test(): Unit = {
    val lastLine = log(None).split("\n").last
    TimeUnit.SECONDS.sleep(3)
    log(Some(1)) should not include lastLine
    log(Some(10)) should include(lastLine)
  }
}
