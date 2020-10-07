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

package oharastream.ohara.connector.jdbc.util

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestTimer extends OharaTest {
  @Test
  def testOverWait(): Unit = {
    val timer = new Timer()
    (0 to 3).foreach(_ => timer.overWait() shouldBe false)
    timer.overWait() shouldBe true
  }

  @Test
  def testSleep(): Unit = {
    val timer          = new Timer()
    val startTimestamp = CommonUtils.current()
    (0 to 4).foreach(_ => timer.overWait())
    timer.sleep()
    CommonUtils.current() - startTimestamp > 1000 shouldBe true
  }

  @Test
  def testLoop(): Unit = {
    var count = 0
    val timer = new Timer()
    do {
      count = count + 1
      timer.sleep()
    } while (!timer.overWait())
    count >= 4 shouldBe true
  }
}
