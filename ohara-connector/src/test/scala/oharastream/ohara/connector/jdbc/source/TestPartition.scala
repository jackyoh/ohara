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

package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.Test

class TestPartition extends OharaTest {
  private[this] val ONE_DAY_TIMESTAMP = 1000 * 60 * 60 * 24
  private[this] val firstTimestamp    = Timestamp.valueOf("2020-07-04 15:15:00")

  @Test
  def test(): Unit = {
    val ONE_HOUR_TIMESTAMP = 1000 * 60 * 60

    val currentTimestamp = new Timestamp(CommonUtils.current())
    firstTimestamp.getTime.to(currentTimestamp.getTime).by(ONE_HOUR_TIMESTAMP).foreach { timestamp =>
      println(s"${new Timestamp(timestamp)} => ${calcTimeRange(new Timestamp(timestamp))}")
    //println(s"${new Timestamp(timestamp)} => ${partitionKey(new Timestamp(timestamp))}")
    }
  }

  private[this] def calcTimeRange(timestamp: Timestamp): String = {
    val avgTimestamp   = (timestamp.getTime - firstTimestamp.getTime) / ONE_DAY_TIMESTAMP
    val startTimestamp = new Timestamp((avgTimestamp * ONE_DAY_TIMESTAMP) + firstTimestamp.getTime)
    val stopTimestamp  = new Timestamp(startTimestamp.getTime + ONE_DAY_TIMESTAMP - 1)
    s"$startTimestamp~$stopTimestamp"
  }

  /*private[this] def partitionKey(timestamp: Timestamp): String = {
    var startTimestamp: Timestamp   = firstTimestamp
    var stopTimestamp: Timestamp    = new Timestamp(startTimestamp.getTime() + ONE_DAY_TIMESTAMP)

    while (!(timestamp.getTime() >= startTimestamp.getTime() && timestamp.getTime() <= stopTimestamp.getTime())) {
      startTimestamp = stopTimestamp
      stopTimestamp = new Timestamp(startTimestamp.getTime() + ONE_DAY_TIMESTAMP)
    }
    s"$startTimestamp~$stopTimestamp"
  }*/
}
