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
import org.scalatest.matchers.should.Matchers._

class TestPartitionKey extends OharaTest {
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int = 86400000 / 2

  @Test
  def test(): Unit = {
    //2020-07-05 11:06:45.664 -- Timestamp: 2020-07-06 11:06:45.664 => 2020-07-06 11:06:45.664~2020-07-07 11:06:45.664
    /*val current = CommonUtils.current()
    println(olderPartitionKey("table1", Timestamp.valueOf("2020-07-05 11:06:45.664"), new Timestamp(current)))
    println(newPartitionKey("table1", Timestamp.valueOf("2020-07-05 11:06:45.664"), new Timestamp(current)))
     */
    val FIVE_DAY_TIMESTAMP = 432000000
    val ONE_DAY_TIMESTAMP  = 86400000
    val ONE_HOUR_TIMESTAMP = 3600000

    val startTimestamp = new Timestamp(CommonUtils.current() - FIVE_DAY_TIMESTAMP)
    val stopTimestamp  = new Timestamp(CommonUtils.current() - ONE_DAY_TIMESTAMP)
    startTimestamp.getTime().to(stopTimestamp.getTime()).by(ONE_HOUR_TIMESTAMP).foreach { i =>
      // val current = CommonUtils.current()
      val olderTimestamp = s"${new Timestamp(i)} => ${olderPartitionKey("table1", startTimestamp, new Timestamp(i))}"
      val newTimestamp   = s"${new Timestamp(i)} => ${newPartitionKey("table1", startTimestamp, new Timestamp(i))}"
      println(s"older: ${olderTimestamp}")
      println(s"new00: ${newTimestamp}")
      println("")
      olderTimestamp.toString shouldBe newTimestamp.toString
    }
  }

  private[this] def newPartitionKey(tableName: String, firstTimestampValue: Timestamp, timestamp: Timestamp): String = {
    val page =
      (timestamp.getTime() - firstTimestampValue.getTime() - 1) / TIMESTAMP_PARTITION_RNAGE
    val startTimestamp = new Timestamp((page * TIMESTAMP_PARTITION_RNAGE) + firstTimestampValue.getTime())
    val stopTimestamp  = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }

  private[this] def olderPartitionKey(
    tableName: String,
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): String = {
    var startTimestamp: Timestamp = firstTimestampValue
    var stopTimestamp: Timestamp  = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    //val currentTimestamp: Timestamp = Timestamp.valueOf("2020-07-10 12:00:00")

    // TODO Refactor this function to remove while loop to calc partition key
    while (!(timestamp.getTime() >= startTimestamp.getTime() && timestamp.getTime() <= stopTimestamp.getTime())) {
      startTimestamp = stopTimestamp
      stopTimestamp = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (timestamp.getTime() < firstTimestampValue.getTime())
        throw new IllegalArgumentException("The timestamp over the first data timestamp")

      /*if (startTimestamp.getTime() > currentTimestamp.getTime() && stopTimestamp.getTime() > Timestamp
            .valueOf("2020-07-13 11:00:00")
            .getTime()) {
        throw new IllegalArgumentException("The timestamp over the current timestamp")
      }*/
    }
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }
}
