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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class Timer {
  private[this] val counter: AtomicInteger = new AtomicInteger(0)
  private[this] val BASE_VALUE: Int        = 100

  /**
    * Is over the wait time, default value is 1 seconds
    * @return true or false
    */
  def overWait(): Boolean = BASE_VALUE * Math.pow(2, counter.getAndAdd(1)) > 1000 // 1000 is 1 seconds

  /**
    * Sleep time is 100 * 2 ^ Wait counter. Unit is MILLISECONDS.
    */
  def sleep(): Unit = TimeUnit.MILLISECONDS.sleep(BASE_VALUE * Math.pow(2, counter.intValue()).toInt)
}
