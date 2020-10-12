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

package oharastream.ohara.common.util;

import java.util.concurrent.TimeUnit;

public class Timer {
  private static final long SLEEP_TIMESTAMP_INIT_VALUE = 100;
  private static final int SLEEP_TIMEOUT = 1000; // 1000 is 1 seconds
  private long sleepTimestamp = SLEEP_TIMESTAMP_INIT_VALUE;

  /**
   * It should be updated to an new value (multiplied by 2) after sleep.
   *
   * @return boolean value
   */
  public boolean timeToSleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(sleepTimestamp);
      sleepTimestamp = sleepTimestamp * 2;
      if (sleepTimestamp >= SLEEP_TIMEOUT) { // Default is 1 seconds
        sleepTimestamp = SLEEP_TIMESTAMP_INIT_VALUE;
        return false;
      } else return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
