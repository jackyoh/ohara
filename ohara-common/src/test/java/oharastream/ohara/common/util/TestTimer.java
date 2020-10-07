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

import java.util.stream.IntStream;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTimer extends OharaTest {

  @Test
  public void testOverWait() {
    Timer timer = new Timer();
    IntStream.range(1, 3).forEach(i -> Assertions.assertFalse(timer.overWait()));
    Assertions.assertTrue(timer.overWait());
  }

  @Test
  public void testSleep() {
    Timer timer = new Timer();
    long startTimestamp = CommonUtils.current();
    IntStream.range(0, 4).forEach(i -> timer.overWait());
    Assertions.assertTrue(CommonUtils.current() - startTimestamp > 1000);
  }

  @Test
  public void testLoop() {
    int count = 0;
    Timer timer = new Timer();
    do {
      count++;
      timer.sleep();
    } while (!timer.overWait());
    Assertions.assertTrue(count >= 4);
  }
}
