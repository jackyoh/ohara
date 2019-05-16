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

package com.island.ohara.configurator.store

import com.island.ohara.common.rule.SmallTest
import org.junit.Test
import org.scalatest.Matchers

class TestStoreBuilder extends SmallTest with Matchers {

  @Test
  def testNullKeySerializer(): Unit =
    an[NullPointerException] should be thrownBy Store.builder[String, String]().keySerializer(null)

  @Test
  def testNullValueSerializer(): Unit =
    an[NullPointerException] should be thrownBy Store.builder[String, String]().valueSerializer(null)

  @Test
  def testNullFolder(): Unit =
    an[NullPointerException] should be thrownBy Store.builder[String, String]().persistentFolder(null)

  @Test
  def testEmptyFolder(): Unit =
    an[IllegalArgumentException] should be thrownBy Store.builder[String, String]().persistentFolder("")
}
