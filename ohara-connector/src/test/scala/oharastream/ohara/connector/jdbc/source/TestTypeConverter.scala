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

import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test

class TestTypeConverter extends OharaTest {
  @Test
  def test(): Unit = {
    val a: Any = java.lang.Integer.valueOf(100)
    val c      = a.asInstanceOf[java.lang.Integer].toInt.asInstanceOf[Byte]
    //val c = a.toInt.asInstanceOf[Byte]
    println(c)
  }
}
