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

package com.island.ohara.connector.jdbc.datatype

import java.sql.ResultSet
import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.common.rule.OharaTest
import org.junit.Test
import org.mockito.Mockito.when
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

class TestOracleDataTypeConverter extends OharaTest with Matchers with MockitoSugar {

  @Test
  def testConverterCharValue(): Unit = {
    val resultSet: ResultSet = mock[ResultSet]
    when(resultSet.getString("column1")).thenReturn("value1")
    val column = RdbColumn("column1", "CHAR", false)
    val oracleDataTypeConverter = new OracleDataTypeConverter()
    val result = oracleDataTypeConverter.converterValue(resultSet, column)
    result shouldBe "value1"
    result.isInstanceOf[String] shouldBe true
  }

  @Test
  def testConverterRawValue(): Unit = {
    val resultSet: ResultSet = mock[ResultSet]
    when(resultSet.getBytes("column1")).thenReturn("aaaa".getBytes)
    val column = RdbColumn("column1", "RAW", false)
    val oracleDataTypeConverter = new OracleDataTypeConverter()
    val result = oracleDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Array[Byte]] shouldBe true
    new String(result.asInstanceOf[Array[Byte]]) shouldBe "aaaa"
  }

  @Test
  def testConverterRawNullValue(): Unit = {
    val resultSet: ResultSet = mock[ResultSet]
    when(resultSet.getBytes("column1")).thenReturn(null)
    val column = RdbColumn("column1", "RAW", false)
    val oracleDataTypeConverter = new OracleDataTypeConverter()
    val result = oracleDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Array[Byte]] shouldBe true
    result.asInstanceOf[Array[Byte]].length shouldBe 0
  }

  @Test
  def testConverterSmallIntValue(): Unit = {
    val resultSet: ResultSet = mock[ResultSet]
    when(resultSet.getInt("column1")).thenReturn(111)
    val column = RdbColumn("column1", "INT", false)
    val oracleDataTypeConverter = new OracleDataTypeConverter()
    val result = oracleDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Integer] shouldBe true
    result.asInstanceOf[Integer] shouldBe 111
  }
}
