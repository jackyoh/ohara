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

import java.sql.{Date, ResultSet, Time, Timestamp}
import java.util.Optional

import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.connector.jdbc.util.DateTimeUtils

trait RDBDataTypeConverter {

  /**
    * Converter result data type to Java object
    * @param resultSet
    * @param column
    * @return data type object
    */
  def converterValue(resultSet: ResultSet, column: RdbColumn): Any = {
    val columnName = column.name
    val typeName = column.dataType.toUpperCase

    if (isIntTypeName(typeName))
      java.lang.Integer.valueOf(resultSet.getInt(columnName))
    else if (isLongTypeName(typeName))
      java.lang.Long.valueOf(resultSet.getLong(columnName))
    else if (isBooleanTypeName(typeName))
      java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))
    else if (isFloatTypeName(typeName))
      java.lang.Float.valueOf(resultSet.getFloat(columnName))
    else if (isDoubeTypeName(typeName))
      java.lang.Double.valueOf(resultSet.getDouble(columnName))
    else if (isBigDecimalTypeName(typeName))
      Optional.ofNullable(resultSet.getBigDecimal(columnName)).orElseGet(() => new java.math.BigDecimal(0L))
    else if (isStringTypeName(typeName))
      Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")
    else if (isDateTypeName(typeName))
      Optional.ofNullable(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Date(0))
    else if (isTimeTypeName(typeName))
      Optional.ofNullable(resultSet.getTime(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Time(0))
    else if (isTimestampTypeName(typeName))
      Optional.ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Timestamp(0))
    else if (isBytesTypeName(typeName))
      Optional.ofNullable(resultSet.getBytes(columnName)).orElseGet(() => Array())
    else
      throw new UnsupportedOperationException(
        s"JDBC Source Connector not support ${typeName} data type in ${columnName} column for ${dataBaseProductName} implement.")
  }

  protected[datatype] def dataBaseProductName: String
  protected[datatype] def isIntTypeName(typeName: String): Boolean
  protected[datatype] def isLongTypeName(typeName: String): Boolean
  protected[datatype] def isBooleanTypeName(typeName: String): Boolean
  protected[datatype] def isFloatTypeName(typeName: String): Boolean
  protected[datatype] def isDoubeTypeName(typeName: String): Boolean
  protected[datatype] def isBigDecimalTypeName(typeName: String): Boolean
  protected[datatype] def isStringTypeName(typeName: String): Boolean
  protected[datatype] def isDateTypeName(typeName: String): Boolean
  protected[datatype] def isTimeTypeName(typeName: String): Boolean
  protected[datatype] def isTimestampTypeName(typeName: String): Boolean
  protected[datatype] def isBytesTypeName(typeName: String): Boolean
}
