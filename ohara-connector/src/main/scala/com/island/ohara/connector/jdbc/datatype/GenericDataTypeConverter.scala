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

import com.island.ohara.client.configurator.v0.QueryApi
import com.island.ohara.connector.jdbc.util.DateTimeUtils

// TODO Coming soon
class GenericDataTypeConverter extends RDBDataTypeConverter {

  private[this] val TYPE_NAME_BOOLEAN: String = "BOOLEAN"
  private[this] val TYPE_NAME_BIT: String = "BIT"
  private[this] val TYPE_NAME_INTEGER: String = "INT"
  private[this] val TYPE_NAME_BIGINT: String = "BIGINT"
  private[this] val TYPE_NAME_FLOAT: String = "FLOAT"
  private[this] val TYPE_NAME_DOUBLE: String = "DOUBLE"
  private[this] val TYPE_NAME_CHAR: String = "CHAR"
  private[this] val TYPE_NAME_VARCHAR: String = "VARCHAR"
  private[this] val TYPE_NAME_LONGVARCHAR: String = "LONGVARCHAR"
  private[this] val TYPE_NAME_TIMESTAMP: String = "TIMESTAMP"
  private[this] val TYPE_NAME_DATE: String = "DATE"
  private[this] val TYPE_NAME_TIME: String = "TIME"
  private[this] val TYPE_NAME_TIMESTAMP6: String = "TIMESTAMP(6)"
  private[this] val TYPE_NAME_VARCHAR2: String = "VARCHAR2"
  private[this] val TYPE_NAME_NUMBER: String = "NUMBER"

  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): Any = {
    val columnName = column.name
    val typeName = column.dataType

    typeName.toUpperCase match {
      case TYPE_NAME_BOOLEAN =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))

      case TYPE_NAME_BIT =>
        java.lang.Byte.valueOf(resultSet.getByte(columnName))

      case TYPE_NAME_INTEGER | TYPE_NAME_NUMBER =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case TYPE_NAME_BIGINT =>
        java.lang.Long.valueOf(resultSet.getLong(columnName))

      case TYPE_NAME_FLOAT =>
        java.lang.Float.valueOf(resultSet.getFloat(columnName))

      case TYPE_NAME_DOUBLE =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))

      case TYPE_NAME_CHAR | TYPE_NAME_VARCHAR | TYPE_NAME_LONGVARCHAR | TYPE_NAME_VARCHAR2 =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case TYPE_NAME_TIMESTAMP | TYPE_NAME_TIMESTAMP6 =>
        Optional
          .ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR))
          .orElseGet(() => new Timestamp(0))

      case TYPE_NAME_DATE =>
        Optional.ofNullable(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Date(0))

      case TYPE_NAME_TIME =>
        Optional.ofNullable(resultSet.getTime(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Time(0))

      case _ =>
        throw new RuntimeException(s"Data type '$typeName' not support on column '$columnName'.")
    }
  }

}
