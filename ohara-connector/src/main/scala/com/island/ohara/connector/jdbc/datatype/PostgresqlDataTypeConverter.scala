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
import java.sql.{ResultSet, Date, Time, Timestamp}

import com.island.ohara.client.configurator.v0.QueryApi
import java.util.Optional

import com.island.ohara.connector.jdbc.util.DateTimeUtils

class PostgresqlDataTypeConverter extends RDBDataTypeConverter {
  private[this] val RDB_TYPE_BOOLEAN = "BOOLEAN"
  private[this] val RDB_TYPE_BIT: String = "BIT"
  private[this] val RDB_TYPE_INTEGER: String = "INT"
  private[this] val RDB_TYPE_INTEGER_2: String = "INT4"
  private[this] val RDB_TYPE_BIGINT: String = "BIGINT"
  private[this] val RDB_TYPE_FLOAT: String = "FLOAT"
  private[this] val RDB_TYPE_FLOAT_2: String = "FLOAT8"
  private[this] val RDB_TYPE_DOUBLE: String = "DOUBLE"
  private[this] val RDB_TYPE_CHAR: String = "CHAR"
  private[this] val RDB_TYPE_VARCHAR: String = "VARCHAR"
  private[this] val RDB_TYPE_LONGVARCHAR: String = "LONGVARCHAR"
  private[this] val RDB_TYPE_TIMESTAMP: String = "TIMESTAMP"
  private[this] val RDB_TYPE_DATE: String = "DATE"
  private[this] val RDB_TYPE_TIME: String = "TIME"

  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): AnyRef = {
    val columnName = column.name
    val typeName = column.dataType

    typeName.toUpperCase match {
      case RDB_TYPE_BOOLEAN =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))

      case RDB_TYPE_BIT =>
        java.lang.Byte.valueOf(resultSet.getByte(columnName))

      case RDB_TYPE_INTEGER | RDB_TYPE_INTEGER_2 =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case RDB_TYPE_BIGINT =>
        java.lang.Long.valueOf(resultSet.getLong(columnName))

      case RDB_TYPE_FLOAT | RDB_TYPE_FLOAT_2 => //TODO Refactor DB datatype for JDBC Source Connector
        java.lang.Float.valueOf(resultSet.getFloat(columnName))

      case RDB_TYPE_DOUBLE =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))

      case RDB_TYPE_CHAR | RDB_TYPE_VARCHAR | RDB_TYPE_LONGVARCHAR =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case RDB_TYPE_TIMESTAMP =>
        Optional
          .ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR))
          .orElseGet(() => new Timestamp(0))

      case RDB_TYPE_DATE =>
        Optional.ofNullable(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Date(0))

      case RDB_TYPE_TIME =>
        Optional.ofNullable(resultSet.getTime(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Time(0))
      case _ =>
        throw new RuntimeException(s"Postgresql not support ${typeName} data type in ${columnName} column.")
    }
  }
}
