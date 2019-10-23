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
import java.sql.{ResultSet, Date, Timestamp}

import com.island.ohara.client.configurator.v0.QueryApi
import java.util.Optional

import com.island.ohara.connector.jdbc.util.DateTimeUtils

class PostgresqlDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_INT2 = "INT2"
  private[this] val TYPE_NAME_INT4 = "INT4"
  private[this] val TYPE_NAME_INT8 = "INT8"
  private[this] val TYPE_NAME_BIT = "BIT"
  private[this] val TYPE_NAME_FLOAT4 = "FLOAT4"
  private[this] val TYPE_NAME_FLOAT8 = "FLOAT8"
  private[this] val TYPE_NAME_NUMERIC = "NUMERIC"
  private[this] val TYPE_NAME_BPCHAR = "BPCHAR"
  private[this] val TYPE_NAME_VARCHAR = "VARCHAR"
  private[this] val TYPE_NAME_DATE = "DATE"
  private[this] val TYPE_NAME_TIME = "TIME"
  private[this] val TYPE_NAME_TIMETZ = "TIMETZ"
  private[this] val TYPE_NAME_TIMESTAMP = "TIMESTAMP"
  private[this] val TYPE_NAME_TIMESTAMPTZ = "TIMESTAMP"
  private[this] val TYPE_NAME_BYTEA = "BYTEA"
  private[this] val TYPE_NAME_BOOL = "BOOL"

  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): Any = {
    val columnName = column.name
    val typeName = column.dataType

    typeName.toUpperCase match {
      case TYPE_NAME_INT2 | TYPE_NAME_INT4 =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case TYPE_NAME_INT8 =>
        java.lang.Long.valueOf(resultSet.getLong(columnName))

      case TYPE_NAME_BIT =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))

      case TYPE_NAME_FLOAT4 =>
        java.lang.Float.valueOf(resultSet.getFloat(columnName))

      case TYPE_NAME_FLOAT8 =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))

      case TYPE_NAME_NUMERIC =>
        Optional.ofNullable(resultSet.getBigDecimal(columnName)).orElseGet(() => new java.math.BigDecimal(0L))

      case TYPE_NAME_BPCHAR | TYPE_NAME_VARCHAR =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case TYPE_NAME_DATE =>
        Optional.ofNullable(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Date(0))

      case TYPE_NAME_TIME | TYPE_NAME_TIMETZ | TYPE_NAME_TIMESTAMP | TYPE_NAME_TIMESTAMPTZ =>
        Optional
          .ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR))
          .orElseGet(() => new Timestamp(0))

      case TYPE_NAME_BYTEA =>
        Optional.ofNullable(resultSet.getBytes(columnName)).orElseGet(() => Array())

      case TYPE_NAME_BOOL =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))

      case _ =>
        throw new RuntimeException(
          s"JDBC Source Connector not support ${typeName} data type in ${columnName} column for postgresql database.")
    }
  }
}
