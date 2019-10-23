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
import java.sql.{Date, ResultSet, Timestamp, Time}
import java.util.Optional

import com.island.ohara.client.configurator.v0.QueryApi
import com.island.ohara.connector.jdbc.util.DateTimeUtils

class MySQLDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_BIT = "BIT"
  private[this] val TYPE_NAME_TINYINT: String = "TINYINT"
  private[this] val TYPE_NAME_BOOL: String = "BOOL"
  private[this] val TYPE_NAME_BOOLEAN: String = "BOOLEAN"
  private[this] val TYPE_NAME_SMALLINT: String = "SMALLINT"
  private[this] val TYPE_NAME_MEDIUMINT: String = "MEDIUMINT"
  private[this] val TYPE_NAME_INT: String = "INT"
  private[this] val TYPE_NAME_INTEGER: String = "INTEGER"
  private[this] val TYPE_NAME_BIGINT: String = "BIGINT"
  private[this] val TYPE_NAME_FLOAT: String = "FLOAT"
  private[this] val TYPE_NAME_DOUBLE: String = "DOUBLE"
  private[this] val TYPE_NAME_DECIMAL: String = "DECIMAL"
  private[this] val TYPE_NAME_DATE: String = "DATE"
  private[this] val TYPE_NAME_DATETIME: String = "DATETIME"
  private[this] val TYPE_NAME_TIMESTAMP: String = "TIMESTAMP"
  private[this] val TYPE_NAME_TIME: String = "TIME"
  private[this] val TYPE_NAME_CHAR: String = "CHAR"
  private[this] val TYPE_NAME_VARCHAR: String = "VARCHAR"
  private[this] val TYPE_NAME_BINARY: String = "BINARY"
  private[this] val TYPE_NAME_VARBINARY: String = "VARBINARY"
  private[this] val TYPE_NAME_TINYBLOB: String = "TINYBLOB"
  private[this] val TYPE_NAME_BLOB: String = "BLOB"
  private[this] val TYPE_NAME_TINYTEXT: String = "TINYTEXT"
  private[this] val TYPE_NAME_TEXT: String = "TEXT"
  private[this] val TYPE_NAME_MEDIUMBLOB: String = "MEDIUMBLOB"
  private[this] val TYPE_NAME_MEDIUMTEXT: String = "MEDIUMTEXT"
  private[this] val TYPE_NAME_LONGBLOB: String = "LONGBLOB"
  private[this] val TYPE_NAME_LONGTEXT: String = "LONGTEXT"
  private[this] val TYPE_NAME_ENUM: String = "ENUM"
  private[this] val TYPE_NAME_SET: String = "SET"

  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): Any = {
    val columnName = column.name
    val typeName = column.dataType

    typeName.toUpperCase match {
      case TYPE_NAME_BIT | TYPE_NAME_BOOL | TYPE_NAME_BOOLEAN =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))

      case TYPE_NAME_TINYINT =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case TYPE_NAME_SMALLINT | TYPE_NAME_MEDIUMINT | TYPE_NAME_INT | TYPE_NAME_INTEGER =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case TYPE_NAME_BIGINT =>
        java.lang.Long.valueOf(resultSet.getLong(columnName))

      case TYPE_NAME_FLOAT =>
        java.lang.Float.valueOf(resultSet.getFloat(columnName))

      case TYPE_NAME_DOUBLE =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))

      case TYPE_NAME_DECIMAL =>
        Optional.ofNullable(resultSet.getBigDecimal(columnName)).orElseGet(() => new java.math.BigDecimal(0L))

      case TYPE_NAME_DATE =>
        Optional.ofNullable(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Date(0))

      case TYPE_NAME_TIME =>
        Optional.ofNullable(resultSet.getTime(columnName, DateTimeUtils.CALENDAR)).orElseGet(() => new Time(0))

      case TYPE_NAME_DATETIME | TYPE_NAME_TIMESTAMP =>
        Optional
          .ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR))
          .orElseGet(() => new Timestamp(0))

      case TYPE_NAME_CHAR | TYPE_NAME_VARCHAR | TYPE_NAME_TINYTEXT | TYPE_NAME_TEXT | TYPE_NAME_MEDIUMTEXT |
          TYPE_NAME_LONGTEXT | TYPE_NAME_ENUM | TYPE_NAME_SET =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case TYPE_NAME_BINARY | TYPE_NAME_VARBINARY | TYPE_NAME_TINYBLOB | TYPE_NAME_BLOB | TYPE_NAME_MEDIUMBLOB |
          TYPE_NAME_LONGBLOB => {
        Optional.ofNullable(resultSet.getBytes(columnName)).orElseGet(() => Array())

      }
      case _ =>
        throw new RuntimeException(
          s"JDBC Source Connector not support ${typeName} data type in ${columnName} column for MySQL database.")
    }
  }
}
