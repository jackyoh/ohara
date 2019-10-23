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
import java.sql.{ResultSet, Timestamp}
import java.util.Optional

import com.island.ohara.client.configurator.v0.QueryApi
import com.island.ohara.connector.jdbc.util.DateTimeUtils

class OracleDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_CHAR: String = "CHAR"
  private[this] val TYPE_NAME_CHARACTER: String = "CHARACTER"
  private[this] val TYPE_NAME_LONG: String = "LONG"
  private[this] val TYPE_NAME_VARCHAR: String = "VARCHAR"
  private[this] val TYPE_NAME_VARCHAR2: String = "VARCHAR2"
  private[this] val TYPE_NAME_NCHAR: String = "NCHAR"
  private[this] val TYPE_NAME_NVARCHAR2: String = "NVARCHAR2"
  private[this] val TYPE_NAME_RAW: String = "RAW"
  private[this] val TYPE_NAME_LONGRAW: String = "LONG RAW"
  private[this] val TYPE_NAME_INT: String = "INT"
  private[this] val TYPE_NAME_INTEGER: String = "INTEGER"
  private[this] val TYPE_NAME_DEC: String = "DEC"
  private[this] val TYPE_NAME_DECIMAL: String = "DECIMAL"
  private[this] val TYPE_NAME_NUMBER: String = "NUMBER"
  private[this] val TYPE_NAME_NUMERIC: String = "NUMERIC"
  private[this] val TYPE_NAME_DOUBLE_PRECISION: String = "DOUBLE PRECISION"
  private[this] val TYPE_NAME_FLOAT: String = "FLOAT"
  private[this] val TYPE_NAME_SMALLINT: String = "SMALLINT"
  private[this] val TYPE_NAME_REAL: String = "REAL"
  private[this] val TYPE_NAME_DATE: String = "DATE"
  private[this] val TYPE_NAME_TIMESTAMP: String = "TIMESTAMP"
  private[this] val TYPE_NAME_INTERVAL_YEAR_TO_MONTH: String = "INTERVAL YEAR TO MONTH"
  private[this] val TYPE_NAME_INTERVAL_DAY_TO_SECOND: String = "INTERVAL DAY TO SECOND"

  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): Any = {
    val columnName = column.name
    val typeName = column.dataType

    typeName.toUpperCase match {
      case TYPE_NAME_CHAR | TYPE_NAME_CHARACTER | TYPE_NAME_LONG | TYPE_NAME_VARCHAR | TYPE_NAME_VARCHAR2 |
          TYPE_NAME_NCHAR | TYPE_NAME_NVARCHAR2 =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case TYPE_NAME_RAW | TYPE_NAME_LONGRAW =>
        Optional.ofNullable(resultSet.getBytes(columnName)).orElseGet(() => Array())

      case TYPE_NAME_INT | TYPE_NAME_INTEGER | TYPE_NAME_SMALLINT =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))

      case TYPE_NAME_DEC | TYPE_NAME_DECIMAL | TYPE_NAME_NUMBER | TYPE_NAME_NUMERIC =>
        Optional.ofNullable(resultSet.getBigDecimal(columnName)).orElseGet(() => new java.math.BigDecimal(0L))

      case TYPE_NAME_DOUBLE_PRECISION | TYPE_NAME_FLOAT =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))

      case TYPE_NAME_REAL =>
        java.lang.Float.valueOf(resultSet.getFloat(columnName))

      case TYPE_NAME_DATE | TYPE_NAME_TIMESTAMP =>
        Optional
          .ofNullable(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR))
          .orElseGet(() => new Timestamp(0))

      case TYPE_NAME_INTERVAL_YEAR_TO_MONTH | TYPE_NAME_INTERVAL_DAY_TO_SECOND =>
        Optional.ofNullable(resultSet.getString(columnName)).orElseGet(() => "null")

      case _ =>
        throw new RuntimeException(
          s"JDBC Source Connector not support ${typeName} data type in ${columnName} column for Oracle database.")
    }
  }
}
