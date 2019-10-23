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

  override protected[datatype] def dataBaseProductName: String = "postgresql"

  override protected[datatype] def isIntTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_INT2 || typeName == TYPE_NAME_INT4

  override protected[datatype] def isLongTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_INT8

  override protected[datatype] def isBooleanTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BIT || typeName == TYPE_NAME_BOOL

  override protected[datatype] def isFloatTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_FLOAT4

  override protected[datatype] def isDoubeTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_FLOAT8

  override protected[datatype] def isBigDecimalTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_NUMERIC

  override protected[datatype] def isStringTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BPCHAR || typeName == TYPE_NAME_VARCHAR

  override protected[datatype] def isDateTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DATE

  override protected[datatype] def isTimestampTypeName(typeName: String): Boolean =
    typeName match {
      case TYPE_NAME_TIME | TYPE_NAME_TIMETZ | TYPE_NAME_TIMESTAMP | TYPE_NAME_TIMESTAMPTZ => true
      case _                                                                               => false
    }

  override protected[datatype] def isBytesTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BYTEA

  override protected[datatype] def isTimeTypeName(typeName: String): Boolean = false
}
