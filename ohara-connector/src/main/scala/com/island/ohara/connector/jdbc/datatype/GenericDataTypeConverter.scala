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

  override protected[datatype] def dataBaseProductName: String = "generic"

  override protected[datatype] def isIntTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_INTEGER || typeName == TYPE_NAME_NUMBER

  override protected[datatype] def isLongTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BIGINT

  override protected[datatype] def isBooleanTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BOOLEAN

  override protected[datatype] def isFloatTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_FLOAT

  override protected[datatype] def isDoubeTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DOUBLE

  override protected[datatype] def isBigDecimalTypeName(typeName: String): Boolean = false

  override protected[datatype] def isStringTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_CHAR || typeName == TYPE_NAME_VARCHAR || typeName == TYPE_NAME_LONGVARCHAR || typeName == TYPE_NAME_VARCHAR2

  override protected[datatype] def isDateTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DATE

  override protected[datatype] def isTimeTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_TIME

  override protected[datatype] def isTimestampTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_TIMESTAMP || typeName == TYPE_NAME_TIMESTAMP6

  override protected[datatype] def isBytesTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BIT
}
