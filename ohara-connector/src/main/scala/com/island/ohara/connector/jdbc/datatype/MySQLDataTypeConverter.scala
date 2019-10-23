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

  override protected[datatype] def dataBaseProductName: String = "mysql"

  override protected[datatype] def isIntTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_TINYINT || typeName == TYPE_NAME_SMALLINT || typeName == TYPE_NAME_MEDIUMINT ||
      typeName == TYPE_NAME_INT || typeName == TYPE_NAME_INTEGER

  override protected[datatype] def isLongTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BIGINT

  override protected[datatype] def isBooleanTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_BIT | typeName == TYPE_NAME_BOOL | typeName == TYPE_NAME_BOOLEAN

  override protected[datatype] def isFloatTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_FLOAT

  override protected[datatype] def isDoubeTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DOUBLE

  override protected[datatype] def isBigDecimalTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DECIMAL

  override protected[datatype] def isStringTypeName(typeName: String): Boolean =
    typeName match {
      case TYPE_NAME_CHAR | TYPE_NAME_VARCHAR | TYPE_NAME_TINYTEXT | TYPE_NAME_TEXT | TYPE_NAME_MEDIUMTEXT |
          TYPE_NAME_LONGTEXT | TYPE_NAME_ENUM | TYPE_NAME_SET =>
        true
      case _ => false
    }

  override protected[datatype] def isDateTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DATE

  override protected[datatype] def isTimestampTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_DATETIME | typeName == TYPE_NAME_TIMESTAMP

  override protected[datatype] def isBytesTypeName(typeName: String): Boolean =
    typeName match {
      case TYPE_NAME_BINARY | TYPE_NAME_VARBINARY | TYPE_NAME_TINYBLOB | TYPE_NAME_BLOB | TYPE_NAME_MEDIUMBLOB |
          TYPE_NAME_LONGBLOB =>
        true
      case _ => false
    }

  override protected[datatype] def isTimeTypeName(typeName: String): Boolean =
    typeName == TYPE_NAME_TIME
}
