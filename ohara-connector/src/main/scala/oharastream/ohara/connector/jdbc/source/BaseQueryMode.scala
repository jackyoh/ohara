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

package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.RowSourceRecord

trait BaseQueryMode {
  /**
    * Query table data from the database
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return
    */
  protected[source] def queryData(
    key: String,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord]

  /**
    * Confirm data write to the topic
    * The start timestamp and stop timestamp range can't change.
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  protected[source] def isCompleted(key: String, startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean

  private[source] def row(schema: Seq[Column], columns: Seq[ColumnInfo[_]]): Row =
    Row.of(
      schema
        .sortBy(_.order)
        .map(s => (s, values(s.name, columns)))
        .map {
          case (s, value) =>
            Cell.of(
              s.newName,
              s.dataType match {
                case DataType.BOOLEAN                 => value.asInstanceOf[Boolean]
                case DataType.SHORT                   => value.asInstanceOf[Short]
                case DataType.INT                     => value.asInstanceOf[Int]
                case DataType.LONG                    => value.asInstanceOf[Long]
                case DataType.FLOAT                   => value.asInstanceOf[Float]
                case DataType.DOUBLE                  => value.asInstanceOf[Double]
                case DataType.BYTE                    => value.asInstanceOf[Byte]
                case DataType.STRING                  => value.asInstanceOf[String]
                case DataType.BYTES | DataType.OBJECT => value
                case _                                => throw new IllegalArgumentException("Unsupported type...")
              }
            )
        }: _*
    )

  private[this] def values(schemaColumnName: String, dbColumnInfo: Seq[ColumnInfo[_]]): Any =
    dbColumnInfo
      .find(_.columnName == schemaColumnName)
      .map(_.value)
      .getOrElse(throw new RuntimeException(s"Database table not have the $schemaColumnName column"))

  private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }
}
