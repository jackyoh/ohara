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
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.RowSourceRecord

trait BaseQueryHandler extends Releasable {
  private[source] def client: DatabaseClient

  private[source] val dbProduct: String = client.connection.getMetaData.getDatabaseProductName

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
  protected[source] def completed(key: String, startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean

  /**
    * Query first row from the database table
    * @param timestampColumnName timestamp column name
    * @return timestamp
    */
  private[source] def tableFirstTimestampValue(tableName: String, timestampColumnName: String): Timestamp = {
    val sql = dbProduct.toUpperCase match {
      case ORACLE.name =>
        s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName FETCH FIRST 1 ROWS ONLY"
      case _ =>
        s"SELECT $timestampColumnName FROM $tableName ORDER BY $timestampColumnName LIMIT 1"
    }

    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getTimestamp(timestampColumnName)
        else new Timestamp(CommonUtils.current())
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  /**
    * Query current timestamp from the database
    * @return timestamp
    */
  private[source] def current(): Timestamp = {
    val query = dbProduct.toUpperCase match {
      case ORACLE.name => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _           => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try {
        if (rs.next()) rs.getTimestamp(1) else new Timestamp(0)
      } finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }

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

  override def close(): Unit = Releasable.close(client)
}
