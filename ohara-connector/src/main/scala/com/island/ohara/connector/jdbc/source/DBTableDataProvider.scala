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

package com.island.ohara.connector.jdbc.source

import java.sql._
import java.util.Calendar

import com.island.ohara.client.configurator.v0.QueryApi.{RdbColumn, RdbTable}
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.util.{Releasable, ReleaseOnce}
import com.island.ohara.connector.jdbc.datatype.{RDBDataTypeConverter, RDBDataTypeConverterFactory}
import com.island.ohara.connector.jdbc.util.DateTimeUtils
import com.typesafe.scalalogging.Logger

/**
  * Connection to database and query data
  *
  */
class DBTableDataProvider(jdbcSourceConnectorConfig: JDBCSourceConnectorConfig) extends ReleaseOnce {
  private[this] lazy val logger = Logger(getClass.getName)

  private[this] val client: DatabaseClient = DatabaseClient.builder
    .url(jdbcSourceConnectorConfig.dbURL)
    .user(jdbcSourceConnectorConfig.dbUserName)
    .password(jdbcSourceConnectorConfig.dbPassword)
    .build

  private[this] val dbProduct: String = client.connection.getMetaData.getDatabaseProductName

  private[this] var queryFlag: Boolean = true

  private[this] var resultSet: ResultSet = _

  private[this] val rdbColumnInfo = columns(jdbcSourceConnectorConfig.dbTableName)

  private[source] def executeQuery(tableName: String,
                                   timeStampColumnName: String,
                                   tsOffset: Timestamp): QueryResultIterator = {
    if (queryFlag) {
      val sql =
        s"SELECT * FROM $tableName WHERE $timeStampColumnName > ? AND $timeStampColumnName < ? ORDER BY $timeStampColumnName"

      val connection: Connection = client.connection
      connection.setAutoCommit(false) //setAutoCommit must be set to false when setting the fetch size

      val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
      preparedStatement.setFetchSize(jdbcSourceConnectorConfig.jdbcFetchDataSize)

      val currentTimestamp: Timestamp = dbCurrentTime(DateTimeUtils.CALENDAR)
      preparedStatement.setTimestamp(1, tsOffset, DateTimeUtils.CALENDAR)
      preparedStatement.setTimestamp(2, currentTimestamp, DateTimeUtils.CALENDAR)

      logger.debug(s"Run executeQuery function. current time is ${currentTimestamp}")
      resultSet = preparedStatement.executeQuery()
      queryFlag = false
    }
    val rdbDataTypeConverter: RDBDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
    new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)
  }

  private[source] def releaseResultSet(queryFlag: Boolean): Unit = {
    logger.debug("close ResultSet ........")
    Releasable.close(resultSet.getStatement())
    Releasable.close(resultSet)
    resultSet = null
    // Use the JDBC fetchSize function, should setting setAutoCommit function to false.
    // Confirm this connection ResultSet to update, need to call connection commit function.
    // Release any database locks currently held by this Connection object
    this.client.connection.commit()
    this.queryFlag = queryFlag
  }

  private[source] def columns(tableName: String): Seq[RdbColumn] = {
    val rdbTables: Seq[RdbTable] = client.tableQuery.tableName(tableName).execute()
    rdbTables.head.columns
  }

  private[source] def isTableExists(tableName: String): Boolean =
    client.tableQuery.tableName(tableName).execute().nonEmpty

  private[source] def dbCurrentTime(cal: Calendar): Timestamp = {
    val query = dbProduct.toLowerCase match {
      case ORACLE_DB_NAME => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _              => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try if (rs.next()) rs.getTimestamp(1, cal)
      else
        throw new RuntimeException(
          s"Unable to get current time from DB using query $query on database $dbProduct"
        )
      finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  /**
    * Do what you want to do when calling closing.
    */
  override def doClose(): Unit = {
    Releasable.close(client)
  }
}
