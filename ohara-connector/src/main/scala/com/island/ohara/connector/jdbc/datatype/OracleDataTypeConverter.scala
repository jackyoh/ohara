package com.island.ohara.connector.jdbc.datatype
import java.sql.ResultSet

import com.island.ohara.client.configurator.v0.QueryApi

class OracleDataTypeConverter extends RDBDataTypeConverter {
  override def converterValue(resultSet: ResultSet, column: QueryApi.RdbColumn): AnyRef = {
    None //TODO Coming soon
  }
}
