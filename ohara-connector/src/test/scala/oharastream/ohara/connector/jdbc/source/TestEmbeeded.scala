package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.testing.service.Database
import org.junit.Test

class TestEmbeeded extends OharaTest {
  private[this] val db        = Database.local()
  private[this] val client    = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"

  @Test
  def test(): Unit = {
    val column1 = RdbColumn("column1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("column2", "varchar(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
  }
}
