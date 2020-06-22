package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.service.Database
import org.junit.{Before, Test}

class TestIncrementField extends OharaTest {
  private[this] val db: Database = Database.local()
  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"

  @Before
  def setup(): Unit = {
    val columns = Seq(
      RdbColumn("c0", "MEDIUMINT NOT NULL AUTO_INCREMENT", true),
      RdbColumn("c1", "TIMESTAMP", false),
      RdbColumn("c2", "VARCHAR(45)", false)
    )
    client.createTable(tableName, columns)

    val sql               = s"INSERT INTO $tableName(c1, c2) VALUES(?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    (1 to 2000).foreach { index =>
      preparedStatement.setTimestamp(1, new Timestamp(CommonUtils.current()))
      preparedStatement.setString(2, s"value-$index")
      preparedStatement.executeUpdate()
    }
  }

  @Test
  def test(): Unit = {
    val preparedStatement = client.connection.prepareStatement(s"SELECT * FROM $tableName ORDER BY c1, c0")
    try {
      val resultSet = preparedStatement.executeQuery()
      while (resultSet.next()) {
        val c0 = resultSet.getInt("c0")
        val c1 = resultSet.getTimestamp("c1")
        val c2 = resultSet.getString("c2")
        println(s"$c0    $c1    $c2")
      }
    } finally Releasable.close(preparedStatement)
  }
}
