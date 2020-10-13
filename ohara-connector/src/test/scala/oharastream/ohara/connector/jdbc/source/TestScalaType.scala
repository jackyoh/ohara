package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class TestScalaType extends OharaTest {
  private[this] val db        = Database.local()
  private[this] val client    = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"

  @BeforeEach
  def setup(): Unit = {
    val schema = Seq(
      RdbColumn("column1", "varchar(45)", true),
      RdbColumn("column2", "int", false),
      RdbColumn("column3", "longblob", false)
    )
    client.createTable(tableName, schema)
    val preparedStatement =
      client.connection.prepareStatement(s"INSERT INTO $tableName(column1, column2, column3) VALUES(?, ?, ?)")
    try {
      preparedStatement.setString(1, "aaa")
      preparedStatement.setInt(2, 100)
      preparedStatement.setBytes(3, "test".getBytes())
      preparedStatement.executeUpdate()
    } finally Releasable.close(preparedStatement)
  }

  @Test
  def test(): Unit = {
    val statement = client.connection.createStatement()
    try {
      val resultSet = statement.executeQuery(s"SELECT * FROM $tableName")
      println("======================================")
      Iterator.continually(resultSet).takeWhile(_.next()).foreach { result =>
        println(result.getString(1).getClass.getName)
        println(result.getInt(2).isInstanceOf[Int])
        println(result.getBigDecimal(2).getClass.getName)
      //result.getBytes(3).isInstanceOf[Array[Byte]]
      }
      println("======================================")
    } finally Releasable.close(statement)
  }

  @AfterEach
  def after(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
