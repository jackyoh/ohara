package oharastream.ohara.connector.jdbc.source

import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.{After, Test}

class TestJDBCSourceConnectorExactlyOnce extends With3Brokers3Workers {
  private[this] val durationTime: Long  = 10000
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "column0"
  //private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def createTable(): Unit = {
    val columns = Seq(RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)) ++
      Seq(RdbColumn("column1", "INT", true)) ++
      (2 to 5).map { index =>
        RdbColumn(s"column${index}", "VARCHAR(45)", false)
      }
    client.createTable(tableName, columns)
  }

  private[this] def inputDataThread(): Releasable = {
    val pool = Executors.newSingleThreadExecutor()
    pool.execute(() => {
      val startTime = CommonUtils.current()
      while ((CommonUtils.current() - startTime) <= durationTime) {
        println("Insert data to table")
      }
    })
    () => {
      pool.shutdown()
      pool.awaitTermination(300, TimeUnit.SECONDS)
    }
  }

  @Test
  def test(): Unit = {
    createTable()
    inputDataThread()
  }

  @After
  def after(): Unit = {
    Releasable.close(inputDataThread)
    Releasable.close(client)
    Releasable.close(db)
  }
}
