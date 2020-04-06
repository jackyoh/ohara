package oharastream.ohara.connector.jdbc.source

import java.sql.{ResultSet, Statement, Timestamp}
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.v0.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Row}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database

import collection.JavaConverters._
import org.junit.{After, Test}

class TestJDBCSourceConnectorExactlyOnce extends With3Brokers3Workers {
  private[this] val durationTime: Long  = 20000
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "c0"
  private[this] val columnSize          = 3
  private[this] val columns = Seq(RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)) ++
    (1 to columnSize).map { index =>
      if (index == 1) RdbColumn(s"c${index}", "VARCHAR(45)", true)
      else RdbColumn(s"c${index}", "VARCHAR(45)", false)
    }
  private[this] val tableTotalNumber = new LongAdder()
  //private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def createTable(): Unit = {
    client.createTable(tableName, columns)
  }

  private[this] def inputDataThread(): Releasable = {
    val pool = Executors.newSingleThreadExecutor()
    pool.execute(() => {
      val startTime: Long = CommonUtils.current()

      val sql                      = s"INSERT INTO $tableName VALUES (${columns.map(_ => "?").mkString(",")})"
      val preparedStatement        = client.connection.prepareStatement(sql)
      val numberOfRowsToFlush: Int = 50

      while ((CommonUtils.current() - startTime) <= durationTime) {
        // 432000000 is 5 days ago
        val timestampData = new Timestamp(CommonUtils.current() - 432000000)

        (0 to numberOfRowsToFlush).map(_ => rowData()).foreach { row =>
          preparedStatement.setTimestamp(1, timestampData)
          row.asScala.zipWithIndex.foreach {
            case (result, index) => {
              preparedStatement.setString(index + 2, result.value().toString)
            }
          }
          preparedStatement.addBatch()
        }
        tableTotalNumber.add(preparedStatement.executeUpdate())
      }
      preparedStatement.close()
    })
    () => {
      pool.shutdown()
      pool.awaitTermination(300, TimeUnit.SECONDS)
    }
  }

  private[this] def rowData(): Row = {
    Row.of(
      (1 to columnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  @Test
  def test(): Unit = {
    createTable()
    inputDataThread()
    Thread.sleep(durationTime)
    val statement: Statement = client.connection.createStatement()
    val resultSet: ResultSet = statement.executeQuery(s"SELECT count(*) FROM $tableName order by c0")
    while (resultSet.next()) {
      println(s"total: ${resultSet.getString(1)}")
    }
    println(s"Table total number is ${tableTotalNumber}")
  }

  @After
  def after(): Unit = {
    Releasable.close(inputDataThread)
    Releasable.close(client)
    Releasable.close(db)
  }
}
