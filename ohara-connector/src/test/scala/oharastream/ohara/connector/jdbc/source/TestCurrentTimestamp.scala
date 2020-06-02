package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.Test

class TestCurrentTimestamp extends OharaTest {
  @Test
  def test(): Unit = {
    val currentTime = new Timestamp(CommonUtils.current())
    println(currentTime)
  }
}
