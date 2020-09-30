package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test

class TestTypeConverter extends OharaTest {

  @Test
  def test(): Unit = {
    val a: Any = java.lang.Integer.valueOf(100)
    val c = a.asInstanceOf[java.lang.Integer].toInt.asInstanceOf[Byte]
    //val c = a.toInt.asInstanceOf[Byte]
    println(c)
  }
}
