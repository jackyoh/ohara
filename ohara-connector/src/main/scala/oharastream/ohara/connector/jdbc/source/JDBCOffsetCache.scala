package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.kafka.connector.RowSourceContext

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class JDBCOffsetCache {
  private[this] val cache = mutable.Map[String, JDBCOffsetInfo]()

  def loadIfNeed(context: RowSourceContext, tableTimestampPartition: String): Unit = {
    if (cache.get(tableTimestampPartition).isEmpty) {
      val offset: Map[String, _] =
        context.offset(Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> tableTimestampPartition).asJava).asScala.toMap
      if (offset.nonEmpty) update(tableTimestampPartition, offsetValue(offset))
    }
  }

  def update(tableTimestampPartition: String, value: JDBCOffsetInfo): Unit = {
    this.cache.put(tableTimestampPartition, value)
  }

  def readOffset(tableTimestampPartition: String): JDBCOffsetInfo = {
    this.cache.get(tableTimestampPartition).getOrElse(JDBCOffsetInfo(0))
  }

  private[this] def offsetValue(offset: Map[String, _]): JDBCOffsetInfo = {
    val index = offset(JDBCOffsetCache.TABLE_OFFSET_KEY).toString().toInt
    JDBCOffsetInfo(index)
  }
}

object JDBCOffsetCache {
  private[source] val TABLE_PARTITION_KEY: String = "jdbc.table.timestamp"
  private[source] val TABLE_OFFSET_KEY: String    = "jdbc.table.info"
}

case class JDBCOffsetInfo(index: Int) {
  override def toString(): String = s"$index"
}
