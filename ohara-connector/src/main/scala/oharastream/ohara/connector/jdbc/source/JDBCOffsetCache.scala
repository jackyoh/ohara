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
