package oharastream.ohara.connector.jdbc.source

import java.util
import scala.jdk.CollectionConverters._
import oharastream.ohara.kafka.connector.{RowSourceConnector, RowSourceTask, TaskSetting}

class MultiNodeJDBCSourceConnector extends RowSourceConnector {
  private[this] var settings: TaskSetting = _

  override def taskClass(): Class[_ <: RowSourceTask] = classOf[MultiNodeJDBCSourceTask]

  override protected def taskSettings(maxTasks: Int): util.List[TaskSetting] = Seq.fill(maxTasks)(settings).asJava

  override protected def run(settings: TaskSetting): Unit = {
    this.settings = settings
  }

  override protected def terminate(): Unit = {
    // Nothing
  }
}
