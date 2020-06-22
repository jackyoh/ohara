package oharastream.ohara.connector.jdbc.source

import java.util

import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

class JDBCSourceIncrementTimestampTask extends RowSourceTask {
  override protected def run(settings: TaskSetting): Unit = ???

  override protected def terminate(): Unit = ???

  override protected def pollRecords(): util.List[RowSourceRecord] = ???
}
