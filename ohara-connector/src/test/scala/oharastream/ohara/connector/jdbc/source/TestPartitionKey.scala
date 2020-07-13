package oharastream.ohara.connector.jdbc.source

import java.sql.Timestamp

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.Test

class TestPartitionKey extends OharaTest {
  private[this] val TIMESTAMP_PARTITION_RNAGE: Int = 86400000
  @Test
  def test(): Unit = {
    //2020-07-05 11:06:45.664 -- Timestamp: 2020-07-06 11:06:45.664 => 2020-07-06 11:06:45.664~2020-07-07 11:06:45.664
    val current = CommonUtils.current()
    println(olderPartitionKey("table1", Timestamp.valueOf("2020-07-05 11:06:45.664"), new Timestamp(current)))
    println(newPartitionKey("table1", Timestamp.valueOf("2020-07-05 11:06:45.664"), new Timestamp(current)))
  }

  private[this] def newPartitionKey(
    tableName: String,
    firstTimestampValue: Timestamp,
    timestamp: Timestamp): String = {

    val page = if (timestamp.getTime() < CommonUtils.current()) {
      (timestamp.getTime() - firstTimestampValue.getTime()) / TIMESTAMP_PARTITION_RNAGE
    } else (timestamp.getTime() - firstTimestampValue.getTime() - TIMESTAMP_PARTITION_RNAGE) / TIMESTAMP_PARTITION_RNAGE

    val startTimestamp = new Timestamp((page * TIMESTAMP_PARTITION_RNAGE) + firstTimestampValue.getTime())
    val stopTimestamp = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }
  private[this] def olderPartitionKey(
    tableName: String,
    firstTimestampValue: Timestamp,
    timestamp: Timestamp): String = {

    var startTimestamp: Timestamp   = firstTimestampValue
    var stopTimestamp: Timestamp    = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)
    val currentTimestamp: Timestamp = Timestamp.valueOf("2020-07-10 12:00:00")

    // TODO Refactor this function to remove while loop to calc partition key
    while (!(timestamp.getTime() >= startTimestamp.getTime() && timestamp.getTime() <= stopTimestamp.getTime())) {
      startTimestamp = stopTimestamp
      stopTimestamp = new Timestamp(startTimestamp.getTime() + TIMESTAMP_PARTITION_RNAGE)

      if (timestamp.getTime() < firstTimestampValue.getTime())
        throw new IllegalArgumentException("The timestamp over the first data timestamp")

      if (startTimestamp.getTime() > currentTimestamp.getTime() && stopTimestamp.getTime() > Timestamp.valueOf("2020-07-10 12:00:00")
        .getTime()) {
        throw new IllegalArgumentException("The timestamp over the current timestamp")
      }
    }
    s"$tableName:${startTimestamp.toString}~${stopTimestamp.toString}"
  }
}
