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

package oharastream.ohara.it.performance

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import oharastream.ohara.client.configurator.v0.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.v0.TopicApi.TopicInfo
import oharastream.ohara.client.configurator.v0.{ConnectorApi, TopicApi}
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.it.WithRemoteWorkers
import oharastream.ohara.kafka.Producer
import com.typesafe.scalalogging.Logger
import org.junit.rules.Timeout
import org.junit.{After, Rule}
import spray.json.JsValue

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.control.Breaks.break

/**
  * the basic infra to test performance for ohara components.
  * all pre-defined configs in this class should NOT be changed arbitrary since they are related to our jenkins.
  *
  * Noted:
  * 1) the sub implementation should have single test case in order to avoid complicated code and timeout
  * 2) the sub implementation does NOT need to generate any report or output since this infra traces all metrics of connector
  *    and topics for sub implementation
  * 3) the reports are located at /tmp/performance/$className/$testName/$random.csv by default. Of course, this is related to jenkins
  *    so please don't change it.
  */
abstract class BasicTestPerformance extends WithRemoteWorkers {
  protected val log: Logger              = Logger(classOf[BasicTestPerformance])
  private[this] var inputDataInfos       = mutable.Seq[DataInfo]()
  private[this] val setupStartTime: Long = CommonUtils.current()

  private[this] val topicKey: TopicKey = TopicKey.of("benchmark", CommonUtils.randomString(5))
  protected val topicApi: TopicApi.Access =
    TopicApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)

  protected val connectorApi: ConnectorApi.Access =
    ConnectorApi.access
      .hostname(configuratorHostname)
      .port(configuratorPort)

  //------------------------------[global properties]------------------------------//
  private[this] val durationOfPerformanceKey     = PerformanceTestingUtils.DURATION_KEY
  private[this] val durationOfPerformanceDefault = 120 seconds
  protected val durationOfPerformance: Duration =
    value(durationOfPerformanceKey).map(Duration.apply).getOrElse(durationOfPerformanceDefault)

  private[this] val timeoutOfInputDataKey               = PerformanceTestingUtils.INPUTDATA_TIMEOUT_KEY
  private[this] val timeoutOfInputDataDefault: Duration = 30 seconds
  protected val timeoutOfInputData: Duration =
    value(timeoutOfInputDataKey).map(Duration(_)).getOrElse(timeoutOfInputDataDefault)

  private[this] val numberOfRowsToFlushKey             = PerformanceTestingUtils.ROW_FLUSH_NUMBER_KEY
  private[this] val numberOfRowsToFlushDefault: String = "2000"
  protected val numberOfRowsToFlush: Int =
    value(numberOfRowsToFlushKey).getOrElse(numberOfRowsToFlushDefault).toInt

  private[this] val wholeTimeout = (durationOfPerformance.toSeconds + timeoutOfInputData.toSeconds) * 2

  @Rule
  override def timeout: Timeout = Timeout.seconds(wholeTimeout)

  private[this] val logMetersFrequencyKey               = PerformanceTestingUtils.LOG_METERS_FREQUENCY_KEY
  private[this] val logMetersFrequencyDefault: Duration = 5 seconds
  protected val logMetersFrequency: Duration =
    value(logMetersFrequencyKey).map(Duration(_)).getOrElse(logMetersFrequencyDefault)

  private[this] val totalSizeInBytes = new LongAdder()
  private[this] val count            = new LongAdder()
  //------------------------------[topic properties]------------------------------//

  private[this] val megabytesOfInputDataKey           = PerformanceTestingUtils.DATA_SIZE_KEY
  private[this] val megabytesOfInputDataDefault: Long = 10000
  protected val sizeOfInputData: Long =
    1024L * 1024L * value(megabytesOfInputDataKey).map(_.toLong).getOrElse(megabytesOfInputDataDefault)

  private[this] val numberOfPartitionsKey     = PerformanceTestingUtils.PARTITION_SIZE_KEY
  private[this] val numberOfPartitionsDefault = 1
  protected val numberOfPartitions: Int =
    value(numberOfPartitionsKey).map(_.toInt).getOrElse(numberOfPartitionsDefault)

  //------------------------------[connector properties]------------------------------//
  private[this] val numberOfConnectorTasksKey     = PerformanceTestingUtils.TASK_SIZE_KEY
  private[this] val numberOfConnectorTasksDefault = 1
  protected val numberOfConnectorTasks: Int =
    value(numberOfConnectorTasksKey).map(_.toInt).getOrElse(numberOfConnectorTasksDefault)

  protected def value(key: String): Option[String] = sys.env.get(key)
  //------------------------------[helper methods]------------------------------//
  private[this] var inputDataThread: Releasable = _

  protected[this] def loopInputDataThread(input: Duration => (Any, Long, Long)): Unit = {
    inputDataThread = {
      val pool = Executors.newSingleThreadExecutor()
      pool.execute(() => {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            input(timeoutOfInputData)
          } catch {
            case interrupException: InterruptedException => {
              log.error("interrup exception", interrupException)
              break
            }
            case e: Throwable => throw e
          }
        }
      })
      () => {
        pool.shutdownNow()
        pool.awaitTermination(durationOfPerformance.toMillis * 10, TimeUnit.MILLISECONDS)
      }
    }
  }

  protected[this] def rowData(): Row = {
    Row.of(
      (0 until 10).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  /**
    * Duration running function for after sleep
    */
  protected def beforeEndSleepUntil(reports: Seq[PerformanceReport]): Unit = {
    Releasable.close(inputDataThread)
  }

  /**
    * create and start the topic.
    * @return topic info
    */
  protected[this] def createTopic(): TopicInfo = {
    result(
      topicApi.request
        .key(topicKey)
        .brokerClusterKey(brokerClusterInfo.key)
        .numberOfPartitions(numberOfPartitions)
        .create()
    )
    await(() => {
      result(topicApi.start(topicKey))
      true
    }, true)
    result(topicApi.get(topicKey))
  }

  protected def setupConnector(
    connectorKey: ConnectorKey,
    className: String,
    settings: Map[String, JsValue]
  ): ConnectorInfo = {
    //Before create and start the connector, need to await
    // worker http server running completed.
    await(
      () => {
        result(
          connectorApi.request
            .settings(settings)
            .key(connectorKey)
            .className(className)
            .topicKey(topicKey)
            .workerClusterKey(workerClusterInfo.key)
            .numberOfTasks(numberOfConnectorTasks)
            .create()
        )
        result(connectorApi.start(connectorKey))
        true
      },
      true
    )
    result(connectorApi.get(connectorKey))
  }

  //------------------------------[metrics function]---------------------------//
  private[this] val metricsFile = new PerformanceDataMetricsFile()

  /**
    * cache all historical meters. we always create an new file with all meters so we have to cache them.
    */
  private[this] val reportBuilders = mutable.Map[ConnectorKey, PerformanceReport.Builder]()

  private[this] def connectorReports(): Seq[PerformanceReport] = {
    val connectorInfos = result(connectorApi.list())
    connectorInfos.map { info =>
      val duration = (info.metrics.meters.flatMap(_.duration) :+ 0L).max / 1000
      val builder  = reportBuilders.getOrElseUpdate(info.key, PerformanceReport.builder)
      builder.connectorKey(info.key)
      builder.className(info.className)

      // Different time get the metrics data, the metrics data maybe same.
      // Must clean the same metrics data, avoid duplication to sum for the metrics
      info.metrics.meters.foreach(
        meter =>
          builder
            .resetValue(duration, meter.name)
            .resetValue(duration, s"${meter.name}(inPerSec)")
      )
      info.metrics.meters.foreach(
        meter =>
          builder
            .record(duration, meter.name, meter.value)
            .record(duration, s"${meter.name}(inPerSec)", meter.valueInPerSec.getOrElse(0.0f))
      )
      builder.build
    }
  }

  private[this] def fetchConnectorMetrics(reports: Seq[PerformanceReport]): Unit = {
    try reports.foreach(metricsFile.logMeters)
    catch {
      case e: Throwable =>
        log.error("failed to log meters", e)
    } finally afterRecodingReports(reports)
  }

  private[this] def fetchDataInfoMetrics(inputDataInfos: Seq[DataInfo]): Unit = {
    try metricsFile.logDataInfos(inputDataInfos)
    catch {
      case e: Throwable =>
        log.error("failed to log input data metrics", e)
    }
  }

  /**
    * invoked after all metrics of connectors are recorded.
    * Noted: it is always invoked even if we fail to record reports
    * @param reports the stuff we record
    */
  protected def afterRecodingReports(reports: Seq[PerformanceReport]): Unit = {
    // nothing by default
  }

  //------------------------------[core functions]------------------------------//

  protected def produce(timeout: Duration): (TopicKey, Long, Long) = {
    val producer = Producer
      .builder()
      .keySerializer(Serializer.ROW)
      .connectionProps(brokerClusterInfo.connectionProps)
      .build()
    try {
      val result: (Long, Long) = generateData(
        numberOfRowsToFlush,
        timeout,
        (rows: Seq[Row]) => {
          val count       = new LongAdder()
          val sizeInBytes = new LongAdder()
          rows.foreach(row => {
            producer
              .sender()
              .topicName(topicKey.topicNameOnKafka())
              .key(row)
              .send()
              .whenComplete {
                case (meta, _) =>
                  if (meta != null) {
                    sizeInBytes.add(meta.serializedKeySize())
                    count.add(1)
                  }
              }
          })
          producer.flush()
          (count.longValue(), sizeInBytes.longValue())
        }
      )
      (topicKey, result._1, result._2)
    } finally Releasable.close(producer)
  }

  final protected[this] def generateData(
    numberOfRowsToFlush: Int,
    timeout: Duration,
    callback: (Seq[Row]) => (Long, Long)
  ): (Long, Long) = {
    val start    = CommonUtils.current()
    var previous = CommonUtils.current()
    while (totalSizeInBytes.longValue() <= sizeOfInputData &&
           CommonUtils.current() - start <= timeout.toMillis) {
      val value: (Long, Long) = callback((0 until numberOfRowsToFlush).map(_ => rowData()))
      count.add(value._1)
      totalSizeInBytes.add(value._2)
      // Input data metrics write to the memory
      if ((CommonUtils.current() - previous) >= logMetersFrequency.toMillis) {
        inputDataInfos = inputDataInfos ++ Seq(
          DataInfo(CommonUtils.current() - setupStartTime, count.longValue(), totalSizeInBytes.longValue())
        )
        previous = CommonUtils.current()
      }
    }
    (count.longValue(), totalSizeInBytes.longValue())
  }

  protected def sleepUntilEnd(): Long = {
    try {
      val end = CommonUtils.current() + durationOfPerformance.toMillis
      while (CommonUtils.current() <= end) {
        val reports = connectorReports()
        fetchConnectorMetrics(reports)
        fetchDataInfoMetrics(inputDataInfos)
        TimeUnit.MILLISECONDS.sleep(logMetersFrequency.toMillis)
      }
    } finally {
      val reports = connectorReports()
      fetchConnectorMetrics(reports)
      fetchDataInfoMetrics(inputDataInfos)
      beforeEndSleepUntil(reports)
    }
    durationOfPerformance.toMillis
  }

  /**
    * When connector is running have used some resource such folder or file.
    * for example: after running connector complete, can't delete the data.
    *
    * This function is after get metrics data, you can run other operating.
    * example delete data.
    */
  protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit = {}

  @After
  def record(): Unit = {
    // Have setup connector on the worker.
    // Need to stop the connector on the worker.
    result(connectorApi.list()).foreach(
      connector =>
        await(
          () => {
            result(connectorApi.stop(connector.key))
            true
          },
          true
        )
    )
    afterStoppingConnectors(result(connectorApi.list()), result(topicApi.list()))
  }
}

final case class DataInfo(duration: Long, messageNumber: Long, messageSize: Long)
