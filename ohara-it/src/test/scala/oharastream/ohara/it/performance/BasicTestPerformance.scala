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

import java.io.{File, FileWriter}
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
  protected val log: Logger            = Logger(classOf[BasicTestPerformance])
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

  private[this] val wholeTimeout = (durationOfPerformance.toSeconds + timeoutOfInputData.toSeconds) * 2

  @Rule
  override def timeout: Timeout = Timeout.seconds(wholeTimeout)

  private[this] val reportOutputFolderKey = PerformanceTestingUtils.REPORT_OUTPUT_KEY
  private[this] val reportOutputFolder: File = mkdir(
    new File(
      value(reportOutputFolderKey).getOrElse("/tmp/performance")
    )
  )

  private[this] val logMetersFrequencyKey               = PerformanceTestingUtils.LOG_METERS_FREQUENCY_KEY
  private[this] val logMetersFrequencyDefault: Duration = 5 seconds
  protected val logMetersFrequency: Duration =
    value(logMetersFrequencyKey).map(Duration(_)).getOrElse(logMetersFrequencyDefault)

  private[this] val totalSizeInBytes              = new LongAdder()
  private[this] val count                         = new LongAdder()
  private[this] var produceDataThread: Releasable = _

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

  protected[this] def loopInputData(): Unit = {
    inputDataThread = {
      val pool = Executors.newSingleThreadExecutor()
      pool.execute(() => {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            setupInputData(timeoutOfInputData)
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

  protected def setupInputData(timeoutOfInputData: Duration): (String, Long, Long) = {
    throw new UnsupportedOperationException("Abstract not support setupInputData function")
  }

  protected def mkdir(folder: File): File = {
    if (!folder.exists() && !folder.mkdirs()) throw new AssertionError(s"failed to create folder on $folder")
    if (folder.exists() && !folder.isDirectory) throw new AssertionError(s"$folder is not a folder")
    folder
  }

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
      info.metrics.meters.foreach(
        meter =>
          builder
            .record(duration, meter.name, meter.value)
            .record(duration, s"${meter.name}(inPerSec)", meter.valueInPerSec.getOrElse(0.0f))
      )
      builder.build
    }
  }

  private[this] def logMeters(report: PerformanceReport): Unit = if (report.records.nonEmpty) {
    def simpleName(className: String): String = {
      val index = className.lastIndexOf(".")
      if (index != -1) className.substring(index + 1)
      else className
    }

    def path(className: String): File = {
      new File(
        mkdir(new File(mkdir(new File(reportOutputFolder, simpleName(className))), this.getClass.getSimpleName)),
        s"${report.key.group()}-${report.key.name()}.csv"
      )
    }
    // we have to fix the order of key-value
    // if we generate line via map.keys and map.values, the order may be different ...
    val headers = report.records.head._2.keySet.toList
    report.records.values.foreach { items =>
      headers.foreach(
        name =>
          if (!items.contains(name))
            throw new RuntimeException(s"$name disappear?? current:${items.keySet.mkString(",")}")
      )
    }
    val file = path(report.className)
    if (file.exists() && !file.delete()) throw new RuntimeException(s"failed to remove file:$file")
    val fileWriter = new FileWriter(file)
    try {
      fileWriter.write("duration," + headers.map(s => s"""\"$s\"""").mkString(","))
      fileWriter.write("\n")
      report.records.foreach {
        case (duration, item) =>
          val line = s"$duration," + headers.map(header => f"${item(header)}%.3f").mkString(",")
          fileWriter.write(line)
          fileWriter.write("\n")
      }
    } finally Releasable.close(fileWriter)
  }

  protected def sleepUntilEnd(): Long = {
    try {
      val end = CommonUtils.current() + durationOfPerformance.toMillis
      while (CommonUtils.current() <= end) {
        val reports = connectorReports()
        fetchConnectorMetrics(reports)
        inputDataMetrics()
        TimeUnit.MILLISECONDS.sleep(logMetersFrequency.toMillis)
      }
    } finally {
      val reports = connectorReports()
      fetchConnectorMetrics(reports)
      beforeEndSleepUntil(reports)
    }
    durationOfPerformance.toMillis
  }

  protected def inputDataMetrics(): Unit = {
    // nothing by default
  }

  /**
    * invoked after all metrics of connectors are recorded.
    * Noted: it is always invoked even if we fail to record reports
    * @param reports the stuff we record
    */
  protected def afterRecodingReports(reports: Seq[PerformanceReport]): Unit = {
    // nothing by default
  }

  /**
    * Duration running function for after sleep
    */
  protected def beforeEndSleepUntil(reports: Seq[PerformanceReport]): Unit = {
    Releasable.close(produceDataThread)
    Releasable.close(inputDataThread)
  }

  /**
    * create and start the topic.
    * @return topic info
    */
  protected def createTopic(): TopicInfo = {
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

  protected[this] def loopProduceData(topicInfo: TopicInfo): Unit = {
    produceDataThread = {
      val pool = Executors.newSingleThreadExecutor()

      pool.execute(() => {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            produce(topicInfo, timeoutOfInputData)
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

  protected def produce(topicInfo: TopicInfo, timeout: Duration): (TopicInfo, Long, Long) = {
    val cellNames: Set[String] = (0 until 10).map(index => s"c$index").toSet
    val numberOfRowsToFlush    = 2000
    val producer = Producer
      .builder()
      .keySerializer(Serializer.ROW)
      .connectionProps(brokerClusterInfo.connectionProps)
      .build()
    var cachedRows = 0
    val start      = CommonUtils.current()

    try {
      while (totalSizeInBytes.longValue() <= sizeOfInputData &&
             (CommonUtils.current() - start) <= timeout.toMillis) {
        producer
          .sender()
          .topicName(topicInfo.key.topicNameOnKafka())
          .key(Row.of(cellNames.map { name =>
            Cell.of(name, CommonUtils.randomString())
          }.toSeq: _*))
          .send()
          .whenComplete {
            case (meta, _) =>
              if (meta != null) {
                totalSizeInBytes.add(meta.serializedKeySize())
                count.add(1)
              }
          }
        cachedRows += 1
        if (cachedRows >= numberOfRowsToFlush) {
          producer.flush()
          cachedRows = 0
        }
      }
    } finally Releasable.close(producer)

    (topicInfo, count.longValue(), totalSizeInBytes.longValue())
  }

  protected def rowData(): Row = {
    Row.of(
      (0 until 10).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  /**
    * When connector is running have used some resource such folder or file.
    * for example: after running connector complete, can't delete the data.
    *
    * This function is after get metrics data, you can run other operating.
    * example delete data.
    */
  protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit = {}

  //------------------------------[core functions]------------------------------//

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

  private[this] def fetchConnectorMetrics(reports: Seq[PerformanceReport]): Unit = {
    try reports.foreach(logMeters)
    catch {
      case e: Throwable =>
        log.error("failed to log meters", e)
    } finally afterRecodingReports(reports)
  }
}
