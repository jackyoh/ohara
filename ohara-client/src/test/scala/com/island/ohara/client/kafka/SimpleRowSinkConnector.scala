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

package com.island.ohara.client.kafka

import java.util

import com.island.ohara.kafka.connector.{RowSinkConnector, RowSinkTask, TaskConfig}
import scala.collection.JavaConverters._
class SimpleRowSinkConnector extends RowSinkConnector {
  private[this] var config: TaskConfig = _
  override protected def _start(props: TaskConfig): Unit = {
    this.config = props
    // check the option
    this.config.raw().get(OUTPUT)
    this.config.raw().get(BROKER)
  }

  override protected def _taskClass(): Class[_ <: RowSinkTask] = classOf[SimpleRowSinkTask]

  override protected def _stop(): Unit = {}

  override protected def _taskConfigs(maxTasks: Int): util.List[TaskConfig] =
    new util.ArrayList[TaskConfig](Seq.fill(maxTasks)(config).asJavaCollection)
}
