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

package com.island.ohara.it

import java.time.Duration

import com.island.ohara.agent.NoSuchClusterException
import com.island.ohara.agent.docker.ContainerState
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.common.util.CommonUtils
import org.junit.rules.Timeout
import org.junit.{AssumptionViolatedException, Rule}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

abstract class IntegrationTest {
  @Rule def timeout: Timeout = Timeout.seconds(720)

  /**
    * Skip all remaining test cases after calling this method.
    *
    * @param message why you want to skip all test cases?
    */
  protected def skipTest(message: String): Unit = throw new AssumptionViolatedException(message)

  protected def result[T](f: Future[T]): T = Await.result(f, 2 minutes)

  /**
    * await until the function return true. If the function fails, the exception is thrown.
    * @param f verification function
    */
  protected def await(f: () => Boolean): Unit = await(f, false)

  /**
    * await until the function return true
    * @param f verification function
    * @param swallowException true if your want to swallow exception and keep running
    */
  protected def await(f: () => Boolean, swallowException: Boolean): Unit =
    CommonUtils.await(
      () =>
        try f()
        catch {
          case _: Throwable if swallowException =>
            false
        },
      Duration.ofMinutes(2)
    )

  /**
    * the creation of cluster is async so you need to wait the cluster to build.
    * @param clusters clusters
    * @param containers containers
    * @param clusterKey cluster key
    */
  protected def assertCluster(
    clusters: () => Seq[ClusterInfo],
    containers: () => Seq[ContainerInfo],
    clusterKey: ObjectKey
  ): Unit =
    await(
      () =>
        try {
          clusters().exists(_.key == clusterKey) &&
          // since we only get "active" containers, all containers belong to the cluster should be running.
          // Currently, both k8s and pure docker have the same context of "RUNNING".
          // It is ok to filter container via RUNNING state.
          containers().nonEmpty &&
          containers().map(_.state).forall(_.equals(ContainerState.RUNNING.name))
        } catch {
          // the collie impl throw exception if the cluster "does not" exist when calling "containers"
          case _: NoSuchClusterException => false
        }
    )

  /**
    * Some ITs require the public hostname to expose service. If this method return none, it means the QA does not prepare
    * the such env for IT.
    * @return hostname or none
    */
  protected def publicHostname: Option[String] = sys.env.get("ohara.it.hostname")

  /**
    * Some ITs require the public port to expose service. If this method return none, it means the QA does not prepare
    * the such env for IT.
    * @return public port or none
    */
  protected def publicPort: Option[Int] = sys.env.get("ohara.it.port").map(_.toInt)
}
