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

package oharastream.ohara.it.collie

import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.{DataCollie, RemoteFolderHandler}
import oharastream.ohara.agent.k8s.K8SClient
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestK8SClient extends OharaTest {
  @Test
  def test(): Unit = {
    val dataCollie = DataCollie(
      Seq(
        Node(
          hostname = "k8s-master",
          port = 22,
          user = "ohara",
          password = "123456"
        )
      )
    )

    val client = K8SClient.builder
      .serverURL("http://10.100.0.185:8080/api/v1")
      .remoteFolderHandler(RemoteFolderHandler(dataCollie))
      .namespace("default")
      .build()
    result(
      client.volumeCreator
        .name("test-aaa")
        .prefixVolumeName("test123")
        .nodeName("k8s-master")
        .path("/tmp/test")
        .create()
    )
  }
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))
}
