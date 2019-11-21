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

package com.island.ohara.configurator

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directives.{complete, get, path}
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import com.island.ohara.agent.ServiceCollie
import com.island.ohara.agent.k8s.K8SClient
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.rule.OharaTest
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.Configurator.Mode
import org.junit.Test
import org.scalatest.Matchers._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.mockito.MockitoSugar._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
class TestConfiguratorBuilder extends OharaTest {
  private[this] def result[T](f: Future[T]): T = Await.result(f, 60 seconds)

  @Test
  def nullHomeFolder(): Unit = an[NullPointerException] should be thrownBy Configurator.builder.homeFolder(null)

  @Test
  def emptyHomeFolder(): Unit = an[IllegalArgumentException] should be thrownBy Configurator.builder.homeFolder("")

  @Test
  def autoMkdirForHomeFolder(): Unit = {
    val folder = CommonUtils.createTempFolder(CommonUtils.randomString(10))
    folder.delete() shouldBe true
    folder.exists() shouldBe false
    Configurator.builder.homeFolder(folder.getCanonicalPath)
    folder.exists() shouldBe true
  }

  @Test
  def duplicatePort(): Unit = an[IllegalArgumentException] should be thrownBy Configurator.builder.port(10).port(20)

  @Test
  def testFakeCluster(): Unit = {
    Seq(
      (1, 1),
      (1, 2),
      (2, 1),
      (10, 10)
    ).foreach {
      case (numberOfBrokers, numberOfWorkers) =>
        val configurator = Configurator.builder.fake(numberOfBrokers, numberOfWorkers).build()
        try {
          result(configurator.serviceCollie.brokerCollie.clusters()).size shouldBe numberOfBrokers
          result(configurator.serviceCollie.workerCollie.clusters()).size shouldBe numberOfWorkers
          result(configurator.serviceCollie.clusters())
          // one broker generates one zk cluster
          .size shouldBe (numberOfBrokers + numberOfBrokers + numberOfWorkers)
          val nodes = result(configurator.store.values[Node]())
          nodes.isEmpty shouldBe false
          result(configurator.serviceCollie.clusters())
            .flatMap(_._2.map(_.nodeName))
            .foreach(name => nodes.exists(_.name == name) shouldBe true)
        } finally configurator.close()
    }
  }

  @Test
  def createWorkerClusterWithoutBrokerCluster(): Unit = {
    an[IllegalArgumentException] should be thrownBy Configurator.builder.fake(0, 1)
  }

  @Test
  def createFakeConfiguratorWithoutClusters(): Unit = {
    val configurator = Configurator.builder.fake(0, 0).build()
    try result(configurator.serviceCollie.clusters()).size shouldBe 0
    finally configurator.close()
  }

  @Test
  def reassignServiceCollieAfterFake(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
    // in fake mode, a fake collie will be created
      .fake(1, 1)
      .serviceCollie(MockitoSugar.mock[ServiceCollie])
      .build()

  @Test
  def reassignK8sClient(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .k8sClient(MockitoSugar.mock[K8SClient])
      .k8sClient(MockitoSugar.mock[K8SClient])
      .build()

  @Test
  def testK8SClientNamespaceDefault(): Unit = {
    val namespace  = "default"
    val podName    = "pod1"
    val logMessage = "start pods ......."
    val apiServer  = k8sServer(namespace, podName, logMessage)
    try {
      val configurator: Configurator = Configurator.builder.k8sApiServer(apiServer.url).build()
      Await.result(configurator.k8sClient.get.log(podName, None), 10 seconds) shouldBe logMessage
    } finally apiServer.close()
  }

  @Test
  def testK8SClientNamespaceAssign(): Unit = {
    val namespace  = "ohara"
    val podName    = "pod1"
    val logMessage = "start pods ......."
    val apiServer  = k8sServer(namespace, podName, logMessage)
    try {
      val configurator: Configurator = Configurator.builder.k8sNamespace(namespace).k8sApiServer(apiServer.url).build()
      Await.result(configurator.k8sClient.get.log(podName, None), 10 seconds) shouldBe logMessage
    } finally apiServer.close()
  }

  @Test
  def testK8SClientNamespaceNone(): Unit = {
    val namespace  = "default"
    val podName    = "pod1"
    val logMessage = "start pods ......."
    val apiServer  = k8sServer(namespace, podName, logMessage)
    try {
      val k8sClient                  = K8SClient.builder.apiServerURL(apiServer.url).build
      val configurator: Configurator = Configurator.builder.k8sClient(k8sClient).build()
      Await.result(configurator.k8sClient.get.log(podName, None), 10 seconds) shouldBe logMessage
    } finally apiServer.close()
  }

  @Test
  def testK8SClientNamespace(): Unit = {
    val namespace  = "ohara"
    val podName    = "pod1"
    val logMessage = "start pods ......."
    val apiServer  = k8sServer(namespace, podName, logMessage)
    try {
      val k8sClient: K8SClient = K8SClient.builder.apiServerURL(apiServer.url).namespace(namespace).build()

      val configurator: Configurator =
        Configurator.builder.k8sClient(k8sClient).build()
      Await.result(configurator.k8sClient.get.log(podName, None), 10 seconds) shouldBe logMessage
    } finally apiServer.close()
  }

  @Test
  def reassignServiceCollie(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .serviceCollie(MockitoSugar.mock[ServiceCollie])
      .serviceCollie(MockitoSugar.mock[ServiceCollie])
      .build()

  @Test
  def reassignHostname(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .hostname(CommonUtils.hostname())
      .hostname(CommonUtils.hostname())
      .build()

  @Test
  def reassignPort(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .port(CommonUtils.availablePort())
      .port(CommonUtils.availablePort())
      .build()

  @Test
  def reassignHomeFolder(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .build()

  @Test
  def reassignHomeFolderAfterFake(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
    // in fake mode, we have created a store
      .fake(1, 1)
      // you can't change the folder of store now
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .build()

  @Test
  def assigningK8sBeforeHomeFolderShouldNotCauseException(): Unit =
    Configurator.builder
      .k8sClient(mock[K8SClient])
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(5)).getAbsolutePath)

  private[this] def toServer(route: server.Route): SimpleServer = {
    implicit val system       = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    val server                = Await.result(Http().bindAndHandle(route, "localhost", 0), 30 seconds)

    new SimpleServer {
      override def hostname: String = server.localAddress.getHostString
      override def port: Int        = server.localAddress.getPort
      override def close(): Unit = {
        Await.result(server.unbind(), 30 seconds)
        Await.result(system.terminate(), 30 seconds)
      }
    }
  }

  @Test
  def testBuild(): Unit = {
    val apiServer           = k8sServer("default", "pod", "log")
    val configuratorBuilder = Configurator.builder
    val configurator        = configuratorBuilder.k8sApiServer(apiServer.url).k8sNamespace("default").build()
    configurator.mode shouldBe Mode.K8S
  }

  private[this] def k8sServer(namespace: String, podName: String, logMessage: String): SimpleServer = {
    val podName = "pod1"
    toServer {
      path("namespaces" / namespace / "pods" / podName / "log") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, logMessage)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, k8sNodesResponse)))
          }
        }
    }
  }

  private[this] val k8sNodesResponse = s"""
        |{"items": [
        |    {
        |      "metadata": {
        |        "name": "node1"
        |      },
        |      "status": {
        |        "conditions": [
        |          {
        |            "type": "Ready",
        |            "status": "True",
        |            "lastHeartbeatTime": "2019-05-14T06:14:46Z",
        |            "lastTransitionTime": "2019-04-15T08:21:11Z",
        |            "reason": "KubeletReady",
        |            "message": "kubelet is posting ready status"
        |          }
        |        ],
        |        "addresses": [
        |          {
        |            "type": "InternalIP",
        |            "address": "10.2.0.4"
        |          },
        |          {
        |            "type": "Hostname",
        |            "address": "ohara-it-02"
        |          }
        |        ],
        |        "images": [
        |          {
        |            "names": [
        |              "quay.io/coreos/etcd@sha256:ea49a3d44a50a50770bff84eab87bac2542c7171254c4d84c609b8c66aefc211",
        |              "quay.io/coreos/etcd:v3.3.9"
        |            ],
        |            "sizeBytes": 39156721
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

  trait SimpleServer extends Releasable {
    def hostname: String
    def port: Int

    def url: String = s"http://$hostname:$port"
  }
}
