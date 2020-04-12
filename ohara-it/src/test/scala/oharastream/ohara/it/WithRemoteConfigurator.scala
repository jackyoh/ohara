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

package oharastream.ohara.it

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.common.util.Releasable
import org.junit.After
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._

/**
  * a basic setup offering a configurator running on remote node.
  * this stuff is also in charge of releasing the configurator after testing.
  */
@RunWith(value = classOf[Parameterized])
abstract class WithRemoteConfigurator(platform: ContainerPlatform) extends IntegrationTest {
  protected[this] val nodeNames: Set[String]             = platform.nodeNames
  protected final val resourceRef                        = platform.setup()
  protected final val containerClient: ContainerClient   = resourceRef.containerClient
  protected final val configuratorHostname: String       = resourceRef.configuratorHostname
  protected final val configuratorPort: Int              = resourceRef.configuratorPort
  protected final val serviceKeyHolder: ServiceKeyHolder = ServiceKeyHolder(containerClient)

  @After
  def releaseConfigurator(): Unit = {
    Releasable.close(serviceKeyHolder)
    Releasable.close(resourceRef)
  }
}

// Test QA server
object WithRemoteConfigurator {
  @Parameters(name = "{index} mode = {0}")
  def parameters: java.util.Collection[ContainerPlatform] = {
    val modes = ContainerPlatform.all
    if (modes.isEmpty) java.util.Collections.singletonList(ContainerPlatform.empty)
    else modes.asJava
  }
}
