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

package oharastream.ohara.agent

import oharastream.ohara.client.configurator.v0.NodeApi.Node
import scala.concurrent.{ExecutionContext, Future}

trait RemoteNodeHandler {
  def validateFolder(nodeName: String, path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse]

  def mkDir(nodeName: String, path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse]

  def listDir(nodeName: String, path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse]

  def deleteDir(nodeName: String, path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse]
}

class SshRemoteNodeHandler(dataCollie: DataCollie) extends RemoteNodeHandler {
  override def validateFolder(nodeName: String, path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse] =
    agent(nodeName)
      .map { agent =>
        val folderName = path.split("/").last
        Seq(
          agent.execute(s"""
             |if [ -d "$path" ]; then
             |  echo "Folder not exists"
             |else
             |  echo "Folder exists"
             |fi
           """.stripMargin).get,
          agent.execute("ls -n " + path + "/../|grep " + folderName + "|awk '{print $3}'").get.trim()
        )
      }
      .map { result =>
        if (result.contains("Folder not exists") || !result.contains("1000"))
          RemoteNodeResponse("Folder validate failed")
        else RemoteNodeResponse("Folder validate success")
      }

  override def mkDir(nodeName: String, path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse] =
    agent(nodeName)
      .map { agent =>
        agent.execute(s"mkdir ${path}")
      }
      .map { result =>
        RemoteNodeResponse(result.getOrElse("create folder success"))
      }

  override def deleteDir(nodeName: String, path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse] =
    agent(nodeName)
      .map { agent =>
        agent.execute(s"rm -rf ${path}")
      }
      .map { result =>
        RemoteNodeResponse(result.getOrElse("delete folder success"))
      }

  override def listDir(nodeName: String, path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse] =
    agent(nodeName)
      .map { agent =>
        agent.execute(s"ls -l ${path}")
      }
      .map { result =>
        RemoteNodeResponse(result.getOrElse(""))
      }

  private[this] def agent(nodeName: String)(implicit executionContext: ExecutionContext): Future[Agent] = {
    dataCollie
      .value[Node](nodeName)
      .map { node =>
        Agent.builder.hostname(node.hostname).user(node._user).password(node._password).port(node._port).build
      }
  }
}

case class RemoteNodeResponse(message: String)
