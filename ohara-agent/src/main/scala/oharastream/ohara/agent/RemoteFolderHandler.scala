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
import oharastream.ohara.common.util.CommonUtils

import scala.concurrent.{ExecutionContext, Future}

trait RemoteFolderHandler {
  /**
    * Validate remote folder exists and own
    * @param path remote folder path
    * @param executionContext thread pool
    * @return result message
    */
  def validateFolder(path: String)(
    implicit executionContext: ExecutionContext
  ): Future[RemoteNodeResponse]

  /**
    * Create folder for the remote node
    * @param path new folder path
    * @param executionContext thread pool
    * @return result message
    */
  def mkDir(path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse]

  /**
    * List folder info for the remote node
    * @param path remote folder path
    * @param executionContext thread pool
    * @return folder info for the list
    */
  def listDir(path: String)(implicit executionContext: ExecutionContext): Future[Seq[FolderInfo]]

  /**
    * Delete folder for the remote node
    * @param path delete folder path
    * @param executionContext thread pool
    * @return result message
    */
  def deleteDir(path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse]
}

object RemoteFolderHandler {
  def builder(): Builder = new Builder()

  class Builder private[agent] extends oharastream.ohara.common.pattern.Builder[RemoteFolderHandler] {
    private var dataCollie: DataCollie = _
    private var hostname: String       = _

    def hostname(hostname: String): Builder = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    def dataCollie(dataCollie: DataCollie): Builder = {
      this.dataCollie = dataCollie
      this
    }

    override def build: RemoteFolderHandler = new RemoteFolderHandler() {
      override def validateFolder(
        path: String
      )(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse] =
        agent(hostname)
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
              RemoteNodeResponse("Folder validate failed, Please check folder exists and folder own UID is 1000")
            else RemoteNodeResponse("Folder validate success")
          }

      override def mkDir(path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse] =
        agent(hostname)
          .map { agent =>
            agent.execute(s"mkdir ${path}")
          }
          .map { result =>
            RemoteNodeResponse(result.getOrElse("create folder success"))
          }

      override def listDir(path: String)(implicit executionContext: ExecutionContext): Future[Seq[FolderInfo]] = {
        agent(hostname)
          .map { agent =>
            agent.execute("ls -l " + path + "|awk '{print $3\",\"$4\",\"$5\",\"$9}'")
          }
          .map { result =>
            result.getOrElse("").split("\n").filter(_.split(",").size == 4).map { record =>
              val values = record.split(",")
              FolderInfo(
                own = values(0),
                group = values(1),
                size = values(2),
                fileName = values(3)
              )
            }
          }
      }
      override def deleteDir(path: String)(implicit executionContext: ExecutionContext): Future[RemoteNodeResponse] =
        agent(hostname)
          .map { agent =>
            agent.execute(s"rm -rf ${path}")
          }
          .map { result =>
            RemoteNodeResponse(result.getOrElse("delete folder success"))
          }
    }

    private[this] def agent(hostname: String)(implicit executionContext: ExecutionContext): Future[Agent] = {
      dataCollie
        .value[Node](hostname)
        .map { node =>
          Agent.builder.hostname(node.hostname).user(node._user).password(node._password).port(node._port).build
        }
    }
  }
}

case class RemoteNodeResponse(message: String)

case class FolderInfo(own: String, group: String, size: String, fileName: String)
