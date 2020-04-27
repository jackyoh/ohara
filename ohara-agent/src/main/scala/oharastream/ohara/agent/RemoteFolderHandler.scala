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

import oharastream.ohara.client.Enum
import oharastream.ohara.client.configurator.v0.NodeApi.Node
import oharastream.ohara.common.util.CommonUtils

import scala.concurrent.{ExecutionContext, Future}

trait RemoteFolderHandler {
  /**
    * Test whether the exist folder
    * @param path folder path
    * @param executionContext thread pool
    * @return True is exist, false is not exist
    */
  def exists(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, Boolean]]

  /**
    * Get the folder UID value for the remote node
    * @param path folder path
    * @param executionContext thread pool
    * @return UID value
    */
  def folderUID(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, Int]]

  /**
    * Create folder for the remote node
    * @param path new folder path
    * @param executionContext thread pool
    * @return result message
    */
  def mkDir(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, RemoteFolderCommandResult]]

  /**
    * List folder info for the remote node
    * @param path remote folder path
    * @param executionContext thread pool
    * @return folder info for the list
    */
  def listDir(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, Seq[FolderInfo]]]

  /**
    * Delete folder for the remote node
    * @param path delete folder path
    * @param executionContext thread pool
    * @return result message
    */
  def deleteDir(path: String)(
    implicit executionContext: ExecutionContext
  ): Future[Map[String, RemoteFolderCommandResult]]
}

object RemoteFolderHandler {
  def builder(): Builder = new Builder()

  class Builder private[agent] extends oharastream.ohara.common.pattern.Builder[RemoteFolderHandler] {
    private var dataCollie: DataCollie = _
    private var hostnames: Seq[String] = _

    def hostNames(hostnames: Seq[String]): Builder = {
      this.hostnames = hostnames.map(hostname => CommonUtils.requireNonEmpty(hostname))
      this
    }

    def dataCollie(dataCollie: DataCollie): Builder = {
      if (this.dataCollie == null) throw new IllegalArgumentException("Please setting the dataCollie function")
      else this.dataCollie = dataCollie
      this
    }

    override def build: RemoteFolderHandler = new RemoteFolderHandler() {
      override def exists(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, Boolean]] =
        agent(hostnames).map { nodes =>
          nodes
            .map { agent =>
              val result = agent.execute(s"""
              |if [ -d "$path" ]; then
              |  echo "Exists"
              |else
              |  echo "NotExists"
              |fi
            """.stripMargin).getOrElse("").trim()
              (agent.hostname, result)
            }
            .map { result =>
              val isExists: Boolean = (result._2 == "Exists")
              (result._1, isExists)
            }
            .toMap
        }

      override def folderUID(path: String)(implicit executionContext: ExecutionContext): Future[Map[String, Int]] =
        agent(hostnames).map { nodes =>
          nodes
            .map { agent =>
              val folderName = path.split("/").last
              val result =
                agent.execute("ls -n " + path + "/../|grep " + folderName + "|awk '{print $3}'").getOrElse("").trim()
              (agent.hostname, result)
            }
            .map { result =>
              (result._1, result._2.toInt)
            }
            .toMap
        }

      override def mkDir(
        path: String
      )(implicit executionContext: ExecutionContext): Future[Map[String, RemoteFolderCommandResult]] =
        agent(hostnames).map { nodes =>
          nodes.map { agent =>
            val result = agent
              .execute(s"mkdir ${path}")
              .map(message => RemoteFolderCommandResult(RemoteFolderState.FAILED, message))
              .getOrElse(RemoteFolderCommandResult(RemoteFolderState.SUCCESS, "create folder success"))
            (agent.hostname, result)
          }.toMap
        }

      override def listDir(
        path: String
      )(implicit executionContext: ExecutionContext): Future[Map[String, Seq[FolderInfo]]] =
        agent(hostnames).map { nodes =>
          nodes
            .map { agent =>
              (agent.hostname, agent.execute("ls -l " + path + "|awk '{print $3\",\"$4\",\"$5\",\"$9}'"))
            }
            .map { result =>
              val folderInfo = result._2.getOrElse("").split("\n").filter(_.split(",").size == 4).toSeq.map { record =>
                val values = record.split(",")
                FolderInfo(
                  owner = values(0),
                  group = values(1),
                  size = values(2),
                  fileName = values(3)
                )
              }
              (result._1, folderInfo)
            }
            .toMap
        }

      override def deleteDir(
        path: String
      )(implicit executionContext: ExecutionContext): Future[Map[String, RemoteFolderCommandResult]] =
        agent(hostnames)
          .map { nodes =>
            nodes.map { agent =>
              val folderNotExists = agent.execute(s"""
                |if [ ! -d "$path" ]; then
                |  echo "NotExists"
                |fi
              """.stripMargin).getOrElse("").trim()
              val remoteResponse =
                if (folderNotExists == "NotExists")
                  RemoteFolderCommandResult(RemoteFolderState.FAILED, "Folder is not exists")
                else
                  agent
                    .execute(s"rm -rf ${path}")
                    .map(message => RemoteFolderCommandResult(RemoteFolderState.FAILED, message))
                    .getOrElse(RemoteFolderCommandResult(RemoteFolderState.SUCCESS, "Delete folder success"))
              (agent.hostname, remoteResponse)
            }.toMap
          }
    }

    private[this] def agent(hostnames: Seq[String])(implicit executionContext: ExecutionContext): Future[Seq[Agent]] = {
      Future
        .sequence(hostnames.map(hostname => dataCollie.value[Node](hostname)))
        .map { nodes =>
          nodes.map { node =>
            Agent.builder
              .hostname(node.hostname)
              .user(node._user)
              .password(node._password)
              .port(node._port)
              .build
          }
        }
    }
  }
}

case class RemoteFolderCommandResult(state: RemoteFolderState, message: String)

case class FolderInfo(owner: String, group: String, size: String, fileName: String)

sealed abstract class RemoteFolderState(val name: String)
object RemoteFolderState extends Enum[RemoteFolderState] {
  case object SUCCESS extends RemoteFolderState("SUCCESS")

  case object FAILED extends RemoteFolderState("FAILED")
}
