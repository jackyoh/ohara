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

package oharastream.ohara.it.connector.smb

import oharastream.ohara.common.rule.OharaTest

case class ITSmbProps(hostname: String, port: Int, username: String, password: String, shareName: String) {
  def toMap: Map[String, String] =
    Map(
      IT_SMB_HOSTNAME_KEY   -> hostname,
      IT_SMB_PORT_KEY       -> port.toString,
      IT_SMB_USERNAME_KEY   -> username,
      IT_SMB_PASSWORD_KEY   -> password,
      IT_SMB_SHARE_NAME_KEY -> shareName
    ).filter(_._2.nonEmpty)
}

object ITSmbProps extends OharaTest {
  def apply(variables: Map[String, String]): ITSmbProps =
    ITSmbProps(
      hostname = variables.getOrElse(
        IT_SMB_HOSTNAME_KEY,
        skipTest(s"Please setting $IT_SMB_HOSTNAME_KEY properties")
      ),
      port = Integer.parseInt(
        variables.getOrElse(
          IT_SMB_PORT_KEY,
          skipTest(s"Please setting $IT_SMB_PORT_KEY properties")
        )
      ),
      username = variables.getOrElse(
        IT_SMB_USERNAME_KEY,
        skipTest(s"Please setting $IT_SMB_USERNAME_KEY properties")
      ),
      password = variables.getOrElse(
        IT_SMB_PASSWORD_KEY,
        skipTest(s"Please settting $IT_SMB_PASSWORD_KEY properties")
      ),
      shareName = variables.getOrElse(
        IT_SMB_SHARE_NAME_KEY,
        skipTest(s"Please setting $IT_SMB_SHARE_NAME_KEY properties")
      )
    )
}
