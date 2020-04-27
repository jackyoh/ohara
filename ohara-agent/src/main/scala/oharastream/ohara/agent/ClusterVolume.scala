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

import oharastream.ohara.client.configurator.v0.VolumeApi.VolumeState
import oharastream.ohara.common.setting.ObjectKey

/**
  * cluster volume. The interface of volume on cluster that it offers key and nodeNames.
  * @param group group
  * @param name name
  * @param driver driver
  * @param path path
  * @param error path
  * @param nodeNames node hosting this volume
  */
case class ClusterVolume(
  group: String,
  name: String,
  driver: String,
  path: String,
  state: Option[VolumeState],
  error: Option[String],
  nodeNames: Set[String]
) {
  /**
    * a helper method used to generate the key of this data.
    * @return key
    */
  def key: ObjectKey = ObjectKey.of(group, name)
}