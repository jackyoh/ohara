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

package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.kafka.connector.TaskSetting
import scala.jdk.CollectionConverters._

class TestJDBCSourceConnectorIncrementTimeRange(timestampInfo: TimestampInfo)
    extends BasicTestJDBCSourceConnectorTimeRange(timestampInfo: TimestampInfo) {
  override protected val jdbcSourceConnectorProps: JDBCSourceConnectorConfig =
    JDBCSourceConnectorConfig(
      TaskSetting.of(
        Map(
          DB_URL                          -> db.url,
          DB_USERNAME                     -> db.user,
          DB_PASSWORD                     -> db.password,
          DB_TABLENAME                    -> tableName,
          INCREMENT_TIMESTAMP_COLUMN_NAME -> s"$incrementColumnName,$timestampColumnName",
          TASK_TOTAL_KEY                  -> "0",
          TASK_HASH_KEY                   -> "0"
        ).asJava
      )
    )
}
