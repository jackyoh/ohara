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

import java.sql.Timestamp

import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector.RowSourceRecord

trait BaseQueryHandler extends Releasable {
  /**
    * Query table data from the database
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return
    */
  protected[source] def queryData(
    key: String,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord]

  /**
    * Confirm data write to the topic
    * The start timestamp and stop timestamp range can't change.
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  protected[source] def completed(key: String, startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean

  /**
    * Query first row from the database table
    * @param timestampColumnName timestamp column name
    * @return timestamp
    */
  protected[source] def tableFirstTimestampValue(timestampColumnName: String): Timestamp

  /**
    * Query current timestamp from the database
    * @return timestamp
    */
  protected[source] def current(): Timestamp
}
