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

package oharastream.ohara.kafka.connector.csv.source;

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.kafka.connector.TaskSetting;

public interface CsvSourceConfig {

  /** @return the count of tasks */
  int total();

  /** @return the hash of this task */
  int hash();

  /** @return the folder containing the csv files */
  String inputFolder();

  /** @return size of file cache */
  int fileCacheSize();

  /** @return the folder storing the processed csv files */
  Optional<String> completedFolder();

  /** @return the folder storing the corrupt csv files */
  Optional<String> errorFolder();

  /** @return the string encode to parse csv files */
  String encode();

  /** @return target topics */
  List<String> topicNames();

  /** @return the rules to control the output records */
  List<Column> columns();

  static CsvSourceConfig of(TaskSetting setting) {
    return of(setting, setting.columns());
  }

  /**
   * this method enable us to override the columns used in csv job.
   *
   * @param setting task settings
   * @param columns new columns
   * @return csv configs
   */
  @VisibleForTesting
  static CsvSourceConfig of(TaskSetting setting, List<Column> columns) {
    return new CsvSourceConfig() {

      @Override
      public int total() {
        return setting.intValue(TASK_TOTAL_KEY);
      }

      @Override
      public int hash() {
        return setting.intValue(TASK_HASH_KEY);
      }

      @Override
      public String inputFolder() {
        return setting.stringValue(INPUT_FOLDER_KEY);
      }

      @Override
      public int fileCacheSize() {
        return setting.intOption(FILE_CACHE_SIZE_KEY).orElse(FILE_CACHE_SIZE_DEFAULT);
      }

      @Override
      public Optional<String> completedFolder() {
        return setting.stringOption(COMPLETED_FOLDER_KEY);
      }

      @Override
      public Optional<String> errorFolder() {
        return setting.stringOption(ERROR_FOLDER_KEY);
      }

      @Override
      public String encode() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.stringOption(FILE_ENCODE_KEY).orElse(FILE_ENCODE_DEFAULT);
      }

      @Override
      public List<String> topicNames() {
        return setting.topicNames();
      }

      @Override
      public List<Column> columns() {
        return Collections.unmodifiableList(columns);
      }
    };
  }
}
