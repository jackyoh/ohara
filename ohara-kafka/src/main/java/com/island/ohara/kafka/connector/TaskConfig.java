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

package com.island.ohara.kafka.connector;

import com.island.ohara.common.data.Column;
import com.island.ohara.common.util.CommonUtils;
import com.island.ohara.kafka.connector.json.ConnectorFormatter;
import com.island.ohara.kafka.connector.json.PropGroup;
import com.island.ohara.kafka.connector.json.PropGroups;
import com.island.ohara.kafka.connector.json.StringList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/** this class carries all required settings for row connectors. */
public class TaskConfig {
  public static TaskConfig of(Map<String, String> options) {
    return new TaskConfig(options);
  }

  private final Map<String, String> raw;

  private TaskConfig(Map<String, String> raw) {
    this.raw = CommonUtils.requireNonEmpty(raw);
    raw.keySet().forEach(CommonUtils::requireNonEmpty);
  }

  public String stringValue(String key) {
    if (raw.containsKey(key)) return raw.get(key);
    else throw new NoSuchElementException(key + " doesn't exist");
  }

  public long longValue(String key) {
    return Long.valueOf(stringValue(key));
  }

  public int intValue(String key) {
    return Integer.valueOf(stringValue(key));
  }

  public double doubleValue(String key) {
    return Double.valueOf(stringValue(key));
  }

  public boolean booleanValue(String key) {
    return Boolean.valueOf(stringValue(key));
  }

  public List<String> stringList(String key) {
    return Optional.ofNullable(raw.get(key))
        .map(StringList::ofKafkaList)
        .orElse(Collections.emptyList());
  }

  public String name() {
    return stringValue(ConnectorFormatter.NAME_KEY);
  }

  public List<String> topicNames() {
    return stringList(ConnectorFormatter.TOPIC_NAMES_KEY);
  }

  public List<PropGroup> propGroups(String key) {
    return Optional.ofNullable(raw.get(key))
        .map(PropGroups::ofJson)
        .orElse(Collections.emptyList());
  }

  public List<Column> columns() {
    return PropGroups.toColumns(propGroups(ConnectorFormatter.COLUMNS_KEY));
  }

  public Map<String, String> raw() {
    return Collections.unmodifiableMap(raw);
  }

  /**
   * Clone this TaskConfig with new setting. The new setting overwrite the old value if the key
   * exists.
   *
   * @param newConfig new setting
   * @return new TaskConfig
   */
  public TaskConfig append(Map<String, String> newConfig) {
    Map<String, String> raw = new HashMap<>(this.raw);
    raw.putAll(newConfig);
    return TaskConfig.of(raw);
  }
}
