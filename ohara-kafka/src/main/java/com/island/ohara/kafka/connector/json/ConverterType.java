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

package com.island.ohara.kafka.connector.json;

/**
 * this is used to represent the internal converter of kafka connector. It marshal/unmarshal the
 * record to/from byte array. If you want to enable producer/consumer to "understand" the data
 * generated by kafka connector. You should assign the None type since the other type will "wrap"
 * the records generated by your connector. Hence, the data you receive is not what you had pushed
 * to topic by connector.
 */
public enum ConverterType {
  NONE("org.apache.kafka.connect.converters.ByteArrayConverter"),
  JSON("org.apache.kafka.connect.json.JsonConverter");
  private final String className;

  ConverterType(String className) {
    this.className = className;
  }

  public String className() {
    return className;
  }
}
