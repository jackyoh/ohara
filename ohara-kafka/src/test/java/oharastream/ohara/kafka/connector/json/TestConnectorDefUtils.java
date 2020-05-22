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

package oharastream.ohara.kafka.connector.json;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.PropGroup;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.util.CommonUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestConnectorDefUtils extends OharaTest {

  @Test
  public void testToConfigDefKey() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .displayName(CommonUtils.randomString())
            .group(CommonUtils.randomString())
            .reference(SettingDef.Reference.WORKER_CLUSTER)
            .orderInGroup(111)
            .optional(CommonUtils.randomString())
            .documentation(CommonUtils.randomString())
            .build();
    Assert.assertEquals(settingDef.key(), ConnectorDefUtils.toConfigKey(settingDef).name);
    Assert.assertEquals(
        settingDef.valueType().name(), ConnectorDefUtils.toConfigKey(settingDef).type.name());
    Assert.assertEquals(settingDef.group(), ConnectorDefUtils.toConfigKey(settingDef).group);
    Assert.assertEquals(
        settingDef.orderInGroup(), ConnectorDefUtils.toConfigKey(settingDef).orderInGroup);
    Assert.assertEquals(
        settingDef.defaultString(), ConnectorDefUtils.toConfigKey(settingDef).defaultValue);
    Assert.assertEquals(
        settingDef.documentation(), ConnectorDefUtils.toConfigKey(settingDef).documentation);
  }

  @Test
  public void testToConfigDefKey2() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .displayName(CommonUtils.randomString())
            .group(CommonUtils.randomString())
            .reference(SettingDef.Reference.WORKER_CLUSTER)
            .orderInGroup(111)
            .optional(CommonUtils.randomString())
            .documentation(CommonUtils.randomString())
            .build();

    SettingDef another =
        ConnectorDefUtils.of(convertConfigKey(ConnectorDefUtils.toConfigKey(settingDef)));
    Assert.assertEquals(settingDef, another);
  }

  /**
   * This method is clone from kafka 1.0.2 source code. We need this method to test our conversion.
   */
  private static ConfigKeyInfo convertConfigKey(ConfigDef.ConfigKey configKey) {
    String name = configKey.name;
    ConfigDef.Type type = configKey.type;
    String typeName = configKey.type.name();

    boolean required = false;
    String defaultValue;
    if (ConfigDef.NO_DEFAULT_VALUE.equals(configKey.defaultValue)) {
      defaultValue = null;
      required = true;
    } else {
      defaultValue = ConfigDef.convertToString(configKey.defaultValue, type);
    }
    String importance = configKey.importance.name();
    String documentation = configKey.documentation;
    String group = configKey.group;
    int orderInGroup = configKey.orderInGroup;
    String width = configKey.width.name();
    String displayName = configKey.displayName;
    List<String> dependents = configKey.dependents;
    return new ConfigKeyInfo(
        name,
        typeName,
        required,
        defaultValue,
        importance,
        documentation,
        group,
        orderInGroup,
        width,
        displayName,
        dependents);
  }

  @Test
  public void testDefaultKeyConverter() {
    Assert.assertEquals(
        ConverterType.NONE.className(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test
  public void testDefaultValueConverter() {
    Assert.assertEquals(
        ConverterType.NONE.className(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test
  public void testDefaultHeaderConverter() {
    Assert.assertEquals(
        ConverterType.NONE.className(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.HEADER_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseStaleConfigKeyInfo() {
    ConfigKeyInfo fake = Mockito.mock(ConfigKeyInfo.class);
    Mockito.when(fake.displayName()).thenReturn(CommonUtils.randomString());
    ConnectorDefUtils.of(fake);
  }

  @Test(expected = NullPointerException.class)
  public void parseStaleConfigKeyInfo2() {
    ConfigKeyInfo fake = Mockito.mock(ConfigKeyInfo.class);
    Mockito.when(fake.displayName()).thenReturn(null);
    ConnectorDefUtils.of(fake);
  }

  @Test
  public void testTableValidator() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(SettingDef.Type.TABLE)
            .build();
    ConfigDef.ConfigKey key = ConnectorDefUtils.toConfigKey(settingDef);
    Assert.assertNotNull(key.validator);
    key.validator.ensureValid(settingDef.key(), null);
    Assert.assertThrows(
        ConfigException.class, () -> key.validator.ensureValid(settingDef.key(), 123));
    Assert.assertThrows(
        ConfigException.class,
        () ->
            key.validator.ensureValid(
                settingDef.key(), Collections.singletonList(CommonUtils.randomString())));

    key.validator.ensureValid(
        settingDef.key(),
        PropGroup.ofColumns(
                Collections.singletonList(
                    Column.builder()
                        .name(CommonUtils.randomString())
                        .dataType(DataType.BOOLEAN)
                        .build()))
            .toJsonString());
  }

  @Test
  public void testTypeConversion() {
    Stream.of(SettingDef.Type.values()).forEach(ConnectorDefUtils::toType);
  }

  @Test
  public void checkReturnBySettingDefShouldBeSame() {
    ConnectorDefUtils.DEFAULT.values().forEach(setting -> Assert.assertNotNull(setting.checker()));
  }

  @Test
  public void testSerialization() {
    ConnectorDefUtils.DEFAULT
        .values()
        .forEach(
            setting -> {
              SettingDef copy = (SettingDef) Serializer.OBJECT.from(Serializer.OBJECT.to(setting));
              Assert.assertEquals(setting, copy);
            });
  }

  @Test
  public void testConnectorClass() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION))
            .count());
  }

  @Test
  public void testTopics() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.TOPIC_NAMES_DEFINITION))
            .count());
  }

  @Test
  public void testColumns() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.COLUMNS_DEFINITION))
            .count());
  }

  @Test
  public void testKeyConverter() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.KEY_CONVERTER_DEFINITION))
            .count());
  }

  @Test
  public void testValueConverter() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.VALUE_CONVERTER_DEFINITION))
            .count());
  }

  @Test
  public void testWorkerClusterName() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION))
            .count());
  }

  @Test
  public void testNumberOfTasks() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION))
            .count());
  }

  @Test
  public void mustHaveTable() {
    Assert.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(definition -> definition.valueType().equals(SettingDef.Type.TABLE))
            .count());
  }

  @Test
  public void testPropKeys() {
    ConnectorDefUtils.DEFAULT
        .values()
        .forEach(
            definition -> {
              if (definition.valueType().equals(SettingDef.Type.TABLE)) {
                Assert.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.ORDER_KEY)));
                Assert.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_DATA_TYPE_KEY)));
                Assert.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_NAME_KEY)));
                Assert.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_NEW_NAME_KEY)));
              } else Assert.assertTrue(definition.tableKeys().isEmpty());
            });
  }

  @Test
  public void checkDuplicate() {
    Assert.assertEquals(
        ConnectorDefUtils.DEFAULT.size(),
        ConnectorDefUtils.DEFAULT.values().stream()
            .map(SettingDef::key)
            .collect(Collectors.toSet())
            .size());
  }

  @Test
  public void testConnectorNameSetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key());
    Assert.assertEquals(setting.necessary(), SettingDef.Necessary.RANDOM_DEFAULT);
    Assert.assertFalse(setting.internal());
    Assert.assertFalse(setting.hasDefault());
    Assert.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assert.assertTrue(setting.tableKeys().isEmpty());
    Assert.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testConnectorKeySetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key());
    Assert.assertEquals(setting.necessary(), SettingDef.Necessary.REQUIRED);
    Assert.assertTrue(setting.internal());
    Assert.assertFalse(setting.hasDefault());
    Assert.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assert.assertEquals(SettingDef.Type.OBJECT_KEY, setting.valueType());
    Assert.assertTrue(setting.tableKeys().isEmpty());
    Assert.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testTagsSetting() {
    SettingDef setting = ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.TAGS_DEFINITION.key());
    Assert.assertEquals(setting.necessary(), SettingDef.Necessary.OPTIONAL);
    Assert.assertFalse(setting.internal());
    Assert.assertFalse(setting.hasDefault());
    Assert.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assert.assertTrue(setting.tableKeys().isEmpty());
    Assert.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testTopicKeysSetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key());
    Assert.assertEquals(setting.necessary(), SettingDef.Necessary.OPTIONAL);
    Assert.assertFalse(setting.internal());
    Assert.assertFalse(setting.hasDefault());
    Assert.assertEquals(SettingDef.Reference.TOPIC, setting.reference());
    Assert.assertTrue(setting.tableKeys().isEmpty());
    Assert.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }
}
