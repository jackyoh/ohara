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

package oharastream.ohara.common.setting;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.exception.ConfigException;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestSettingDef extends OharaTest {

  @Test(expected = NullPointerException.class)
  public void nullKey() {
    SettingDef.builder().key(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyKey() {
    SettingDef.builder().key("");
  }

  @Test(expected = NullPointerException.class)
  public void nullDefaultWithString() {
    SettingDef.builder().optional((String) null);
  }

  @Test(expected = NullPointerException.class)
  public void nullDefaultWithDuration() {
    SettingDef.builder().optional((Duration) null);
  }

  @Test(expected = NullPointerException.class)
  public void nullDocumentation() {
    SettingDef.builder().documentation(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyDocumentation() {
    SettingDef.builder().documentation("");
  }

  @Test(expected = NullPointerException.class)
  public void nullReference() {
    SettingDef.builder().reference(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullGroup() {
    SettingDef.builder().group(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyGroup() {
    SettingDef.builder().group("");
  }

  @Test(expected = NullPointerException.class)
  public void nullDisplay() {
    SettingDef.builder().displayName(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyDisplay() {
    SettingDef.builder().displayName("");
  }

  @Test
  public void testOnlyKey() {
    String key = CommonUtils.randomString(5);
    SettingDef def = SettingDef.builder().key(key).build();
    Assert.assertEquals(key, def.key());
    Assert.assertNotNull(def.displayName());
    Assert.assertNotNull(def.documentation());
    Assert.assertNotNull(def.valueType());
    Assert.assertNotNull(def.group());
    Assert.assertNotNull(def.reference());
    // yep. the default value should be null
    Assert.assertFalse(def.hasDefault());
  }

  @Test
  public void testGetterWithEditableAndDefaultValue() {
    String key = CommonUtils.randomString(5);
    String displayName = CommonUtils.randomString(5);
    String group = CommonUtils.randomString(5);
    SettingDef.Reference reference = SettingDef.Reference.WORKER_CLUSTER;
    int orderInGroup = 100;
    String valueDefault = CommonUtils.randomString(5);
    String documentation = CommonUtils.randomString(5);
    SettingDef def =
        SettingDef.builder()
            .key(key)
            .displayName(displayName)
            .group(group)
            .reference(reference)
            .orderInGroup(orderInGroup)
            .optional(valueDefault)
            .documentation(documentation)
            .build();

    Assert.assertEquals(key, def.key());
    Assert.assertEquals(SettingDef.Type.STRING, def.valueType());
    Assert.assertEquals(displayName, def.displayName());
    Assert.assertEquals(group, def.group());
    Assert.assertEquals(reference, def.reference());
    Assert.assertEquals(orderInGroup, def.orderInGroup());
    Assert.assertEquals(valueDefault, def.defaultString());
    Assert.assertEquals(documentation, def.documentation());
    Assert.assertEquals(def.necessary(), SettingDef.Necessary.OPTIONAL);
    Assert.assertFalse(def.internal());
  }

  @Test
  public void testGetterWithoutEditableAndDefaultValue() {
    String key = CommonUtils.randomString(5);
    SettingDef.Type type = SettingDef.Type.TABLE;
    String displayName = CommonUtils.randomString(5);
    String group = CommonUtils.randomString(5);
    SettingDef.Reference reference = SettingDef.Reference.WORKER_CLUSTER;
    int orderInGroup = 100;
    String documentation = CommonUtils.randomString(5);
    SettingDef def =
        SettingDef.builder()
            .key(key)
            .required(type)
            .displayName(displayName)
            .group(group)
            .reference(reference)
            .orderInGroup(orderInGroup)
            .documentation(documentation)
            .permission(SettingDef.Permission.READ_ONLY)
            .internal()
            .build();

    Assert.assertEquals(key, def.key());
    Assert.assertEquals(type, def.valueType());
    Assert.assertEquals(displayName, def.displayName());
    Assert.assertEquals(group, def.group());
    Assert.assertEquals(reference, def.reference());
    Assert.assertEquals(orderInGroup, def.orderInGroup());
    Assert.assertFalse(def.hasDefault());
    Assert.assertEquals(documentation, def.documentation());
    Assert.assertEquals(def.necessary(), SettingDef.Necessary.REQUIRED);
    Assert.assertTrue(def.internal());
  }

  @Test
  public void testTableChecker() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(
                Arrays.asList(
                    TableColumn.builder()
                        .name("a")
                        .recommendedItems(new HashSet<>(Arrays.asList("a0", "a1")))
                        .build(),
                    TableColumn.builder().name("b").build()))
            .build();
    // there is default value so null is ok
    settingDef.checker().accept(null);
    // illegal format
    Assert.assertThrows(ConfigException.class, () -> settingDef.checker().accept(123));
    // illegal format
    Assert.assertThrows(
        ConfigException.class, () -> settingDef.checker().accept(Collections.emptyList()));
    // neglect column "b"
    Assert.assertThrows(
        ConfigException.class,
        () ->
            settingDef
                .checker()
                .accept(Collections.singletonList(Collections.singletonMap("a", "c"))));

    // too many items
    Map<String, String> goodMap = new HashMap<>();
    goodMap.put("a", "a0");
    goodMap.put("b", "c");
    settingDef.checker().accept(PropGroup.of(Collections.singletonList(goodMap)).toJsonString());

    Map<String, String> illegalColumnMap = new HashMap<>(goodMap);
    illegalColumnMap.put("dddd", "fff");
    Assert.assertThrows(
        ConfigException.class,
        () ->
            settingDef
                .checker()
                .accept(PropGroup.of(Collections.singletonList(illegalColumnMap)).toJsonString()));
  }

  @Test
  public void testDurationChecker() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.DURATION)
            .build();
    Assert.assertThrows(ConfigException.class, () -> settingDef.checker().accept(null));
    Assert.assertThrows(ConfigException.class, () -> settingDef.checker().accept(123));
    Assert.assertThrows(
        ConfigException.class, () -> settingDef.checker().accept(Collections.emptyList()));
    settingDef.checker().accept(Duration.ofHours(3).toString());
    settingDef.checker().accept("10 MILLISECONDS");
    settingDef.checker().accept("10 SECONDS");
  }

  @Test
  public void testSetDisplayName() {
    String displayName = CommonUtils.randomString();
    SettingDef settingDef =
        SettingDef.builder()
            .displayName(displayName)
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.STRING)
            .build();
    Assert.assertEquals(displayName, settingDef.displayName());
  }

  @Test
  public void testPortType() {
    SettingDef s =
        SettingDef.builder().required(SettingDef.Type.REMOTE_PORT).key("port.key").build();
    // pass
    s.checker().accept(100);
    Assert.assertThrows(ConfigException.class, () -> s.checker().accept(-1));
    Assert.assertThrows(ConfigException.class, () -> s.checker().accept(0));
    Assert.assertThrows(ConfigException.class, () -> s.checker().accept(100000000));
  }

  @Test
  public void testTagsType() {
    SettingDef s = SettingDef.builder().required(SettingDef.Type.TAGS).key("tags.key").build();
    // pass
    s.checker().accept("{\"a\": \"b\"}");
    s.checker().accept("{\"123\":456}");
    s.checker().accept(Collections.emptyList());
    // not a jsonObject
    Assert.assertThrows(
        ConfigException.class, () -> s.checker().accept(CommonUtils.randomString()));
    Assert.assertThrows(ConfigException.class, () -> s.checker().accept("{abc}"));
    Assert.assertThrows(ConfigException.class, () -> s.checker().accept("{\"123\"}"));
  }

  @Test
  public void testSerialization() {
    SettingDef setting =
        SettingDef.builder().required(SettingDef.Type.TAGS).key("tags.key").build();
    SettingDef copy = (SettingDef) Serializer.OBJECT.from(Serializer.OBJECT.to(setting));
    Assert.assertEquals(setting, copy);
  }

  @Test
  public void testTopicKeysType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.OBJECT_KEYS)
            .build();
    // pass
    def.checker()
        .accept(
            JsonUtils.toString(
                Collections.singleton(
                    TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString()))));
    // empty array is illegal
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("[]"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("{}"));
    Assert.assertThrows(
        ConfigException.class, () -> def.checker().accept(CommonUtils.randomString()));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(100000000));
  }

  @Test
  public void testDuration() {
    Duration duration = Duration.ofHours(10);
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optional(duration).build();
    Assert.assertEquals(def.defaultDuration(), duration);
    Assert.assertTrue(
        def.toJsonString(),
        def.toJsonString()
            .contains("\"defaultValue\":" + "\"" + duration.toMillis() + " milliseconds\""));
  }

  @Test(expected = ConfigException.class)
  public void testRejectNullValue() {
    SettingDef.builder().key(CommonUtils.randomString()).build().checker().accept(null);
  }

  @Test
  public void testOptionNullValue() {
    // pass
    SettingDef.builder()
        .key(CommonUtils.randomString())
        .optional(SettingDef.Type.STRING)
        .build()
        .checker()
        .accept(null);
  }

  @Test
  public void testOptionNullValueWithDefault() {
    // pass
    SettingDef.builder()
        .key(CommonUtils.randomString())
        .optional("abc")
        .build()
        .checker()
        .accept(null);
  }

  @Test
  public void testBooleanType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.BOOLEAN)
            .build();
    // only accept "true" or "false"
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("aaa"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(123));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(null));
    def.checker().accept(false);
    def.checker().accept("true");
    // case in-sensitive
    def.checker().accept("FaLse");

    // optional definition
    SettingDef defOption =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(SettingDef.Type.BOOLEAN)
            .build();
    // only accept "true" or "false"
    Assert.assertThrows(ConfigException.class, () -> defOption.checker().accept("aaa"));
    Assert.assertThrows(ConfigException.class, () -> defOption.checker().accept(123));
    // since we don't have any default value, the "null" will be passed since it is optional
    defOption.checker().accept(null);
    defOption.checker().accept(false);
    defOption.checker().accept("true");
    // case in-sensitive
    defOption.checker().accept("FaLse");
  }

  @Test
  public void testStringType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.STRING)
            .build();

    def.checker().accept("aaa");
    def.checker().accept(111);
  }

  @Test
  public void testStringTypeWithRecommendedValues() {
    Set<String> recommendedValues =
        new HashSet<>(
            Arrays.asList(
                CommonUtils.randomString(),
                CommonUtils.randomString(),
                CommonUtils.randomString()));

    // required with recommended values(default is null)
    SettingDef settingDef1 =
        SettingDef.builder().key(CommonUtils.randomString()).required(recommendedValues).build();
    Assert.assertEquals(settingDef1.recommendedValues(), recommendedValues);
  }

  @Test
  public void testShortType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.SHORT)
            .build();

    def.checker().accept(111);

    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(11111111111L));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testIntType() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).required(SettingDef.Type.INT).build();

    def.checker().accept(111);

    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(11111111111L));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testLongType() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).required(SettingDef.Type.LONG).build();

    def.checker().accept(111);
    def.checker().accept(11111111111L);

    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testDoubleType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.DOUBLE)
            .build();

    def.checker().accept(111);
    def.checker().accept(11111111111L);
    def.checker().accept(2.2);

    Assert.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assert.assertThrows(ConfigException.class, () -> def.checker().accept(""));
  }

  @Test
  public void testArrayType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.ARRAY)
            .build();
    // pass
    def.checker().accept("[gg]");
    def.checker().accept("[\"aa\", \"bb\"]");
    def.checker().accept("[123]");
    def.checker().accept(Arrays.asList("ab", "cd"));

    // empty array is ok
    def.checker().accept(Collections.emptyList());
    def.checker().accept("[]");
  }

  @Test
  public void testKafkaArrayType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.ARRAY)
            .build();
    // since connector use "xxx,yyy" to use in kafka format
    // we should pass this (these cases are not json array)
    def.checker().accept("abc");
    def.checker().accept(111);
    def.checker().accept("null");
    def.checker().accept("abc,def");
    def.checker().accept("123 , 456");

    // empty string means empty list, it is ok
    def.checker().accept("");
  }

  @Test
  public void testBindingPort() throws IOException {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.BINDING_PORT)
            .build();
    def.checker().accept(CommonUtils.availablePort());

    int port = CommonUtils.availablePort();
    try (ServerSocket server = new ServerSocket(port)) {
      Assert.assertThrows(ConfigException.class, () -> def.checker().accept(server.getLocalPort()));
    }
    def.checker().accept(port);
  }

  @Test
  public void doubleUnderScoreIsIllegal() {
    SettingDef.builder().key("aaa").build();
    Assert.assertThrows(
        IllegalArgumentException.class, () -> SettingDef.builder().key("aaa__").build());
  }

  @Test(expected = NullPointerException.class)
  public void nullRecommendedValues() {
    SettingDef.builder().optional("aa", null);
  }

  @Test(expected = NullPointerException.class)
  public void nullBlacklist() {
    SettingDef.builder().blacklist(null);
  }

  @Test
  public void defaultBuild() {
    // all fields should have default value except for key
    SettingDef.builder().key(CommonUtils.randomString()).build();
  }

  @Test
  public void testShortDefault() {
    short defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    // jackson convert the number to int or long only
    Assert.assertEquals(copy.defaultShort(), defaultValue);
  }

  @Test
  public void testIntDefault() {
    int defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultInt(), defaultValue);
  }

  @Test
  public void testLongDefault() {
    long defaultValue = Long.MAX_VALUE;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultLong(), defaultValue);
  }

  @Test
  public void testDoubleDefault() {
    double defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultDouble(), defaultValue, 0);
  }

  @Test
  public void testStringDefault() {
    String defaultValue = "asd";
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultString(), defaultValue);
  }

  @Test
  public void testDurationDefault() {
    Duration defaultValue = Duration.ofMillis(12345);
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultDuration(), defaultValue);
  }

  @Test
  public void testBooleanDefault() {
    boolean defaultValue = true;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assert.assertEquals(copy.defaultBoolean(), defaultValue);
  }

  @Test
  public void testRecommendedValues() {
    Set<String> recommendedValues =
        new HashSet<>(
            Arrays.asList(
                CommonUtils.randomString(),
                CommonUtils.randomString(),
                CommonUtils.randomString()));
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(CommonUtils.randomString(), recommendedValues)
            .build();
    Assert.assertEquals(settingDef.recommendedValues(), recommendedValues);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpaceInKey() {
    SettingDef.builder().key(" ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEqualInKey() {
    SettingDef.builder().key("=");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQuoteInKey() {
    SettingDef.builder().key("\"");
  }

  @Test
  public void testDotInKey() {
    SettingDef.builder().key(".");
  }

  @Test
  public void testSlashInKey() {
    SettingDef.builder().key("-");
  }

  @Test
  public void testUnderLineInKey() {
    SettingDef.builder().key("_");
  }

  @Test
  public void testDefaultDuration() {
    Assert.assertEquals(
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(Duration.ofMillis(10))
            .build()
            .defaultValue()
            .get(),
        "10 milliseconds");

    Assert.assertEquals(
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(Duration.ofMillis(1))
            .build()
            .defaultValue()
            .get(),
        "1 millisecond");
  }

  @Test
  public void testOptionalPort() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optionalPort(12345).build();
    Assert.assertEquals(def.valueType(), SettingDef.Type.REMOTE_PORT);
    Assert.assertEquals(def.defaultPort(), 12345);
  }

  @Test
  public void testOptionalBindingPort() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optionalBindingPort(12345).build();
    Assert.assertEquals(def.valueType(), SettingDef.Type.BINDING_PORT);
    Assert.assertEquals(def.defaultPort(), 12345);
  }

  @Test
  public void testNameStringRegex() {
    Assert.assertFalse("_".matches(SettingDef.NAME_STRING_REGEX));
    Assert.assertFalse("-".matches(SettingDef.NAME_STRING_REGEX));
    // upper case is illegal
    Assert.assertFalse("A".matches(SettingDef.NAME_STRING_REGEX));
    // dot is illegal
    Assert.assertTrue("a.".matches(SettingDef.NAME_STRING_REGEX));
    // the length limit is 25
    Assert.assertFalse(CommonUtils.randomString(100).matches(SettingDef.NAME_STRING_REGEX));
  }

  @Test
  public void testGroupStringRegex() {
    Assert.assertFalse("_".matches(SettingDef.GROUP_STRING_REGEX));
    Assert.assertFalse("-".matches(SettingDef.GROUP_STRING_REGEX));
    // upper case is illegal
    Assert.assertFalse("A".matches(SettingDef.GROUP_STRING_REGEX));
    // dot is illegal
    Assert.assertTrue("a.".matches(SettingDef.GROUP_STRING_REGEX));
    // the length limit is 25
    Assert.assertFalse(CommonUtils.randomString(100).matches(SettingDef.GROUP_STRING_REGEX));
  }

  @Test
  public void testHostnameRegex() {
    Assert.assertTrue("aAbB-".matches(SettingDef.HOSTNAME_REGEX));
    // dash is illegal
    Assert.assertFalse("a_".matches(SettingDef.HOSTNAME_REGEX));
    // dot is legal
    Assert.assertTrue("a.".matches(SettingDef.HOSTNAME_REGEX));
    // the length limit is 25
    Assert.assertFalse(CommonUtils.randomString(100).matches(SettingDef.HOSTNAME_REGEX));
  }

  @Test
  public void nullFieldShouldBeRemovedFromJsonString() {
    checkNullField(SettingDef.builder().key(CommonUtils.randomString()).build());
  }

  @Test
  public void nullFieldInConvertingJsonString() {
    checkNullField(
        SettingDef.of(SettingDef.builder().key(CommonUtils.randomString()).build().toJsonString()));
  }

  private static void checkNullField(SettingDef def) {
    Assert.assertFalse(def.toJsonString(), def.toJsonString().contains(SettingDef.REGEX_KEY));
    Assert.assertFalse(
        def.toJsonString(), def.toJsonString().contains(SettingDef.DEFAULT_VALUE_KEY));
    Assert.assertFalse(def.toJsonString(), def.toJsonString().contains(SettingDef.PREFIX_KEY));
  }

  @Test
  public void testDefaultShortOnBindingPort() {
    Assert.assertEquals(
        12345,
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optionalBindingPort(12345)
            .build()
            .defaultInt());
  }

  @Test
  public void testDefaultShortOnRemotePort() {
    Assert.assertEquals(
        12345,
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optionalRemotePort(12345)
            .build()
            .defaultInt());
  }
}
