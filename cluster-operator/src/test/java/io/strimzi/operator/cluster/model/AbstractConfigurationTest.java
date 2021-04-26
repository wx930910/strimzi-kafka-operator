/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.strimzi.operator.common.InvalidConfigParameterException;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.json.JsonObject;

public class AbstractConfigurationTest {

	public static AbstractConfiguration mockAbstractConfiguration3(JsonObject jsonOptions) {
		List<String> mockFieldVariableFORBIDDEN_PREFIXES = asList("forbidden.option");
		AbstractConfiguration mockInstance = mock(AbstractConfiguration.class,
				withSettings().useConstructor(jsonOptions, mockFieldVariableFORBIDDEN_PREFIXES)
						.defaultAnswer(Mockito.CALLS_REAL_METHODS));
		return mockInstance;
	}

	public static AbstractConfiguration mockAbstractConfiguration3(String configuration) {
		List<String> mockFieldVariableFORBIDDEN_PREFIXES = asList("forbidden.option");
		AbstractConfiguration mockInstance = mock(AbstractConfiguration.class,
				withSettings().useConstructor(configuration, mockFieldVariableFORBIDDEN_PREFIXES)
						.defaultAnswer(Mockito.CALLS_REAL_METHODS));
		return mockInstance;
	}

	public static AbstractConfiguration mockAbstractConfiguration1(String configuration) {
		List<String> mockFieldVariableFORBIDDEN_PREFIXES = null;
		Map<String, String> mockFieldVariableDEFAULTS = null;
		AbstractConfiguration mockInstance = mock(AbstractConfiguration.class,
				withSettings()
						.useConstructor(configuration, mockFieldVariableFORBIDDEN_PREFIXES, mockFieldVariableDEFAULTS)
						.defaultAnswer(Mockito.CALLS_REAL_METHODS));
		return mockInstance;
	}

	public static AbstractConfiguration mockAbstractConfiguration2(JsonObject jsonOptions) {
		List<String> mockFieldVariableFORBIDDEN_PREFIXES = null;
		Map<String, String> mockFieldVariableDEFAULTS = null;
		AbstractConfiguration mockInstance = mock(AbstractConfiguration.class,
				withSettings()
						.useConstructor(jsonOptions, mockFieldVariableFORBIDDEN_PREFIXES, mockFieldVariableDEFAULTS)
						.defaultAnswer(Mockito.CALLS_REAL_METHODS));
		return mockInstance;
	}

	static OrderedProperties createProperties(String... keyValues) {
		OrderedProperties properties = new OrderedProperties();
		for (int i = 0; i < keyValues.length; i += 2) {
			properties.addPair(keyValues[i], keyValues[i + 1]);
		}
		return properties;
	}

	OrderedProperties createWithDefaults(String... keyValues) {
		return createProperties(keyValues).addMapPairs(defaultConfiguration.asMap());
	}

	final private OrderedProperties defaultConfiguration = createProperties("default.option", "hello");

	@Test
	public void testConfigurationStringDefaults() {
		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1("");
		assertThat(config.asOrderedProperties(), is(defaultConfiguration));
	}

	@Test
	public void testConfigurationStringOverridingDefaults() {
		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1("default.option=world");
		assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world")));
	}

	@Test
	public void testConfigurationStringOverridingDefaultsWithMore() {
		AbstractConfiguration config = AbstractConfigurationTest
				.mockAbstractConfiguration1("default.option=world" + LINE_SEPARATOR + "var1=aaa" + LINE_SEPARATOR);
		assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world", "var1", "aaa")));
	}

	@Test
	public void testDefaultsFromJson() {
		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(new JsonObject());
		assertThat(config.asOrderedProperties(), is(defaultConfiguration));
	}

	@Test
	public void testJsonOverridingDefaults() {
		AbstractConfiguration config = AbstractConfigurationTest
				.mockAbstractConfiguration2(new JsonObject().put("default.option", "world"));
		assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world")));
	}

	@Test
	public void testJsonOverridingDefaultsWithMore() {
		AbstractConfiguration config = AbstractConfigurationTest
				.mockAbstractConfiguration2(new JsonObject().put("default.option", "world").put("var1", "aaa"));
		assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world", "var1", "aaa")));
	}

	@Test
	public void testEmptyConfigurationString() {
		String configuration = "";
		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration3(configuration);
		assertThat(config.getConfiguration().isEmpty(), is(true));
	}

	@Test
	public void testEmptyJson() {
		JsonObject configuration = new JsonObject();
		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration3(configuration);
		assertThat(config.getConfiguration().isEmpty(), is(true));
	}

	@Test
	public void testNonEmptyConfigurationString() {
		String configuration = "var1=aaa" + LINE_SEPARATOR + "var2=bbb" + LINE_SEPARATOR + "var3=ccc" + LINE_SEPARATOR;
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testNonEmptyJson() {
		JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc");
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testConfigurationStringWithDuplicates() {
		String configuration = "var1=aaa" + LINE_SEPARATOR + "var2=bbb" + LINE_SEPARATOR + "var3=ccc" + LINE_SEPARATOR
				+ "var2=ddd" + LINE_SEPARATOR;
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "ddd", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testJsonWithDuplicates() {
		JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("var2",
				"ddd");
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "ddd", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testConfigurationStringWithForbiddenKeys() {
		String configuration = "var1=aaa" + LINE_SEPARATOR + "var2=bbb" + LINE_SEPARATOR + "var3=ccc" + LINE_SEPARATOR
				+ "forbidden.option=ddd" + LINE_SEPARATOR;
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testConfigurationStringWithForbiddenKeysInUpperCase() {
		String configuration = "var1=aaa" + LINE_SEPARATOR + "var2=bbb" + LINE_SEPARATOR + "var3=ccc" + LINE_SEPARATOR
				+ "FORBIDDEN.OPTION=ddd" + LINE_SEPARATOR;
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration1(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testJsonWithForbiddenKeys() {
		JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc")
				.put("forbidden.option", "ddd");
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testJsonWithForbiddenKeysPrefix() {
		JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc")
				.put("forbidden.option.first", "ddd").put("forbidden.option.second", "ddd");
		OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc", "var2", "bbb", "var1", "aaa");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testJsonWithDifferentTypes() {
		JsonObject configuration = new JsonObject().put("var1", 1).put("var2", "bbb").put("var3",
				new JsonObject().put("xxx", "yyy"));
		try {
			AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
			fail("Expected it to throw an exception");
		} catch (InvalidConfigParameterException e) {
			assertThat(e.getKey(), is("var3"));
		}
	}

	@Test
	public void testWithHostPort() {
		JsonObject configuration = new JsonObject().put("option.with.port", "my-server:9092");
		OrderedProperties expectedConfiguration = createWithDefaults("option.with.port", "my-server:9092");

		AbstractConfiguration config = AbstractConfigurationTest.mockAbstractConfiguration2(configuration);
		assertThat(config.asOrderedProperties(), is(expectedConfiguration));
	}

	@Test
	public void testKafkaZookeeperTimeout() {
		Map<String, Object> conf = new HashMap<>();
		conf.put("valid", "validValue");
		conf.put("zookeeper.connection.whatever", "invalid");
		conf.put("security.invalid1", "invalid");
		conf.put("zookeeper.connection.timeout.ms", "42"); // valid
		conf.put("zookeeper.connection.timeout", "42"); // invalid

		KafkaConfiguration kc = new KafkaConfiguration(conf.entrySet());

		assertThat(kc.asOrderedProperties().asMap().get("valid"), is("validValue"));
		assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.whatever"), is(nullValue()));
		assertThat(kc.asOrderedProperties().asMap().get("security.invalid1"), is(nullValue()));
		assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout.ms"), is("42"));
		assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout"), is(nullValue()));
	}

	@Test
	public void testKafkaCipherSuiteOverride() {
		Map<String, Object> conf = new HashMap<>();
		conf.put("ssl.cipher.suites", "cipher1,cipher2,cipher3"); // valid

		KafkaConfiguration kc = new KafkaConfiguration(conf.entrySet());

		assertThat(kc.asOrderedProperties().asMap().get("ssl.cipher.suites"), is("cipher1,cipher2,cipher3"));
	}

	@Test
	public void testKafkaConnectHostnameVerification() {
		Map<String, Object> conf = new HashMap<>();
		conf.put("key.converter", "my.package.Converter"); // valid
		conf.put("ssl.endpoint.identification.algorithm", ""); // valid
		conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

		KafkaConnectConfiguration configuration = new KafkaConnectConfiguration(conf.entrySet());

		assertThat(configuration.asOrderedProperties().asMap().get("key.converter"), is("my.package.Converter"));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
	}

	@Test
	public void testKafkaMirrorMakerConsumerHostnameVerification() {
		Map<String, Object> conf = new HashMap<>();
		conf.put("compression.type", "zstd"); // valid
		conf.put("ssl.endpoint.identification.algorithm", ""); // valid
		conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

		KafkaMirrorMakerConsumerConfiguration configuration = new KafkaMirrorMakerConsumerConfiguration(
				conf.entrySet());

		assertThat(configuration.asOrderedProperties().asMap().get("compression.type"), is("zstd"));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
	}

	@Test
	public void testKafkaMirrorMakerProducerHostnameVerification() {
		Map<String, Object> conf = new HashMap<>();
		conf.put("compression.type", "zstd"); // valid
		conf.put("ssl.endpoint.identification.algorithm", ""); // valid
		conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

		KafkaMirrorMakerProducerConfiguration configuration = new KafkaMirrorMakerProducerConfiguration(
				conf.entrySet());

		assertThat(configuration.asOrderedProperties().asMap().get("compression.type"), is("zstd"));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
		assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
	}

	@Test
	public void testSplittingOfPrefixes() {
		String prefixes = "prefix1.field,prefix2.field , prefix3.field, prefix4.field,, ";
		List<String> prefixList = asList("prefix1.field", "prefix2.field", "prefix3.field", "prefix4.field");

		assertThat(AbstractConfiguration.splitPrefixesToList(prefixes).equals(prefixList), is(true));
	}
}

class TestConfiguration {
}

class TestConfigurationWithoutDefaults {

//    static {
//        FORBIDDEN_PREFIXES = asList(
//                "forbidden.option");
//    }

}
