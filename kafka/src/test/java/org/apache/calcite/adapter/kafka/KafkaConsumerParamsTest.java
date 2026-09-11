/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@code consumer.params} filtering in
 * {@link KafkaTableFactory}: parameters that make the Kafka client load
 * classes named in the model must be rejected by default.
 */
class KafkaConsumerParamsTest {
  /** Set by {@link NotADeserializer}'s static initializer. A class named as
   * a deserializer in {@code consumer.params} must not have any of its code
   * run unless it implements {@code Deserializer}. */
  static final AtomicBoolean NOT_A_DESERIALIZER_INITIALIZED = new AtomicBoolean(false);

  /** Stands in for an arbitrary untrusted class named as a deserializer. */
  public static class NotADeserializer {
    static {
      NOT_A_DESERIALIZER_INITIALIZED.set(true);
    }
  }

  private static Map<String, String> params(String key, String value) {
    final Map<String, String> params = new HashMap<>();
    params.put(key, value);
    return params;
  }

  @Test void testHarmlessConnectivityParamsAllowed() {
    final Map<String, String> params = new HashMap<>();
    params.put("group.id", "g");
    params.put("max.poll.records", "100");
    params.put("security.protocol", "PLAINTEXT");
    assertDoesNotThrow(() -> KafkaTableFactory.checkConsumerParams(params));
  }

  @Test void testJaasConfigRejected() {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            KafkaTableFactory.checkConsumerParams(
                params("sasl.jaas.config",
                    "com.sun.security.auth.module.JndiLoginModule required"
                        + " user.provider.url=\"ldap://example/o\";")));
    assertThat(e.getMessage(), containsString("sasl.jaas.config"));
  }

  @Test void testInterceptorClassesRejected() {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            KafkaTableFactory.checkConsumerParams(
                params("interceptor.classes", "com.bad.Interceptor")));
    assertThat(e.getMessage(), containsString("interceptor.classes"));
  }

  @Test void testCallbackHandlerClassRejected() {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            KafkaTableFactory.checkConsumerParams(
                params("sasl.client.callback.handler.class", "com.bad.Handler")));
    assertThat(e.getMessage(), containsString("sasl.client.callback.handler.class"));
  }

  /** A deserializer that is not actually a {@code Deserializer} is rejected
   * without being initialized. */
  @Test void testNonDeserializerClassRejectedUninitialized() {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            KafkaTableFactory.checkConsumerParams(
                params("value.deserializer", NotADeserializer.class.getName())));
    assertThat(e.getMessage(), containsString(NotADeserializer.class.getName()));
    assertThat("static initializer of a rejected class must not run",
        NOT_A_DESERIALIZER_INITIALIZED.get(), is(false));
  }

  /** A genuine {@code Deserializer} implementation is still accepted. */
  @Test void testRealDeserializerAllowed() {
    assertDoesNotThrow(() ->
        KafkaTableFactory.checkConsumerParams(
            params("key.deserializer", StringDeserializer.class.getName())));
  }
}
