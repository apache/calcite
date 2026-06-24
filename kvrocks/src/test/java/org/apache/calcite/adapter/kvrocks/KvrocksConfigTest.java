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
package org.apache.calcite.adapter.kvrocks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link KvrocksConfig}.
 *
 * <p>These tests do not require a running Kvrocks instance.
 */
class KvrocksConfigTest {

  @Test void getters() {
    KvrocksConfig config = new KvrocksConfig("localhost", 6666, 0, "secret");
    assertEquals("localhost", config.getHost());
    assertEquals(6666, config.getPort());
    assertEquals(0, config.getDatabase());
    assertEquals("secret", config.getPassword());
  }

  @Test void nullPasswordIsAllowed() {
    KvrocksConfig config = new KvrocksConfig("127.0.0.1", 6379, 1, null);
    assertEquals("127.0.0.1", config.getHost());
    assertEquals(6379, config.getPort());
    assertEquals(1, config.getDatabase());
    assertNull(config.getPassword());
  }

  @Test void namespaceTokenTakesPrecedenceOverPassword() {
    KvrocksConfig config =
        new KvrocksConfig("localhost", 6666, 0,
            "administrator-token", "namespace-token");

    assertEquals("administrator-token", config.getPassword());
    assertEquals("namespace-token", config.getNamespace());
    assertEquals("namespace-token", config.getAuthToken());
  }

  @Test void passwordIsUsedWithoutNamespaceToken() {
    KvrocksConfig config =
        new KvrocksConfig("localhost", 6666, 0, "administrator-token", null);

    assertNull(config.getNamespace());
    assertEquals("administrator-token", config.getAuthToken());
  }
}
