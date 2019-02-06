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
package org.apache.calcite.adapter.elasticsearch;

import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Basic tests for parsing ES version in different formats
 */
public class ElasticsearchVersionTest {

  @Test
  public void versions() {
    assertEquals(ElasticsearchVersion.fromString("2.3.4"), ElasticsearchVersion.ES2);
    assertEquals(ElasticsearchVersion.fromString("2.0.0"), ElasticsearchVersion.ES2);
    assertEquals(ElasticsearchVersion.fromString("5.6.1"), ElasticsearchVersion.ES5);
    assertEquals(ElasticsearchVersion.fromString("6.0.1"), ElasticsearchVersion.ES6);
    assertEquals(ElasticsearchVersion.fromString("7.0.1"), ElasticsearchVersion.ES7);
    assertEquals(ElasticsearchVersion.fromString("111.0.1"), ElasticsearchVersion.UNKNOWN);
    assertEquals(ElasticsearchVersion.fromString("2020.12.12"), ElasticsearchVersion.UNKNOWN);

    assertFails("");
    assertFails(".");
    assertFails(".1.2");
    assertFails("1.2");
    assertFails("0");
    assertFails("b");
    assertFails("a.b");
    assertFails("aa");
    assertFails("a.b.c");
    assertFails("2.2");
    assertFails("a.2");
    assertFails("2.2.0a");
    assertFails("2a.2.0");
  }

  private static void assertFails(String version) {
    try {
      ElasticsearchVersion.fromString(version);
      fail(String.format(Locale.ROOT, "Should fail for version %s", version));
    } catch (IllegalArgumentException ignore) {
      // expected
    }
  }
}

// End ElasticsearchVersionTest.java
