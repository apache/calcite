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

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.apache.calcite.adapter.elasticsearch.ElasticsearchVersion.fromString;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Basic tests for parsing Elasticsearch version in different formats.
 */
class ElasticsearchVersionTest {

  @Test void versions() {
    assertThat(ElasticsearchVersion.ES2, is(fromString("2.3.4")));
    assertThat(ElasticsearchVersion.ES2, is(fromString("2.0.0")));
    assertThat(ElasticsearchVersion.ES5, is(fromString("5.6.1")));
    assertThat(ElasticsearchVersion.ES6, is(fromString("6.0.1")));
    assertThat(ElasticsearchVersion.ES7, is(fromString("7.0.1")));
    assertThat(ElasticsearchVersion.UNKNOWN, is(fromString("111.0.1")));
    assertThat(ElasticsearchVersion.UNKNOWN, is(fromString("2020.12.12")));

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
      fromString(version);
      fail(String.format(Locale.ROOT, "Should fail for version %s", version));
    } catch (IllegalArgumentException ignore) {
      // expected
    }
  }
}
