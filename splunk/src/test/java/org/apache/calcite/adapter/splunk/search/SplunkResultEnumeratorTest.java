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
package org.apache.calcite.adapter.splunk.search;

import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl.SplunkResultEnumerator;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests {@link SplunkResultEnumerator}. */
class SplunkResultEnumeratorTest {
  /** Returns an enumerator over a CSV export with header {@code a,b} and the
   * single row {@code 1,2}, projecting the one field the caller wants. */
  private static SplunkResultEnumerator enumerator(String wantedField) {
    return new SplunkResultEnumerator(
        new ByteArrayInputStream("a,b\n1,2\n".getBytes(StandardCharsets.UTF_8)),
        Collections.singletonList(wantedField));
  }

  @Test void aWantedFieldInTheHeaderYieldsItsValue() {
    try (SplunkResultEnumerator e = enumerator("b")) {
      assertThat(e.moveNext(), is(true));
      assertThat(e.current(), is("2"));
    }
  }

  /** A row still counts when the header lacks the wanted field, and its value
   * is null. */
  @Test void aWantedFieldMissingFromTheHeaderYieldsNull() {
    try (SplunkResultEnumerator e = enumerator("c")) {
      assertThat(e.moveNext(), is(true));
      assertThat(e.current(), nullValue());
    }
  }
}
