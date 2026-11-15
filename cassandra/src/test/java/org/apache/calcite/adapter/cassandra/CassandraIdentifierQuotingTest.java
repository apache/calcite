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
package org.apache.calcite.adapter.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests documenting how {@link CqlIdentifier#fromInternal} is used in
 * {@link CassandraTable#query} to emit the keyspace, column-family, column
 * and alias tokens of a generated CQL {@code SELECT} statement. Any of
 * those pieces is copied verbatim into the query text, so a value that
 * contains punctuation or the {@code "} delimiter must be routed through
 * the driver so that it cannot alter the semantics of the emitted CQL.
 */
class CassandraIdentifierQuotingTest {

  /** Plain names round-trip unchanged. */
  @Test void plainNameUnquoted() {
    assertThat(CqlIdentifier.fromInternal("field").asCql(true), is("field"));
  }

  /** An uppercase name gets a double-quoted rendering so its case is
   * preserved on the server side. */
  @Test void mixedCaseGetsQuoted() {
    assertThat(CqlIdentifier.fromInternal("Field").asCql(true),
        is("\"Field\""));
  }

  /** A name containing a space cannot be emitted verbatim; the driver
   * wraps it in double quotes. */
  @Test void nameWithSpaceGetsQuoted() {
    assertThat(CqlIdentifier.fromInternal("a b").asCql(true),
        is("\"a b\""));
  }

  /** A double quote inside the value would otherwise terminate the
   * enclosing quoted identifier and shift the token boundary; the
   * driver escapes it by doubling. */
  @Test void embeddedDoubleQuoteIsDoubled() {
    assertThat(CqlIdentifier.fromInternal("a\"b").asCql(true),
        is("\"a\"\"b\""));
  }

  /** A trailing double quote must also be doubled or it would consume
   * the closing quote of the enclosing identifier. */
  @Test void trailingDoubleQuoteIsDoubled() {
    assertThat(CqlIdentifier.fromInternal("a\"").asCql(true),
        is("\"a\"\"\""));
  }

  /** Punctuation that would carry CQL structure (a comma, a
   * semicolon, a parenthesis) is neutralised by the same quoting. */
  @Test void punctuationIsQuoted() {
    assertThat(CqlIdentifier.fromInternal("a,b").asCql(true),
        is("\"a,b\""));
    assertThat(CqlIdentifier.fromInternal("a;b").asCql(true),
        is("\"a;b\""));
    assertThat(CqlIdentifier.fromInternal("a(b)").asCql(true),
        is("\"a(b)\""));
  }
}
