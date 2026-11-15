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
package org.apache.calcite.adapter.geode.util;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the OQL identifier allowlist in {@link GeodeUtils}.
 *
 * <p>OQL provides no way to quote an identifier, so a name that is
 * copied verbatim into a generated query must match a strict
 * allowlist; otherwise it could shift the boundaries of the emitted
 * statement.
 */
class GeodeUtilsTest {

  @Test void simpleNameAccepted() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("field"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("_field"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("$field"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("Field1"), is(true));
  }

  @Test void syntheticExprNameAccepted() {
    // Calcite emits synthetic aggregate column names like EXPR$0; the
    // pattern must accept them so a pushed-down aggregate is not
    // rejected on legitimate input.
    assertThat(GeodeUtils.isSafeOqlIdentifier("EXPR$0"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("EXPR$10"), is(true));
  }

  @Test void dottedPathAccepted() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("a.b"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a.b.c"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a.$b"), is(true));
  }

  @Test void bracketedIndexAccepted() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[0]"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[123]"), is(true));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a.b[7]"), is(true));
    // A subscript may be followed by a further dotted field.
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[0].c"), is(true));
  }

  @Test void nullRejected() {
    assertThat(GeodeUtils.isSafeOqlIdentifier(null), is(false));
  }

  @Test void emptyRejected() {
    assertThat(GeodeUtils.isSafeOqlIdentifier(""), is(false));
  }

  @Test void leadingDigitRejected() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("1field"), is(false));
  }

  @Test void whitespaceRejected() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("a b"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a\tb"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a\nb"), is(false));
  }

  @Test void quotesRejected() {
    // A value containing a single or double quote would let a fragment
    // pushed as an alias terminate the surrounding OQL token.
    assertThat(GeodeUtils.isSafeOqlIdentifier("a'b"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a\"b"), is(false));
  }

  @Test void commaRejected() {
    // A comma in an alias would split the select list.
    assertThat(GeodeUtils.isSafeOqlIdentifier("a,b"), is(false));
  }

  @Test void punctuationRejected() {
    assertThat(GeodeUtils.isSafeOqlIdentifier("a-b"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a+b"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a b"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("(a)"), is(false));
  }

  @Test void bracketedNonIntegerRejected() {
    // Only integer subscripts are safe; anything else could carry
    // OQL punctuation into the emitted statement.
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[b]"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a['b']"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[]"), is(false));
    assertThat(GeodeUtils.isSafeOqlIdentifier("a[0"), is(false));
  }

  @Test void checkSafeOqlIdentifierReturnsName() {
    assertThat(GeodeUtils.checkSafeOqlIdentifier("field"), is("field"));
    assertThat(GeodeUtils.checkSafeOqlIdentifier("EXPR$0"), is("EXPR$0"));
  }

  @Test void checkSafeOqlIdentifierThrowsOnUnsafe() {
    RuntimeException e =
        assertThrows(RuntimeException.class,
            () -> GeodeUtils.checkSafeOqlIdentifier("a b"));
    assertThat(e.getMessage(),
        containsString("not a simple OQL identifier"));
  }

  @Test void checkSafeOqlIdentifierThrowsOnNull() {
    assertThrows(RuntimeException.class,
        () -> GeodeUtils.checkSafeOqlIdentifier(null));
  }
}
