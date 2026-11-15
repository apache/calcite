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
package org.apache.calcite.adapter.geode.rel;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the identifier validation {@link GeodeTable} applies to
 * every SQL-controlled name (aliases, field paths, aggregate calls, ORDER BY
 * entries) before concatenating it into an OQL statement.
 *
 * <p>SQL quoted identifiers may contain arbitrary characters, and OQL has
 * no identifier quoting mechanism, so anything that is not a plain
 * identifier/path must be rejected: otherwise a SQL-supplied alias could
 * shift the structure of the emitted OQL (for example a nested query on a
 * region that is not exposed by the schema). The tests do not require a
 * running Geode cluster.
 */
class GeodeTableOqlValidationTest {

  @Test void plainAliasesAccepted() {
    assertThat(GeodeTable.checkOqlIdentifier("state"), is("state"));
    assertThat(GeodeTable.checkOqlIdentifier("_id"), is("_id"));
    // Calcite-generated aliases contain '$'
    assertThat(GeodeTable.checkOqlIdentifier("EXPR$0"), is("EXPR$0"));
  }

  @Test void aliasWithEmbeddedOqlRejected() {
    // OQL supports method invocation: without validation a SQL-supplied
    // alias could call methods on server-side objects.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            GeodeTable.checkOqlIdentifier(
                "f, e.getClass().getClassLoader() AS leak"));
    assertThat(e.getMessage(), containsString("only plain identifiers"));
  }

  @Test void aliasWithNestedQueryRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlIdentifier(
            "x FROM (SELECT s.ssn FROM /SecretRegion s)"));
  }

  @Test void aliasWithWhitespaceOrEmptyRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlIdentifier("a b"));
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlIdentifier(""));
    // Null is a caller bug and surfaces as NullPointerException, not as a shape violation
    assertThrows(NullPointerException.class, () ->
        GeodeTable.checkOqlIdentifier(null));
  }

  @Test void fieldPathsAccepted() {
    assertThat(GeodeTable.checkOqlFieldPath("pop"), is("pop"));
    assertThat(GeodeTable.checkOqlFieldPath("primaryAddress.postalCode"),
        is("primaryAddress.postalCode"));
    assertThat(GeodeTable.checkOqlFieldPath("loc[0]"), is("loc[0]"));
  }

  @Test void fieldPathWithEmbeddedPredicateRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlFieldPath(
            "pop AND (SELECT COUNT(*) FROM /SecretRegion) > 0"));
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlFieldPath("loc['0']"));
  }

  @Test void aggregateCallsAccepted() {
    assertThat(GeodeTable.checkOqlAggregateCall("SUM(pop)"), is("SUM(pop)"));
    assertThat(GeodeTable.checkOqlAggregateCall("COUNT(itemNumber)"),
        is("COUNT(itemNumber)"));
    assertThat(GeodeTable.checkOqlAggregateCall("AVG(loc[1])"),
        is("AVG(loc[1])"));
  }

  @Test void aggregateCallWithExtraContentRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlAggregateCall("SUM(pop), e.getClass()"));
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlAggregateCall("SUM(pop) FROM /Other"));
  }

  @Test void orderByEntriesAccepted() {
    assertThat(GeodeTable.checkOqlOrderByEntry("state ASC"), is("state ASC"));
    assertThat(GeodeTable.checkOqlOrderByEntry("loc[0] DESC"),
        is("loc[0] DESC"));
  }

  @Test void orderByEntryWithExtraContentRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlOrderByEntry(
            "state ASC LIMIT 1 -- extra DESC"));
    assertThrows(IllegalArgumentException.class, () ->
        GeodeTable.checkOqlOrderByEntry("state"));
  }
}
