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
package org.apache.calcite.sql.validate;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link SqlNameMatchers}.
 */
class SqlNameMatchersTest {

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6539">[CALCITE-6539]
   * Improve did-you-mean suggestions for misspelled SQL identifiers</a>. */
  @Test void testBestMatchesSkipCaseAndWhitespaceOnlyCandidates() {
    assertThat(SqlNameMatchers.bestMatches("Path", ImmutableList.of("PATH")),
        is(ImmutableList.<String>of()));
    assertThat(SqlNameMatchers.bestMatches("path", ImmutableList.of(" path ")),
        is(ImmutableList.<String>of()));
    assertThat(SqlNameMatchers.bestMatches("path ", ImmutableList.of("path")),
        is(ImmutableList.<String>of()));
    assertThat(SqlNameMatchers.bestMatches("path", ImmutableList.of(" patch ")),
        is(ImmutableList.of(" patch ")));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6539">[CALCITE-6539]
   * Improve did-you-mean suggestions for misspelled SQL identifiers</a>.
   *
   * <p>Prefix/suffix expansions by one character are legitimate typos
   * (e.g., "NAM" → "NAME") and should be suggested. */
  @Test void testBestMatchesSuggestOneCharacterPrefixOrSuffixExpansions() {
    assertThat(SqlNameMatchers.bestMatches("ab", ImmutableList.of("abc")),
        is(ImmutableList.of("abc")));
    assertThat(SqlNameMatchers.bestMatches("ab", ImmutableList.of("zab")),
        is(ImmutableList.of("zab")));
    assertThat(SqlNameMatchers.bestMatches("nam", ImmutableList.of("name")),
        is(ImmutableList.of("name")));
    assertThat(SqlNameMatchers.bestMatches("empn", ImmutableList.of("empno")),
        is(ImmutableList.of("empno")));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6539">[CALCITE-6539]
   * Improve did-you-mean suggestions for misspelled SQL identifiers</a>.
   *
   * <p>For object/table names: digit-only differences (TABLE2 → TABLE1) and
   * different digit counts (TABLE2 → TABLEX) are suppressed.
   * For column names: different digit counts are allowed. */
  @Test void testBestObjectMatchRejectsDigitOnlyDifferences() {
    assertThat(SqlNameMatchers.bestObjectMatch("TABLE2", ImmutableList.of("TABLE1")),
        nullValue());
    assertThat(SqlNameMatchers.bestObjectMatch("TABLE2", ImmutableList.of("TABLEX")),
        nullValue());
    assertThat(SqlNameMatchers.bestObjectMatch("TABLE2", ImmutableList.of("TABLF2")),
        is("TABLF2"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6539">[CALCITE-6539]
   * Improve did-you-mean suggestions for misspelled SQL identifiers</a>.
   *
   * <p>For column/field names, digit count differences should not suppress
   * suggestions (e.g., "empno" → "empno2" is valid). */
  @Test void testBestMatchAllowsDigitCountDifferencesForColumns() {
    // Column names: digit count difference is allowed
    assertThat(SqlNameMatchers.bestMatch("empno", ImmutableList.of("empno2")),
        is("empno2"));
    assertThat(SqlNameMatchers.bestMatch("col", ImmutableList.of("col1")),
        is("col1"));
    // Object names: digit count difference is still rejected
    assertThat(SqlNameMatchers.bestObjectMatch("empno", ImmutableList.of("empno2")),
        nullValue());
  }
}
