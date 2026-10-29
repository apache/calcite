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
package org.apache.calcite.schema.lookup;

import com.google.common.base.Strings;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Test for {@link LikePattern}.
 */
class LikePatternTest {
  @Test void testMatcher() {
    checkMatcher(true, "%", "abc");
    checkMatcher(true, "abc", "abc");
    checkMatcher(false, "abc", "abcd"); // trailing char fails match
    checkMatcher(false, "abc", "0abc"); // leading char fails match
    checkMatcher(false, "abc", "aBc"); // case-sensitive match
    checkMatcher(true, "a[b]c", "a[b]c"); // nothing special about brackets
    checkMatcher(true, "a$c", "a$c"); // nothing special about dollar
    checkMatcher(false, "a$", "a"); // nothing special about dollar
    checkMatcher(true, "a%c", "ac");
    checkMatcher(true, "a%c", "abbbc");
    checkMatcher(false, "a%c", "acccd");

    // escape using back-slash
    checkMatcher(true, "a\\%c", "a%c");
    checkMatcher(false, "a\\%c", "abc");
    checkMatcher(false, "a\\%c", "a\\%c");

    // multiple wild-cards
    checkMatcher(true, "a%c%d", "abcdaaad");
    checkMatcher(false, "a%c%d", "abcdc");
    checkMatcher(true, "%", "anything");
    checkMatcher(true, "meta%", "metadata");
    checkMatcher(false, "meta%", "other");
    checkMatcher(true, "%data", "metadata");
    checkMatcher(true, "m%a", "metadata");
    checkMatcher(false, "m%z", "metadata");
    checkMatcher(true, "a%%b", "ab");
    checkMatcher(true, "a%b%c", "axxbyyc");
    checkMatcher(false, "a%b%c", "axxcyyb");
    checkMatcher(true, "%a%", "a");
    checkMatcher(true, "%a%", "aa");
    checkMatcher(true, "%a%", "aaa");
    checkMatcher(false, "a%a", "a");
    checkMatcher(true, "%a%a", "aa");
    checkMatcher(false, "a%a%a", "aa");
    checkMatcher(true, "%a%ab%", "aab");
    checkMatcher(true, "%a%ab%", "abab");
    checkMatcher(true, "%ab%abc%", "ababc");
    checkMatcher(true, "%ab%abc%", "aababc");
    checkMatcher(true, "%ab%abc%", "abxabc");
    checkMatcher(true, "%ab%abc%", "ababcd");
    checkMatcher(false, "%ab%abc%", "ababxc");

    // A run of '%' wild-cards is equivalent to a single one
    checkMatcher(true, "%%", "");
    checkMatcher(true, "m%%%a", "metadata");
    checkMatcher(false, "m%%%z", "metadata");

    // An escaped '%' is a literal, and does not end a run
    checkMatcher(true, "100\\%", "100%");
    checkMatcher(false, "100\\%", "100x");
    checkMatcher(true, "%\\%%", "50% off");
    checkMatcher(false, "%\\%%", "half off");
    checkMatcher(true, "a\\%", "a%");
    checkMatcher(false, "a\\%", "ab");
  }

  private void checkMatcher(boolean b, String pattern, String abc) {
    assertEquals(b, LikePattern.matcher(pattern).apply(abc));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7906">[CALCITE-7906]
   * LikePattern matching time grows exponentially with the number of '%' wildcards</a>. */
  @Test void testMatcherManyWildcards() {
    final String name = Strings.repeat("a", 40);
    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
      // Consecutive wildcards
      assertThat(
          LikePattern.matcher(Strings.repeat("%", 10) + "x").apply(name),
          is(false));
      // Wildcards separated by literals
      assertThat(
          LikePattern.matcher(Strings.repeat("%a", 10) + "%x").apply(name),
          is(false));
    });
  }
}
