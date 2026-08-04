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
package org.apache.calcite.linq4j.function;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for {@link Functions}.
 */
class FunctionTest {
  /** Unit test for {@link Functions#filter}. */
  @Test void testFilter() {
    final List<String> abc = Arrays.asList("A", "B", "C", "D");
    // a miss, then a hit
    assertThat(Functions.filter(abc, v1 -> !v1.equals("B")),
        hasToString("[A, C, D]"));
    // a hit, then all misses
    assertThat(Functions.filter(abc, v1 -> v1.equals("A")),
        hasToString("[A]"));
    // two hits, then a miss
    assertThat(Functions.filter(abc, v1 -> !v1.equals("C")),
        hasToString("[A, B, D]"));
    assertSame(Collections.emptyList(),
        Functions.filter(abc, Functions.falsePredicate1()));
    assertSame(abc,
        Functions.filter(abc, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#exists}. */
  @Test void testExists() {
    final List<Integer> ints = Arrays.asList(1, 10, 2);
    final List<Integer> empty = Collections.emptyList();
    assertFalse(
        Functions.exists(ints, v1 -> v1 > 20));
    assertFalse(
        Functions.exists(empty, Functions.falsePredicate1()));
    assertFalse(
        Functions.exists(empty, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#all}. */
  @Test void testAll() {
    final List<Integer> ints = Arrays.asList(1, 10, 2);
    final List<Integer> empty = Collections.emptyList();
    assertFalse(
        Functions.all(ints, v1 -> v1 > 20));
    assertTrue(
        Functions.all(ints, v1 -> v1 < 20));
    assertFalse(
        Functions.all(ints, v1 -> v1 < 10));
    assertTrue(
        Functions.all(empty, Functions.falsePredicate1()));
    assertTrue(
        Functions.all(empty, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#compareMaps}. Maps are unordered, so the
   * comparison must not depend on insertion order, and must return 0 exactly
   * when the maps have equal contents; see
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7689">[CALCITE-7689]
   * MAP equality compares maps by insertion order</a>. */
  @Test void testCompareMaps() {
    // Equal contents, same and different insertion order
    assertThat(Functions.compareMaps(map(1, 2, 3, 4), map(1, 2, 3, 4)), is(0));
    assertThat(Functions.compareMaps(map(1, 2, 3, 4), map(3, 4, 1, 2)), is(0));
    assertThat(Functions.compareMaps(map(3, 4, 1, 2), map(1, 2, 3, 4)), is(0));

    // Different value for one key: unequal, antisymmetric
    final Map<Object, Object> a = map(1, 2, 3, 4);
    final Map<Object, Object> c = map(1, 2, 3, 5);
    assertTrue(Functions.compareMaps(a, c) < 0);
    assertTrue(Functions.compareMaps(c, a) > 0);

    // Different keys and different sizes
    assertTrue(Functions.compareMaps(map(1, 2), map(2, 2)) != 0);
    assertTrue(Functions.compareMaps(map(1, 2), map(1, 2, 3, 4)) < 0);
    assertTrue(Functions.compareMaps(map(1, 2, 3, 4), map(1, 2)) > 0);

    // The order must be transitive: b equals a, so b and a must compare to c
    // with the same sign
    final Map<Object, Object> b = map(3, 4, 1, 2);
    assertTrue(Functions.compareMaps(b, c) < 0);

    // Null values compare equal to each other and follow the same rule
    assertThat(Functions.compareMaps(map(1, null, 3, 4), map(3, 4, 1, null)),
        is(0));
    assertTrue(Functions.compareMaps(map(1, null, 3, 4), map(1, 2, 3, 4)) != 0);

    // Nested maps as values are also compared by contents
    assertThat(
        Functions.compareMaps(map("k", map(1, 2, 3, 4)),
            map("k", map(3, 4, 1, 2))),
        is(0));
  }

  /** Creates a {@link LinkedHashMap} whose iteration order is the order of
   * the given alternating keys and values. */
  private static Map<Object, Object> map(Object... kv) {
    final Map<Object, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      result.put(kv[i], kv[i + 1]);
    }
    return result;
  }

  /** Unit test for {@link Functions#generate}. */
  @Test void testGenerate() {
    final IntFunction<String> xx =
        new IntFunction<String>() {
          public String apply(int a0) {
            return a0 == 0 ? "0" : "x" + apply(a0 - 1);
          }
        };
    assertThat(Functions.generate(0, xx), hasToString("[]"));
    assertThat(Functions.generate(1, xx), hasToString("[0]"));
    assertThat(Functions.generate(3, xx), hasToString("[0, x0, xx0]"));
    try {
      final List<String> generate = Functions.generate(-2, xx);
      fail("expected error, got " + generate);
    } catch (IllegalArgumentException e) {
      // ok
    }
  }
}
