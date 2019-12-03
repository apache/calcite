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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for {@link Functions}.
 */
public class FunctionTest {
  /** Unit test for {@link Functions#filter}. */
  @Test public void testFilter() {
    final List<String> abc = Arrays.asList("A", "B", "C", "D");
    // a miss, then a hit
    assertEquals("[A, C, D]",
        Functions.filter(abc, v1 -> !v1.equals("B")).toString());
    // a hit, then all misses
    assertEquals("[A]",
        Functions.filter(abc, v1 -> v1.equals("A")).toString());
    // two hits, then a miss
    assertEquals("[A, B, D]",
        Functions.filter(abc, v1 -> !v1.equals("C")).toString());
    assertSame(Collections.emptyList(),
        Functions.filter(abc, Functions.falsePredicate1()));
    assertSame(abc,
        Functions.filter(abc, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#exists}. */
  @Test public void testExists() {
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
  @Test public void testAll() {
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

  /** Unit test for {@link Functions#generate}. */
  @Test public void testGenerate() {
    final Function1<Integer, String> xx =
        new Function1<Integer, String>() {
          public String apply(Integer a0) {
            return a0 == 0 ? "0" : "x" + apply(a0 - 1);
          }
        };
    assertEquals(
        "[]", Functions.generate(0, xx).toString());
    assertEquals(
        "[0]", Functions.generate(1, xx).toString());
    assertEquals(
        "[0, x0, xx0]", Functions.generate(3, xx).toString());
    try {
      final List<String> generate = Functions.generate(-2, xx);
      fail("expected error, got " + generate);
    } catch (IllegalArgumentException e) {
      // ok
    }
  }
}
