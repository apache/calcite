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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test for {@link Functions}.
 */
public class FunctionTest {
  /** Unit test for {@link Functions#filter}. */
  @Test public void testFilter() {
    final List<String> abc = Arrays.asList("A", "B", "C", "D");
    // a miss, then a hit
    Assert.assertEquals("[A, C, D]",
        Functions.filter(abc, v1 -> !v1.equals("B")).toString());
    // a hit, then all misses
    Assert.assertEquals("[A]",
        Functions.filter(abc, v1 -> v1.equals("A")).toString());
    // two hits, then a miss
    Assert.assertEquals("[A, B, D]",
        Functions.filter(abc, v1 -> !v1.equals("C")).toString());
    Assert.assertSame(Collections.emptyList(),
        Functions.filter(abc, Functions.falsePredicate1()));
    Assert.assertSame(abc,
        Functions.filter(abc, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#exists}. */
  @Test public void testExists() {
    final List<Integer> ints = Arrays.asList(1, 10, 2);
    final List<Integer> empty = Collections.emptyList();
    Assert.assertFalse(
        Functions.exists(ints, v1 -> v1 > 20));
    Assert.assertFalse(
        Functions.exists(empty, Functions.falsePredicate1()));
    Assert.assertFalse(
        Functions.exists(empty, Functions.truePredicate1()));
  }

  /** Unit test for {@link Functions#all}. */
  @Test public void testAll() {
    final List<Integer> ints = Arrays.asList(1, 10, 2);
    final List<Integer> empty = Collections.emptyList();
    Assert.assertFalse(
        Functions.all(ints, v1 -> v1 > 20));
    Assert.assertTrue(
        Functions.all(ints, v1 -> v1 < 20));
    Assert.assertFalse(
        Functions.all(ints, v1 -> v1 < 10));
    Assert.assertTrue(
        Functions.all(empty, Functions.falsePredicate1()));
    Assert.assertTrue(
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
    Assert.assertEquals(
        "[]", Functions.generate(0, xx).toString());
    Assert.assertEquals(
        "[0]", Functions.generate(1, xx).toString());
    Assert.assertEquals(
        "[0, x0, xx0]", Functions.generate(3, xx).toString());
    try {
      final List<String> generate = Functions.generate(-2, xx);
      Assert.fail("expected error, got " + generate);
    } catch (IllegalArgumentException e) {
      // ok
    }
  }
}

// End FunctionTest.java
