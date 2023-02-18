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
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performs a randomized test of {@link EnumerableDefaults#orderBy(Enumerable, Function1, Comparator, int, int)}.
 */
class LimitSortTest {

  /** Row class. */
  private static class Row {
    String key;
    int index;

    @Override public String toString() {
      return this.key + "/" + this.index;
    }
  }

  private Stream<Row> rowStream(long seed) {
    Random rnd = new Random(seed);
    int n = rnd.nextInt(1_000_000);
    return IntStream.range(0, n).mapToObj(i -> {
      int a = n < 2 ? 0 : rnd.nextInt(n / 2);
      String k = Integer.toString(a, Character.MAX_RADIX);
      Row r = new Row();
      r.key = "" + k;
      r.index = i;
      return r;
    });
  }

  private Enumerable<Row> enumerable(long seed) {
    return Linq4j.asEnumerable(() -> this.rowStream(seed).iterator());
  }

  @Test void test() {
    for (int i = 0; i < 5; i++) {
      long seed = System.nanoTime() ^ System.currentTimeMillis();
      try {
        this.randomizedTest(seed);
      } catch (AssertionError e) {
        // replace with AssertionFailedError
        throw new RuntimeException("Failed for seed " + seed, e);
      }
    }
  }

  private void randomizedTest(final long seed) {
    Random rnd = new Random(seed);
    int fetch = rnd.nextInt(10_000) + 1;
    int tmp = rnd.nextInt(10_000);
    int offset = Math.max(0, (int) (tmp - .1 * tmp));

    Comparator<String> cmp = Comparator.<String>naturalOrder()::compare;
    Enumerable<Row> ordered = EnumerableDefaults.orderBy(
        this.enumerable(seed),
        s -> s.key,
        cmp,
        offset, fetch
    );

    List<Row> result = ordered.toList();
    assertTrue(
        result.size() <= fetch,
        "Fetch " + fetch + " has not been respected, result size was " + result.size()
            + ", offset " + offset);

    // check result is sorted correctly
    for (int i = 1; i < result.size(); i++) {
      Row left = result.get(i - 1);
      Row right = result.get(i);
      // use left < right instead of <=, as rows might not appear twice
      assertTrue(isSmaller(left, right),
          "The following elements have not been ordered correctly: " + left + " " + right);
    }

    // check offset and fetch size have been respected
    Row first;
    Row last;
    if (result.isEmpty()) {
      // may happen if the offset is bigger than the number of items
      first = null;
      last = null;
    } else {
      first = result.get(0);
      last = result.get(result.size() - 1);
    }

    int totalItems = 0;
    int actOffset = 0;
    int actFetch = 0;
    for (Row r : (Iterable<Row>) this.rowStream(seed)::iterator) {
      totalItems++;
      if (isSmaller(r, first)) {
        actOffset++;
      } else if (isSmallerEq(r, last)) {
        actFetch++;
      }
    }

    // we can skip at most 'totalItems'
    int expOffset = Math.min(offset, totalItems);
    assertEquals(expOffset, actOffset, "Offset has not been respected.");
    // we can only fetch items if there are enough
    int expFetch = Math.min(totalItems - expOffset, fetch);
    assertEquals(expFetch, actFetch, "Fetch has not been respected.");
  }

  /** A comparison function that takes the order of creation into account. */
  private static boolean isSmaller(Row left, Row right) {
    if (right == null) {
      return true;
    }

    int c = left.key.compareTo(right.key);
    if (c != 0) {
      return c < 0;
    }
    return left.index < right.index;
  }

  /** See {@link #isSmaller(Row, Row)}. */
  private static boolean isSmallerEq(Row left, Row right) {
    if (right == null) {
      return true;
    }

    int c = left.key.compareTo(right.key);
    if (c != 0) {
      return c < 0;
    }
    return left.index <= right.index;
  }
}
