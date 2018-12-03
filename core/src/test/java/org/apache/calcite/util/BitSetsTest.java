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
package org.apache.calcite.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link org.apache.calcite.util.BitSets}.
 */
public class BitSetsTest {
  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#toIter(java.util.BitSet)}.
   */
  @Test public void testToIterBitSet() {
    BitSet bitSet = new BitSet();

    assertToIterBitSet("", bitSet);
    bitSet.set(0);
    assertToIterBitSet("0", bitSet);
    bitSet.set(1);
    assertToIterBitSet("0, 1", bitSet);
    bitSet.clear();
    bitSet.set(10);
    assertToIterBitSet("10", bitSet);
  }

  /**
   * Tests that iterating over a BitSet yields the expected string.
   *
   * @param expected Expected string
   * @param bitSet   Bit set
   */
  private void assertToIterBitSet(final String expected, BitSet bitSet) {
    StringBuilder buf = new StringBuilder();
    for (int i : BitSets.toIter(bitSet)) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append(Integer.toString(i));
    }
    assertEquals(expected, buf.toString());
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#toList(java.util.BitSet)}.
   */
  @Test public void testToListBitSet() {
    BitSet bitSet = new BitSet(10);
    assertEquals(BitSets.toList(bitSet), Collections.<Integer>emptyList());
    bitSet.set(5);
    assertEquals(BitSets.toList(bitSet), Arrays.asList(5));
    bitSet.set(3);
    assertEquals(BitSets.toList(bitSet), Arrays.asList(3, 5));
  }

  /**
   * Tests the method {@link org.apache.calcite.util.BitSets#of(int...)}.
   */
  @Test public void testBitSetOf() {
    assertEquals(
        BitSets.toList(BitSets.of(0, 4, 2)),
        Arrays.asList(0, 2, 4));
    assertEquals(
        BitSets.toList(BitSets.of()),
        Collections.<Integer>emptyList());
  }

  /**
   * Tests the method {@link org.apache.calcite.util.BitSets#range(int, int)}.
   */
  @Test public void testBitSetsRange() {
    assertEquals(
        BitSets.toList(BitSets.range(0, 4)),
        Arrays.asList(0, 1, 2, 3));
    assertEquals(
        BitSets.toList(BitSets.range(1, 4)),
        Arrays.asList(1, 2, 3));
    assertEquals(
        BitSets.toList(BitSets.range(2, 2)),
        Collections.<Integer>emptyList());
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#toArray(java.util.BitSet)}.
   */
  @Test public void testBitSetsToArray() {
    int[][] arrays = {{}, {0}, {0, 2}, {1, 65}, {100}};
    for (int[] array : arrays) {
      assertThat(BitSets.toArray(BitSets.of(array)), equalTo(array));
    }
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#union(java.util.BitSet, java.util.BitSet...)}.
   */
  @Test public void testBitSetsUnion() {
    assertThat(BitSets.union(BitSets.of(1), BitSets.of(3)).toString(),
        equalTo("{1, 3}"));
    assertThat(BitSets.union(BitSets.of(1), BitSets.of(3, 100)).toString(),
        equalTo("{1, 3, 100}"));
    assertThat(
        BitSets.union(BitSets.of(1), BitSets.of(2), BitSets.of(), BitSets.of(3))
            .toString(),
        equalTo("{1, 2, 3}"));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#contains(java.util.BitSet, java.util.BitSet)}.
   */
  @Test public void testBitSetsContains() {
    assertTrue(BitSets.contains(BitSets.range(0, 5), BitSets.range(2, 4)));
    assertTrue(BitSets.contains(BitSets.range(0, 5), BitSets.of(4)));
    assertFalse(BitSets.contains(BitSets.range(0, 5), BitSets.of(14)));
    assertFalse(BitSets.contains(BitSets.range(20, 25), BitSets.of(14)));
    final BitSet empty = BitSets.of();
    assertTrue(BitSets.contains(BitSets.range(20, 25), empty));
    assertTrue(BitSets.contains(empty, empty));
    assertFalse(BitSets.contains(empty, BitSets.of(0)));
    assertFalse(BitSets.contains(empty, BitSets.of(1)));
    assertFalse(BitSets.contains(empty, BitSets.of(1000)));
    assertTrue(BitSets.contains(BitSets.of(1, 4, 7), BitSets.of(1, 4, 7)));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#of(ImmutableIntList)}.
   */
  @Test public void testBitSetOfImmutableIntList() {
    ImmutableIntList list = ImmutableIntList.of();
    assertThat(BitSets.of(list), equalTo(new BitSet()));

    list = ImmutableIntList.of(2, 70, 5, 0);
    assertThat(BitSets.of(list), equalTo(BitSets.of(0, 2, 5, 70)));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.BitSets#previousClearBit(java.util.BitSet, int)}.
   */
  @Test public void testPreviousClearBit() {
    assertThat(BitSets.previousClearBit(BitSets.of(), 10), equalTo(10));
    assertThat(BitSets.previousClearBit(BitSets.of(), 0), equalTo(0));
    assertThat(BitSets.previousClearBit(BitSets.of(), -1), equalTo(-1));
    try {
      final int actual = BitSets.previousClearBit(BitSets.of(), -2);
      fail("expected exception, got " + actual);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    assertThat(BitSets.previousClearBit(BitSets.of(0, 1, 3, 4), 4), equalTo(2));
    assertThat(BitSets.previousClearBit(BitSets.of(0, 1, 3, 4), 3), equalTo(2));
    assertThat(BitSets.previousClearBit(BitSets.of(0, 1, 3, 4), 2), equalTo(2));
    assertThat(BitSets.previousClearBit(BitSets.of(0, 1, 3, 4), 1),
        equalTo(-1));
    assertThat(BitSets.previousClearBit(BitSets.of(1, 3, 4), 1), equalTo(0));
  }

  /**
   * Tests the method {@link BitSets#closure(java.util.SortedMap)}
   */
  @Test public void testClosure() {
    final SortedMap<Integer, BitSet> empty = new TreeMap<>();
    assertThat(BitSets.closure(empty), equalTo(empty));

    // Map with an an entry for each position.
    final SortedMap<Integer, BitSet> map = new TreeMap<>();
    map.put(0, BitSets.of(3));
    map.put(1, BitSets.of());
    map.put(2, BitSets.of(7));
    map.put(3, BitSets.of(4, 12));
    map.put(4, BitSets.of());
    map.put(5, BitSets.of());
    map.put(6, BitSets.of());
    map.put(7, BitSets.of());
    map.put(8, BitSets.of());
    map.put(9, BitSets.of());
    map.put(10, BitSets.of());
    map.put(11, BitSets.of());
    map.put(12, BitSets.of());
    final String original = map.toString();
    final String expected =
        "{0={3, 4, 12}, 1={}, 2={7}, 3={3, 4, 12}, 4={4, 12}, 5={}, 6={}, 7={7}, 8={}, 9={}, 10={}, 11={}, 12={4, 12}}";
    assertThat(BitSets.closure(map).toString(), equalTo(expected));
    assertThat("argument modified", map.toString(), equalTo(original));

    // Now a similar map with missing entries. Same result.
    final SortedMap<Integer, BitSet> map2 = new TreeMap<>();
    map2.put(0, BitSets.of(3));
    map2.put(2, BitSets.of(7));
    map2.put(3, BitSets.of(4, 12));
    map2.put(9, BitSets.of());
    final String original2 = map2.toString();
    assertThat(BitSets.closure(map2).toString(), equalTo(expected));
    assertThat("argument modified", map2.toString(), equalTo(original2));
  }
}

// End BitSetsTest.java
