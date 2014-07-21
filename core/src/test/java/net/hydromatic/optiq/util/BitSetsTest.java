/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.util;

import org.eigenbase.util.ImmutableIntList;

import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link net.hydromatic.optiq.util.BitSets}.
 */
public class BitSetsTest {
  /**
   * Tests the method
   * {@link net.hydromatic.optiq.util.BitSets#toIter(java.util.BitSet)}.
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
   * {@link net.hydromatic.optiq.util.BitSets#toList(java.util.BitSet)}.
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
   * Tests the method {@link net.hydromatic.optiq.util.BitSets#of(int...)}.
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
   * Tests the method {@link net.hydromatic.optiq.util.BitSets#range(int, int)}.
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
   * {@link net.hydromatic.optiq.util.BitSets#toArray(java.util.BitSet)}.
   */
  @Test public void testBitSetsToArray() {
    int[][] arrays = {{}, {0}, {0, 2}, {1, 65}, {100}};
    for (int[] array : arrays) {
      assertThat(BitSets.toArray(BitSets.of(array)), equalTo(array));
    }
  }

  /**
   * Tests the method
   * {@link net.hydromatic.optiq.util.BitSets#union(java.util.BitSet, java.util.BitSet...)}.
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
   * {@link net.hydromatic.optiq.util.BitSets#contains(java.util.BitSet, java.util.BitSet)}.
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
   * {@link net.hydromatic.optiq.util.BitSets#of(org.eigenbase.util.ImmutableIntList)}.
   */
  @Test public void testBitSetOfImmutableIntList() {
    ImmutableIntList list = ImmutableIntList.of();
    assertThat(BitSets.of(list), equalTo(new BitSet()));

    list = ImmutableIntList.of(2, 70, 5, 0);
    assertThat(BitSets.of(list), equalTo(BitSets.of(0, 2, 5, 70)));
  }
}

// End BitSetsTest.java

