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

import org.apache.calcite.runtime.Utilities;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.junit.Test;

import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link org.apache.calcite.util.ImmutableBitSet}.
 */
public class ImmutableBitSetTest {
  /** Tests the method {@link ImmutableBitSet#iterator()}. */
  @Test public void testIterator() {
    assertToIterBitSet("", ImmutableBitSet.of());
    assertToIterBitSet("0", ImmutableBitSet.of(0));
    assertToIterBitSet("0, 1", ImmutableBitSet.of(0, 1));
    assertToIterBitSet("10", ImmutableBitSet.of(10));
  }

  /**
   * Tests that iterating over an
   * {@link org.apache.calcite.util.ImmutableBitSet} yields the expected string.
   *
   * @param expected Expected string
   * @param bitSet   Bit set
   */
  private void assertToIterBitSet(String expected, ImmutableBitSet bitSet) {
    StringBuilder buf = new StringBuilder();
    for (int i : bitSet) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append(Integer.toString(i));
    }
    assertEquals(expected, buf.toString());
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#toList()}.
   */
  @Test public void testToList() {
    assertThat(ImmutableBitSet.of().toList(),
        equalTo(Collections.<Integer>emptyList()));
    assertThat(ImmutableBitSet.of(5).toList(), equalTo(Arrays.asList(5)));
    assertThat(ImmutableBitSet.of(3, 5).toList(), equalTo(Arrays.asList(3, 5)));
    assertThat(ImmutableBitSet.of(63).toList(), equalTo(Arrays.asList(63)));
    assertThat(ImmutableBitSet.of(64).toList(), equalTo(Arrays.asList(64)));
    assertThat(ImmutableBitSet.of(3, 63).toList(),
        equalTo(Arrays.asList(3, 63)));
    assertThat(ImmutableBitSet.of(3, 64).toList(),
        equalTo(Arrays.asList(3, 64)));
    assertThat(ImmutableBitSet.of(0, 4, 2).toList(),
        equalTo(Arrays.asList(0, 2, 4)));
  }

  /**
   * Tests the method {@link BitSets#range(int, int)}.
   */
  @Test public void testRange() {
    assertEquals(ImmutableBitSet.range(0, 4).toList(),
        Arrays.asList(0, 1, 2, 3));
    assertEquals(ImmutableBitSet.range(1, 4).toList(),
        Arrays.asList(1, 2, 3));
    assertEquals(ImmutableBitSet.range(4).toList(),
        Arrays.asList(0, 1, 2, 3));
    assertEquals(ImmutableBitSet.range(0).toList(),
        Collections.<Integer>emptyList());
    assertEquals(ImmutableBitSet.range(2, 2).toList(),
        Collections.<Integer>emptyList());

    assertThat(ImmutableBitSet.range(63, 66).toString(),
        equalTo("{63, 64, 65}"));
    assertThat(ImmutableBitSet.range(65, 68).toString(),
        equalTo("{65, 66, 67}"));
    assertThat(ImmutableBitSet.range(65, 65).toString(), equalTo("{}"));
    assertThat(ImmutableBitSet.range(65, 65).length(), equalTo(0));
    assertThat(ImmutableBitSet.range(65, 165).cardinality(), equalTo(100));

    // Same tests as above, using a builder.
    assertThat(ImmutableBitSet.builder().set(63, 66).build().toString(),
        equalTo("{63, 64, 65}"));
    assertThat(ImmutableBitSet.builder().set(65, 68).build().toString(),
        equalTo("{65, 66, 67}"));
    assertThat(ImmutableBitSet.builder().set(65, 65).build().toString(),
        equalTo("{}"));
    assertThat(ImmutableBitSet.builder().set(65, 65).build().length(),
        equalTo(0));
    assertThat(ImmutableBitSet.builder().set(65, 165).build().cardinality(),
        equalTo(100));

    final ImmutableBitSet e0 = ImmutableBitSet.range(0, 0);
    final ImmutableBitSet e1 = ImmutableBitSet.of();
    assertTrue(e0.equals(e1));
    assertThat(e0.hashCode(), equalTo(e1.hashCode()));

    // Empty builder returns the singleton empty set.
    assertTrue(ImmutableBitSet.builder().build() == ImmutableBitSet.of());
  }

  @Test public void testCompare() {
    final List<ImmutableBitSet> sorted = getSortedList();
    for (int i = 0; i < sorted.size(); i++) {
      for (int j = 0; j < sorted.size(); j++) {
        final ImmutableBitSet set0 = sorted.get(i);
        final ImmutableBitSet set1 = sorted.get(j);
        int c = set0.compareTo(set1);
        if (c == 0) {
          assertTrue(i == j || i == 3 && j == 4 || i == 4 && j == 3);
        } else {
          assertEquals(c, Utilities.compare(i, j));
        }
        assertEquals(c == 0, set0.equals(set1));
        assertEquals(c == 0, set1.equals(set0));
      }
    }
  }

  @Test public void testCompare2() {
    final List<ImmutableBitSet> sorted = getSortedList();
    Collections.sort(sorted, ImmutableBitSet.COMPARATOR);
    assertThat(sorted.toString(),
        equalTo("[{0, 1, 3}, {0, 1}, {1, 1000}, {1}, {1}, {2, 3}, {}]"));
  }

  private List<ImmutableBitSet> getSortedList() {
    return Arrays.asList(
        ImmutableBitSet.of(),
        ImmutableBitSet.of(0, 1),
        ImmutableBitSet.of(0, 1, 3),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(1, 1000),
        ImmutableBitSet.of(2, 3));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#toArray}.
   */
  @Test public void testToArray() {
    int[][] arrays = {{}, {0}, {0, 2}, {1, 65}, {100}};
    for (int[] array : arrays) {
      assertThat(ImmutableBitSet.of(array).toArray(), equalTo(array));
    }
  }

  /**
   * Tests the methods
   * {@link org.apache.calcite.util.ImmutableBitSet#toList} and
   * {@link org.apache.calcite.util.ImmutableBitSet#asList}.
   */
  @Test public void testAsList() {
    final List<ImmutableBitSet> list = getSortedList();
    for (ImmutableBitSet bitSet : list) {
      final IntList list1 = bitSet.toList();
      final List<Integer> listView = bitSet.asList();
      assertThat(list1.size(), equalTo(bitSet.cardinality()));
      assertThat(list1.toString(), equalTo(listView.toString()));
      assertTrue(list1.equals(listView));
      assertThat(list1.hashCode(), equalTo(listView.hashCode()));
    }
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#union(ImmutableBitSet)}.
   */
  @Test public void testUnion() {
    assertThat(ImmutableBitSet.of(1).union(ImmutableBitSet.of(3)).toString(),
        equalTo("{1, 3}"));
    assertThat(ImmutableBitSet.of(1).union(ImmutableBitSet.of(3, 100))
            .toString(),
        equalTo("{1, 3, 100}"));
    ImmutableBitSet x =
        ImmutableBitSet.builder(ImmutableBitSet.of(1))
            .addAll(ImmutableBitSet.of(2))
            .addAll(ImmutableBitSet.of())
            .addAll(ImmutableBitSet.of(3))
            .build();
    assertThat(x.toString(), equalTo("{1, 2, 3}"));
  }

  @Test public void testIntersect() {
    assertThat(ImmutableBitSet.of(1, 2, 3, 100, 200)
        .intersect(ImmutableBitSet.of(2, 100)).toString(), equalTo("{2, 100}"));
    assertTrue(ImmutableBitSet.of(1, 3, 5, 101, 20001)
        .intersect(ImmutableBitSet.of(2, 100)) == ImmutableBitSet.of());
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#contains(org.apache.calcite.util.ImmutableBitSet)}.
   */
  @Test public void testBitSetsContains() {
    assertTrue(ImmutableBitSet.range(0, 5)
        .contains(ImmutableBitSet.range(2, 4)));
    assertTrue(ImmutableBitSet.range(0, 5).contains(ImmutableBitSet.range(4)));
    assertFalse(ImmutableBitSet.range(0, 5).contains(ImmutableBitSet.of(14)));
    assertFalse(ImmutableBitSet.range(20, 25).contains(ImmutableBitSet.of(14)));
    final ImmutableBitSet empty = ImmutableBitSet.of();
    assertTrue(ImmutableBitSet.range(20, 25).contains(empty));
    assertTrue(empty.contains(empty));
    assertFalse(empty.contains(ImmutableBitSet.of(0)));
    assertFalse(empty.contains(ImmutableBitSet.of(1)));
    assertFalse(empty.contains(ImmutableBitSet.of(63)));
    assertFalse(empty.contains(ImmutableBitSet.of(64)));
    assertFalse(empty.contains(ImmutableBitSet.of(1000)));
    assertTrue(ImmutableBitSet.of(1, 4, 7)
        .contains(ImmutableBitSet.of(1, 4, 7)));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#of(org.apache.calcite.util.ImmutableIntList)}.
   */
  @Test public void testBitSetOfImmutableIntList() {
    ImmutableIntList list = ImmutableIntList.of();
    assertThat(ImmutableBitSet.of(list), equalTo(ImmutableBitSet.of()));

    list = ImmutableIntList.of(2, 70, 5, 0);
    assertThat(ImmutableBitSet.of(list),
        equalTo(ImmutableBitSet.of(0, 2, 5, 70)));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#previousClearBit(int)}.
   */
  @Test public void testPreviousClearBit() {
    assertThat(ImmutableBitSet.of().previousClearBit(10), equalTo(10));
    assertThat(ImmutableBitSet.of().previousClearBit(0), equalTo(0));
    assertThat(ImmutableBitSet.of().previousClearBit(-1), equalTo(-1));
    try {
      final int actual = ImmutableBitSet.of().previousClearBit(-2);
      fail("expected exception, got " + actual);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    assertThat(ImmutableBitSet.of(0, 1, 3, 4).previousClearBit(4), equalTo(2));
    assertThat(ImmutableBitSet.of(0, 1, 3, 4).previousClearBit(3), equalTo(2));
    assertThat(ImmutableBitSet.of(0, 1, 3, 4).previousClearBit(2), equalTo(2));
    assertThat(ImmutableBitSet.of(0, 1, 3, 4).previousClearBit(1),
        equalTo(-1));
    assertThat(ImmutableBitSet.of(1, 3, 4).previousClearBit(1), equalTo(0));
  }

  @Test public void testBuilder() {
    assertThat(ImmutableBitSet.builder().set(9)
            .set(100)
            .set(1000)
            .clear(250)
            .set(88)
            .clear(100)
            .clear(1000)
            .build().toString(),
        equalTo("{9, 88}"));
  }

  /** Unit test for
   * {@link org.apache.calcite.util.ImmutableBitSet.Builder#build(ImmutableBitSet)}. */
  @Test public void testBuilderUseOriginal() {
    final ImmutableBitSet fives = ImmutableBitSet.of(5, 10, 15);
    final ImmutableBitSet fives2 =
        ImmutableBitSet.builder(fives).clear(2).set(10).build(fives);
    assertTrue(fives2 == fives);
    final ImmutableBitSet fives3 =
        ImmutableBitSet.builder(fives).clear(2).set(10).build();
    assertTrue(fives3 != fives);
    assertTrue(fives3.equals(fives));
    assertTrue(fives3.equals(fives2));
  }

  @Test public void testIndexOf() {
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(0), equalTo(0));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(2), equalTo(1));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(3), equalTo(-1));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(4), equalTo(2));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(5), equalTo(-1));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(-1), equalTo(-1));
    assertThat(ImmutableBitSet.of(0, 2, 4).indexOf(-2), equalTo(-1));
    assertThat(ImmutableBitSet.of().indexOf(-1), equalTo(-1));
    assertThat(ImmutableBitSet.of().indexOf(-2), equalTo(-1));
    assertThat(ImmutableBitSet.of().indexOf(0), equalTo(-1));
    assertThat(ImmutableBitSet.of().indexOf(1000), equalTo(-1));
  }

  @Test public void testNth() {
    assertThat(ImmutableBitSet.of(0, 2, 4).nth(0), equalTo(0));
    assertThat(ImmutableBitSet.of(0, 2, 4).nth(1), equalTo(2));
    assertThat(ImmutableBitSet.of(0, 2, 4).nth(2), equalTo(4));
    assertThat(ImmutableBitSet.of(0, 2, 63).nth(2), equalTo(63));
    assertThat(ImmutableBitSet.of(0, 2, 64).nth(2), equalTo(64));
    assertThat(ImmutableBitSet.of(64).nth(0), equalTo(64));
    assertThat(ImmutableBitSet.of(64, 65).nth(0), equalTo(64));
    assertThat(ImmutableBitSet.of(64, 65).nth(1), equalTo(65));
    assertThat(ImmutableBitSet.of(64, 128).nth(1), equalTo(128));
    try {
      ImmutableBitSet.of().nth(0);
      fail("expected throw");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      ImmutableBitSet.of().nth(1);
      fail("expected throw");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      ImmutableBitSet.of(64).nth(1);
      fail("expected throw");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      ImmutableBitSet.of(64).nth(-1);
      fail("expected throw");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
  }

  /** Tests the method
   * {@link org.apache.calcite.util.BitSets#closure(java.util.SortedMap)}. */
  @Test public void testClosure() {
    final SortedMap<Integer, ImmutableBitSet> empty = Maps.newTreeMap();
    assertThat(ImmutableBitSet.closure(empty), equalTo(empty));

    // Currently you need an entry for each position, otherwise you get an NPE.
    // We should fix that.
    final SortedMap<Integer, ImmutableBitSet> map = Maps.newTreeMap();
    map.put(0, ImmutableBitSet.of(3));
    map.put(1, ImmutableBitSet.of());
    map.put(2, ImmutableBitSet.of(7));
    map.put(3, ImmutableBitSet.of(4, 12));
    map.put(4, ImmutableBitSet.of());
    map.put(5, ImmutableBitSet.of());
    map.put(6, ImmutableBitSet.of());
    map.put(7, ImmutableBitSet.of());
    map.put(8, ImmutableBitSet.of());
    map.put(9, ImmutableBitSet.of());
    map.put(10, ImmutableBitSet.of());
    map.put(11, ImmutableBitSet.of());
    map.put(12, ImmutableBitSet.of());
    String original = map.toString();
    assertThat(ImmutableBitSet.closure(map).toString(),
        equalTo(
            "{0={3, 4, 12}, 1={}, 2={7}, 3={3, 4, 12}, 4={4, 12}, 5={}, 6={}, 7={7}, 8={}, 9={}, 10={}, 11={}, 12={4, 12}}"));
    assertThat("argument modified", map.toString(), equalTo(original));
  }

  @Test public void testPowerSet() {
    final ImmutableBitSet empty = ImmutableBitSet.of();
    assertThat(Iterables.size(empty.powerSet()), equalTo(1));
    assertThat(empty.powerSet().toString(), equalTo("[{}]"));

    final ImmutableBitSet single = ImmutableBitSet.of(2);
    assertThat(Iterables.size(single.powerSet()), equalTo(2));
    assertThat(single.powerSet().toString(), equalTo("[{}, {2}]"));

    final ImmutableBitSet two = ImmutableBitSet.of(2, 10);
    assertThat(Iterables.size(two.powerSet()), equalTo(4));
    assertThat(two.powerSet().toString(), equalTo("[{}, {10}, {2}, {2, 10}]"));

    final ImmutableBitSet seventeen = ImmutableBitSet.range(3, 20);
    assertThat(Iterables.size(seventeen.powerSet()), equalTo(131072));
  }

  @Test public void testCreateLongs() {
    assertThat(ImmutableBitSet.valueOf(0L), equalTo(ImmutableBitSet.of()));
    assertThat(ImmutableBitSet.valueOf(0xAL),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(ImmutableBitSet.valueOf(0xAL, 0, 0),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(ImmutableBitSet.valueOf(0, 0, 0xAL, 0),
        equalTo(ImmutableBitSet.of(129, 131)));
  }

  @Test public void testCreateLongBuffer() {
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {})),
        equalTo(ImmutableBitSet.of()));
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {0xAL})),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(
        ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {0, 0, 0xAL, 0})),
        equalTo(ImmutableBitSet.of(129, 131)));
  }

  @Test public void testToLongArray() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    assertThat(ImmutableBitSet.valueOf(bitSet.toLongArray()),
        equalTo(bitSet));
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(bitSet.toLongArray())),
        equalTo(bitSet));
  }

  @Test public void testSet() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    final ImmutableBitSet bitSet2 = ImmutableBitSet.of(29, 4, 1969, 30);
    assertThat(bitSet.set(30), equalTo(bitSet2));
    assertThat(bitSet.set(30).set(30), equalTo(bitSet2));
    assertThat(bitSet.set(29), equalTo(bitSet));
    assertThat(bitSet.setIf(30, false), equalTo(bitSet));
    assertThat(bitSet.setIf(30, true), equalTo(bitSet2));
  }

  @Test public void testClear() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    final ImmutableBitSet bitSet2 = ImmutableBitSet.of(4, 1969);
    assertThat(bitSet.clear(29), equalTo(bitSet2));
    assertThat(bitSet.clear(29).clear(29), equalTo(bitSet2));
    assertThat(bitSet.clear(29).clear(4).clear(29).clear(1969),
        equalTo(ImmutableBitSet.of()));
    assertThat(bitSet.clearIf(29, false), equalTo(bitSet));
    assertThat(bitSet.clearIf(29, true), equalTo(bitSet2));
  }
}

// End ImmutableBitSetTest.java
