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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import org.junit.jupiter.api.Test;

import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link org.apache.calcite.util.ImmutableBitSet}.
 */
class ImmutableBitSetTest {
  /** Tests the method {@link ImmutableBitSet#iterator()}. */
  @Test void testIterator() {
    assertToIterBitSet("", ImmutableBitSet.of());
    assertToIterBitSet("0", ImmutableBitSet.of(0));
    assertToIterBitSet("0, 1", ImmutableBitSet.of(0, 1));
    assertToIterBitSet("10", ImmutableBitSet.of(10));

    check((bitSet, list) -> {
      final List<Integer> list2 = new ArrayList<>();
      for (Integer integer : bitSet) {
        list2.add(integer);
      }
      assertThat(list2, equalTo(list));
    });
  }

  /** Tests the method {@link ImmutableBitSet#of(int)}. */
  @Test void testSingletonConstructor() {
    IntConsumer c = i -> {
      final ImmutableBitSet s0 = ImmutableBitSet.of(i);
      final ImmutableBitSet s1 = ImmutableBitSet.of(ImmutableIntList.of(i));
      final ImmutableBitSet s2 = ImmutableBitSet.of(Collections.singleton(i));
      final ImmutableBitSet s3 =
          ImmutableBitSet.of(99, 100).set(i).clear(100).clear(99);
      assertThat(s0.cardinality(), is(1));
      assertThat(s0, is(s1));
      assertThat(s0, is(s2));
      assertThat(s0, is(s3));
      assertThat(s1, is(s2));
      assertThat(s1, is(s3));
      assertThat(s2, is(s3));
    };
    c.accept(0);
    c.accept(1);
    c.accept(63);
    c.accept(64);
  }

  @Test void testNegative() {
    assertThrows(IndexOutOfBoundsException.class,
        () -> ImmutableBitSet.of(-1));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ImmutableBitSet.of(-2));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ImmutableBitSet.of(1, 10, -1, 63));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ImmutableBitSet.of(-1, 10));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ImmutableBitSet.of(Collections.singleton(-2)));
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
      buf.append(i);
    }
    assertThat(buf, hasToString(expected));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#toList()}.
   */
  @Test void testToList() {
    check((bitSet, list) -> assertThat(bitSet.toList(), equalTo(list)));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#forEachInt}.
   */
  @Test void testForEachInt() {
    check((bitSet, list) -> {
      final List<Integer> list2 = new ArrayList<>();
      bitSet.forEachInt(list2::add);
      assertThat(list2, equalTo(list));
    });
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#forEach}.
   */
  @Test void testForEachInteger() {
    check((bitSet, list) -> {
      final List<Integer> list2 = new ArrayList<>();
      bitSet.forEach(list2::add);
      assertThat(list2, equalTo(list));
    });
  }

  private void check(BiConsumer<ImmutableBitSet, List<Integer>> consumer) {
    consumer.accept(ImmutableBitSet.of(), Collections.emptyList());
    consumer.accept(ImmutableBitSet.of(5), Collections.singletonList(5));
    consumer.accept(ImmutableBitSet.of(3, 5), Arrays.asList(3, 5));
    consumer.accept(ImmutableBitSet.of(63), Collections.singletonList(63));
    consumer.accept(ImmutableBitSet.of(64), Collections.singletonList(64));
    consumer.accept(ImmutableBitSet.of(3, 63), Arrays.asList(3, 63));
    consumer.accept(ImmutableBitSet.of(3, 64), Arrays.asList(3, 64));
    consumer.accept(ImmutableBitSet.of(0, 4, 2), Arrays.asList(0, 2, 4));
  }

  /**
   * Tests the method {@link BitSets#range(int, int)}.
   */
  @Test void testRange() {
    final List<Integer> list0123 = Arrays.asList(0, 1, 2, 3);
    final List<Integer> list123 = Arrays.asList(1, 2, 3);
    final List<Integer> listEmpty = Collections.emptyList();

    assertThat(ImmutableBitSet.range(0, 4).toList(), is(list0123));
    assertThat(ImmutableBitSet.range(1, 4).toList(), is(list123));
    assertThat(ImmutableBitSet.range(4).toList(), is(list0123));
    assertThat(ImmutableBitSet.range(0).toList(), is(listEmpty));
    assertThat(ImmutableBitSet.range(2, 2).toList(), is(listEmpty));

    assertThat(ImmutableBitSet.range(63, 66),
        hasToString("{63, 64, 65}"));
    assertThat(ImmutableBitSet.range(65, 68),
        hasToString("{65, 66, 67}"));
    assertThat(ImmutableBitSet.range(65, 65), hasToString("{}"));
    assertThat(ImmutableBitSet.range(65, 65).length(), equalTo(0));
    assertThat(ImmutableBitSet.range(65, 165).cardinality(), equalTo(100));

    // Same tests as above, using a builder.
    assertThat(ImmutableBitSet.builder().set(63, 66).build(),
        hasToString("{63, 64, 65}"));
    assertThat(ImmutableBitSet.builder().set(65, 68).build(),
        hasToString("{65, 66, 67}"));
    assertThat(ImmutableBitSet.builder().set(65, 65).build(),
        hasToString("{}"));
    assertThat(ImmutableBitSet.builder().set(65, 65).build().length(),
        equalTo(0));
    assertThat(ImmutableBitSet.builder().set(65, 165).build().cardinality(),
        equalTo(100));

    final ImmutableBitSet e0 = ImmutableBitSet.range(0, 0);
    final ImmutableBitSet e1 = ImmutableBitSet.of();
    assertThat(e0, is(e1));
    assertThat(e0.hashCode(), equalTo(e1.hashCode()));

    // Empty builder returns the singleton empty set.
    assertThat(ImmutableBitSet.builder().build(),
        sameInstance(ImmutableBitSet.of()));
  }

  @Test void testCompare() {
    final List<ImmutableBitSet> sorted = getSortedList();
    for (int i = 0; i < sorted.size(); i++) {
      for (int j = 0; j < sorted.size(); j++) {
        final ImmutableBitSet set0 = sorted.get(i);
        final ImmutableBitSet set1 = sorted.get(j);
        int c = set0.compareTo(set1);
        if (c == 0) {
          assertTrue(i == j || i == 3 && j == 4 || i == 4 && j == 3);
        } else {
          assertThat(Utilities.compare(i, j), is(c));
        }
        assertThat(set0.equals(set1), is(c == 0));
        assertThat(set1.equals(set0), is(c == 0));
      }
    }
  }

  @Test void testCompare2() {
    final List<ImmutableBitSet> sorted = getSortedList();
    sorted.sort(ImmutableBitSet.COMPARATOR);
    assertThat(sorted,
        hasToString("[{0, 1, 3}, {0, 1}, {1, 1000}, {1}, {1}, {2, 3}, {}]"));
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
  @Test void testToArray() {
    int[][] arrays = {{}, {0}, {0, 2}, {1, 65}, {100}};
    for (int[] array : arrays) {
      assertThat(ImmutableBitSet.of(array).toArray(), equalTo(array));
    }
  }

  /**
   * Tests the methods
   * {@link org.apache.calcite.util.ImmutableBitSet#toList}, and
   * {@link org.apache.calcite.util.ImmutableBitSet#asList} and
   * {@link org.apache.calcite.util.ImmutableBitSet#asSet}.
   */
  @Test void testAsList() {
    final List<ImmutableBitSet> list = getSortedList();

    // create a set of integers in and not in the lists
    final Set<Integer> integers = new HashSet<>();
    for (ImmutableBitSet set : list) {
      for (Integer integer : set) {
        integers.add(integer);
        integers.add(integer + 1);
        integers.add(integer + 10);
      }
    }

    for (ImmutableBitSet bitSet : list) {
      final List<Integer> list1 = bitSet.toList();
      final List<Integer> listView = bitSet.asList();
      final Set<Integer> setView = bitSet.asSet();
      assertThat(list1, hasSize(bitSet.cardinality()));
      assertThat(listView, hasSize(bitSet.cardinality()));
      assertThat(setView, hasSize(bitSet.cardinality()));
      assertThat(list1, hasToString(listView.toString()));
      assertThat(list1, hasToString(setView.toString()));
      assertThat(list1.equals(listView), is(true));
      assertThat(list1.hashCode(), equalTo(listView.hashCode()));

      final Set<Integer> set = new HashSet<>(list1);
      assertThat(setView.hashCode(), is(set.hashCode()));
      assertThat(setView, equalTo(set));

      for (Integer integer : integers) {
        final boolean b = list1.contains(integer);
        assertThat(listView.contains(integer), is(b));
        assertThat(setView.contains(integer), is(b));
      }
    }
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#union(ImmutableBitSet)}.
   */
  @Test void testUnion() {
    assertThat(ImmutableBitSet.of(1).union(ImmutableBitSet.of(3)),
        hasToString("{1, 3}"));
    assertThat(ImmutableBitSet.of(1).union(ImmutableBitSet.of(3, 100)),
        hasToString("{1, 3, 100}"));
    ImmutableBitSet x =
        ImmutableBitSet.of(1)
            .rebuild()
            .addAll(ImmutableBitSet.of(2))
            .addAll(ImmutableBitSet.of())
            .addAll(ImmutableBitSet.of(3))
            .build();
    assertThat(x, hasToString("{1, 2, 3}"));
  }

  @Test void testIntersect() {
    assertThat(ImmutableBitSet.of(1, 2, 3, 100, 200)
        .intersect(ImmutableBitSet.of(2, 100)),
        hasToString("{2, 100}"));
    assertThat(ImmutableBitSet.of(1, 3, 5, 101, 20001)
        .intersect(ImmutableBitSet.of(2, 100)),
        sameInstance(ImmutableBitSet.of()));
  }

  /**
   * Tests the method
   * {@link org.apache.calcite.util.ImmutableBitSet#contains(org.apache.calcite.util.ImmutableBitSet)}.
   */
  @Test void testBitSetsContains() {
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
  @Test void testBitSetOfImmutableIntList() {
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
  @Test void testPreviousClearBit() {
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

  @Test void testBuilder() {
    assertThat(ImmutableBitSet.builder().set(9)
            .set(100)
            .set(1000)
            .clear(250)
            .set(88)
            .clear(100)
            .clear(1000)
            .build(),
        hasToString("{9, 88}"));
  }

  /** Unit test for
   * {@link org.apache.calcite.util.ImmutableBitSet.Builder#build(ImmutableBitSet)}. */
  @Test void testBuilderUseOriginal() {
    final ImmutableBitSet fives = ImmutableBitSet.of(5, 10, 15);
    final ImmutableBitSet fives1 =
        fives.rebuild().clear(2).set(10).build();
    assertSame(fives1, fives);
    final ImmutableBitSet fives2 =
        ImmutableBitSet.builder().addAll(fives).clear(2).set(10).build(fives);
    assertSame(fives2, fives);
    final ImmutableBitSet fives3 =
        ImmutableBitSet.builder().addAll(fives).clear(2).set(10).build();
    assertNotSame(fives3, fives);
    assertThat(fives, is(fives3));
    assertThat(fives2, is(fives3));
  }

  @Test void testIndexOf() {
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

  /** Tests {@link ImmutableBitSet.Builder#buildAndReset()}. */
  @Test void testReset() {
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    builder.set(2);
    assertThat(builder.build(), hasToString("{2}"));
    try {
      builder.set(4);
      fail("expected exception");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("can only use builder once"));
    }
    try {
      final ImmutableBitSet bitSet = builder.build();
      fail("expected exception, got " + bitSet);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("can only use builder once"));
    }
    try {
      final ImmutableBitSet bitSet = builder.buildAndReset();
      fail("expected exception, got " + bitSet);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("can only use builder once"));
    }

    final ImmutableBitSet.Builder builder2 = ImmutableBitSet.builder();
    builder2.set(2);
    assertThat(builder2.buildAndReset(), hasToString("{2}"));
    assertThat(builder2.buildAndReset(), hasToString("{}"));
    builder2.set(151);
    builder2.set(3);
    assertThat(builder2.buildAndReset(), hasToString("{3, 151}"));
  }

  @Test void testNth() {
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
  @Test void testClosure() {
    final SortedMap<Integer, ImmutableBitSet> empty = new TreeMap<>();
    assertThat(ImmutableBitSet.closure(empty), equalTo(empty));

    // Currently you need an entry for each position, otherwise you get an NPE.
    // We should fix that.
    final SortedMap<Integer, ImmutableBitSet> map = new TreeMap<>();
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
    final String original = map.toString();
    final String expected =
        "{0={3, 4, 12}, 1={}, 2={7}, 3={3, 4, 12}, 4={4, 12}, 5={}, 6={}, 7={7}, 8={}, 9={}, 10={}, 11={}, 12={4, 12}}";
    assertThat(ImmutableBitSet.closure(map), hasToString(expected));
    assertThat("argument modified", map, hasToString(original));

    // Now a similar map with missing entries. Same result.
    final SortedMap<Integer, ImmutableBitSet> map2 = new TreeMap<>();
    map2.put(0, ImmutableBitSet.of(3));
    map2.put(2, ImmutableBitSet.of(7));
    map2.put(3, ImmutableBitSet.of(4, 12));
    map2.put(9, ImmutableBitSet.of());
    final String original2 = map2.toString();
    assertThat(ImmutableBitSet.closure(map2), hasToString(expected));
    assertThat("argument modified", map2, hasToString(original2));
  }

  @Test void testPowerSet() {
    final ImmutableBitSet empty = ImmutableBitSet.of();
    assertThat(Iterables.size(empty.powerSet()), equalTo(1));
    assertThat(empty.powerSet(), hasToString("[{}]"));

    final ImmutableBitSet single = ImmutableBitSet.of(2);
    assertThat(Iterables.size(single.powerSet()), equalTo(2));
    assertThat(single.powerSet(), hasToString("[{}, {2}]"));

    final ImmutableBitSet two = ImmutableBitSet.of(2, 10);
    assertThat(Iterables.size(two.powerSet()), equalTo(4));
    assertThat(two.powerSet(), hasToString("[{}, {10}, {2}, {2, 10}]"));

    final ImmutableBitSet seventeen = ImmutableBitSet.range(3, 20);
    assertThat(Iterables.size(seventeen.powerSet()), equalTo(131072));
  }

  @Test void testCreateLongs() {
    assertThat(ImmutableBitSet.valueOf(0L), equalTo(ImmutableBitSet.of()));
    assertThat(ImmutableBitSet.valueOf(0xAL),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(ImmutableBitSet.valueOf(0xAL, 0, 0),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(ImmutableBitSet.valueOf(0, 0, 0xAL, 0),
        equalTo(ImmutableBitSet.of(129, 131)));
  }

  @Test void testCreateLongBuffer() {
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {})),
        equalTo(ImmutableBitSet.of()));
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {0xAL})),
        equalTo(ImmutableBitSet.of(1, 3)));
    assertThat(
        ImmutableBitSet.valueOf(LongBuffer.wrap(new long[] {0, 0, 0xAL, 0})),
        equalTo(ImmutableBitSet.of(129, 131)));
  }

  @Test void testToLongArray() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    assertThat(ImmutableBitSet.valueOf(bitSet.toLongArray()),
        equalTo(bitSet));
    assertThat(ImmutableBitSet.valueOf(LongBuffer.wrap(bitSet.toLongArray())),
        equalTo(bitSet));
  }

  @Test void testSet() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    final ImmutableBitSet bitSet2 = ImmutableBitSet.of(29, 4, 1969, 30);
    assertThat(bitSet.set(30), equalTo(bitSet2));
    assertThat(bitSet.set(30).set(30), equalTo(bitSet2));
    assertThat(bitSet.set(29), equalTo(bitSet));
    assertThat(bitSet.setIf(30, false), equalTo(bitSet));
    assertThat(bitSet.setIf(30, true), equalTo(bitSet2));
  }

  @Test void testClear() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    final ImmutableBitSet bitSet2 = ImmutableBitSet.of(4, 1969);
    assertThat(bitSet.clear(29), equalTo(bitSet2));
    assertThat(bitSet.clear(29).clear(29), equalTo(bitSet2));
    assertThat(bitSet.clear(29).clear(4).clear(29).clear(1969),
        equalTo(ImmutableBitSet.of()));
    assertThat(bitSet.clearIf(29, false), equalTo(bitSet));
    assertThat(bitSet.clearIf(29, true), equalTo(bitSet2));
  }

  @Test void testSet2() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    final ImmutableBitSet bitSet2 = ImmutableBitSet.of(29, 4, 1969, 30);
    assertThat(bitSet.set(30, false), sameInstance(bitSet));
    assertThat(bitSet.set(30, true), equalTo(bitSet2));
    assertThat(bitSet.set(29, true), sameInstance(bitSet));
  }

  @Test void testShift() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    assertThat(bitSet.shift(0), is(bitSet));
    assertThat(bitSet.shift(1), is(ImmutableBitSet.of(30, 5, 1970)));
    assertThat(bitSet.shift(-4), is(ImmutableBitSet.of(25, 0, 1965)));
    try {
      final ImmutableBitSet x = bitSet.shift(-5);
      fail("Expected error, got " + x);
    } catch (ArrayIndexOutOfBoundsException ignored) {
      // Exact message is not specified by Java
    }
    final ImmutableBitSet empty = ImmutableBitSet.of();
    assertThat(empty.shift(-100), is(empty));
  }

  @Test void testGet2() {
    final ImmutableBitSet bitSet = ImmutableBitSet.of(29, 4, 1969);
    assertThat(bitSet.get(0, 8), is(ImmutableBitSet.of(4)));
    assertThat(bitSet.get(0, 5), is(ImmutableBitSet.of(4)));
    assertThat(bitSet.get(0, 4), is(ImmutableBitSet.of()));
    assertThat(bitSet.get(4, 4), is(ImmutableBitSet.of()));
    assertThat(bitSet.get(5, 5), is(ImmutableBitSet.of()));
    assertThat(bitSet.get(4, 5), is(ImmutableBitSet.of(4)));
    assertThat(bitSet.get(4, 1000), is(ImmutableBitSet.of(4, 29)));
    assertThat(bitSet.get(4, 32), is(ImmutableBitSet.of(4, 29)));
    assertThat(bitSet.get(2000, 10000), is(ImmutableBitSet.of()));
    assertThat(bitSet.get(1000, 10000), is(ImmutableBitSet.of(1969)));
    assertThat(bitSet.get(5, 10000), is(ImmutableBitSet.of(29, 1969)));
    assertThat(bitSet.get(65, 10000), is(ImmutableBitSet.of(1969)));

    final ImmutableBitSet emptyBitSet = ImmutableBitSet.of();
    assertThat(emptyBitSet.get(0, 4), is(ImmutableBitSet.of()));
    assertThat(emptyBitSet.get(0, 0), is(ImmutableBitSet.of()));
    assertThat(emptyBitSet.get(0, 10000), is(ImmutableBitSet.of()));
    assertThat(emptyBitSet.get(7, 10000), is(ImmutableBitSet.of()));
    assertThat(emptyBitSet.get(73, 10000), is(ImmutableBitSet.of()));
  }

  /**
   * Test case for {@link ImmutableBitSet#allContain(Collection, int)}.
   */
  @Test void testAllContain() {
    ImmutableBitSet set1 = ImmutableBitSet.of(0, 1, 2, 3);
    ImmutableBitSet set2 = ImmutableBitSet.of(2, 3, 4, 5);
    ImmutableBitSet set3 = ImmutableBitSet.of(3, 4, 5, 6);

    Collection<ImmutableBitSet> collection1 = ImmutableList.of(set1, set2, set3);
    assertTrue(ImmutableBitSet.allContain(collection1, 3));
    assertFalse(ImmutableBitSet.allContain(collection1, 0));

    Collection<ImmutableBitSet> collection2 = ImmutableList.of(set1, set2);
    assertTrue(ImmutableBitSet.allContain(collection2, 2));
    assertTrue(ImmutableBitSet.allContain(collection2, 3));
    assertFalse(ImmutableBitSet.allContain(collection2, 4));
  }

  /**
   * Test case for {@link ImmutableBitSet#anyMatch(IntPredicate)}
   * and {@link ImmutableBitSet#allMatch(IntPredicate)}.
   *
   * <p>Checks a variety of predicates (is even, is zero, always true,
   * always false) and their negations on a variety of bit sets.
   */
  @Test void testAnyMatch() {
    BiConsumer<ImmutableBitSet, IntPredicate> c = (bitSet, predicate) -> {
      final Set<Integer> integerSet = new HashSet<>(bitSet.asList());
      assertThat(bitSet.anyMatch(predicate),
          is(integerSet.stream().anyMatch(predicate::test)));
      assertThat(bitSet.allMatch(predicate),
          is(integerSet.stream().allMatch(predicate::test)));
    };

    BiConsumer<ImmutableBitSet, IntPredicate> c2 = (bitSet, predicate) -> {
      c.accept(bitSet, predicate);
      c.accept(bitSet, predicate.negate());
    };

    final ImmutableBitSet set0 = ImmutableBitSet.of();
    final ImmutableBitSet set1 = ImmutableBitSet.of(0, 1, 2, 3);
    final ImmutableBitSet set2 = ImmutableBitSet.of(0, 2, 4, 8);
    Consumer<IntPredicate> c3 = predicate -> {
      c2.accept(set0, predicate);
      c2.accept(set1, predicate);
      c2.accept(set2, predicate);
    };

    final IntPredicate isZero = i -> i == 0;
    final IntPredicate isEven = i -> i % 2 == 0;
    final IntPredicate alwaysTrue = i -> true;
    final IntPredicate alwaysFalse = i -> false;
    c3.accept(isZero);
    c3.accept(isEven);
    c3.accept(alwaysTrue);
    c3.accept(alwaysFalse);
  }

  /** Test case for
   * {@link org.apache.calcite.util.ImmutableBitSet#toImmutableBitSet()}. */
  @Test void testCollector() {
    checkCollector(0, 20);
    checkCollector();
    checkCollector(1, 63);
    checkCollector(1, 63, 1);
    checkCollector(0, 257);
    checkCollector(1024, 257);
  }

  private void checkCollector(int... integers) {
    final List<Integer> list = Ints.asList(integers);
    final List<Integer> sortedUniqueList = new ArrayList<>(new TreeSet<>(list));
    final ImmutableBitSet bitSet =
        list.stream().collect(ImmutableBitSet.toImmutableBitSet());
    assertThat(bitSet.asList(), is(sortedUniqueList));
  }
}
