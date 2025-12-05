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

import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.runtime.MapEntry;
import org.apache.calcite.runtime.PairList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.BiPredicate;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/** Unit test for {@code PairList}. */
class PairListTest {
  /** Equivalent to {@link Pair#left} but without calling
   * {@link PairList#leftList()}. */
  private static <T, U> List<T> left(
      final List<? extends Map.Entry<? extends T, ? extends U>> pairs) {
    return Util.transform(pairs, Map.Entry::getKey);
  }

  /** Equivalent to {@link Pair#right} but without calling
   * {@link PairList#rightList()}. */
  private static <T, U> List<U> right(
      final List<? extends Map.Entry<? extends T, ? extends U>> pairs) {
    return Util.transform(pairs, Map.Entry::getValue);
  }

  /** Compares a {@link PairList} with a {@link List} that should have
   * equivalent contents. */
  private <T, U> void validate(PairList<T, U> pairList,
      List<? extends Map.Entry<T, U>> list) {
    assertThat(pairList.isEmpty(), is(list.isEmpty()));
    assertThat(pairList, hasSize(list.size()));
    assertThat(pairList.leftList(), hasSize(list.size()));
    assertThat(pairList.rightList(), hasSize(list.size()));
    assertThat(pairList.leftList(), is(left(list)));
    assertThat(pairList.leftList(), instanceOf(RandomAccess.class));
    assertThat(pairList.rightList(), is(right(list)));
    assertThat(pairList.rightList(), instanceOf(RandomAccess.class));

    // Check PairList.left(int) and PairList.right(int)
    for (int i = 0; i < list.size(); i++) {
      Map.Entry<T, U> entry = list.get(i);
      assertThat(pairList.left(i), is(entry.getKey()));
      assertThat(pairList.right(i), is(entry.getValue()));
    }

    final List<Map.Entry<T, U>> list2 = new ArrayList<>(pairList);
    assertThat(list2, is(list));

    // Check PairList.forEach(Consumer)
    list2.clear();
    //noinspection UseBulkOperation
    pairList.forEach(p -> list2.add(p));
    assertThat(list2, is(list));

    // Check PairList.forEach(BiConsumer)
    list2.clear();
    pairList.forEach((t, u) -> list2.add(Pair.of(t, u)));
    assertThat(list2, is(list));

    // Check PairList.forEachIndexed
    list2.clear();
    pairList.forEachIndexed((i, t, u) -> {
      assertThat(i, is(list2.size()));
      list2.add(Pair.of(t, u));
    });
    assertThat(list2, is(list));

    // Check PairList.immutable()
    // Skip if there are no null keys or values
    if (list.stream().anyMatch(e -> e.getKey() == null)) {
      // PairList.immutable should throw if there are null keys
      try {
        Object o = pairList.immutable();
        fail("expected error, got " + o);
      } catch (NullPointerException e) {
        assertThat(e.getMessage(), startsWith("key at index"));
      }
    } else if (list.stream().anyMatch(e -> e.getValue() == null)) {
      // PairList.immutable should throw if there are null values
      try {
        Object o = pairList.immutable();
        fail("expected error, got " + o);
      } catch (NullPointerException e) {
        assertThat(e.getMessage(), startsWith("value at index"));
      }
    } else {
      final PairList<T, U> immutablePairList = pairList.immutable();
      assertThat(immutablePairList, hasSize(list.size()));
      assertThat(immutablePairList, is(list));

      assertThat(pairList.reversed(), is(Lists.reverse(list)));
      assertThat(immutablePairList.reversed(), is(Lists.reverse(list)));

      list2.clear();
      immutablePairList.forEach((k, v) -> list2.add(Pair.of(k, v)));
      assertThat(list2, is(list));
    }
  }

  /** Basic test for {@link PairList}. */
  @Test void testPairList() {
    final PairList<Integer, String> pairList = PairList.of();
    final List<Map.Entry<Integer, String>> list = new ArrayList<>();

    validate(pairList, list);

    // add(T, U)
    pairList.add(1, "a");
    list.add(Pair.of(1, "a"));
    validate(pairList, list);

    // add(Pair<T, U>)
    pairList.add(Pair.of(2, "b"));
    list.add(Pair.of(2, "b"));
    validate(pairList, list);

    // add(T, U)
    pairList.add(2, "bb");
    list.add(Pair.of(2, "bb"));
    validate(pairList, list);

    // add(int, Pair<T, U>)
    pairList.add(0, Pair.of(3, "c"));
    list.add(0, Pair.of(3, "c"));
    validate(pairList, list);

    // add(int, T, U)
    pairList.add(0, 4, "d");
    list.add(0, Pair.of(4, "d"));
    validate(pairList, list);

    // remove(int)
    Map.Entry<Integer, String> x = pairList.remove(1);
    Map.Entry<Integer, String> y = list.remove(1);
    assertThat(x, is(y));
    validate(pairList, list);

    // clear()
    pairList.clear();
    list.clear();
    validate(pairList, list);

    // clear() again
    pairList.clear();
    list.clear();
    validate(pairList, list);

    // add(T, U) having called clear
    pairList.add(-1, "c");
    list.add(Pair.of(-1, "c"));
    validate(pairList, list);

    // addAll(PairList)
    final PairList<Integer, String> pairList8 = PairList.copyOf(8, "x", 7, "y");
    pairList.addAll(pairList8);
    list.addAll(pairList8);
    validate(pairList, list);

    // addAll(int, PairList)
    pairList.addAll(3, pairList8);
    list.addAll(3, pairList8);
    validate(pairList, list);

    PairList<Integer, String> immutablePairList = pairList.immutable();
    assertThrows(UnsupportedOperationException.class, () ->
        immutablePairList.add(0, ""));
    validate(immutablePairList, list);

    // set(int, Pair<T, U>)
    pairList.set(2, 0, "p");
    list.set(2, Pair.of(0, "p"));
    validate(pairList, list);

    // set(int, T, U)
    pairList.set(1, Pair.of(88, "q"));
    list.set(1, Pair.of(88, "q"));
    validate(pairList, list);
  }

  @Test void testAddAll() {
    PairList<String, Integer> pairList = PairList.of();

    // MutablePairList (0 entries)
    pairList.addAll(PairList.of());
    assertThat(pairList, hasSize(0));

    // MutablePairList (1 entry)
    pairList.addAll(PairList.of("a", 1));
    assertThat(pairList, hasSize(1));

    // MutablePairList (2 entries)
    pairList.addAll(PairList.of(ImmutableMap.of("b", 2, "c", 3)));
    assertThat(pairList, hasSize(3));

    // EmptyImmutablePairList
    pairList.addAll(ImmutablePairList.of());
    assertThat(pairList, hasSize(3));

    // ImmutableList (0 entries)
    pairList.addAll(ImmutableList.of());
    assertThat(pairList, hasSize(3));

    // SingletonImmutablePairList
    pairList.addAll(ImmutablePairList.of("d", 4));
    assertThat(pairList, hasSize(4));

    // ImmutableList (1 entry)
    pairList.addAll(ImmutableList.of(new MapEntry<>("e", 5)));
    assertThat(pairList, hasSize(5));

    // MutablePairList (2 entries)
    pairList.addAll(PairList.copyOf("f", 6, "g", 7));
    assertThat(pairList, hasSize(7));

    // ArrayImmutablePairList (2 entries, created from MutablePairList)
    pairList.addAll(PairList.copyOf("h", 8, "i", 9).immutable());
    assertThat(pairList, hasSize(9));

    // ArrayImmutablePairList (3 entries, created using copyOf)
    pairList.addAll(ImmutablePairList.copyOf("j", 10, "k", 11, "l", 12));
    assertThat(pairList, hasSize(12));

    // ArrayImmutablePairList (2 entries, created using copyOf)
    pairList.addAll(ImmutablePairList.copyOf("m", 13, "n", 14));
    assertThat(pairList, hasSize(14));

    // ArrayImmutablePairList (1 entry, created using copyOf)
    pairList.addAll(ImmutablePairList.copyOf("o", 15));
    assertThat(pairList, hasSize(15));

    assertThat(pairList,
        hasToString("[<a, 1>, <b, 2>, <c, 3>, <d, 4>, <e, 5>, <f, 6>, "
            + "<g, 7>, <h, 8>, <i, 9>, <j, 10>, <k, 11>, <l, 12>, "
            + "<m, 13>, <n, 14>, <o, 15>]"));
  }

  /** Tests {@link PairList#of(Map)} and {@link PairList#toImmutableMap()}. */
  @Test void testPairListOfMap() {
    final ImmutableMap<String, Integer> map = ImmutableMap.of("a", 1, "b", 2);
    final PairList<String, Integer> pairList = PairList.of(map);
    assertThat(pairList, hasSize(2));
    assertThat(pairList, hasToString("[<a, 1>, <b, 2>]"));

    final List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
    validate(pairList, list);

    final ImmutableMap<String, Integer> map2 = pairList.toImmutableMap();
    assertThat(map2, is(map));

    // After calling toImmutableMap, you can modify the list and call
    // toImmutableMap again.
    pairList.add("c", 3);
    list.add(Pair.of("c", 3));
    validate(pairList, list);
    assertThat(pairList, hasToString("[<a, 1>, <b, 2>, <c, 3>]"));
    final ImmutableMap<String, Integer> map3 = pairList.toImmutableMap();
    assertThat(map3, hasToString("{a=1, b=2, c=3}"));

    final Map<String, Integer> emptyMap = ImmutableMap.of();
    final PairList<String, Integer> emptyPairList = PairList.of(emptyMap);
    assertThat(emptyPairList.isEmpty(), is(true));
    validate(emptyPairList, Collections.emptyList());
  }

  /** Tests {@link PairList#withCapacity(int)}. */
  @Test void testPairListWithCapacity() {
    final PairList<String, Integer> list = PairList.withCapacity(100);
    assertThat(list, hasSize(0));
    assertThat(list, empty());
    assertThat(list, hasToString("[]"));

    list.add("a", 1);
    list.add("b", 2);
    assertThat(list, hasSize(2));
    assertThat(list, hasToString("[<a, 1>, <b, 2>]"));

    final Map.Entry<String, Integer> entry = list.remove(0);
    assertThat(entry.getKey(), is("a"));
    assertThat(entry.getValue(), is(1));
    assertThat(list, hasToString("[<b, 2>]"));
  }

  @Test void testPairListOf() {
    final PairList<String, Integer> list0 = PairList.of();
    assertThat(list0, hasSize(0));
    assertThat(list0, empty());
    assertThat(list0, hasToString("[]"));

    final PairList<String, Integer> list1 = PairList.of("a", 1);
    assertThat(list1, hasSize(1));
    assertThat(list1, hasToString("[<a, 1>]"));

    final PairList<String, Integer> list3 =
        PairList.copyOf("a", 1, "b", null, "c", 3);
    assertThat(list3, hasSize(3));
    assertThat(list3, hasToString("[<a, 1>, <b, null>, <c, 3>]"));

    assertThrows(IllegalArgumentException.class,
        () -> PairList.copyOf("a", 1, "b", 2, "c"),
        "odd number of arguments");
  }

  @Test void testTransform() {
    final PairList<String, Integer> list3 =
        PairList.copyOf("a", 1, null, 5, "c", 3);
    assertThat(list3.transform((s, i) -> s + i),
        isListOf("a1", "null5", "c3"));
    assertThat(list3.transform2((s, i) -> s + i),
        isListOf("a1", "null5", "c3"));

    final PairList<String, Integer> list0 = PairList.of();
    assertThat(list0.transform((s, i) -> s + i), empty());

    final BiPredicate<String, Integer> gt2 = (s, i) -> i > 2;
    assertThat(list3.anyMatch(gt2), is(true));
    assertThat(list3.allMatch(gt2), is(false));
    assertThat(list3.noMatch(gt2), is(false));

    final BiPredicate<String, Integer> negative = (s, i) -> i < 0;
    assertThat(list3.anyMatch(negative), is(false));
    assertThat(list3.allMatch(negative), is(false));
    assertThat(list3.noMatch(negative), is(true));

    final BiPredicate<String, Integer> positive = (s, i) -> i > 0;
    assertThat(list3.anyMatch(positive), is(true));
    assertThat(list3.allMatch(positive), is(true));
    assertThat(list3.noMatch(positive), is(false));

    final BiPredicate<String, Integer> isNull = (s, i) -> s == null;
    assertThat(list3.anyMatch(isNull), is(true));
    assertThat(list3.allMatch(isNull), is(false));
    assertThat(list3.noMatch(isNull), is(false));

    // All predicates behave the same on the empty list
    Arrays.asList(gt2, negative, positive, isNull).forEach(p -> {
      assertThat(list0.anyMatch(p), is(false));
      assertThat(list0.allMatch(p), is(true)); // trivially
      assertThat(list0.noMatch(p), is(true));
    });
  }

  @Test void testBuilder() {
    final PairList.Builder<String, Integer> b = PairList.builder();
    final List<Pair<String, Integer>> list = new ArrayList<>();

    final PairList<String, Integer> list0 = b.build();
    validate(list0, list);

    final ImmutablePairList<String, Integer> list0i = b.buildImmutable();
    validate(list0i, list);

    b.add("a", 1);
    list.add(Pair.of("a", 1));
    final PairList<String, Integer> list1 = b.build();
    validate(list1, list);

    b.add("b", 2);
    b.add("c", null);
    list.add(Pair.of("b", 2));
    list.add(Pair.of("c", null));
    final PairList<String, Integer> list3 = b.build();
    validate(list3, list);

    // Reverse PairList in place
    list3.reverse();
    validate(list3, Lists.reverse(list));

    // Singleton list with null key
    final PairList.Builder<String, Integer> b2 = PairList.builder();
    list.clear();
    b2.add(null, 5);
    list.add(Pair.of(null, 5));
    validate(b2.build(), list);

    // Singleton list with null value
    final PairList.Builder<String, Integer> b3 = PairList.builder();
    list.clear();
    b3.add("x", null);
    list.add(Pair.of("x", null));
    validate(b3.build(), list);
  }

  @Test void testReversed() {
    final PairList<Integer, Integer> list = PairList.of();
    assertThat("empty list", list, hasToString("[]"));

    list.reverse();
    assertThat("empty list, reversed", list, hasToString("[]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));

    list.add(1, 2);
    list.reverse();
    assertThat("singleton list, reversed", list, hasToString("[<1, 2>]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));

    list.reverse();
    assertThat("singleton list reversed twice", list,
        hasToString("[<1, 2>]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));

    list.add(3, 4);
    list.reverse();
    assertThat("list with even length, reversed", list,
        hasToString("[<3, 4>, <1, 2>]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));

    list.reverse();
    assertThat("list with even length, reversed twice", list,
        hasToString("[<1, 2>, <3, 4>]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));

    list.add(5, 6);
    list.reverse();
    assertThat("list with odd length, reversed", list,
        hasToString("[<5, 6>, <3, 4>, <1, 2>]"));
    assertThat(list.reversed(), is(Lists.reverse(list)));
    assertThat(list.reversed().reversed(), is(list));
  }
}
