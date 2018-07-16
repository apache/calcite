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

import org.apache.calcite.linq4j.function.Function0;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit and performance test for {@link ChunkList}.
 */
public class ChunkListTest {
  /**
   * Unit test for {@link ChunkList}.
   */
  @Test public void testChunkList() {
    final ChunkList<Integer> list = new ChunkList<>();
    final ChunkList<Integer> list0 = new ChunkList<>(list);
    final ChunkList<Integer> list1 = new ChunkList<>(list);
    list1.add(123);
    assertEquals(0, list.size());
    assertEquals(0, list0.size());
    assertEquals(1, list1.size());
    assertTrue(list.isEmpty());
    assertEquals("[]", list.toString());

    try {
      list.remove(0);
      fail("expected exception");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }

    try {
      list.get(-1);
      fail("expected exception");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }

    try {
      list.get(0);
      fail("expected exception");
    } catch (IndexOutOfBoundsException e) {
      // ok
    }

    list.add(7);
    assertEquals(1, list.size());
    assertEquals(7, (int) list.get(0));
    assertFalse(list.isEmpty());
    assertEquals("[7]", list.toString());

    list.add(9);
    list.add(null);
    list.add(11);
    assertEquals(4, list.size());
    assertEquals(7, (int) list.get(0));
    assertEquals(9, (int) list.get(1));
    assertNull(list.get(2));
    assertEquals(11, (int) list.get(3));
    assertFalse(list.isEmpty());
    assertEquals("[7, 9, null, 11]", list.toString());

    assertTrue(list.contains(9));
    assertFalse(list.contains(8));

    list.addAll(Collections.nCopies(70, 1));
    assertEquals(74, list.size());
    assertEquals(1, (int) list.get(40));
    assertEquals(1, (int) list.get(70));

    int n = 0;
    for (Integer integer : list) {
      Util.discard(integer);
      ++n;
    }
    assertEquals(n, list.size());

    int i = list.indexOf(null);
    assertEquals(2, i);

    // can't sort if null is present
    list.set(2, 123);

    i = list.indexOf(null);
    assertEquals(-1, i);

    // sort an empty list
    Collections.sort(list0);
    assertThat(list0.isEmpty(), is(true));

    // sort a list with 1 element
    Collections.sort(list1);
    assertThat(list1.size(), is(1));

    Collections.sort(list);
    assertEquals(74, list.size());

    list.remove((Integer) 7);
    Collections.sort(list);
    assertEquals(1, (int) list.get(3));

    // remove all instances of a value that exists
    boolean b = list.removeAll(Collections.singletonList(9));
    assertTrue(b);

    // remove all instances of a non-existent value
    b = list.removeAll(Collections.singletonList(99));
    assertFalse(b);

    // remove all instances of a value that occurs in the last chunk
    list.add(12345);
    b = list.removeAll(Collections.singletonList(12345));
    assertTrue(b);

    // remove all instances of a value that occurs in the last chunk but
    // not as the last value
    list.add(12345);
    list.add(123);
    b = list.removeAll(Collections.singletonList(12345));
    assertTrue(b);

    assertThat(new ChunkList<>(Collections.nCopies(1000, 77)).size(),
        is(1000));

    // add to an empty list via iterator
    //noinspection MismatchedQueryAndUpdateOfCollection
    final ChunkList<String> list2 = new ChunkList<>();
    list2.listIterator(0).add("x");
    assertEquals("[x]", list2.toString());

    // add at start
    list2.add(0, "y");
    assertEquals("[y, x]", list2.toString());

    list2.remove(0);
    assertEquals("[x]", list2.toString());

    // clear a list of length 5, one element at a time, using an iterator
    list2.clear();
    list2.addAll(ImmutableList.of("a", "b", "c", "d", "e"));
    assertThat(list2.size(), is(5));
    final ListIterator<String> listIterator = list2.listIterator(0);
    assertThat(listIterator.next(), is("a"));
    listIterator.remove();
    assertThat(listIterator.next(), is("b"));
    listIterator.remove();
    assertThat(listIterator.next(), is("c"));
    listIterator.remove();
    assertThat(listIterator.next(), is("d"));
    listIterator.remove();
    assertThat(list2.size(), is(1));
    assertThat(listIterator.next(), is("e"));
    listIterator.remove();
    assertThat(list2.size(), is(0));
  }

  /** Clears lists of various sizes. */
  @Test public void testClear() {
    checkListClear(0);
    checkListClear(1);
    checkListClear(2);
    checkListClear(32);
    checkListClear(64);
    checkListClear(65);
    checkListClear(66);
    checkListClear(100);
    checkListClear(127);
    checkListClear(128);
    checkListClear(129);
  }

  private void checkListClear(int n) {
    for (int i = 0; i < 4; i++) {
      ChunkList<String> list = new ChunkList<>(Collections.nCopies(n, "z"));
      assertThat(list.size(), is(n));
      switch (i) {
      case 0:
        list.clear();
        break;
      case 1:
        for (int j = 0; j < n; j++) {
          list.remove(0);
        }
        break;
      case 2:
        for (int j = 0; j < n; j++) {
          list.remove(list.size() - 1);
        }
        break;
      case 3:
        Random random = new Random();
        for (int j = 0; j < n; j++) {
          list.remove(random.nextInt(list.size()));
        }
        break;
      }
      assertThat(list.isEmpty(), is(true));
    }
  }

  /**
   * Removing via an iterator.
   */
  @Test public void testIterator() {
    final ChunkList<String> list = new ChunkList<>();
    list.add("a");
    list.add("b");
    final ListIterator<String> listIterator = list.listIterator(0);
    try {
      listIterator.remove();
      fail("excepted exception");
    } catch (IllegalStateException e) {
      // ok
    }
    listIterator.next();
    listIterator.remove();
    assertThat(list.size(), is(1));
    assertThat(listIterator.hasNext(), is(true));
    listIterator.next();
    listIterator.remove();
    assertThat(list.size(), is(0));
    assertThat(listIterator.hasNext(), is(false));
  }

  /**
   * Unit test for {@link ChunkList} that applies random
   * operations.
   */
  @Test public void testRandom() {
    final int iterationCount = 10000;
    checkRandom(new Random(1), new ChunkList<Integer>(),
        new ArrayList<Integer>(), iterationCount);
    final Random random = new Random(2);
    for (int j = 0; j < 10; j++) {
      checkRandom(random, new ChunkList<Integer>(), new ArrayList<Integer>(),
          iterationCount);
    }
    final ChunkList<Integer> chunkList =
        new ChunkList<>(Collections.nCopies(1000, 5));
    final List<Integer> referenceList = new ArrayList<>(chunkList);
    checkRandom(new Random(3), chunkList, referenceList, iterationCount);
  }

  void checkRandom(
      Random random,
      ChunkList<Integer> list,
      List<Integer> list2,
      int iterationCount) {
    int removeCount = 0;
    int addCount = 0;
    int size;
    int e;
    final int initialCount = list.size();
    for (int i = 0; i < iterationCount; i++) {
      assert list.isValid(true);
      switch (random.nextInt(10)) {
      case 0:
        // remove last
        if (!list.isEmpty()) {
          assertThat(list2.isEmpty(), is(false));
          list.remove(list.size() - 1);
          list2.remove(list2.size() - 1);
          ++removeCount;
        }
        break;
      case 1:
        // add to end
        e = random.nextInt(1000);
        list.add(e);
        list2.add(e);
        ++addCount;
        break;
      case 2:
        int n = 0;
        size = list.size();
        assertThat(list.size(), is(list2.size()));
        for (Integer integer : list) {
          Util.discard(integer);
          assertTrue(n++ < size);
        }
        break;
      case 3:
        // remove all instances of a particular value
        size = list.size();
        final List<Integer> zz = Collections.singletonList(random.nextInt(500));
        boolean b = list.removeAll(zz);
        boolean b2 = list2.removeAll(zz);
        assertThat(b, is(b2));
        if (b) {
          assertTrue(list.size() < size);
          assertTrue(list2.size() < size);
        } else {
          assertTrue(list.size() == size);
          assertTrue(list2.size() == size);
        }
        removeCount += size - list.size();
        break;
      case 4:
        // remove at random position
        if (!list.isEmpty()) {
          e = random.nextInt(list.size());
          list.remove(e);
          list2.remove(e);
          ++removeCount;
        }
        break;
      case 5:
        // add at random position
        int count = random.nextInt(list.size() + 1);
        ListIterator<Integer> it = list.listIterator();
        ListIterator<Integer> it2 = list2.listIterator();
        for (int j = 0; j < count; j++) {
          it.next();
          it2.next();
        }
        size = list.size();
        it.add(size);
        it2.add(size);
        ++addCount;
        break;
      case 6:
        // clear
        if (random.nextInt(200) == 0) {
          removeCount += list.size();
          list.clear();
          list2.clear();
        }
        break;
      default:
        // add at random position
        int pos = random.nextInt(list.size() + 1);
        e = list.size();
        list.add(pos, e);
        list2.add(pos, e);
        ++addCount;
        break;
      }
      assertEquals(list.size(), initialCount + addCount - removeCount);
      assertEquals(list, list2);
    }
  }

  @Test public void testPerformance() {
    if (!Benchmark.enabled()) {
      return;
    }
    //noinspection unchecked
    final Iterable<Pair<Function0<List<Integer>>, String>> factories0 =
        Pair.zip(
            Arrays.asList(ArrayList::new, LinkedList::new, ChunkList::new),
            Arrays.asList("ArrayList", "LinkedList", "ChunkList-64"));
    final List<Pair<Function0<List<Integer>>, String>> factories1 =
        new ArrayList<>();
    for (Pair<Function0<List<Integer>>, String> pair : factories0) {
      factories1.add(pair);
    }
    List<Pair<Function0<List<Integer>>, String>> factories =
        factories1.subList(2, 3);
    Iterable<Pair<Integer, String>> sizes =
        Pair.zip(
            Arrays.asList(100000, 1000000, 10000000),
            Arrays.asList("100k", "1m", "10m"));
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      new Benchmark("add 10m values, " + pair.right, statistician -> {
        final List<Integer> list = pair.left.apply();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
          list.add(1);
        }
        statistician.record(start);
        return null;
      },
      10).run();
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      new Benchmark("iterate over 10m values, " + pair.right, statistician -> {
        final List<Integer> list = pair.left.apply();
        list.addAll(Collections.nCopies(10000000, 1));
        long start = System.currentTimeMillis();
        int count = 0;
        for (Integer integer : list) {
          count += integer;
        }
        statistician.record(start);
        assert count == 10000000;
        return null;
      },
      10).run();
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      for (final Pair<Integer, String> size : sizes) {
        if (size.left > 1000000) {
          continue;
        }
        new Benchmark("delete 10% of " + size.right + " values, " + pair.right,
            statistician -> {
              final List<Integer> list = pair.left.apply();
              list.addAll(Collections.nCopies(size.left, 1));
              long start = System.currentTimeMillis();
              int n = 0;
              for (Iterator<Integer> it = list.iterator(); it.hasNext();) {
                Integer integer = it.next();
                Util.discard(integer);
                if (n++ % 10 == 0) {
                  it.remove();
                }
              }
              statistician.record(start);
              return null;
            },
            10).run();
      }
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      for (final Pair<Integer, String> size : sizes) {
        if (size.left > 1000000) {
          continue;
        }
        new Benchmark("get from " + size.right + " values, "
            + (size.left / 1000) + " times, " + pair.right, statistician -> {
          final List<Integer> list = pair.left.apply();
          list.addAll(Collections.nCopies(size.left, 1));
          final int probeCount = size.left / 1000;
          final Random random = new Random(1);
          long start = System.currentTimeMillis();
          int n = 0;
          for (int i = 0; i < probeCount; i++) {
            n += list.get(random.nextInt(list.size()));
          }
          assert n == probeCount;
          statistician.record(start);
          return null;
        },
        10).run();
      }
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      for (final Pair<Integer, String> size : sizes) {
        if (size.left > 1000000) {
          continue;
        }
        new Benchmark(
            "add " + size.right
            + " values, delete 10%, insert 20%, get 1%, using "
            + pair.right, statistician -> {
          final List<Integer> list = pair.left.apply();
          final int probeCount = size.left / 100;
          long start = System.currentTimeMillis();
          list.addAll(Collections.nCopies(size.left, 1));
          final Random random = new Random(1);
          for (Iterator<Integer> it = list.iterator();
               it.hasNext();) {
            Integer integer = it.next();
            Util.discard(integer);
            if (random.nextInt(10) == 0) {
              it.remove();
            }
          }
          for (ListIterator<Integer> it = list.listIterator();
               it.hasNext();) {
            Integer integer = it.next();
            Util.discard(integer);
            if (random.nextInt(5) == 0) {
              it.add(2);
            }
          }
          int n = 0;
          for (int i = 0; i < probeCount; i++) {
            n += list.get(random.nextInt(list.size()));
          }
          assert n > probeCount;
          statistician.record(start);
          return null;
        },
        10).run();
      }
    }
  }
}

// End ChunkListTest.java
