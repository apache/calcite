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
package org.eigenbase.util;

import java.util.*;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.linq4j.function.Function1;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit and performance test for {@link ChunkList}.
 */
public class ChunkListTest {
  /**
   * Unit test for {@link ChunkList}.
   */
  @Test public void testChunkList() {
    final ChunkList<Integer> list = new ChunkList<Integer>();
    assertEquals(0, list.size());
    assertTrue(list.isEmpty());
    assertEquals("[]", list.toString());

    try {
      list.remove(0);
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

    list.addAll(Collections.nCopies(40, 1));
    assertEquals(44, list.size());
    assertEquals(1, (int) list.get(40));

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

    Collections.sort(list);

    list.remove((Integer) 7);
    Collections.sort(list);
    assertEquals(1, (int) list.get(3));

    // remove all instances of a value that exists
    boolean b = list.removeAll(Arrays.asList(9));
    assertTrue(b);

    // remove all instances of a non-existent value
    b = list.removeAll(Arrays.asList(99));
    assertFalse(b);

    // remove all instances of a value that occurs in the last chunk
    list.add(12345);
    b = list.removeAll(Arrays.asList(12345));
    assertTrue(b);

    // remove all instances of a value that occurs in the last chunk but
    // not as the last value
    list.add(12345);
    list.add(123);
    b = list.removeAll(Arrays.asList(12345));
    assertTrue(b);

    assertEquals(
        1000, new ChunkList<Integer>(Collections.nCopies(1000, 77)).size());

    // add to an empty list via iterator
    //noinspection MismatchedQueryAndUpdateOfCollection
    final ChunkList<String> list2 = new ChunkList<String>();
    list2.listIterator(0).add("x");
    assertEquals("[x]", list2.toString());

    // add at start
    list2.add(0, "y");
    assertEquals("[y, x]", list2.toString());
  }

  /**
   * Unit test for {@link ChunkList} that applies random
   * operations.
   */
  @Test public void testRandom() {
    final int iterationCount = 10000;
    checkRandom(new Random(1), new ChunkList<Integer>(), iterationCount);
    final Random random = new Random(2);
    for (int j = 0; j < 10; j++) {
      checkRandom(random, new ChunkList<Integer>(), iterationCount);
    }
    checkRandom(
        new Random(3), new ChunkList<Integer>(Collections.nCopies(1000, 5)),
        iterationCount);
  }

  void checkRandom(
      Random random,
      ChunkList<Integer> list,
      int iterationCount) {
    int removeCount = 0;
    int addCount = 0;
    final int initialCount = list.size();
    for (int i = 0; i < iterationCount; i++) {
      assert list.isValid(true);
      switch (random.nextInt(8)) {
      case 0:
        // remove last
        if (!list.isEmpty()) {
          list.remove(list.size() - 1);
          ++removeCount;
        }
        break;
      case 1:
        // add to end
        list.add(random.nextInt(1000));
        ++addCount;
        break;
      case 2:
        int n = 0;
        final int size = list.size();
        for (Integer integer : list) {
          Util.discard(integer);
          assertTrue(n++ < size);
        }
        break;
      case 3:
        // remove all instances of a particular value
        int sizeBefore = list.size();
        boolean b = list.removeAll(
            Collections.singletonList(random.nextInt(500)));
        if (b) {
          assertTrue(list.size() < sizeBefore);
        } else {
          assertTrue(list.size() == sizeBefore);
        }
        removeCount += sizeBefore - list.size();
        break;
      case 4:
        // remove at random position
        if (!list.isEmpty()) {
          list.remove(random.nextInt(list.size()));
          ++removeCount;
        }
        break;
      case 5:
        // add at random position
        int count = random.nextInt(list.size() + 1);
        ListIterator<Integer> it = list.listIterator();
        for (int j = 0; j < count; j++) {
          it.next();
        }
        it.add(list.size());
        ++addCount;
        break;
      default:
        // add at random position
        list.add(random.nextInt(list.size() + 1), list.size());
        ++addCount;
        break;
      }
      assertEquals(list.size(), initialCount + addCount - removeCount);
    }
  }

  @Test public void testPerformance() {
    if (!Benchmark.enabled()) {
      return;
    }
    //noinspection unchecked
    final Iterable<Pair<Function0<List<Integer>>, String>> factories0 =
        Pair.zip(
            Arrays.asList(
                new Function0<List<Integer>>() {
                  public List<Integer> apply() {
                    return new ArrayList<Integer>();
                  }
                },
                new Function0<List<Integer>>() {
                  public List<Integer> apply() {
                    return new LinkedList<Integer>();
                  }
                },
                new Function0<List<Integer>>() {
                  public List<Integer> apply() {
                    return new ChunkList<Integer>();
                  }
                }),
            Arrays.asList("ArrayList", "LinkedList", "ChunkList-64"));
    final List<Pair<Function0<List<Integer>>, String>> factories1 =
        new ArrayList<Pair<Function0<List<Integer>>, String>>();
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
      new Benchmark(
          "add 10m values, " + pair.right,
          new Function1<Benchmark.Statistician, Void>() {
            public Void apply(Benchmark.Statistician statistician) {
              final List<Integer> list = pair.left.apply();
              long start = System.currentTimeMillis();
              for (int i = 0; i < 10000000; i++) {
                list.add(1);
              }
              statistician.record(start);
              return null;
            }
          },
          10).run();
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      new Benchmark(
          "iterate over 10m values, " + pair.right,
          new Function1<Benchmark.Statistician, Void>() {
            public Void apply(Benchmark.Statistician statistician) {
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
            }
          },
          10).run();
    }
    for (final Pair<Function0<List<Integer>>, String> pair : factories) {
      for (final Pair<Integer, String> size : sizes) {
        if (size.left > 1000000) {
          continue;
        }
        new Benchmark(
            "delete 10% of " + size.right + " values, " + pair.right,
            new Function1<Benchmark.Statistician, Void>() {
              public Void apply(Benchmark.Statistician statistician) {
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
              }
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
            "get from " + size.right + " values, " + (size.left / 1000)
            + " times, " + pair.right,
            new Function1<Benchmark.Statistician, Void>() {
              public Void apply(Benchmark.Statistician statistician) {
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
              }
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
            + pair.right,
            new Function1<Benchmark.Statistician, Void>() {
              public Void apply(Benchmark.Statistician statistician) {
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
              }
            },
            10).run();
      }
    }
  }
}

// End ChunkListTest.java
