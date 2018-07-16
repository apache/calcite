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

import com.google.common.collect.ImmutableSortedMap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utility functions for {@link BitSet}.
 */
public final class BitSets {
  private BitSets() {
    throw new AssertionError("no instances!");
  }

  /**
   * Returns true if all bits set in the second parameter are also set in the
   * first. In other words, whether x is a super-set of y.
   *
   * @param set0 Containing bitmap
   * @param set1 Bitmap to be checked
   *
   * @return Whether all bits in set1 are set in set0
   */
  public static boolean contains(BitSet set0, BitSet set1) {
    for (int i = set1.nextSetBit(0); i >= 0; i = set1.nextSetBit(i + 1)) {
      if (!set0.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if all bits set in the second parameter are also set in the
   * first. In other words, whether x is a super-set of y.
   *
   * @param set0 Containing bitmap
   * @param set1 Bitmap to be checked
   *
   * @return Whether all bits in set1 are set in set0
   */
  public static boolean contains(BitSet set0, ImmutableBitSet set1) {
    for (int i = set1.nextSetBit(0); i >= 0; i = set1.nextSetBit(i + 1)) {
      if (!set0.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns an iterable over the bits in a bitmap that are set to '1'.
   *
   * <p>This allows you to iterate over a bit set using a 'foreach' construct.
   * For instance:
   *
   * <blockquote><code>
   * BitSet bitSet;<br>
   * for (int i : Util.toIter(bitSet)) {<br>
   * &nbsp;&nbsp;print(i);<br>
   * }</code></blockquote>
   *
   * @param bitSet Bit set
   * @return Iterable
   */
  public static Iterable<Integer> toIter(final BitSet bitSet) {
    return () -> new Iterator<Integer>() {
      int i = bitSet.nextSetBit(0);

      public boolean hasNext() {
        return i >= 0;
      }

      public Integer next() {
        int prev = i;
        i = bitSet.nextSetBit(i + 1);
        return prev;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public static Iterable<Integer> toIter(final ImmutableBitSet bitSet) {
    return bitSet;
  }

  /**
   * Converts a bitset to a list.
   *
   * <p>The list is mutable, and future changes to the list do not affect the
   * contents of the bit set.
   *
   * @param bitSet Bit set
   * @return List of set bits
   */
  public static List<Integer> toList(final BitSet bitSet) {
    final List<Integer> list = new ArrayList<>();
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      list.add(i);
    }
    return list;
  }

  /**
   * Converts a BitSet to an array.
   *
   * @param bitSet Bit set
   * @return Array of set bits
   */
  public static int[] toArray(final BitSet bitSet) {
    final int[] integers = new int[bitSet.cardinality()];
    int j = 0;
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      integers[j++] = i;
    }
    return integers;
  }

  /**
   * Creates a bitset with given bits set.
   *
   * <p>For example, {@code of(0, 3)} returns a bit set with bits {0, 3}
   * set.
   *
   * @param bits Array of bits to set
   * @return Bit set
   */
  public static BitSet of(int... bits) {
    final BitSet bitSet = new BitSet();
    for (int bit : bits) {
      bitSet.set(bit);
    }
    return bitSet;
  }

  /**
   * Creates a BitSet with given bits set.
   *
   * <p>For example, {@code of(new Integer[] {0, 3})} returns a bit set
   * with bits {0, 3} set.
   *
   * @param bits Array of bits to set
   * @return Bit set
   */
  public static BitSet of(Integer[] bits) {
    final BitSet bitSet = new BitSet();
    for (int bit : bits) {
      bitSet.set(bit);
    }
    return bitSet;
  }

  /**
   * Creates a BitSet with given bits set.
   *
   * <p>For example, {@code of(Arrays.asList(0, 3)) } returns a bit set
   * with bits {0, 3} set.
   *
   * @param bits Collection of bits to set
   * @return Bit set
   */
  public static BitSet of(Iterable<? extends Number> bits) {
    final BitSet bitSet = new BitSet();
    for (Number bit : bits) {
      bitSet.set(bit.intValue());
    }
    return bitSet;
  }

  /**
   * Creates a BitSet with given bits set.
   *
   * <p>For example, {@code of(ImmutableIntList.of(0, 3))} returns a bit set
   * with bits {0, 3} set.
   *
   * @param bits Collection of bits to set
   * @return Bit set
   */
  public static BitSet of(ImmutableIntList bits) {
    final BitSet bitSet = new BitSet();
    for (int i = 0, n = bits.size(); i < n; i++) {
      bitSet.set(bits.getInt(i));
    }
    return bitSet;
  }

  /**
   * Creates a bitset with bits from {@code fromIndex} (inclusive) to
   * specified {@code toIndex} (exclusive) set to {@code true}.
   *
   * <p>For example, {@code range(0, 3)} returns a bit set with bits
   * {0, 1, 2} set.
   *
   * @param fromIndex Index of the first bit to be set.
   * @param toIndex   Index after the last bit to be set.
   * @return Bit set
   */
  public static BitSet range(int fromIndex, int toIndex) {
    final BitSet bitSet = new BitSet();
    if (toIndex > fromIndex) {
      // Avoid http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
      // "BitSet internal invariants may be violated"
      bitSet.set(fromIndex, toIndex);
    }
    return bitSet;
  }

  /** Creates a BitSet with bits between 0 and {@code toIndex} set. */
  public static BitSet range(int toIndex) {
    return range(0, toIndex);
  }

  /** Sets all bits in a given BitSet corresponding to integers from a list. */
  public static void setAll(BitSet bitSet, Iterable<? extends Number> list) {
    for (Number number : list) {
      bitSet.set(number.intValue());
    }
  }

  /** Returns a BitSet that is the union of the given BitSets. Does not modify
   * any of the inputs. */
  public static BitSet union(BitSet set0, BitSet... sets) {
    final BitSet s = (BitSet) set0.clone();
    for (BitSet set : sets) {
      s.or(set);
    }
    return s;
  }

  /** Returns the previous clear bit.
   *
   * <p>Has same behavior as {@link BitSet#previousClearBit}, but that method
   * does not exist before 1.7. */
  public static int previousClearBit(BitSet bitSet, int fromIndex) {
    if (fromIndex < -1) {
      throw new IndexOutOfBoundsException();
    }
    while (fromIndex >= 0) {
      if (!bitSet.get(fromIndex)) {
        return fromIndex;
      }
      --fromIndex;
    }
    return -1;
  }

  /** Computes the closure of a map from integers to bits.
   *
   * <p>The input must have an entry for each position.
   *
   * <p>Does not modify the input map or its bit sets. */
  public static SortedMap<Integer, BitSet> closure(
      SortedMap<Integer, BitSet> equivalence) {
    if (equivalence.isEmpty()) {
      return ImmutableSortedMap.of();
    }
    int length = equivalence.lastKey();
    for (BitSet bitSet : equivalence.values()) {
      length = Math.max(length, bitSet.length());
    }
    if (equivalence.size() < length
        || equivalence.firstKey() != 0) {
      SortedMap<Integer, BitSet> old = equivalence;
      equivalence = new TreeMap<>();
      for (int i = 0; i < length; i++) {
        final BitSet bitSet = old.get(i);
        equivalence.put(i, bitSet == null ? new BitSet() : bitSet);
      }
    }
    final Closure closure = new Closure(equivalence);
    return closure.closure;
  }

  /** Populates a {@link BitSet} from an iterable, such as a list of integer. */
  public static void populate(BitSet bitSet, Iterable<? extends Number> list) {
    for (Number number : list) {
      bitSet.set(number.intValue());
    }
  }

  /** Populates a {@link BitSet} from an
   *  {@link ImmutableIntList}. */
  public static void populate(BitSet bitSet, ImmutableIntList list) {
    for (int i = 0; i < list.size(); i++) {
      bitSet.set(list.getInt(i));
    }
  }

  /**
   * Setup equivalence Sets for each position. If i and j are equivalent then
   * they will have the same equivalence Set. The algorithm computes the
   * closure relation at each position for the position wrt to positions
   * greater than it. Once a closure is computed for a position, the closure
   * Set is set on all its descendants. So the closure computation bubbles up
   * from lower positions and the final equivalence Set is propagated down
   * from the lowest element in the Set.
   */
  private static class Closure {
    private SortedMap<Integer, BitSet> equivalence;
    private final SortedMap<Integer, BitSet> closure = new TreeMap<>();

    Closure(SortedMap<Integer, BitSet> equivalence) {
      this.equivalence = equivalence;
      final ImmutableIntList keys =
          ImmutableIntList.copyOf(equivalence.keySet());
      for (int pos : keys) {
        computeClosure(pos);
      }
    }

    private BitSet computeClosure(int pos) {
      BitSet o = closure.get(pos);
      if (o != null) {
        return o;
      }
      BitSet b = equivalence.get(pos);
      o = (BitSet) b.clone();
      int i = b.nextSetBit(pos + 1);
      for (; i >= 0; i = b.nextSetBit(i + 1)) {
        o.or(computeClosure(i));
      }
      closure.put(pos, o);
      i = o.nextSetBit(pos + 1);
      for (; i >= 0; i = b.nextSetBit(i + 1)) {
        closure.put(i, o);
      }
      return o;
    }
  }
}

// End BitSets.java
