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
package org.apache.optiq.util;

import java.util.*;

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
    return new Iterable<Integer>() {
      public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
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
    };
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
  public static IntList toList(final BitSet bitSet) {
    final IntList list = new IntList();
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      list.add(i);
    }
    return list;
  }

  /**
   * Converts a bitset to an array.
   *
   * @param bitSet Bit set
   * @return List of set bits
   */
  public static Integer[] toArray(final BitSet bitSet) {
    final List<Integer> list = toList(bitSet);
    return list.toArray(new Integer[list.size()]);
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
   * Creates a bitset with given bits set.
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
   * Creates a bitset with given bits set.
   *
   * <p>For example, {@code of(Arrays.asList(0, 3)) } returns a bit set
   * with bits {0, 3} set.
   *
   * @param bits Collection of bits to set
   * @return Bit set
   */
  public static BitSet of(Collection<? extends Number> bits) {
    final BitSet bitSet = new BitSet();
    for (Number bit : bits) {
      bitSet.set(bit.intValue());
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

  public static BitSet range(int toIndex) {
    return range(0, toIndex);
  }
}

// End BitSets.java
