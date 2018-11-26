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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.runtime.Utilities;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.nio.LongBuffer;
import java.util.AbstractList;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;

/**
 * An immutable list of bits.
 */
public class ImmutableBitSet
    implements Iterable<Integer>, Serializable, Comparable<ImmutableBitSet> {
  /** Compares bit sets topologically, so that enclosing bit sets come first,
   * using natural ordering to break ties. */
  public static final Comparator<ImmutableBitSet> COMPARATOR = (o1, o2) -> {
    if (o1.equals(o2)) {
      return 0;
    }
    if (o1.contains(o2)) {
      return -1;
    }
    if (o2.contains(o1)) {
      return 1;
    }
    return o1.compareTo(o2);
  };

  public static final Ordering<ImmutableBitSet> ORDERING =
      Ordering.from(COMPARATOR);

  // BitSets are packed into arrays of "words."  Currently a word is
  // a long, which consists of 64 bits, requiring 6 address bits.
  // The choice of word size is determined purely by performance concerns.
  private static final int ADDRESS_BITS_PER_WORD = 6;
  private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

  /* Used to shift left or right for a partial word mask */
  private static final long WORD_MASK = 0xffffffffffffffffL;

  private static final long[] EMPTY_LONGS = new long[0];

  private static final ImmutableBitSet EMPTY =
      new ImmutableBitSet(EMPTY_LONGS);

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static final
      com.google.common.base.Function<? super BitSet, ImmutableBitSet>
      FROM_BIT_SET = ImmutableBitSet::fromBitSet;

  private final long[] words;

  /** Private constructor. Does not copy the array. */
  private ImmutableBitSet(long[] words) {
    this.words = words;
    assert words.length == 0
        ? words == EMPTY_LONGS
        : words[words.length - 1] != 0L;
  }

  /** Creates an ImmutableBitSet with no bits. */
  public static ImmutableBitSet of() {
    return EMPTY;
  }

  public static ImmutableBitSet of(int... bits) {
    int max = -1;
    for (int bit : bits) {
      max = Math.max(bit, max);
    }
    if (max == -1) {
      return EMPTY;
    }
    long[] words = new long[wordIndex(max) + 1];
    for (int bit : bits) {
      int wordIndex = wordIndex(bit);
      words[wordIndex] |= 1L << bit;
    }
    return new ImmutableBitSet(words);
  }

  public static ImmutableBitSet of(Iterable<Integer>  bits) {
    if (bits instanceof ImmutableBitSet) {
      return (ImmutableBitSet) bits;
    }
    int max = -1;
    for (int bit : bits) {
      max = Math.max(bit, max);
    }
    if (max == -1) {
      return EMPTY;
    }
    long[] words = new long[wordIndex(max) + 1];
    for (int bit : bits) {
      int wordIndex = wordIndex(bit);
      words[wordIndex] |= 1L << bit;
    }
    return new ImmutableBitSet(words);
  }

  /**
   * Creates an ImmutableBitSet with given bits set.
   *
   * <p>For example, <code>of(ImmutableIntList.of(0, 3))</code> returns a bit
   * set with bits {0, 3} set.
   *
   * @param bits Collection of bits to set
   * @return Bit set
   */
  public static ImmutableBitSet of(ImmutableIntList bits) {
    return builder().addAll(bits).build();
  }

  /**
   * Returns a new immutable bit set containing all the bits in the given long
   * array.
   *
   * <p>More precisely,
   *
   * <blockquote>{@code ImmutableBitSet.valueOf(longs).get(n)
   *   == ((longs[n/64] & (1L<<(n%64))) != 0)}</blockquote>
   *
   * <p>for all {@code n < 64 * longs.length}.
   *
   * <p>This method is equivalent to
   * {@code ImmutableBitSet.valueOf(LongBuffer.wrap(longs))}.
   *
   * @param longs a long array containing a little-endian representation
   *        of a sequence of bits to be used as the initial bits of the
   *        new bit set
   * @return a {@code ImmutableBitSet} containing all the bits in the long
   *         array
   */
  public static ImmutableBitSet valueOf(long... longs) {
    int n = longs.length;
    while (n > 0 && longs[n - 1] == 0) {
      --n;
    }
    if (n == 0) {
      return EMPTY;
    }
    return new ImmutableBitSet(Arrays.copyOf(longs, n));
  }

  /**
   * Returns a new immutable bit set containing all the bits in the given long
   * buffer.
   */
  public static ImmutableBitSet valueOf(LongBuffer longs) {
    longs = longs.slice();
    int n = longs.remaining();
    while (n > 0 && longs.get(n - 1) == 0) {
      --n;
    }
    if (n == 0) {
      return EMPTY;
    }
    long[] words = new long[n];
    longs.get(words);
    return new ImmutableBitSet(words);
  }

  /**
   * Returns a new immutable bit set containing all the bits in the given
   * {@link BitSet}.
   */
  public static ImmutableBitSet fromBitSet(BitSet input) {
    return ImmutableBitSet.of(BitSets.toIter(input));
  }

  /**
   * Creates an ImmutableBitSet with bits from {@code fromIndex} (inclusive) to
   * specified {@code toIndex} (exclusive) set to {@code true}.
   *
   * <p>For example, {@code range(0, 3)} returns a bit set with bits
   * {0, 1, 2} set.
   *
   * @param fromIndex Index of the first bit to be set.
   * @param toIndex   Index after the last bit to be set.
   * @return Bit set
   */
  public static ImmutableBitSet range(int fromIndex, int toIndex) {
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException();
    }
    if (toIndex < 0) {
      throw new IllegalArgumentException();
    }
    if (fromIndex == toIndex) {
      return EMPTY;
    }
    int startWordIndex = wordIndex(fromIndex);
    int endWordIndex   = wordIndex(toIndex - 1);
    long[] words = new long[endWordIndex + 1];

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask  = WORD_MASK >>> -toIndex;
    if (startWordIndex == endWordIndex) {
      // One word
      words[startWordIndex] |= firstWordMask & lastWordMask;
    } else {
      // First word, middle words, last word
      words[startWordIndex] |= firstWordMask;
      for (int i = startWordIndex + 1; i < endWordIndex; i++) {
        words[i] = WORD_MASK;
      }
      words[endWordIndex] |= lastWordMask;
    }
    return new ImmutableBitSet(words);
  }

  /** Creates an ImmutableBitSet with bits between 0 and {@code toIndex} set. */
  public static ImmutableBitSet range(int toIndex) {
    return range(0, toIndex);
  }

  /**
   * Given a bit index, return word index containing it.
   */
  private static int wordIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }

  /** Computes the power set (set of all sets) of this bit set. */
  public Iterable<ImmutableBitSet> powerSet() {
    List<List<ImmutableBitSet>> singletons = new ArrayList<>();
    for (int bit : this) {
      singletons.add(
          ImmutableList.of(ImmutableBitSet.of(), ImmutableBitSet.of(bit)));
    }
    return Iterables.transform(Linq4j.product(singletons),
        ImmutableBitSet::union);
  }

  /**
   * Returns the value of the bit with the specified index. The value
   * is {@code true} if the bit with the index {@code bitIndex}
   * is currently set in this {@code ImmutableBitSet}; otherwise, the result
   * is {@code false}.
   *
   * @param  bitIndex   the bit index
   * @return the value of the bit with the specified index
   * @throws IndexOutOfBoundsException if the specified index is negative
   */
  public boolean get(int bitIndex) {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
    }
    int wordIndex = wordIndex(bitIndex);
    return (wordIndex < words.length)
        && ((words[wordIndex] & (1L << bitIndex)) != 0);
  }

  /**
   * Returns a new {@code ImmutableBitSet}
   * composed of bits from this {@code ImmutableBitSet}
   * from {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
   *
   * @param  fromIndex index of the first bit to include
   * @param  toIndex index after the last bit to include
   * @return a new {@code ImmutableBitSet} from a range of
   *         this {@code ImmutableBitSet}
   * @throws IndexOutOfBoundsException if {@code fromIndex} is negative,
   *         or {@code toIndex} is negative, or {@code fromIndex} is
   *         larger than {@code toIndex}
   */
  public ImmutableBitSet get(int fromIndex, int toIndex) {
    checkRange(fromIndex, toIndex);
    final Builder builder = builder();
    for (int i = nextSetBit(fromIndex); i >= 0 && i < toIndex;
         i = nextSetBit(i + 1)) {
      builder.set(i);
    }
    return builder.build();
  }

  /**
   * Checks that fromIndex ... toIndex is a valid range of bit indices.
   */
  private static void checkRange(int fromIndex, int toIndex) {
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
    }
    if (toIndex < 0) {
      throw new IndexOutOfBoundsException("toIndex < 0: " + toIndex);
    }
    if (fromIndex > toIndex) {
      throw new IndexOutOfBoundsException("fromIndex: " + fromIndex
          + " > toIndex: " + toIndex);
    }
  }

  /**
   * Returns a string representation of this bit set. For every index
   * for which this {@code BitSet} contains a bit in the set
   * state, the decimal representation of that index is included in
   * the result. Such indices are listed in order from lowest to
   * highest, separated by ",&nbsp;" (a comma and a space) and
   * surrounded by braces, resulting in the usual mathematical
   * notation for a set of integers.
   *
   * <p>Example:
   * <pre>
   * BitSet drPepper = new BitSet();</pre>
   * Now {@code drPepper.toString()} returns "{@code {}}".
   * <pre>
   * drPepper.set(2);</pre>
   * Now {@code drPepper.toString()} returns "{@code {2}}".
   * <pre>
   * drPepper.set(4);
   * drPepper.set(10);</pre>
   * Now {@code drPepper.toString()} returns "{@code {2, 4, 10}}".
   *
   * @return a string representation of this bit set
   */
  public String toString() {
    int numBits = words.length * BITS_PER_WORD;
    StringBuilder b = new StringBuilder(6 * numBits + 2);
    b.append('{');

    int i = nextSetBit(0);
    if (i != -1) {
      b.append(i);
      for (i = nextSetBit(i + 1); i >= 0; i = nextSetBit(i + 1)) {
        int endOfRun = nextClearBit(i);
        do {
          b.append(", ").append(i);
        }
        while (++i < endOfRun);
      }
    }

    b.append('}');
    return b.toString();
  }

  /**
   * Returns true if the specified {@code ImmutableBitSet} has any bits set to
   * {@code true} that are also set to {@code true} in this
   * {@code ImmutableBitSet}.
   *
   * @param  set {@code ImmutableBitSet} to intersect with
   * @return boolean indicating whether this {@code ImmutableBitSet} intersects
   *         the specified {@code ImmutableBitSet}
   */
  public boolean intersects(ImmutableBitSet set) {
    for (int i = Math.min(words.length, set.words.length) - 1; i >= 0; i--) {
      if ((words[i] & set.words[i]) != 0) {
        return true;
      }
    }
    return false;
  }

  /** Returns the number of bits set to {@code true} in this
   * {@code ImmutableBitSet}.
   *
   * @see #size() */
  public int cardinality() {
    return countBits(words);
  }

  private static int countBits(long[] words) {
    int sum = 0;
    for (long word : words) {
      sum += Long.bitCount(word);
    }
    return sum;
  }

  /**
   * Returns the hash code value for this bit set. The hash code
   * depends only on which bits are set within this {@code ImmutableBitSet}.
   *
   * <p>The hash code is defined using the same calculation as
   * {@link java.util.BitSet#hashCode()}.
   *
   * @return the hash code value for this bit set
   */
  public int hashCode() {
    long h = 1234;
    for (int i = words.length; --i >= 0;) {
      h ^= words[i] * (i + 1);
    }
    return (int) ((h >> 32) ^ h);
  }

  /**
   * Returns the number of bits of space actually in use by this
   * {@code ImmutableBitSet} to represent bit values.
   * The maximum element in the set is the size - 1st element.
   *
   * @return the number of bits currently in this bit set
   *
   * @see #cardinality()
   */
  public int size() {
    return words.length * BITS_PER_WORD;
  }

  /**
   * Compares this object against the specified object.
   * The result is {@code true} if and only if the argument is
   * not {@code null} and is a {@code ImmutableBitSet} object that has
   * exactly the same set of bits set to {@code true} as this bit
   * set.
   *
   * @param  obj the object to compare with
   * @return {@code true} if the objects are the same;
   *         {@code false} otherwise
   * @see    #size()
   */
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ImmutableBitSet)) {
      return false;
    }
    ImmutableBitSet set = (ImmutableBitSet) obj;
    return Arrays.equals(words, set.words);
  }

  /** Compares this ImmutableBitSet with another, using a lexicographic
   * ordering.
   *
   * <p>Bit sets {@code (), (0), (0, 1), (0, 1, 3), (1), (2, 3)} are in sorted
   * order.</p>
   */
  public int compareTo(@Nonnull ImmutableBitSet o) {
    int i = 0;
    for (;;) {
      int n0 = nextSetBit(i);
      int n1 = o.nextSetBit(i);
      int c = Utilities.compare(n0, n1);
      if (c != 0 || n0 < 0) {
        return c;
      }
      i = n0 + 1;
    }
  }

  /**
   * Returns the index of the first bit that is set to {@code true}
   * that occurs on or after the specified starting index. If no such
   * bit exists then {@code -1} is returned.
   *
   * <p>Based upon {@link BitSet#nextSetBit}.
   *
   * @param  fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or {@code -1} if there
   *         is no such bit
   * @throws IndexOutOfBoundsException if the specified index is negative
   */
  public int nextSetBit(int fromIndex) {
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
    }
    int u = wordIndex(fromIndex);
    if (u >= words.length) {
      return -1;
    }
    long word = words[u] & (WORD_MASK << fromIndex);

    while (true) {
      if (word != 0) {
        return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
      }
      if (++u == words.length) {
        return -1;
      }
      word = words[u];
    }
  }

  /**
   * Returns the index of the first bit that is set to {@code false}
   * that occurs on or after the specified starting index.
   *
   * @param  fromIndex the index to start checking from (inclusive)
   * @return the index of the next clear bit
   * @throws IndexOutOfBoundsException if the specified index is negative
   */
  public int nextClearBit(int fromIndex) {
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
    }
    int u = wordIndex(fromIndex);
    if (u >= words.length) {
      return fromIndex;
    }
    long word = ~words[u] & (WORD_MASK << fromIndex);

    while (true) {
      if (word != 0) {
        return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
      }
      if (++u == words.length) {
        return words.length * BITS_PER_WORD;
      }
      word = ~words[u];
    }
  }

  /**
   * Returns the index of the nearest bit that is set to {@code false}
   * that occurs on or before the specified starting index.
   * If no such bit exists, or if {@code -1} is given as the
   * starting index, then {@code -1} is returned.
   *
   * @param  fromIndex the index to start checking from (inclusive)
   * @return the index of the previous clear bit, or {@code -1} if there
   *         is no such bit
   * @throws IndexOutOfBoundsException if the specified index is less
   *         than {@code -1}
   */
  public int previousClearBit(int fromIndex) {
    if (fromIndex < 0) {
      if (fromIndex == -1) {
        return -1;
      }
      throw new IndexOutOfBoundsException("fromIndex < -1: " + fromIndex);
    }

    int u = wordIndex(fromIndex);
    if (u >= words.length) {
      return fromIndex;
    }
    long word = ~words[u] & (WORD_MASK >>> -(fromIndex + 1));

    while (true) {
      if (word != 0) {
        return (u + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
      }
      if (u-- == 0) {
        return -1;
      }
      word = ~words[u];
    }
  }

  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      int i = nextSetBit(0);

      public boolean hasNext() {
        return i >= 0;
      }

      public Integer next() {
        int prev = i;
        i = nextSetBit(i + 1);
        return prev;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Converts this bit set to a list. */
  public List<Integer> toList() {
    final List<Integer> list = new ArrayList<>();
    for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
      list.add(i);
    }
    return list;
  }

  /** Creates a view onto this bit set as a list of integers.
   *
   * <p>The {@code cardinality} and {@code get} methods are both O(n), but
   * the iterator is efficient. The list is memory efficient, and the CPU cost
   * breaks even (versus {@link #toList}) if you intend to scan it only once. */
  public List<Integer> asList() {
    return new AbstractList<Integer>() {
      @Override public Integer get(int index) {
        return nth(index);
      }

      @Override public int size() {
        return cardinality();
      }

      @Nonnull @Override public Iterator<Integer> iterator() {
        return ImmutableBitSet.this.iterator();
      }
    };
  }

  /** Creates a view onto this bit set as a set of integers.
   *
   * <p>The {@code size} and {@code contains} methods are both O(n), but the
   * iterator is efficient. */
  public Set<Integer> asSet() {
    return new AbstractSet<Integer>() {
      @Nonnull public Iterator<Integer> iterator() {
        return ImmutableBitSet.this.iterator();
      }

      public int size() {
        return cardinality();
      }

      @Override public boolean contains(Object o) {
        return ImmutableBitSet.this.get((Integer) o);
      }
    };
  }

  /**
   * Converts this bit set to an array.
   *
   * <p>Each entry of the array is the ordinal of a set bit. The array is
   * sorted.
   *
   * @return Array of set bits
   */
  public int[] toArray() {
    final int[] integers = new int[cardinality()];
    int j = 0;
    for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
      integers[j++] = i;
    }
    return integers;
  }

  /**
   * Converts this bit set to an array of little-endian words.
   */
  public long[] toLongArray() {
    return words.length == 0 ? words : words.clone();
  }

  /** Returns the union of this immutable bit set with a {@link BitSet}. */
  public ImmutableBitSet union(BitSet other) {
    return rebuild() // remember "this" and try to re-use later
        .addAll(BitSets.toIter(other))
        .build();
  }

  /** Returns the union of this bit set with another. */
  public ImmutableBitSet union(ImmutableBitSet other) {
    return rebuild() // remember "this" and try to re-use later
        .addAll(other)
        .build(other); // try to re-use "other"
  }

  /** Returns the union of a number of bit sets. */
  public static ImmutableBitSet union(
      Iterable<? extends ImmutableBitSet> sets) {
    final Builder builder = builder();
    for (ImmutableBitSet set : sets) {
      builder.addAll(set);
    }
    return builder.build();
  }

  /** Returns a bit set with all the bits in this set that are not in
   * another.
   *
   * @see BitSet#andNot(java.util.BitSet) */
  public ImmutableBitSet except(ImmutableBitSet that) {
    final Builder builder = rebuild();
    builder.removeAll(that);
    return builder.build();
  }

  /** Returns a bit set with all the bits set in both this set and in
   * another.
   *
   * @see BitSet#and */
  public ImmutableBitSet intersect(ImmutableBitSet that) {
    final Builder builder = rebuild();
    builder.intersect(that);
    return builder.build();
  }

  /**
   * Returns true if all bits set in the second parameter are also set in the
   * first. In other words, whether x is a super-set of y.
   *
   * @param set1 Bitmap to be checked
   *
   * @return Whether all bits in set1 are set in set0
   */
  public boolean contains(ImmutableBitSet set1) {
    for (int i = set1.nextSetBit(0); i >= 0; i = set1.nextSetBit(i + 1)) {
      if (!get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * The ordinal of a given bit, or -1 if it is not set.
   */
  public int indexOf(int bit) {
    for (int i = nextSetBit(0), k = 0;; i = nextSetBit(i + 1), ++k) {
      if (i < 0) {
        return -1;
      }
      if (i == bit) {
        return k;
      }
    }
  }

  /** Computes the closure of a map from integers to bits.
   *
   * <p>The input must have an entry for each position.
   *
   * <p>Does not modify the input map or its bit sets. */
  public static SortedMap<Integer, ImmutableBitSet> closure(
      SortedMap<Integer, ImmutableBitSet> equivalence) {
    if (equivalence.isEmpty()) {
      return ImmutableSortedMap.of();
    }
    int length = equivalence.lastKey();
    for (ImmutableBitSet bitSet : equivalence.values()) {
      length = Math.max(length, bitSet.length());
    }
    if (equivalence.size() < length
        || equivalence.firstKey() != 0) {
      SortedMap<Integer, ImmutableBitSet> old = equivalence;
      equivalence = new TreeMap<>();
      for (int i = 0; i < length; i++) {
        final ImmutableBitSet bitSet = old.get(i);
        equivalence.put(i, bitSet == null ? ImmutableBitSet.of() : bitSet);
      }
    }
    final Closure closure = new Closure(equivalence);
    return closure.closure;
  }

  /**
   * Returns the "logical size" of this {@code ImmutableBitSet}: the index of
   * the highest set bit in the {@code ImmutableBitSet} plus one. Returns zero
   * if the {@code ImmutableBitSet} contains no set bits.
   *
   * @return the logical size of this {@code ImmutableBitSet}
   */
  public int length() {
    if (words.length == 0) {
      return 0;
    }
    return BITS_PER_WORD * (words.length - 1)
        + (BITS_PER_WORD - Long.numberOfLeadingZeros(words[words.length - 1]));
  }

  /**
   * Returns true if this {@code ImmutableBitSet} contains no bits that are set
   * to {@code true}.
   */
  public boolean isEmpty() {
    return words.length == 0;
  }

  /** Creates an empty Builder. */
  public static Builder builder() {
    return new Builder(EMPTY_LONGS);
  }

  @Deprecated // to be removed before 2.0
  public static Builder builder(ImmutableBitSet bitSet) {
    return bitSet.rebuild();
  }

  /** Creates a Builder whose initial contents are the same as this
   * ImmutableBitSet. */
  public Builder rebuild() {
    return new Rebuilder(this);
  }

  /** Returns the {@code n}th set bit.
   *
   * @throws java.lang.IndexOutOfBoundsException if n is less than 0 or greater
   * than the number of bits set */
  public int nth(int n) {
    int start = 0;
    for (long word : words) {
      final int bitCount = Long.bitCount(word);
      if (n < bitCount) {
        while (word != 0) {
          if ((word & 1) == 1) {
            if (n == 0) {
              return start;
            }
            --n;
          }
          word >>= 1;
          ++start;
        }
      }
      start += 64;
      n -= bitCount;
    }
    throw new IndexOutOfBoundsException("index out of range: " + n);
  }

  /** Returns a bit set the same as this but with a given bit set. */
  public ImmutableBitSet set(int i) {
    return union(ImmutableBitSet.of(i));
  }

  /** Returns a bit set the same as this but with a given bit set (if b is
   * true) or unset (if b is false). */
  public ImmutableBitSet set(int i, boolean b) {
    if (get(i) == b) {
      return this;
    }
    return b ? set(i) : clear(i);
  }

  /** Returns a bit set the same as this but with a given bit set if condition
   * is true. */
  public ImmutableBitSet setIf(int bit, boolean condition) {
    return condition ? set(bit) : this;
  }

  /** Returns a bit set the same as this but with a given bit cleared. */
  public ImmutableBitSet clear(int i) {
    return except(ImmutableBitSet.of(i));
  }

  /** Returns a bit set the same as this but with a given bit cleared if
   * condition is true. */
  public ImmutableBitSet clearIf(int i, boolean condition) {
    return condition ? except(ImmutableBitSet.of(i)) : this;
  }

  /** Returns a {@link BitSet} that has the same contents as this
   * {@code ImmutableBitSet}. */
  public BitSet toBitSet() {
    return BitSets.of(this);
  }

  /** Permutes a bit set according to a given mapping. */
  public ImmutableBitSet permute(Map<Integer, Integer> map) {
    final Builder builder = builder();
    for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
      builder.set(map.get(i));
    }
    return builder.build();
  }

  /** Permutes a collection of bit sets according to a given mapping. */
  public static Iterable<ImmutableBitSet> permute(
      Iterable<ImmutableBitSet> bitSets,
      final Map<Integer, Integer> map) {
    return Iterables.transform(bitSets, bitSet -> bitSet.permute(map));
  }

  /** Returns a bit set with every bit moved up {@code offset} positions.
   * Offset may be negative, but throws if any bit ends up negative. */
  public ImmutableBitSet shift(int offset) {
    if (offset == 0) {
      return this;
    }
    final Builder builder = builder();
    for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
      builder.set(i + offset);
    }
    return builder.build();
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
    private SortedMap<Integer, ImmutableBitSet> equivalence;
    private final SortedMap<Integer, ImmutableBitSet> closure =
        new TreeMap<>();

    Closure(SortedMap<Integer, ImmutableBitSet> equivalence) {
      this.equivalence = equivalence;
      final ImmutableIntList keys =
          ImmutableIntList.copyOf(equivalence.keySet());
      for (int pos : keys) {
        computeClosure(pos);
      }
    }

    private ImmutableBitSet computeClosure(int pos) {
      ImmutableBitSet o = closure.get(pos);
      if (o != null) {
        return o;
      }
      final ImmutableBitSet b = equivalence.get(pos);
      o = b;
      int i = b.nextSetBit(pos + 1);
      for (; i >= 0; i = b.nextSetBit(i + 1)) {
        o = o.union(computeClosure(i));
      }
      closure.put(pos, o);
      i = o.nextSetBit(pos + 1);
      for (; i >= 0; i = b.nextSetBit(i + 1)) {
        closure.put(i, o);
      }
      return o;
    }
  }

  /** Builder. */
  public static class Builder {
    private long[] words;

    private Builder(long[] words) {
      this.words = words;
    }

    /** Builds an ImmutableBitSet from the contents of this Builder.
     *
     * <p>After calling this method, the Builder cannot be used again. */
    public ImmutableBitSet build() {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      if (words.length == 0) {
        return EMPTY;
      }
      long[] words = this.words;
      this.words = null; // prevent re-use of builder
      return new ImmutableBitSet(words);
    }

    /** Builds an ImmutableBitSet from the contents of this Builder.
     *
     * <p>After calling this method, the Builder may be used again. */
    public ImmutableBitSet buildAndReset() {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      if (words.length == 0) {
        return EMPTY;
      }
      long[] words = this.words;
      this.words = EMPTY_LONGS; // reset for next use
      return new ImmutableBitSet(words);
    }

    /** Builds an ImmutableBitSet from the contents of this Builder, using
     * an existing ImmutableBitSet if it happens to have the same contents.
     *
     * <p>Supplying the existing bit set if useful for set operations,
     * where there is a significant chance that the original bit set is
     * unchanged. We save memory because we use the same copy. For example:
     *
     * <blockquote><pre>
     * ImmutableBitSet primeNumbers;
     * ImmutableBitSet hundreds = ImmutableBitSet.of(100, 200, 300);
     * return primeNumbers.except(hundreds);</pre></blockquote>
     *
     * <p>After calling this method, the Builder cannot be used again. */
    public ImmutableBitSet build(ImmutableBitSet bitSet) {
      if (wouldEqual(bitSet)) {
        return bitSet;
      }
      return build();
    }

    public Builder set(int bit) {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      int wordIndex = wordIndex(bit);
      if (wordIndex >= words.length) {
        words = Arrays.copyOf(words, wordIndex + 1);
      }
      words[wordIndex] |= 1L << bit;
      return this;
    }

    public boolean get(int bitIndex) {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      if (bitIndex < 0) {
        throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
      }
      int wordIndex = wordIndex(bitIndex);
      return (wordIndex < words.length)
          && ((words[wordIndex] & (1L << bitIndex)) != 0);
    }

    private void trim(int wordCount) {
      while (wordCount > 0 && words[wordCount - 1] == 0L) {
        --wordCount;
      }
      if (wordCount == words.length) {
        return;
      }
      if (wordCount == 0) {
        words = EMPTY_LONGS;
      } else {
        words = Arrays.copyOfRange(words, 0, wordCount);
      }
    }

    public Builder clear(int bit) {
      int wordIndex = wordIndex(bit);
      if (wordIndex < words.length) {
        words[wordIndex] &= ~(1L << bit);
        trim(words.length);
      }
      return this;
    }

    /** Returns whether the bit set that would be created by this Builder would
     * equal a given bit set. */
    public boolean wouldEqual(ImmutableBitSet bitSet) {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      return Arrays.equals(words, bitSet.words);
    }

    /** Returns the number of set bits. */
    public int cardinality() {
      if (words == null) {
        throw new IllegalArgumentException("can only use builder once");
      }
      return countBits(words);
    }

    /** Sets all bits in a given bit set. */
    public Builder addAll(ImmutableBitSet bitSet) {
      for (Integer bit : bitSet) {
        set(bit);
      }
      return this;
    }

    /** Sets all bits in a given list of bits. */
    public Builder addAll(Iterable<Integer> integers) {
      for (Integer integer : integers) {
        set(integer);
      }
      return this;
    }

    /** Sets all bits in a given list of {@code int}s. */
    public Builder addAll(ImmutableIntList integers) {
      //noinspection ForLoopReplaceableByForEach
      for (int i = 0; i < integers.size(); i++) {
        set(integers.get(i));
      }
      return this;
    }

    /** Clears all bits in a given bit set. */
    public Builder removeAll(ImmutableBitSet bitSet) {
      for (Integer bit : bitSet) {
        clear(bit);
      }
      return this;
    }

    /** Sets a range of bits, from {@code from} to {@code to} - 1. */
    public Builder set(int fromIndex, int toIndex) {
      if (fromIndex > toIndex) {
        throw new IllegalArgumentException();
      }
      if (toIndex < 0) {
        throw new IllegalArgumentException();
      }
      if (fromIndex < toIndex) {
        // Increase capacity if necessary
        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex   = wordIndex(toIndex - 1);
        if (endWordIndex >= words.length) {
          words = Arrays.copyOf(words, endWordIndex + 1);
        }

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask  = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex) {
          // One word
          words[startWordIndex] |= firstWordMask & lastWordMask;
        } else {
          // First word, middle words, last word
          words[startWordIndex] |= firstWordMask;
          for (int i = startWordIndex + 1; i < endWordIndex; i++) {
            words[i] = WORD_MASK;
          }
          words[endWordIndex] |= lastWordMask;
        }
      }
      return this;
    }

    public boolean isEmpty() {
      return words.length == 0;
    }

    public void intersect(ImmutableBitSet that) {
      int x = Math.min(words.length, that.words.length);
      for (int i = 0; i < x; i++) {
        words[i] &= that.words[i];
      }
      trim(x);
    }
  }

  /** Refinement of {@link Builder} that remembers its original
   * {@link org.apache.calcite.util.ImmutableBitSet} and tries to use it
   * when {@link #build} is called. */
  private static class Rebuilder extends Builder {
    private final ImmutableBitSet originalBitSet;

    private Rebuilder(ImmutableBitSet originalBitSet) {
      super(originalBitSet.words.clone());
      this.originalBitSet = originalBitSet;
    }

    @Override public ImmutableBitSet build() {
      if (wouldEqual(originalBitSet)) {
        return originalBitSet;
      }
      return super.build();
    }

    @Override public ImmutableBitSet build(ImmutableBitSet bitSet) {
      // We try to re-use both originalBitSet and bitSet.
      if (wouldEqual(originalBitSet)) {
        return originalBitSet;
      }
      return super.build(bitSet);
    }
  }
}

// End ImmutableBitSet.java
