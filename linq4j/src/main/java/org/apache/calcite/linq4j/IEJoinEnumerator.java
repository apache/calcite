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
package org.apache.calcite.linq4j;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.ExpressionType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** Enumerator that implements IEJoin for two inequality predicates.
 *
 * <p>Both inputs are materialized; rows with a null key are discarded. The
 * entries are sorted by each key. A right entry satisfies predicate 1 exactly
 * when it follows a left entry in {@code firstOrder}, and predicate 2 exactly
 * when it precedes that left entry in {@code secondOrder}. Sort directions and
 * tie-breaking depend on the operators; see {@link #entryComparator}.
 *
 * <p>{@code permutation} maps positions in {@code secondOrder} to positions in
 * {@code firstOrder}. Scanning in the second order, each right entry sets its
 * first-order position in {@code activeRights}. For each left entry, set bits
 * after its first-order position identify the right entries satisfying both
 * predicates.
 *
 * <p>For example, for {@code l.x < r.x AND l.y > r.y}, let the left row be
 * {@code l=(2,8)} and the right rows be {@code a=(4,3)}, {@code b=(1,6)}, and
 * {@code c=(5,9)}:
 *
 * <pre>
 * position:             0  1  2  3
 * firstOrder (x asc):   b  l  a  c
 * secondOrder (y asc):  a  b  l  c
 * permutation:          2  0  1  3
 * </pre>
 *
 * <p>When the scan reaches l, {@code activeRights} contains positions 0 (b)
 * and 2 (a). Searching after l's position, 1, yields only a. Row b fails
 * predicate 1, and row c has not yet been visited because it fails predicate 2.
 *
 * <p>Based on the union-array algorithm in Section 4.2 of Khayyat et al.,
 * <a href="https://doi.org/10.14778/2831360.2831362">"Lightning Fast and Space
 * Efficient Inequality Joins," PVLDB 8(13), 2015</a>.
 *
 * @param <TLeft> Left row type
 * @param <TRight> Right row type
 * @param <TKey1> First key type
 * @param <TKey2> Second key type
 * @param <TResult> Result row type
 */
final class IEJoinEnumerator<TLeft, TRight, TKey1, TKey2, TResult>
    implements Enumerator<TResult> {
  private final List<TLeft> leftRows = new ArrayList<>();

  private final List<TRight> rightRows = new ArrayList<>();

  /** Entries from both inputs in first-key order. */
  private final List<Entry<TKey1, TKey2>> firstOrder;

  /** Maps second-order positions to {@link #firstOrder} positions. */
  private int[] permutation;

  /** First-order positions of right entries already visited in the
   * second-order scan. */
  private final BitSet activeRights = new BitSet();

  private final Function2<? super TLeft, ? super TRight, TResult> resultSelector;

  /** Next index in {@link #permutation} to visit. */
  private int secondPosition;

  /** Inclusive first-order position from which to resume searching for
   * matches for {@link #currentLeft}. */
  private int nextBit;

  /** Left entry whose matches are being emitted, or null between entries. */
  private @Nullable Entry<TKey1, TKey2> currentLeft;

  private @Nullable TResult current;

  /** Whether {@link #current} is a valid result. Kept separately because the
   * result selector may itself return null. */
  private boolean hasCurrent;

  /** Reads and closes both input enumerators, caches the rows and their keys,
   * and prepares both sort orders. Rows with either key null are excluded.
   * {@link #reset()} reuses this state without reading the inputs again.
   *
   * <p>Selectors with the same numeric suffix form a predicate of the form
   * {@code leftKey operator rightKey}, using the corresponding comparator.
   * Comparators define key order; this class derives sort directions and
   * equal-key tie-breaks from the operators. The result selector is called
   * only as {@link #moveNext()} emits matching pairs.
   */
  IEJoinEnumerator(Enumerable<TLeft> left, Enumerable<TRight> right,
      Function1<? super TLeft, TKey1> leftKeySelector1,
      Function1<? super TRight, TKey1> rightKeySelector1,
      Function1<? super TLeft, TKey2> leftKeySelector2,
      Function1<? super TRight, TKey2> rightKeySelector2,
      Comparator<? super TKey1> comparator1,
      Comparator<? super TKey2> comparator2,
      ExpressionType operator1, ExpressionType operator2,
      Function2<? super TLeft, ? super TRight, TResult> resultSelector) {
    this.resultSelector = resultSelector;

    final List<Entry<TKey1, TKey2>> entries = new ArrayList<>();
    try (Enumerator<TLeft> enumerator = left.enumerator()) {
      while (enumerator.moveNext()) {
        final TLeft row = enumerator.current();
        final @Nullable TKey1 key1 = leftKeySelector1.apply(row);
        final @Nullable TKey2 key2 = leftKeySelector2.apply(row);
        if (key1 != null && key2 != null) {
          final int rowIndex = leftRows.size();
          leftRows.add(row);
          entries.add(new Entry<>(true, rowIndex, key1, key2));
        }
      }
    }
    try (Enumerator<TRight> enumerator = right.enumerator()) {
      while (enumerator.moveNext()) {
        final TRight row = enumerator.current();
        final @Nullable TKey1 key1 = rightKeySelector1.apply(row);
        final @Nullable TKey2 key2 = rightKeySelector2.apply(row);
        if (key1 != null && key2 != null) {
          final int rowIndex = rightRows.size();
          rightRows.add(row);
          entries.add(new Entry<>(false, rowIndex, key1, key2));
        }
      }
    }

    firstOrder = new ArrayList<>(entries);
    firstOrder.sort(
        entryComparator(comparator1, operator1, true));
    for (int i = 0; i < firstOrder.size(); i++) {
      firstOrder.get(i).firstPosition = i;
    }

    final List<Entry<TKey1, TKey2>> secondOrder = new ArrayList<>(entries);
    secondOrder.sort(
        entryComparator(comparator2, operator2, false));
    permutation = new int[secondOrder.size()];
    for (int i = 0; i < secondOrder.size(); i++) {
      permutation[i] = secondOrder.get(i).firstPosition;
    }
  }

  /** Orders entries by key 1 if {@code isFirstOrder}, or by key 2 otherwise.
   *
   * <p>For equal keys from different inputs, strict operators put the right
   * entry first in the first order and last in the second order, excluding the
   * pair. Non-strict operators reverse both tie-breaks to include the pair.
   */
  @SuppressWarnings("unchecked")
  private static <TKey1, TKey2, TKey> Comparator<Entry<TKey1, TKey2>>
      entryComparator(Comparator<? super TKey> comparator,
      ExpressionType operator, boolean isFirstOrder) {
    final boolean greaterThan =
        operator == ExpressionType.GreaterThan
            || operator == ExpressionType.GreaterThanOrEqual;
    final boolean descending =
        isFirstOrder ? greaterThan : !greaterThan;
    final boolean strict =
        operator == ExpressionType.LessThan
            || operator == ExpressionType.GreaterThan;
    final boolean leftSideFirst = isFirstOrder != strict;
    return (entry1, entry2) -> {
      final TKey key1 = isFirstOrder
          ? castNonNull((TKey) entry1.key1)
          : castNonNull((TKey) entry1.key2);
      final TKey key2 = isFirstOrder
          ? castNonNull((TKey) entry2.key1)
          : castNonNull((TKey) entry2.key2);
      final int c = descending
          ? comparator.compare(key2, key1)
          : comparator.compare(key1, key2);
      if (c != 0 || entry1.isLeft == entry2.isLeft) {
        return c;
      }

      return entry1.isLeft == leftSideFirst ? -1 : 1;
    };
  }

  @Override public TResult current() {
    if (!hasCurrent) {
      throw new NoSuchElementException();
    }
    return castNonNull(current);
  }

  @Override public boolean moveNext() {
    hasCurrent = false;
    while (true) {
      // Rights already seen in secondOrder satisfy predicate 2; active bits
      // after currentLeft's position in firstOrder also satisfy predicate 1.
      if (currentLeft != null) {
        final int bit = activeRights.nextSetBit(nextBit);
        if (bit >= 0) {
          nextBit = bit + 1;
          final Entry<TKey1, TKey2> right = firstOrder.get(bit);
          current =
              resultSelector.apply(leftRows.get(currentLeft.rowIndex),
                  rightRows.get(right.rowIndex));
          hasCurrent = true;
          return true;
        }
        currentLeft = null;
      }

      if (secondPosition >= permutation.length) {
        current = null;
        return false;
      }

      final int firstPosition = permutation[secondPosition++];
      final Entry<TKey1, TKey2> entry = firstOrder.get(firstPosition);
      if (entry.isLeft) {
        currentLeft = entry;
        nextBit = firstPosition + 1;
      } else {
        activeRights.set(firstPosition);
      }
    }
  }

  @Override public void reset() {
    activeRights.clear();
    secondPosition = 0;
    nextBit = 0;
    currentLeft = null;
    current = null;
    hasCurrent = false;
  }

  @Override public void close() {
    reset();
    leftRows.clear();
    rightRows.clear();
    firstOrder.clear();
    permutation = new int[0];
  }

  /** Row entry shared by the two sorted orders.
   *
   * @param <TKey1> First key type
   * @param <TKey2> Second key type
   */
  private static final class Entry<TKey1, TKey2> {
    final boolean isLeft;

    /** Index into {@code leftRows} or {@code rightRows}, according to
     * {@link #isLeft}. */
    final int rowIndex;
    final TKey1 key1;
    final TKey2 key2;
    /** Index in {@code firstOrder}, assigned after sorting. */
    int firstPosition;

    private Entry(boolean isLeft, int rowIndex, TKey1 key1, TKey2 key2) {
      this.isLeft = isLeft;
      this.rowIndex = rowIndex;
      this.key1 = key1;
      this.key2 = key2;
    }
  }
}
