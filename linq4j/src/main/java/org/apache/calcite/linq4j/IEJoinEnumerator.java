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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** Enumerator that implements IEJoin for two inequality predicates.
 *
 * <p>Implements the union-array algorithm described by Khayyat et al. in
 * <a href="https://doi.org/10.14778/2831360.2831362">"Lightning Fast and Space
 * Efficient Inequality Joins," PVLDB 8(13), 2015</a>.
 *
 * <p>In Section 4.2, {@code firstOrder}, temporary {@code secondOrder},
 * {@code permutation}, and {@code activeRights} correspond to merged
 * {@code L1/L1'}, merged {@code L2/L2'}, merged {@code P/P'}, and extended
 * {@code B'}, respectively. {@code Entry.isLeft} records the source input.
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
  private final List<Entry<TKey1, TKey2>> firstOrder;
  private int[] permutation;
  private final BitSet activeRights = new BitSet();
  private final Function2<? super TLeft, ? super TRight, TResult> resultSelector;

  private int secondPosition;
  private int nextBit;
  private @Nullable Entry<TKey1, TKey2> currentLeft;
  private @Nullable TResult current;
  private boolean hasCurrent;

  IEJoinEnumerator(Enumerable<TLeft> left, Enumerable<TRight> right,
      Function1<? super TLeft, TKey1> leftKeySelector1,
      Function1<? super TRight, TKey1> rightKeySelector1,
      Function1<? super TLeft, TKey2> leftKeySelector2,
      Function1<? super TRight, TKey2> rightKeySelector2,
      Comparator<? super TKey1> comparator1,
      Comparator<? super TKey2> comparator2,
      InequalityOperator operator1, InequalityOperator operator2,
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

  @SuppressWarnings("unchecked")
  private static <TKey1, TKey2, TKey> Comparator<Entry<TKey1, TKey2>>
      entryComparator(Comparator<? super TKey> comparator,
      InequalityOperator operator, boolean isFirstOrder) {
    final boolean greaterThan =
        operator == InequalityOperator.GREATER_THAN
            || operator == InequalityOperator.GREATER_THAN_OR_EQUAL;
    final boolean descending =
        isFirstOrder ? greaterThan : !greaterThan;
    // Equal right keys follow a left entry in firstOrder and precede it in
    // secondOrder only for non-strict operators.
    final boolean strict =
        operator == InequalityOperator.LESS_THAN
            || operator == InequalityOperator.GREATER_THAN;
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
    final int rowIndex;
    final TKey1 key1;
    final TKey2 key2;
    int firstPosition;

    private Entry(boolean isLeft, int rowIndex, TKey1 key1, TKey2 key2) {
      this.isLeft = isLeft;
      this.rowIndex = rowIndex;
      this.key1 = key1;
      this.key2 = key2;
    }
  }
}
