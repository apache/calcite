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

import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Performs a union (or union all) of all its inputs (which must be already sorted),
 * respecting the order.
 *
 * @param <TSource> record type
 * @param <TKey> sort key
 */
final class MergeUnionEnumerator<TSource, TKey> implements Enumerator<TSource> {
  private final Enumerator<TSource>[] inputs;
  private final TSource[] currentInputsValues;
  private final boolean[] inputsFinished;
  private final Function1<TSource, TKey> sortKeySelector;
  private final Comparator<TKey> sortComparator;
  private TSource currentValue;
  private int activeInputs;

  // Set to control duplicates, only used if "all" is false
  private final @Nullable Set<EnumerableDefaults.Wrapped<TSource>> processed;
  private final @Nullable Function1<TSource, EnumerableDefaults.Wrapped<TSource>> wrapper;
  private @Nullable TKey currentKeyInProcessedSet;

  private static final Object NOT_INIT = new Object();

  MergeUnionEnumerator(
      List<Enumerable<TSource>> sources,
      Function1<TSource, TKey> sortKeySelector,
      Comparator<TKey> sortComparator,
      boolean all,
      EqualityComparer<TSource> equalityComparer) {
    this.sortKeySelector = sortKeySelector;
    this.sortComparator = sortComparator;

    if (all) {
      this.processed = null;
      this.wrapper = null;
    } else {
      this.processed = new HashSet<>();
      this.wrapper = EnumerableDefaults.wrapperFor(equalityComparer);
    }

    final int size = sources.size();
    //noinspection unchecked
    this.inputs = new Enumerator[size];
    int i = 0;
    for (Enumerable<TSource> source : sources) {
      this.inputs[i++] = source.enumerator();
    }

    //noinspection unchecked
    this.currentInputsValues = (TSource[]) new Object[size];
    this.activeInputs = this.currentInputsValues.length;
    this.inputsFinished = new boolean[size];
    //noinspection unchecked
    this.currentValue = (TSource) NOT_INIT;

    initEnumerators();
  }

  @RequiresNonNull("inputs")
  @SuppressWarnings("method.invocation.invalid")
  private void initEnumerators(@UnknownInitialization MergeUnionEnumerator<TSource, TKey> this) {
    for (int i = 0; i < inputs.length; i++) {
      moveEnumerator(i);
    }
  }

  private void moveEnumerator(int i) {
    final Enumerator<TSource> enumerator = inputs[i];
    if (!enumerator.moveNext()) {
      activeInputs--;
      inputsFinished[i] = true;
      @Nullable TSource[] auxInputsValues = currentInputsValues;
      auxInputsValues[i] = null;
    } else {
      currentInputsValues[i] = enumerator.current();
      inputsFinished[i] = false;
    }
  }

  private boolean checkNotDuplicated(TSource value) {
    if (processed == null) {
      return true; // UNION ALL: no need to check duplicates
    }

    // check duplicates
    @SuppressWarnings("dereference.of.nullable")
    final EnumerableDefaults.Wrapped<TSource> wrapped = wrapper.apply(value);
    if (!processed.contains(wrapped)) {
      final TKey key = sortKeySelector.apply(value);
      if (!processed.isEmpty()) {
        // Since inputs are sorted, we do not need to keep in the set all the items that we
        // have previously returned, just the ones with the same key, as soon as we see a new
        // key, we can clear the set containing the items belonging to the previous key
        @SuppressWarnings("argument.type.incompatible")
        final int sortComparison = sortComparator.compare(key, currentKeyInProcessedSet);
        if (sortComparison != 0) {
          processed.clear();
          currentKeyInProcessedSet = key;
        }
      } else {
        currentKeyInProcessedSet = key;
      }
      processed.add(wrapped);
      return true;
    }
    return false;
  }

  private int compare(TSource e1, TSource e2) {
    final TKey key1 = sortKeySelector.apply(e1);
    final TKey key2 = sortKeySelector.apply(e2);
    return sortComparator.compare(key1, key2);
  }

  @Override public TSource current() {
    if (currentValue == NOT_INIT) {
      throw new NoSuchElementException();
    }
    return currentValue;
  }

  @Override public boolean moveNext() {
    while (activeInputs > 0) {
      int candidateIndex = -1;
      for (int i = 0; i < currentInputsValues.length; i++) {
        if (!inputsFinished[i]) {
          candidateIndex = i;
          break;
        }
      }

      if (activeInputs > 1) {
        for (int i = candidateIndex + 1; i < currentInputsValues.length; i++) {
          if (inputsFinished[i]) {
            continue;
          }

          final int comp =
              compare(currentInputsValues[candidateIndex],
                  currentInputsValues[i]);
          if (comp > 0) {
            candidateIndex = i;
          }
        }
      }

      if (checkNotDuplicated(currentInputsValues[candidateIndex])) {
        currentValue = currentInputsValues[candidateIndex];
        moveEnumerator(candidateIndex);
        return true;
      } else {
        moveEnumerator(candidateIndex);
        // continue loop
      }
    }
    return false;
  }

  @Override public void reset() {
    for (Enumerator<TSource> enumerator : inputs) {
      enumerator.reset();
    }
    if (processed != null) {
      processed.clear();
      currentKeyInProcessedSet = null;
    }
    //noinspection unchecked
    currentValue = (TSource) NOT_INIT;
    activeInputs = currentInputsValues.length;
    initEnumerators();
  }

  @Override public void close() {
    for (Enumerator<TSource> enumerator : inputs) {
      enumerator.close();
    }
  }
}
