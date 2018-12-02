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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.CircularArrayList;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/** Workspace that matches patterns against an automaton.
 *
 * @param <E> Type of rows matched by this automaton */
public class Matcher<E> {
  private final Automaton automaton;
  private final ImmutableList<Predicate<E>> predicates;

  // The following members are work space. They can be shared among partitions,
  // but only one thread can use them at a time. Putting them here saves the
  // expense of creating a fresh object each call to "match".

  private final ImmutableBitSet emptyStateSet = ImmutableBitSet.of();
  private final ImmutableBitSet startSet;
  private final List<Integer> rowSymbols = new ArrayList<>();
  private final ImmutableBitSet.Builder nextStateBuilder =
      ImmutableBitSet.builder();

  /** Creates a Matcher; use {@link #builder}. */
  private Matcher(Automaton automaton,
      ImmutableList<Predicate<E>> predicates) {
    this.automaton = Objects.requireNonNull(automaton);
    this.predicates = Objects.requireNonNull(predicates);
    final ImmutableBitSet.Builder startSetBuilder =
        ImmutableBitSet.builder();
    startSetBuilder.set(automaton.startState.id);
    automaton.epsilonSuccessors(automaton.startState.id, startSetBuilder);
    startSet = startSetBuilder.build();
  }

  public static <E> Builder<E> builder(Automaton automaton) {
    return new Builder<>(automaton);
  }

  public List<List<E>> match(E... rows) {
    return match(Arrays.asList(rows));
  }

  public List<List<E>> match(Iterable<E> rows) {
    final ImmutableList.Builder<List<E>> resultMatches =
        ImmutableList.builder();
    final PartitionState<E> partitionState = new PartitionState<>();
    for (E row : rows) {
      matchOne(partitionState, resultMatches, row);
    }
    return resultMatches.build();
  }

  /** Feeds a single input row into the given partition state,
   * and writes the resulting output rows (if any). */
  protected void matchOne(PartitionState<E> partitionState,
      ImmutableList.Builder<List<E>> resultMatches,
      E row) {
    // Add this row to the states.
    partitionState.bufferedRows.add(row);
    partitionState.stateSets.add(startSet);

    // Compute the set of symbols whose predicates that evaluate to true
    // for this row.
    rowSymbols.clear();
    for (Ord<Predicate<E>> predicate : Ord.zip(predicates)) {
      if (predicate.e.test(row)) {
        rowSymbols.add(predicate.i);
      }
    }

    // TODO: Should we short-cut if symbols is empty?
    // TODO: Merge states with epsilon-successors

    // Now process the states of matches, oldest first, and compute the
    // successors based on the predicates that are true for the current
    // row.
    for (int i = 0; i < partitionState.stateSets.size();) {
      final ImmutableBitSet stateSet = partitionState.stateSets.get(i);
      assert nextStateBuilder.isEmpty();
      for (int symbol : rowSymbols) {
        for (int state : stateSet) {
          automaton.successors(state, symbol, nextStateBuilder);
        }
      }
      final ImmutableBitSet nextStateSet = nextStateBuilder.buildAndReset();
      if (nextStateSet.isEmpty()) {
        if (i == 0) {
          // Don't add the stateSet if it is empty and would be the oldest.
          // The first item in stateSets must not be empty.
          partitionState.bufferedRows.remove(0);
          partitionState.stateSets.remove(0);
        } else {
          partitionState.stateSets.set(i++, emptyStateSet);
        }
      } else if (nextStateSet.get(automaton.endState.id)) {
        resultMatches.add(
            ImmutableList.copyOf(
                partitionState.bufferedRows.subList(i,
                    partitionState.bufferedRows.size())));
        if (i == 0) {
          // Don't add the stateSet if it is empty and would be the oldest.
          // The first item in stateSets must not be empty.
          partitionState.bufferedRows.remove(0);
          partitionState.stateSets.remove(0);
        } else {
          // Set state to empty so that it is not considered for any
          // further matches, and will be removed when it is the oldest.
          partitionState.stateSets.set(i++, emptyStateSet);
        }
      } else {
        partitionState.stateSets.set(i++, nextStateSet);
      }
    }
  }

  void matchTwo(E e, CircularArrayList<E> recentRows, int matchCount) {
    // TODO
  }

  /** State for each partition.
   *
   * @param <E> Row type */
  static class PartitionState<E> {
    /** Rows that have arrived recently and might yield a match. */
    final CircularArrayList<E> bufferedRows =
        new CircularArrayList<>();

    /** The state of each recent row.
     *
     * <p>This collection always has the same number of entries as
     * {@link #bufferedRows}.
     *
     * <p>Because this automaton is non-deterministic, a row may be in more
     * than more than one state at a time, hence we use a bit set rather than
     * an integer.
     */
    final CircularArrayList<ImmutableBitSet> stateSets =
        new CircularArrayList<>();
  }

  /** Builds a Matcher.
   *
   * @param <E> Type of rows matched by this automaton */
  static public class Builder<E> {
    final Automaton automaton;
    final Map<String, Predicate<E>> symbolPredicates = new HashMap<>();

    Builder(Automaton automaton) {
      this.automaton = automaton;
    }

    Builder<E> add(String symbolName, Predicate<E> predicate) {
      symbolPredicates.put(symbolName, predicate);
      return this;
    }

    public Matcher<E> build() {
      final Set<String> graphSymbolNames =
          new TreeSet<>(automaton.symbolNames);
      if (!symbolPredicates.keySet().equals(graphSymbolNames)) {
        throw new IllegalArgumentException("not all symbols in the graph ["
            + graphSymbolNames + "] have predicates ["
            + symbolPredicates.keySet() + "]");
      }
      final ImmutableList.Builder<Predicate<E>> builder =
          ImmutableList.builder();
      for (String symbolName : automaton.symbolNames) {
        builder.add(symbolPredicates.get(symbolName));
      }
      return new Matcher<>(automaton, builder.build());
    }
  }
}

// End Matcher.java
