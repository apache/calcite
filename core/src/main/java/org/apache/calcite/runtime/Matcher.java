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
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.sun.corba.se.spi.orbutil.threadpool.Work;

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
class Matcher<E> {
  private final Automaton automaton;
  private final ImmutableList<Predicate<E>> predicates;

  /** Creates a Matcher; use {@link #builder}. */
  private Matcher(Automaton automaton,
      ImmutableList<Predicate<E>> predicates) {
    this.automaton = Objects.requireNonNull(automaton);
    this.predicates = Objects.requireNonNull(predicates);
  }

  static <E> Builder<E> builder(Automaton automaton) {
    return new Builder<>(automaton);
  }

  public List<List<E>> match(E... rows) {
    return match(Arrays.asList(rows));
  }

  public List<List<E>> match(Iterable<E> rows) {
    final ImmutableList.Builder<List<E>> resultMatches =
        ImmutableList.builder();
    final Workspace workspace = new Workspace(automaton);
    final PartitionState<E> partitionState = new PartitionState<>();
    for (E row : rows) {
      matchOne(workspace, partitionState, resultMatches, row);
    }
    return resultMatches.build();
  }

  /** Work space that can be shared among partitions. */
  static class Workspace {
    final ImmutableBitSet emptyStateSet = ImmutableBitSet.of();
    final ImmutableBitSet startSet;
    final List<Integer> rowSymbols = new ArrayList<>();
    final ImmutableBitSet.Builder nextStateBuilder =
        ImmutableBitSet.builder();

    Workspace(Automaton automaton) {
      final ImmutableBitSet.Builder startSetBuilder =
          ImmutableBitSet.builder();
      startSetBuilder.set(automaton.startState.id);
      automaton.epsilonSuccessors(automaton.startState.id, startSetBuilder);
      startSet = startSetBuilder.build();
    }
  }

  /** Work space for each partition. */
  static class PartitionState<E> {
    final CircularArrayList<E> bufferedRows =
        new CircularArrayList<>();
    final CircularArrayList<ImmutableBitSet> stateSets =
        new CircularArrayList<>();
    final List<Pair<E, ImmutableBitSet>> rowStateSets =
        Pair.zipMutable(bufferedRows, stateSets);
  }

  protected void matchOne(Workspace workspace,
      PartitionState<E> partitionState,
      ImmutableList.Builder<List<E>> resultMatches,
      E row) {
    // Add this row to the states.
    partitionState.rowStateSets.add(Pair.of(row, workspace.startSet));

    // Compute the set of symbols whose predicates that evaluate to true
    // for this row.
    workspace.rowSymbols.clear();
    for (Ord<Predicate<E>> predicate : Ord.zip(predicates)) {
      if (predicate.e.test(row)) {
        workspace.rowSymbols.add(predicate.i);
      }
    }

    // TODO: Should we short-cut if symbols is empty?
    // TODO: Merge states with epsilon-successors

    // Now process the states of matches, oldest first, and compute the
    // successors based on the predicates that are true for the current
    // row.
    for (int i = 0; i < partitionState.rowStateSets.size();) {
      final Pair<E, ImmutableBitSet> rowStateSet =
          partitionState.rowStateSets.get(i);
      assert workspace.nextStateBuilder.isEmpty();
      for (int symbol : workspace.rowSymbols) {
        for (int state : rowStateSet.right) {
          automaton.successors(state, symbol, workspace.nextStateBuilder);
        }
      }
      final ImmutableBitSet nextStateSet =
          workspace.nextStateBuilder.buildAndReset();
      if (nextStateSet.isEmpty()) {
        if (i == 0) {
          // Don't add the stateSet if it is empty and would be the oldest.
          // The first item in stateSets must not be empty.
          partitionState.rowStateSets.remove(0);
        } else {
          partitionState.stateSets.set(i++, workspace.emptyStateSet);
        }
      } else if (nextStateSet.get(automaton.endState.id)) {
        resultMatches.add(
            ImmutableList.copyOf(
                partitionState.bufferedRows.subList(i,
                    partitionState.bufferedRows.size())));
        if (i == 0) {
          // Don't add the stateSet if it is empty and would be the oldest.
          // The first item in stateSets must not be empty.
          partitionState.rowStateSets.remove(0);
        } else {
          // Set state to empty so that it is not considered for any
          // further matches, and will be removed when it is the oldest.
          partitionState.stateSets.set(i++, workspace.emptyStateSet);
        }
      } else {
        partitionState.stateSets.set(i++, nextStateSet);
      }
    }
  }

  void matchTwo(E e, CircularArrayList<E> recentRows, int matchCount) {
    // TODO
  }

/** Builds a Matcher.
   *
   * @param <E> Type of rows matched by this automaton */
  static class Builder<E> {
    final Automaton automaton;
    final Map<String, Predicate<E>> symbolPredicates =
        new HashMap<>();

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
