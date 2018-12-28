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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Workspace that partialMatches patterns against an automaton.
 *
 * @param <E> Type of rows matched by this automaton
 */
public class Matcher<E> {
  private final Automaton automaton;
  private final ImmutableMap<String, BiPredicate<E, List<E>>> predicates;

  // The following members are work space. They can be shared among partitions,
  // but only one thread can use them at a time. Putting them here saves the
  // expense of creating a fresh object each call to "match".

  private final ImmutableList<Tuple<Integer>> emptyStateSet = ImmutableList.of();
  private final ImmutableBitSet startSet;
  private final List<Integer> rowSymbols = new ArrayList<>();
  private final DFA dfa;

  /**
   * Creates a Matcher; use {@link #builder}.
   */
  private Matcher(Automaton automaton,
                  ImmutableMap<String, BiPredicate<E, List<E>>> predicates) {
    this.automaton = Objects.requireNonNull(automaton);
    this.predicates = Objects.requireNonNull(predicates);
    final ImmutableBitSet.Builder startSetBuilder =
        ImmutableBitSet.builder();
    startSetBuilder.set(automaton.startState.id);
    automaton.epsilonSuccessors(automaton.startState.id, startSetBuilder);
    startSet = startSetBuilder.build();
    // Build the DFA
    dfa = new DFA(automaton);
  }

  public static <E> Builder<E> builder(Automaton automaton) {
    return new Builder<>(automaton);
  }

  public List<List<E>> match(E... rows) {
    return match(Arrays.asList(rows));
  }

  public List<List<E>> match(Iterable<E> rows) {
    final ImmutableList.Builder<List<E>> resultMatchBuilder =
        ImmutableList.builder();
    final Consumer<List<E>> resultMatchConsumer = resultMatchBuilder::add;
    final PartitionState<E> partitionState = createPartitionState();
    for (E row : rows) {
      matchOne(row, partitionState, resultMatchConsumer);
    }
    return resultMatchBuilder.build();
  }

  public PartitionState<E> createPartitionState() {
    return new PartitionState<>();
  }

  /**
   * Feeds a single input row into the given partition state,
   * and writes the resulting output rows (if any).
   * <p>
   * This method ignores the symbols that caused a transition.
   */
  protected void matchOne(E row, PartitionState<E> partitionState,
                          Consumer<List<E>> resultMatches) {
    List<PartialMatch<E>> matches = matchOneWithSymbols(row, partitionState);
    for (PartialMatch<E> pm : matches) {
      resultMatches.accept(pm.rows);
    }
  }

  protected List<PartialMatch<E>> matchOneWithSymbols(E row, PartitionState<E> partitionState) {
    final HashSet<PartialMatch<E>> newMatches = new HashSet<>();
    for (Map.Entry<String, BiPredicate<E, List<E>>> predicate : predicates.entrySet()) {
      for (PartialMatch<E> pm : partitionState.getPartialMatches()) {
        // Remove this match
        if (predicate.getValue().test(row, pm.getRows())) {
          // Check if we have transitions from here
          final List<DFA.Transition> transitions = dfa.getTransitions().stream()
              .filter(t -> predicate.getKey().equals(t.getSymbol()))
              .filter(t -> pm.currentState.equals(t.getFromState()))
              .collect(Collectors.toList());

          for (DFA.Transition transition : transitions) {
            // System.out.println("Append new transition to ");
            final PartialMatch<E> newMatch = pm.append(transition.getSymbol(), row, transition.getToState());
            newMatches.add(newMatch);
          }
        }
      }
      // Check if a new Match starts here
      if (predicate.getValue().test(row, Collections.emptyList())) {
        final List<DFA.Transition> transitions = dfa.getTransitions().stream()
            .filter(t -> predicate.getKey().equals(t.getSymbol()))
            .filter(t -> dfa.startState.equals(t.getFromState()))
            .collect(Collectors.toList());

        for (DFA.Transition transition : transitions) {
          System.out.println("Start new Partial Match for row " + row + " with symbol " + transition.getSymbol());
          final PartialMatch<E> newMatch = new PartialMatch<>(-1L,
              ImmutableList.of(transition.getSymbol()), ImmutableList.of(row), transition.getToState());
          newMatches.add(newMatch);
        }
      }
    }

    // Remove all current partitions
    partitionState.clearPartitions();
    // Add all partial matches
    partitionState.addPartialMatches(newMatches);
    // Check if one of the new Matches is in a final state, otherwise add them
    // and go on
    final ImmutableList.Builder<PartialMatch<E>> builder = ImmutableList.builder();
    for (PartialMatch<E> match : newMatches) {
      if (dfa.getEndStates().contains(match.currentState)) {
        // This is the match, handle all "open" partial matches with a suitable
        // strategy
        // TODO add strategy
        // Return it!
        builder.add(match);
      }
    }
    return builder.build();

    // TODO CHeck if one is final and... deal with it...
//        // Add this row to the states.
//        partitionState.bufferedRows.add(row);
//        partitionState.stateSets.add(
//                startSet.toList().stream()
//                        .map(i -> new Tuple<>(null, i))
//                        .collect(Collectors.toList())
//        );
//
//        // Compute the set of symbols whose predicates that evaluate to true
//        // for this row.
//        rowSymbols.clear();
//        for (Ord<BiPredicate<E, List<E>>> predicate : Ord.zip(predicates)) {
//            if (predicate.e.test(row, partitionState.bufferedRows)) {
//                rowSymbols.add(predicate.i);
//            }
//        }
//
//        // TODO: Should we short-cut if symbols is empty?
//        // TODO: Merge states with epsilon-successors
//
//        // Now process the states of partialMatches, oldest first, and compute the
//        // successors based on the predicates that are true for the current
//        // row.
//        for (int i = 0; i < partitionState.stateSets.size(); ) {
//            final List<Tuple<Integer>> stateSet = partitionState.stateSets.get(i);
//            ImmutableList.Builder<Tuple<Integer>> nextStateBuilder =
//                    ImmutableList.builder();
//            for (int symbol : rowSymbols) {
//                for (Tuple<Integer> state : stateSet) {
//                    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
//                    automaton.successors(state.row, symbol, builder);
//                    // Convert to List
//                    for (Integer bit : builder.build()) {
//                        nextStateBuilder.add(new Tuple<>(automaton.symbolNames.get(symbol), bit));
//                    }
//                }
//            }
//            final ImmutableList<Tuple<Integer>> nextStateSet = nextStateBuilder.build();
//            if (nextStateSet.isEmpty()) {
//                if (i == 0) {
//                    // Don't add the stateSet if it is empty and would be the oldest.
//                    // The first item in stateSets must not be empty.
//                    partitionState.bufferedRows.remove(0);
//                    partitionState.stateSets.remove(0);
//                } else {
//                    partitionState.stateSets.set(i++, emptyStateSet);
//                }
//            } else if (contains(nextStateSet, automaton.endState.id)) {
//                final ImmutableList<E> list = ImmutableList.copyOf(
//                        partitionState.bufferedRows.subList(i,
//                                partitionState.bufferedRows.size()));
//                final ImmutableList.Builder<Tuple<E>> builder = ImmutableList.builder();
//                for (int j = i; j < partitionState.bufferedRows.size(); j++) {
//                    builder.add(new Tuple<>("", partitionState.bufferedRows.get(j)));
//                }
//                resultMatches.accept(builder.build());
//                if (i == 0) {
//                    // Don't add the stateSet if it is empty and would be the oldest.
//                    // The first item in stateSets must not be empty.
//                    partitionState.bufferedRows.remove(0);
//                    partitionState.stateSets.remove(0);
//                } else {
//                    // Set state to empty so that it is not considered for any
//                    // further partialMatches, and will be removed when it is the oldest.
//                    partitionState.stateSets.set(i++, emptyStateSet);
//                }
//            } else {
//                partitionState.stateSets.set(i++, nextStateSet);
//            }
//        }
  }

  private static boolean contains(ImmutableList<Tuple<Integer>> list, int id) {
    for (Tuple<Integer> tuple : list) {
      if (tuple.row == id) {
        return true;
      }
    }
    return false;
  }

  /**
   * State for each partition.
   *
   * @param <E> Row type
   */
  static class PartitionState<E> {

    private final Set<PartialMatch<E>> partialMatches = new HashSet<>();

    public void addPartialMatches(Collection<PartialMatch<E>> matches) {
      partialMatches.addAll(matches);
    }

    public Set<PartialMatch<E>> getPartialMatches() {
      return ImmutableSet.copyOf(partialMatches);
    }

    public void removePartialMatch(PartialMatch<E> pm) {
      partialMatches.remove(pm);
    }

    public void clearPartitions() {
      partialMatches.clear();
    }
  }

  /**
   * Partial Match of the NFA.
   * This class is Immutable and the {@link #copy()} and {@link #append(String, Object, DFA.MultiState)}
   * methods generate new Instances.
   */
  static class PartialMatch<E> {

    private final long startRow;
    private final ImmutableList<String> symbols;
    private final ImmutableList<E> rows;
    private final DFA.MultiState currentState;

    public PartialMatch(long startRow, ImmutableList<String> symbols, ImmutableList<E> rows, DFA.MultiState currentState) {
      this.startRow = startRow;
      this.symbols = symbols;
      this.rows = rows;
      this.currentState = currentState;
    }

    public long getStartRow() {
      return startRow;
    }

    public ImmutableList<String> getSymbols() {
      return symbols;
    }

    public ImmutableList<E> getRows() {
      return this.rows;
    }

    public DFA.MultiState getCurrentState() {
      return currentState;
    }

    public PartialMatch<E> copy() {
      return new PartialMatch<>(startRow, symbols, rows, currentState);
    }

    public PartialMatch<E> append(String symbol, E row, DFA.MultiState toState) {
      ImmutableList<String> symbols = ImmutableList.<String>builder()
          .addAll(this.symbols)
          .add(symbol)
          .build();
      ImmutableList<E> rows = ImmutableList.<E>builder()
          .addAll(this.rows)
          .add(row)
          .build();
      return new PartialMatch<>(startRow, symbols, rows, toState);
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PartialMatch<?> that = (PartialMatch<?>) o;
      return startRow == that.startRow &&
          Objects.equals(symbols, that.symbols) &&
          Objects.equals(rows, that.rows) &&
          Objects.equals(currentState, that.currentState);
    }

    @Override public int hashCode() {
      return Objects.hash(startRow, symbols, rows, currentState);
    }

    @Override public String toString() {
      return "PartialMatch{" +
          "startRow=" + startRow +
          ", symbols=" + symbols +
          ", rows=" + rows +
          ", currentState=" + currentState +
          '}';
    }
  }

  /**
   * Builds a Matcher.
   *
   * @param <E> Type of rows matched by this automaton
   */
  public static class Builder<E> {
    final Automaton automaton;
    final Map<String, BiPredicate<E, List<E>>> symbolPredicates =
        new HashMap<>();

    Builder(Automaton automaton) {
      this.automaton = automaton;
    }

    /**
     * Associates a predicate with a symbol.
     */
    public Builder<E> add(String symbolName,
                          BiPredicate<E, List<E>> predicate) {
      symbolPredicates.put(symbolName, predicate);
      return this;
    }

    public Matcher<E> build() {
      final Set<String> predicateSymbolsNotInGraph =
          Sets.newTreeSet(symbolPredicates.keySet());
      predicateSymbolsNotInGraph.removeAll(automaton.symbolNames);
      if (!predicateSymbolsNotInGraph.isEmpty()) {
        throw new IllegalArgumentException("not all predicate symbols ["
            + predicateSymbolsNotInGraph + "] are in graph ["
            + automaton.symbolNames + "]");
      }
      final ImmutableMap.Builder<String, BiPredicate<E, List<E>>> builder =
          ImmutableMap.builder();
      for (String symbolName : automaton.symbolNames) {
        // If a symbol does not have a predicate, it defaults to true.
        // By convention, "STRT" is used for the start symbol, but it could be
        // anything.
        builder.put(symbolName,
            symbolPredicates.getOrDefault(symbolName, (e, list) -> true));
      }
      return new Matcher<>(automaton, builder.build());
    }
  }

  static class Tuple<E> {

    String symbol;
    E row;

    public Tuple(String symbol, E row) {
      this.symbol = symbol;
      this.row = row;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Tuple<?> tuple = (Tuple<?>) o;
      return symbol == tuple.symbol &&
          Objects.equals(row, tuple.row);
    }

    @Override
    public int hashCode() {
      return Objects.hash(symbol, row);
    }

    @Override
    public String toString() {
      return "(" + symbol + ", " + row + ")";
    }
  }
}

// End Matcher.java
