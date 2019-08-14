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

import org.apache.calcite.linq4j.MemoryFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Workspace that partialMatches patterns against an automaton.
 * @param <E> Type of rows matched by this automaton
 */
public class Matcher<E> {
  private final DeterministicAutomaton dfa;
  private final ImmutableMap<String, Predicate<MemoryFactory.Memory<E>>> predicates;

  // The following members are work space. They can be shared among partitions,
  // but only one thread can use them at a time. Putting them here saves the
  // expense of creating a fresh object each call to "match".

  private final ImmutableList<Tuple<Integer>> emptyStateSet = ImmutableList.of();
  private final ImmutableBitSet startSet;
  private final List<Integer> rowSymbols = new ArrayList<>();

  /**
   * Creates a Matcher; use {@link #builder}.
   */
  private Matcher(Automaton automaton,
      ImmutableMap<String, Predicate<MemoryFactory.Memory<E>>> predicates) {
    this.predicates = Objects.requireNonNull(predicates);
    final ImmutableBitSet.Builder startSetBuilder =
        ImmutableBitSet.builder();
    startSetBuilder.set(automaton.startState.id);
    automaton.epsilonSuccessors(automaton.startState.id, startSetBuilder);
    startSet = startSetBuilder.build();
    // Build the DFA
    dfa = new DeterministicAutomaton(automaton);
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
    final PartitionState<E> partitionState = createPartitionState(0, 0);
    for (E row : rows) {
      partitionState.getMemoryFactory().add(row);
      matchOne(partitionState.getRows(), partitionState, resultMatchConsumer);
    }
    return resultMatchBuilder.build();
  }

  public PartitionState<E> createPartitionState(int history, int future) {
    return new PartitionState<>(history, future);
  }

  /**
   * Feeds a single input row into the given partition state,
   * and writes the resulting output rows (if any).
   * This method ignores the symbols that caused a transition.
   */
  protected void matchOne(MemoryFactory.Memory<E> rows,
      PartitionState<E> partitionState, Consumer<List<E>> resultMatches) {
    List<PartialMatch<E>> matches = matchOneWithSymbols(rows, partitionState);
    for (PartialMatch<E> pm : matches) {
      resultMatches.accept(pm.rows);
    }
  }

  protected List<PartialMatch<E>> matchOneWithSymbols(MemoryFactory.Memory<E> rows,
      PartitionState<E> partitionState) {
    final HashSet<PartialMatch<E>> newMatches = new HashSet<>();
    for (Map.Entry<String, Predicate<MemoryFactory.Memory<E>>> predicate
        : predicates.entrySet()) {
      for (PartialMatch<E> pm : partitionState.getPartialMatches()) {
        // Remove this match
        if (predicate.getValue().test(rows)) {
          // Check if we have transitions from here
          final List<DeterministicAutomaton.Transition> transitions =
              dfa.getTransitions().stream()
                  .filter(t -> predicate.getKey().equals(t.symbol))
                  .filter(t -> pm.currentState.equals(t.fromState))
                  .collect(Collectors.toList());

          for (DeterministicAutomaton.Transition transition : transitions) {
            // System.out.println("Append new transition to ");
            final PartialMatch<E> newMatch = pm.append(transition.symbol,
                rows.get(), transition.toState);
            newMatches.add(newMatch);
          }
        }
      }
      // Check if a new Match starts here
      if (predicate.getValue().test(rows)) {
        final List<DeterministicAutomaton.Transition> transitions =
            dfa.getTransitions().stream()
                .filter(t -> predicate.getKey().equals(t.symbol))
                .filter(t -> dfa.startState.equals(t.fromState))
                .collect(Collectors.toList());

        for (DeterministicAutomaton.Transition transition : transitions) {
          final PartialMatch<E> newMatch = new PartialMatch<>(-1L,
              ImmutableList.of(transition.symbol), ImmutableList.of(rows.get()),
              transition.toState);
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
    final ImmutableList.Builder<PartialMatch<E>> builder =
        ImmutableList.builder();
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
  }

  /**
   * State for each partition.
   *
   * @param <E> Row type
   */
  static class PartitionState<E> {
    private final Set<PartialMatch<E>> partialMatches = new HashSet<>();
    private final MemoryFactory<E> memoryFactory;

    PartitionState(int history, int future) {
      this.memoryFactory = new MemoryFactory<>(history, future);
    }

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

    public MemoryFactory.Memory<E> getRows() {
      return memoryFactory.create();
    }

    public MemoryFactory<E> getMemoryFactory() {
      return this.memoryFactory;
    }
  }

  /**
   * Partial match of the NFA.
   *
   * <p>This class is immutable; the {@link #copy()} and
   * {@link #append(String, Object, DeterministicAutomaton.MultiState)}
   * methods generate new instances.
   *
   * @param <E> Row type
   */
  static class PartialMatch<E> {
    final long startRow;
    final ImmutableList<String> symbols;
    final ImmutableList<E> rows;
    final DeterministicAutomaton.MultiState currentState;

    PartialMatch(long startRow, ImmutableList<String> symbols,
        ImmutableList<E> rows, DeterministicAutomaton.MultiState currentState) {
      this.startRow = startRow;
      this.symbols = symbols;
      this.rows = rows;
      this.currentState = currentState;
    }

    public PartialMatch<E> copy() {
      return new PartialMatch<>(startRow, symbols, rows, currentState);
    }

    public PartialMatch<E> append(String symbol, E row,
        DeterministicAutomaton.MultiState toState) {
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
      return o == this
          || o instanceof PartialMatch
          && startRow == ((PartialMatch) o).startRow
          && Objects.equals(symbols, ((PartialMatch) o).symbols)
          && Objects.equals(rows, ((PartialMatch) o).rows)
          && Objects.equals(currentState, ((PartialMatch) o).currentState);
    }

    @Override public int hashCode() {
      return Objects.hash(startRow, symbols, rows, currentState);
    }

    @Override public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < rows.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append("(");
        sb.append(symbols.get(i));
        sb.append(", ");
        sb.append(rows.get(i));
        sb.append(")");
      }
      sb.append("]");
      return sb.toString();
    }
  }

  /**
   * Builds a Matcher.
   *
   * @param <E> Type of rows matched by this automaton
   */
  public static class Builder<E> {
    final Automaton automaton;
    final Map<String, Predicate<MemoryFactory.Memory<E>>> symbolPredicates =
        new HashMap<>();

    Builder(Automaton automaton) {
      this.automaton = automaton;
    }

    /**
     * Associates a predicate with a symbol.
     */
    public Builder<E> add(String symbolName,
        Predicate<MemoryFactory.Memory<E>> predicate) {
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
      final ImmutableMap.Builder<String, Predicate<MemoryFactory.Memory<E>>> builder =
          ImmutableMap.builder();
      for (String symbolName : automaton.symbolNames) {
        // If a symbol does not have a predicate, it defaults to true.
        // By convention, "STRT" is used for the start symbol, but it could be
        // anything.
        builder.put(symbolName,
            symbolPredicates.getOrDefault(symbolName, e -> true));
      }
      return new Matcher<>(automaton, builder.build());
    }
  }

  /**
   * Represents a Tuple of a symbol and a row
   *
   * @param <E> Type of Row
   */
  static class Tuple<E> {
    final String symbol;
    final E row;

    Tuple(String symbol, E row) {
      this.symbol = symbol;
      this.row = row;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Tuple
          && ((Tuple) o).symbol.equals(symbol)
          && Objects.equals(row, ((Tuple) o).row);
    }

    @Override public int hashCode() {
      return Objects.hash(symbol, row);
    }

    @Override public String toString() {
      return "(" + symbol + ", " + row + ")";
    }
  }
}

// End Matcher.java
