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
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.CircularArrayList;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Workspace that matches patterns against an automaton.
 *
 * @param <E> Type of rows matched by this automaton
 */
public class Matcher<E> {
    private final Automaton automaton;
    private final ImmutableList<BiPredicate<E, List<E>>> predicates;

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
                    ImmutableList<BiPredicate<E, List<E>>> predicates) {
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
        final ImmutableList.Builder<List<Tuple<E>>> resultMatchBuilder =
                ImmutableList.builder();

        matchOneWithSymbols(row, partitionState, resultMatchBuilder::add);

        for (List<Tuple<E>> list : resultMatchBuilder.build()) {
            resultMatches.accept(
                    ImmutableList.copyOf(
                            list.stream()
                                    .map(tuple -> tuple.row)
                                    .collect(Collectors.toList()
                                    ))
            );
        }
    }

    protected void matchOneWithSymbols(E row, PartitionState<E> partitionState,
                                       Consumer<List<Tuple<E>>> resultMatches) {
        // Add this row to the states.
        partitionState.bufferedRows.add(row);
        partitionState.stateSets.add(
                startSet.toList().stream()
                        .map(i -> new Tuple<>(null, i))
                        .collect(Collectors.toList())
        );

        // Compute the set of symbols whose predicates that evaluate to true
        // for this row.
        rowSymbols.clear();
        for (Ord<BiPredicate<E, List<E>>> predicate : Ord.zip(predicates)) {
            if (predicate.e.test(row, partitionState.bufferedRows)) {
                rowSymbols.add(predicate.i);
            }
        }

        // TODO: Should we short-cut if symbols is empty?
        // TODO: Merge states with epsilon-successors

        // Now process the states of matches, oldest first, and compute the
        // successors based on the predicates that are true for the current
        // row.
        for (int i = 0; i < partitionState.stateSets.size(); ) {
            final List<Tuple<Integer>> stateSet = partitionState.stateSets.get(i);
            ImmutableList.Builder<Tuple<Integer>> nextStateBuilder =
                    ImmutableList.builder();
            for (int symbol : rowSymbols) {
                for (Tuple<Integer> state : stateSet) {
                    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
                    automaton.successors(state.row, symbol, builder);
                    // Convert to List
                    for (Integer bit : builder.build()) {
                        nextStateBuilder.add(new Tuple<>(automaton.symbolNames.get(symbol), bit));
                    }
                }
            }
            final ImmutableList<Tuple<Integer>> nextStateSet = nextStateBuilder.build();
            if (nextStateSet.isEmpty()) {
                if (i == 0) {
                    // Don't add the stateSet if it is empty and would be the oldest.
                    // The first item in stateSets must not be empty.
                    partitionState.bufferedRows.remove(0);
                    partitionState.stateSets.remove(0);
                } else {
                    partitionState.stateSets.set(i++, emptyStateSet);
                }
            } else if (contains(nextStateSet, automaton.endState.id)) {
                final ImmutableList<E> list = ImmutableList.copyOf(
                        partitionState.bufferedRows.subList(i,
                                partitionState.bufferedRows.size()));
                final ImmutableList.Builder<Tuple<E>> builder = ImmutableList.builder();
                for (int j = i; j < partitionState.bufferedRows.size(); j++) {
                    builder.add(new Tuple<>("", partitionState.bufferedRows.get(j)));
                }
                resultMatches.accept(builder.build());
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

        private Set<Path<E>> paths;
        /**
         * Rows that have arrived recently and might yield a match.
         */
        final CircularArrayList<E> bufferedRows =
                new CircularArrayList<>();

        /**
         * The state of each recent row.
         *
         * <p>This collection always has the same number of entries as
         * {@link #bufferedRows}.
         *
         * <p>Because this automaton is non-deterministic, a row may be in more
         * than more than one state at a time, hence we use a bit set rather than
         * an integer.
         */
        final CircularArrayList<List<Tuple<Integer>>> stateSets =
                new CircularArrayList<>();

        public void add(E row, int symbol, Set<Integer> states) {

        }

        /**
         * A path through the NFA which conserves the rows and the symbols that occured as state transitions.
         *
         * Each Path contains it's history (which is important in case of a match).
         * It contains the current set of states (only varying by epsilon transitions).
         *
         * Then
         * @param <E> Row type
         */
        static class Path<E> {

            private List<Integer> symbols;
            private List<E> rows;
            private Set<Integer> states;

            Path(List<Integer> symbols, List<E> rows, Set<Integer> states) {
                this.states = states;
                assert symbols.size() == rows.size();
                this.symbols = symbols;
                this.rows = rows;
            }

            public Path() {
                this(new ArrayList<>(), new ArrayList<>(), new HashSet<>());
            }

            public void add(int symbol, E row) {
                this.symbols.add(symbol);
                this.rows.add(row);
            }

            @Override
            public String toString() {
                return "Path{" +
                        "symbols=" + symbols +
                        ", rows=" + rows +
                        '}';
            }
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
            final ImmutableList.Builder<BiPredicate<E, List<E>>> builder =
                    ImmutableList.builder();
            for (String symbolName : automaton.symbolNames) {
                // If a symbol does not have a predicate, it defaults to true.
                // By convention, "STRT" is used for the start symbol, but it could be
                // anything.
                builder.add(
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
