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
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A deterministic finite automaton (DFA).
 *
 * <p>It is constructed from a
 * {@link Automaton nondeterministic finite state automaton (NFA)}.
 */
public class DeterministicAutomaton {
  final MultiState startState;
  private final Automaton automaton;
  private final ImmutableSet<MultiState> endStates;
  private final ImmutableList<Transition> transitions;

  /** Constructs the DFA from an epsilon-NFA. */
  DeterministicAutomaton(Automaton automaton) {
    this.automaton = Objects.requireNonNull(automaton);
    // Calculate eps closure of start state
    final Set<MultiState> traversedStates = new HashSet<>();
    // Add transitions
    this.startState = epsilonClosure(automaton.startState);

    final ImmutableList.Builder<Transition> transitionsBuilder =
        ImmutableList.builder();
    traverse(startState, transitionsBuilder, traversedStates);
    // Store transitions
    transitions = transitionsBuilder.build();

    // Calculate final States
    final ImmutableSet.Builder<MultiState> endStateBuilder =
        ImmutableSet.builder();
    traversedStates.stream()
        .filter(ms -> ms.contains(automaton.endState))
        .forEach(endStateBuilder::add);
    this.endStates = endStateBuilder.build();
  }

  MultiState getStartState() {
    return startState;
  }

  public ImmutableSet<MultiState> getEndStates() {
    return endStates;
  }

  public ImmutableList<Transition> getTransitions() {
    return transitions;
  }

  private void traverse(MultiState start,
      ImmutableList.Builder<Transition> transitionsBuilder,
      Set<MultiState> traversedStates) {
    traversedStates.add(start);
    final Set<MultiState> newStates = new HashSet<>();
    for (int symbol = 0; symbol < automaton.symbolNames.size(); symbol++) {
      final Optional<MultiState> next = addTransitions(start, symbol, transitionsBuilder);
      next.ifPresent(newStates::add);
    }
    // Remove all already known states
    newStates.removeAll(traversedStates);
    // If we have really new States, then traverse them
    newStates.forEach(s -> traverse(s, transitionsBuilder, traversedStates));
  }

  private Optional<MultiState> addTransitions(MultiState start, int symbol,
      ImmutableList.Builder<Transition> transitionsBuilder) {
    final ImmutableSet.Builder<Automaton.State> builder = ImmutableSet.builder();
    for (Automaton.SymbolTransition transition : this.automaton.getTransitions()) {
      // Consider only transitions for the given symbol
      if (transition.symbol != symbol) {
        continue;
      }
      // Consider only those emitting from current state
      if (!start.contains(transition.fromState)) {
        continue;
      }
      // ...
      builder.addAll(epsilonClosure(transition.toState).states);
    }
    final ImmutableSet<Automaton.State> stateSet = builder.build();
    if (stateSet.isEmpty()) {
      return Optional.empty();
    }
    final MultiState next = new MultiState(builder.build());
    final Transition transition =
        new Transition(start, next, symbol, automaton.symbolNames.get(symbol));
    // Add the state to the list and add the transition in the table
    transitionsBuilder.add(transition);
    return Optional.of(next);
  }

  private MultiState epsilonClosure(Automaton.State state) {
    final Set<Automaton.State> closure = new HashSet<>();
    finder(state, closure);
    return new MultiState(ImmutableSet.copyOf(closure));
  }

  private void finder(Automaton.State state, Set<Automaton.State> closure) {
    closure.add(state);
    final Set<Automaton.State> newStates =
        automaton.getEpsilonTransitions().stream()
            .filter(t -> t.fromState.equals(state))
            .map(t -> t.toState)
            .collect(Collectors.toSet());
    newStates.removeAll(closure);
    // Recursively call all "new" states
    for (Automaton.State s : newStates) {
      finder(s, closure);
    }
  }

  /** Transition between states. */
  static class Transition {
    final MultiState fromState;
    final MultiState toState;
    final int symbolId;
    final String symbol;

    Transition(MultiState fromState, MultiState toState, int symbolId,
        String symbol) {
      this.fromState = Objects.requireNonNull(fromState);
      this.toState = Objects.requireNonNull(toState);
      this.symbolId = symbolId;
      this.symbol = Objects.requireNonNull(symbol);
    }
  }

  /**
   * A state of the deterministic finite automaton. Consists of a set of states
   * from the underlying eps-NFA.
   */
  static class MultiState {
    private final ImmutableSet<Automaton.State> states;

    MultiState(Automaton.State... states) {
      this(ImmutableSet.copyOf(states));
    }

    MultiState(ImmutableSet<Automaton.State> states) {
      this.states = Objects.requireNonNull(states);
    }

    public boolean contains(Automaton.State state) {
      return states.contains(state);
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof MultiState
          && Objects.equals(states, ((MultiState) o).states);
    }

    @Override public int hashCode() {
      return Objects.hash(states);
    }

    @Override public String toString() {
      return states.toString();
    }
  }
}

// End DeterministicAutomaton.java
