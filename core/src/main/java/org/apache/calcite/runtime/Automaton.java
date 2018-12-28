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
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;

/** A finite-state automaton (Nondeterministic).
 *
 * <p>It is used to implement the {@link org.apache.calcite.rel.core.Match}
 * relational expression (for the {@code MATCH_RECOGNIZE} clause in SQL).
 *
 * @see Pattern
 * @see AutomatonBuilder
 */
public class Automaton {
  final State startState;
  final State endState;
  private final ImmutableList<SymbolTransition> transitions;
  private final ImmutableList<EpsilonTransition> epsilonTransitions;
  final ImmutableList<String> symbolNames;

  /** Use an {@link AutomatonBuilder}. */
  Automaton(State startState, State endState,
      ImmutableList<SymbolTransition> transitions,
      ImmutableList<EpsilonTransition> epsilonTransitions,
      ImmutableList<String> symbolNames) {
    this.startState = Objects.requireNonNull(startState);
    this.endState = Objects.requireNonNull(endState);
    this.transitions = Objects.requireNonNull(transitions);
    this.epsilonTransitions = epsilonTransitions;
    this.symbolNames = Objects.requireNonNull(symbolNames);
  }

  /** Returns the set of states, represented as a bit set, that the graph is
   * in when starting from {@code state} and receiving {@code symbol}.
   *
   * @param fromState Initial state
   * @param symbol Symbol received
   * @param bitSet Set of successor states (output)
   */
  void successors(int fromState, int symbol,
      ImmutableBitSet.Builder bitSet) {
    for (SymbolTransition transition : transitions) {
      if (transition.fromState.id == fromState
          && transition.symbol == symbol) {
        epsilonSuccessors(transition.toState.id, bitSet);
      }
    }
  }

  void epsilonSuccessors(int state, ImmutableBitSet.Builder bitSet) {
    bitSet.set(state);
    for (EpsilonTransition transition : epsilonTransitions) {
      if (transition.fromState.id == state) {
        if (!bitSet.get(transition.toState.id)) {
          epsilonSuccessors(transition.toState.id, bitSet);
        }
      }
    }
  }

  /** Node in the finite-state automaton. A state has a number of
   * transitions to other states, each labeled with the symbol that
   * causes that transition. */
  static class State {
    final int id;

    State(int id) {
      this.id = id;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      State state = (State) o;
      return id == state.id;
    }

    @Override public int hashCode() {
      return Objects.hash(id);
    }

    @Override public String toString() {
      return "State{" +
          "id=" + id +
          '}';
    }
  }

  /** Transition from one state to another in the finite-state automaton. */
  abstract static class Transition {
    final State fromState;
    final State toState;

    Transition(State fromState, State toState) {
      this.fromState = Objects.requireNonNull(fromState);
      this.toState = Objects.requireNonNull(toState);
    }
  }

  /** A transition caused by reading a symbol. */
  static class SymbolTransition extends Transition {
    final int symbol;

    SymbolTransition(State fromState, State toState, int symbol) {
      super(fromState, toState);
      this.symbol = symbol;
    }

    @Override public String toString() {
      return symbol + ":" + fromState.id + "->" + toState.id;
    }
  }

  /** A transition that may happen without reading a symbol. */
  static class EpsilonTransition extends Transition {
    EpsilonTransition(State fromState, State toState) {
      super(fromState, toState);
    }

    @Override public String toString() {
      return "epsilon:" + fromState.id + "->" + toState.id;
    }
  }

  /**
   * A deterministic finite automaton (DFA) which can be constructed from the nondeterministic {@link Automaton}.
   */
  public static class DeterministicAutomaton {

    final MultiState startState;
    private final Automaton automaton;
     private final ImmutableSet<MultiState> endStates;
    // private final ImmutableList<SymbolTransition> transitions;

    DeterministicAutomaton(Automaton automaton) {
      this.automaton = automaton;
      // Construct the DFA from NFA
      // Calculate eps closure of start state
      this.startState = epsilonClosure(automaton.startState);
      final HashSet<MultiState> traversedStates = new HashSet<>();
      // Add transitions
      final State state = automaton.startState;
      final MultiState closure = epsilonClosure(state);
      traversedStates.add(closure);
      // Calculate ...
      final ImmutableList.Builder<Transition> transitionsBuilder = ImmutableList.builder();

      traverse(closure, transitionsBuilder, traversedStates);

      // Calculate final States
      final ImmutableSet.Builder<MultiState> endStateBuilder = ImmutableSet.builder();
      traversedStates.stream()
          .filter(ms -> ms.contains(automaton.endState))
          .forEach(endStateBuilder::add);
      this.endStates = endStateBuilder.build();

      System.out.println("New States are:");
      traversedStates.forEach(System.out::println);

      System.out.println("End States are:");
      endStates.forEach(System.out::println);
    }

    private void traverse(MultiState start, ImmutableList.Builder<Transition> transitionsBuilder, HashSet<MultiState> traversedStates) {
      traversedStates.add(start);
      final HashSet<MultiState> newStates = new HashSet<>();
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
      final ImmutableSet.Builder<State> builder = ImmutableSet.builder();
      for (SymbolTransition transition : this.automaton.transitions) {
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
      final ImmutableSet<State> stateSet = builder.build();
      if (stateSet.isEmpty()) {
        return Optional.empty();
      }
      final MultiState next = new MultiState(builder.build());
      System.out.println(start + "-" + automaton.symbolNames.get(symbol) + "-> " + next);
      final Transition transition = new Transition(start, next, symbol);
      // Add the state to the list and add the transition in the table
      transitionsBuilder.add(transition);
      return Optional.of(next);
    }

    private MultiState epsilonClosure(State state) {
      final ImmutableSet.Builder<State> builder = ImmutableSet.builder();
      builder.add(state);
      automaton.epsilonTransitions.stream()
          .filter(t -> t.fromState.equals(state))
          .map(t -> t.toState)
          .forEach(builder::add);
      return new MultiState(builder.build());
    }

    static class Transition {
      MultiState fromState;
      MultiState toState;
      int symbol;

      public Transition(MultiState fromState, MultiState toState, int symbol) {
        this.fromState = fromState;
        this.toState = toState;
        this.symbol = symbol;
      }
    }

    static class MultiState {

      private ImmutableSet<State> states;

      public MultiState(State... states) {
        this.states = ImmutableSet.copyOf(states);
      }

      public MultiState(ImmutableSet<State> states) {
        this.states = states;
      }

      public boolean contains(State state) {
        return states.contains(state);
      }

      @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiState that = (MultiState) o;
        return Objects.equals(states, that.states);
      }

      @Override public int hashCode() {
        return Objects.hash(states);
      }

      @Override public String toString() {
        return "MultiState{" +
            "states=" + states +
            '}';
      }
    }
  }
}

// End Automaton.java
