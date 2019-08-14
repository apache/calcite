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

import org.apache.calcite.runtime.Automaton.EpsilonTransition;
import org.apache.calcite.runtime.Automaton.State;
import org.apache.calcite.runtime.Automaton.SymbolTransition;
import org.apache.calcite.runtime.Automaton.Transition;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builds a state-transition graph for deterministic finite automaton. */
public class AutomatonBuilder {
  private final Map<String, Integer> symbolIds = new HashMap<>();
  private final List<State> stateList = new ArrayList<>();
  private final List<Transition> transitionList = new ArrayList<>();
  private final State startState = createState();
  private final State endState = createState();

  /** Adds a pattern as a start-to-end transition. */
  AutomatonBuilder add(Pattern pattern) {
    return add(pattern, startState, endState);
  }

  private AutomatonBuilder add(Pattern pattern, State fromState,
      State toState) {
    final Pattern.AbstractPattern p = (Pattern.AbstractPattern) pattern;
    switch (p.op) {
    case SEQ:
      final Pattern.OpPattern pSeq = (Pattern.OpPattern) p;
      return seq(fromState, toState, pSeq.patterns);

    case STAR:
      final Pattern.OpPattern pStar = (Pattern.OpPattern) p;
      return star(fromState, toState, pStar.patterns.get(0));

    case PLUS:
      final Pattern.OpPattern pPlus = (Pattern.OpPattern) p;
      return plus(fromState, toState, pPlus.patterns.get(0));

    case REPEAT:
      final Pattern.RepeatPattern pRepeat = (Pattern.RepeatPattern) p;
      return repeat(fromState, toState, pRepeat.patterns.get(0),
          pRepeat.minRepeat, pRepeat.maxRepeat);

    case SYMBOL:
      final Pattern.SymbolPattern pSymbol = (Pattern.SymbolPattern) p;
      return symbol(fromState, toState, pSymbol.name);

    case OR:
      final Pattern.OpPattern pOr = (Pattern.OpPattern) p;
      return or(fromState, toState, pOr.patterns.get(0), pOr.patterns.get(1));

    case OPTIONAL:
      // Rewrite as {0,1}
      final Pattern.OpPattern pOptional = (Pattern.OpPattern) p;
      return optional(fromState, toState, pOptional.patterns.get(0));

    default:
      throw new AssertionError("unknown op " + p.op);
    }
  }

  private State createState() {
    final State state = new State(stateList.size());
    stateList.add(state);
    return state;
  }

  /** Builds the automaton. */
  public Automaton build() {
    final ImmutableList.Builder<SymbolTransition> symbolTransitions =
        ImmutableList.builder();
    final ImmutableList.Builder<EpsilonTransition> epsilonTransitions =
        ImmutableList.builder();
    for (Transition transition : transitionList) {
      if (transition instanceof SymbolTransition) {
        symbolTransitions.add((SymbolTransition) transition);
      }
      if (transition instanceof EpsilonTransition) {
        epsilonTransitions.add((EpsilonTransition) transition);
      }
    }
    // Sort the symbols by their ids, which are assumed consecutive and
    // starting from zero.
    final ImmutableList<String> symbolNames = symbolIds.entrySet()
        .stream()
        .sorted(Comparator.comparingInt(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .collect(Util.toImmutableList());
    return new Automaton(stateList.get(0), endState,
        symbolTransitions.build(), epsilonTransitions.build(), symbolNames);
  }

  /** Adds a symbol transition. */
  AutomatonBuilder symbol(State fromState, State toState,
      String name) {
    Objects.requireNonNull(name);
    final int symbolId =
        symbolIds.computeIfAbsent(name, k -> symbolIds.size());
    transitionList.add(new SymbolTransition(fromState, toState, symbolId));
    return this;
  }

  /** Adds a transition made up of a sequence of patterns. */
  AutomatonBuilder seq(State fromState, State toState,
      List<Pattern> patterns) {
    State prevState = fromState;
    for (int i = 0; i < patterns.size(); i++) {
      Pattern pattern = patterns.get(i);
      final State nextState;
      if (i == patterns.size() - 1) {
        nextState = toState;
      } else {
        nextState = createState();
      }
      add(pattern, prevState, nextState);
      prevState = nextState;
    }
    return this;
  }

  /** Adds a transition for the 'or' pattern. */
  AutomatonBuilder or(State fromState, State toState, Pattern left,
      Pattern right) {
    //
    //             left
    //         / -------->  toState
    //  fromState
    //         \ --------> toState
    //             right

    add(left, fromState, toState);
    add(right, fromState, toState);
    return this;
  }

  /** Adds a transition made up of the Kleene star applied to a pattern. */
  AutomatonBuilder star(State fromState, State toState, Pattern pattern) {
    //
    //     +------------------------- e ------------------------+
    //     |                +-------- e -------+                |
    //     |                |                  |                |
    //     |                V                  |                V
    // fromState -----> beforeState -----> afterState -----> toState
    //             e               pattern              e
    //
    final State beforeState = createState();
    final State afterState = createState();
    transitionList.add(new EpsilonTransition(fromState, beforeState));
    add(pattern, beforeState, afterState);
    transitionList.add(new EpsilonTransition(afterState, beforeState));
    transitionList.add(new EpsilonTransition(afterState, toState));
    transitionList.add(new EpsilonTransition(fromState, toState));
    return this;
  }

  /** Adds a transition made up of a pattern repeated 1 or more times. */
  AutomatonBuilder plus(State fromState, State toState, Pattern pattern) {
    //
    //                      +-------- e -------+
    //                      |                  |
    //                      V                  |
    // fromState -----> beforeState -----> afterState -----> toState
    //             e               pattern              e
    //
    final State beforeState = createState();
    final State afterState = createState();
    transitionList.add(new EpsilonTransition(fromState, beforeState));
    add(pattern, beforeState, afterState);
    transitionList.add(new EpsilonTransition(afterState, beforeState));
    transitionList.add(new EpsilonTransition(afterState, toState));
    return this;
  }

  /** Adds a transition made up of a pattern repeated between {@code minRepeat}
   * and {@code maxRepeat} times. */
  AutomatonBuilder repeat(State fromState, State toState, Pattern pattern,
      int minRepeat, int maxRepeat) {
    // Diagram for repeat(1, 3)
    //                              +-------- e --------------+
    //                              |           +---- e ----+ |
    //                              |           |           V V
    // fromState ---> state0 ---> state1 ---> state2 ---> state3 ---> toState
    //            e        pattern     pattern     pattern        e
    //
    Preconditions.checkArgument(0 <= minRepeat);
    Preconditions.checkArgument(minRepeat <= maxRepeat);
    Preconditions.checkArgument(1 <= maxRepeat);
    State prevState = fromState;
    for (int i = 0; i <= maxRepeat; i++) {
      final State s = createState();
      if (i == 0) {
        transitionList.add(new EpsilonTransition(fromState, s));
      } else {
        add(pattern, prevState, s);
      }
      if (i >= minRepeat) {
        transitionList.add(new EpsilonTransition(s, toState));
      }
      prevState = s;
    }
    transitionList.add(new EpsilonTransition(prevState, toState));
    return this;
  }

  private AutomatonBuilder optional(State fromState, State toState, Pattern pattern) {
    add(pattern, fromState, toState);
    transitionList.add(new EpsilonTransition(fromState, toState));
    return this;
  }
}

// End AutomatonBuilder.java
