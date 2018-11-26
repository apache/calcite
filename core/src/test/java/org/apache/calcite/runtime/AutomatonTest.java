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
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link Enumerables.Automaton}. */
public class AutomatonTest {
  @Test public void testSimple() {
    // pattern(a)
    final Pattern p = new PatternBuilder().symbol("a").build();
    assertThat(p.toString(), is("a"));

    final String[] rows = {"", "a", "", "a"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p).add("a", s -> s.contains("a")).build();
    final String expected = "[[a], [a]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testSequence() {
    // pattern(a b)
    final Pattern p =
        new PatternBuilder().symbol("a").symbol("b").seq().build();
    assertThat(p.toString(), is("a b"));

    final String[] rows = {"", "a", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p)
            .add("a", s -> s.contains("a"))
            .add("b", s -> s.contains("b"))
            .build();
    final String expected = "[[a, ab], [ab, b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testStar() {
    // pattern(a* b)
    final Pattern p = new PatternBuilder()
        .symbol("a").star()
        .symbol("b").seq().build();
    assertThat(p.toString(), is("(a)* b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p)
            .add("a", s -> s.contains("a"))
            .add("b", s -> s.contains("b"))
            .build();
    final String expected = "[[b], [ab], [a, ab], [ab], [b], [b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testPlus() {
    // pattern(a+ b)
    final Pattern p = new PatternBuilder()
        .symbol("a").plus()
        .symbol("b").seq().build();
    assertThat(p.toString(), is("(a)+ b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p)
            .add("a", s -> s.contains("a"))
            .add("b", s -> s.contains("b"))
            .build();
    final String expected = "[[ab, a, ab], [a, ab], [ab, b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testRepeat() {
    // pattern(a b{0, 2} c)
    checkRepeat(0, 2, "a (b){0, 2} c", "[[a, c], [a, b, c], [a, b, b, c]]");
    // pattern(a b{0, 1} c)
    checkRepeat(0, 2, "a (b){0, 1} c", "[[a, c], [a, b, c]]");
    // pattern(a b{1, 1} c)
    checkRepeat(1, 1, "a (b){1} c", "[[a, b, c]]");
    // pattern(a b{1,3} c)
    checkRepeat(1, 3, "a (b){1, 3} c",
        "[[a, b, c], [a, b, b, c], [a, b, b, b, c]]");
    // pattern(a b{1,2} c)
    checkRepeat(1, 2, "a (b){1, 2} c", "[[a, b, c], [a, b, b, c]]");
    // pattern(a b{2,3} c)
    checkRepeat(2, 3, "a (b){2, 3} c", "[[a, b, b, c], [a, b, b, b, c]]");
  }

  private void checkRepeat(int minRepeat, int maxRepeat, String pattern,
      String expected) {
    final Pattern p = new PatternBuilder()
        .symbol("a")
        .symbol("b").repeat(minRepeat, maxRepeat).seq()
        .symbol("c").seq()
        .build();
    assertThat(p.toString(), is(pattern));

    final String rows = "acabcabbcabbbcabbbbcabdbc";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p)
            .add("a", c -> c == 'a')
            .add("b", c -> c == 'b')
            .add("c", c -> c == 'c')
            .build();
    assertThat(matcher.match(chars(rows)).toString(), is(expected));
  }

  @Test public void testRepeatComposite() {
    // pattern(a (b a){1, 2} c)
    final Pattern p = new PatternBuilder()
        .symbol("a")
        .symbol("b").symbol("a").seq()
        .repeat(1, 2).seq()
        .symbol("c").seq()
        .build();
    assertThat(p.toString(), is("a (b a){1, 2} c"));

    final String rows = "acabcabbcabbbcabbbbcabdbcabacababcababac";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p)
            .add("a", c -> c == 'a')
            .add("b", c -> c == 'b')
            .add("c", c -> c == 'c')
            .build();
    assertThat(matcher.match(chars(rows)).toString(),
        is("[[a, b, a, c], [a, b, a, b, a, c], [a, b, a, c]]"));
  }

  /** Converts a string into an iterable collection of its characters. */
  private static Iterable<Character> chars(String s) {
    return new AbstractList<Character>() {
      @Override public Character get(int index) {
        return s.charAt(index);
      }

      @Override public int size() {
        return s.length();
      }
    };
  }

  /** Builds a pattern expression. */
  static class PatternBuilder {
    final Stack<Pattern> stack = new Stack<>();

    private PatternBuilder push(Pattern item) {
      stack.push(item);
      return this;
    }

    /** Returns the resulting pattern. */
    Pattern build() {
      if (stack.size() != 1) {
        throw new AssertionError("expected stack to have one item, but was "
            + stack);
      }
      return stack.pop();
    }

    /** Creates a pattern that matches symbol,
     * and pushes it onto the stack.
     *
     * @see SymbolPattern */
    PatternBuilder symbol(String symbolName) {
      return push(new SymbolPattern(symbolName));
    }

    /** Creates a pattern that matches the two patterns at the top of the
     * stack in sequence,
     * and pushes it onto the stack. */
    PatternBuilder seq() {
      final Pattern pattern1 = stack.pop();
      final Pattern pattern0 = stack.pop();
      return push(new OpPattern(PatternOp.SEQ, pattern0, pattern1));
    }

    /** Creates a pattern that matches the patterns at the top
     * of the stack zero or more times,
     * and pushes it onto the stack. */
    PatternBuilder star() {
      return push(new OpPattern(PatternOp.STAR, stack.pop()));
    }

    /** Creates a pattern that matches the patterns at the top
     * of the stack one or more times,
     * and pushes it onto the stack. */
    PatternBuilder plus() {
      return push(new OpPattern(PatternOp.PLUS, stack.pop()));
    }

    /** Creates a pattern that matches either of the two patterns at the top
     * of the stack,
     * and pushes it onto the stack. */
    PatternBuilder or() {
      final Pattern pattern1 = stack.pop();
      final Pattern pattern0 = stack.pop();
      return push(new OpPattern(PatternOp.OR, pattern0, pattern1));
    }

    public PatternBuilder repeat(int minRepeat, int maxRepeat) {
      final Pattern pattern = stack.pop();
      return push(new RepeatPattern(minRepeat, maxRepeat, pattern));
    }
  }

  /** Regular expression. */
  abstract static class Pattern {
    final PatternOp op;

    Pattern(PatternOp op) {
      this.op = Objects.requireNonNull(op);
    }

    /** Adds this pattern as a transition between two states in an
     * Automaton. */
    abstract AutomatonBuilder accept(AutomatonBuilder builder, State fromState,
        State toState);
  }

  /** Pattern that matches a symbol. */
  static class SymbolPattern extends Pattern {
    final String name;

    SymbolPattern(String name) {
      super(PatternOp.SYMBOL);
      this.name = Objects.requireNonNull(name);
    }

    @Override public String toString() {
      return name;
    }

    AutomatonBuilder accept(AutomatonBuilder builder, State fromState,
        State toState) {
      builder.symbol(fromState, toState, name);
      return builder;
    }
  }

  /** Pattern with one or more arguments. */
  static class OpPattern extends Pattern {
    final ImmutableList<Pattern> patterns;

    OpPattern(PatternOp op, Pattern... patterns) {
      super(op);
      Preconditions.checkArgument(patterns.length >= op.minArity);
      Preconditions.checkArgument(op.maxArity == -1
          || patterns.length <= op.maxArity);
      this.patterns = ImmutableList.copyOf(patterns);
    }

    @Override public String toString() {
      switch (op) {
      case SEQ:
        return patterns.stream().map(Object::toString)
            .collect(Collectors.joining(" "));
      case STAR:
        return "(" + patterns.get(0) + ")*";
      case PLUS:
        return "(" + patterns.get(0) + ")+";
      default:
        throw new AssertionError("unknown op " + op);
      }
    }

    @Override AutomatonBuilder accept(AutomatonBuilder builder,
        State fromState, State toState) {
      switch (op) {
      case SEQ:
        return builder.seq(fromState, toState, patterns);
      case STAR:
        return builder.star(fromState, toState, patterns.get(0));
      case PLUS:
        return builder.plus(fromState, toState, patterns.get(0));
      default:
        throw new AssertionError("unknown op " + op);
      }
    }
  }

  /** Pattern that matches a pattern repeated between {@code minRepeat}
   * and {@code maxRepeat} times. */
  static class RepeatPattern extends OpPattern {
    final int minRepeat;
    final int maxRepeat;

    RepeatPattern(int minRepeat, int maxRepeat, Pattern pattern) {
      super(PatternOp.REPEAT, pattern);
      this.minRepeat = minRepeat;
      this.maxRepeat = maxRepeat;
    }

    @Override public String toString() {
      return "(" + patterns.get(0) + "){" + minRepeat
          + (maxRepeat == minRepeat ? "" : (", " + maxRepeat))
          + "}";
    }

    @Override AutomatonBuilder accept(AutomatonBuilder builder,
        State fromState, State toState) {
      return builder.repeat(fromState, toState, patterns.get(0), minRepeat,
          maxRepeat);
    }
  }

  /** Builds a state-transition graph for deterministic finite automaton.
   * Helps us write tests for automata.
   * In production code, the automaton is created based on a parse tree.
   */
  static class AutomatonBuilder {
    final Map<String, Integer> symbolIds = new HashMap<>();
    final List<State> stateList = new ArrayList<>();
    final List<Transition> transitionList = new ArrayList<>();
    final State startState = createState();
    final State endState = createState();

    /** Adds a pattern as a start-to-end transition. */
    AutomatonBuilder add(Pattern pattern) {
      return pattern.accept(this, startState, endState);
    }

    State createState() {
      final State state = new State(stateList.size());
      stateList.add(state);
      return state;
    }

    /** Adds a symbol transition. */
    void symbol(State fromState, State toState, String name) {
      Objects.requireNonNull(name);
      final int symbolId =
          symbolIds.computeIfAbsent(name, k -> symbolIds.size());
      transitionList.add(new SymbolTransition(fromState, toState, symbolId));
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
        pattern.accept(this, prevState, nextState);
        prevState = nextState;
      }
      return this;
    }

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
      pattern.accept(this, beforeState, afterState);
      transitionList.add(new EpsilonTransition(afterState, beforeState));
      transitionList.add(new EpsilonTransition(afterState, toState));
      transitionList.add(new EpsilonTransition(fromState, toState));
      return this;
    }

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
      pattern.accept(this, beforeState, afterState);
      transitionList.add(new EpsilonTransition(afterState, beforeState));
      transitionList.add(new EpsilonTransition(afterState, toState));
      return this;
    }

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
          pattern.accept(this, prevState, s);
        }
        if (i >= minRepeat) {
          transitionList.add(new EpsilonTransition(s, toState));
        }
        prevState = s;
      }
      transitionList.add(new EpsilonTransition(prevState, toState));
      return this;
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
  }

  /** Transition from one state to another in the finite-state automaton. */
  abstract static class Transition {
    final State fromState;
    final State toState;

    protected Transition(State fromState, State toState) {
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

  /** A finite-state automaton. */
  static class Automaton {
    final State startState;
    final State endState;
    final ImmutableList<SymbolTransition> transitions;
    final ImmutableList<EpsilonTransition> epsilonTransitions;
    final ImmutableList<String> symbolNames;

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
  }

  /** Workspace that matches patterns against an automaton.
   *
   * @param <E> Type of rows matched by this automaton */
  static class Matcher<E> {
    private final Automaton automaton;
    private final ImmutableList<Predicate<E>> predicates;

    /** Creates a Matcher; use {@link #builder}. */
    private Matcher(Automaton automaton,
        ImmutableList<Predicate<E>> predicates) {
      this.automaton = Objects.requireNonNull(automaton);
      this.predicates = Objects.requireNonNull(predicates);
    }

    static <E> Builder<E> builder(Pattern pattern) {
      final Automaton automaton = new AutomatonBuilder().add(pattern).build();
      return new Builder<>(automaton);
    }

    public List<List<E>> match(E... rows) {
      return match(Arrays.asList(rows));
    }

    public List<List<E>> match(Iterable<E> rows) {
      final ImmutableBitSet emptyStateSet = ImmutableBitSet.of();

      final ImmutableBitSet.Builder startSetBuilder =
          ImmutableBitSet.builder();
      startSetBuilder.set(automaton.startState.id);
      automaton.epsilonSuccessors(automaton.startState.id, startSetBuilder);
      final ImmutableBitSet startSet = startSetBuilder.build();

      final ImmutableList.Builder<List<E>> resultMatches =
          ImmutableList.builder();
      final CircularArrayList<E> bufferedRows =
          new CircularArrayList<>();
      final CircularArrayList<ImmutableBitSet> stateSets =
          new CircularArrayList<>();
      final List<Pair<E, ImmutableBitSet>> rowStateSets =
          Pair.zipMutable(bufferedRows, stateSets);
      final List<Integer> rowSymbols = new ArrayList<>();
      final ImmutableBitSet.Builder nextStateBuilder =
          ImmutableBitSet.builder();
      for (E row : rows) {
        // Add this row to the states.
        rowStateSets.add(Pair.of(row, startSet));

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
        for (int i = 0; i < rowStateSets.size();) {
          final Pair<E, ImmutableBitSet> rowStateSet = rowStateSets.get(i);
          assert nextStateBuilder.isEmpty();
          for (int symbol : rowSymbols) {
            for (int state : rowStateSet.right) {
              automaton.successors(state, symbol, nextStateBuilder);
            }
          }
          final ImmutableBitSet nextStateSet =
              nextStateBuilder.buildAndReset();
          if (nextStateSet.isEmpty()) {
            if (i == 0) {
              // Don't add the stateSet if it is empty and would be the oldest.
              // The first item in stateSets must not be empty.
              rowStateSets.remove(0);
            } else {
              stateSets.set(i++, emptyStateSet);
            }
          } else if (nextStateSet.get(automaton.endState.id)) {
            resultMatches.add(
                ImmutableList.copyOf(
                    bufferedRows.subList(i, bufferedRows.size())));
            if (i == 0) {
              // Don't add the stateSet if it is empty and would be the oldest.
              // The first item in stateSets must not be empty.
              rowStateSets.remove(0);
            } else {
              // Set state to empty so that it is not considered for any
              // further matches, and will be removed when it is the oldest.
              stateSets.set(i++, emptyStateSet);
            }
          } else {
            stateSets.set(i++, nextStateSet);
          }
        }
      }
      return resultMatches.build();
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

  /** Operator that constructs composite {@link Pattern} instances. */
  enum PatternOp {
    /** A leaf pattern, consisting of a single symbol. */
    SYMBOL(0, 0),
    /** Pattern that matches one pattern followed by another. */
    SEQ(2, -1),
    /** Pattern that matches one pattern or another. */
    OR(2, -1),
    /** Pattern that matches a pattern repeated zero or more times. */
    STAR(1, 1),
    /** Pattern that matches a pattern repeated one or more times. */
    PLUS(1, 1),
    /** Pattern that matches a pattern repeated between {@code minRepeat}
     * and {@code maxRepeat} times. */
    REPEAT(1, 1);

    private final int minArity;
    private final int maxArity;

    PatternOp(int minArity, int maxArity) {
      this.minArity = minArity;
      this.maxArity = maxArity;
    }
  }
}

// End AutomatonTest.java
