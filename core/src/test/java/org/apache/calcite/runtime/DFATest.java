package org.apache.calcite.runtime;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class DFATest {

  @Test
  public void convertAutomaton() {
    final Pattern.PatternBuilder builder = Pattern.builder();
    final Pattern pattern = builder.symbol("A")
        .repeat(1, 2)
        .build();
    final Automaton automaton = pattern.toAutomaton();

    final DFA da =
        new DFA(automaton);

    assertThat(da.startState,
        equalTo(new DFA.MultiState(
            new Automaton.State(0), new Automaton.State(2)
        )));

    // Result should have three states
    // 0 -A-> 1 -A-> 2
    // 1 and 2 should be final
    assertThat(da.getTransitions().size(), equalTo(3));
    assertThat(da.getEndStates().size(), equalTo(2));
  }

  @Test public void convertAutomaton2() {
    final Pattern.PatternBuilder builder = Pattern.builder();
    final Pattern pattern = builder
        .symbol("A")
        .symbol("B")
        .or()
        .build();
    final Automaton automaton = pattern.toAutomaton();

    final DFA da =
        new DFA(automaton);;

    // Result should have two transitions
    // 0 -A-> 1
    //   -B->
    // 1 should be final
    assertThat(da.getTransitions().size(), equalTo(2));
    assertThat(da.getEndStates().size(), equalTo(1));
  }

  @Test public void convertAutomaton3() {
    final Pattern.PatternBuilder builder = Pattern.builder();
    final Pattern pattern = builder
        .symbol("A")
        .symbol("B").star().seq()
        .build();
    final Automaton automaton = pattern.toAutomaton();

    final DFA da =
        new DFA(automaton);;

    // Result should have two transitions
    // 0 -A-> 1 -B-> 2 (which again goes to 2 on a "B")
    // 1 should be final
    assertThat(da.getTransitions().size(), equalTo(3));
    assertThat(da.getEndStates().size(), equalTo(2));
  }
}