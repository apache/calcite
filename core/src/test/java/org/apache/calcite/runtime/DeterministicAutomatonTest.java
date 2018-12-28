package org.apache.calcite.runtime;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class DeterministicAutomatonTest {

  @Test
  public void convertAutomaton() {
    final Pattern.PatternBuilder builder = Pattern.builder();
    final Pattern pattern = builder.symbol("A")
        .repeat(1, 2)
        .build();
    final Automaton automaton = pattern.toAutomaton();

    final Automaton.DeterministicAutomaton da =
        new Automaton.DeterministicAutomaton(automaton);

    assertThat(da.startState,
        equalTo(new Automaton.DeterministicAutomaton.MultiState(
            new Automaton.State(0), new Automaton.State(2)
        )));
  }
}