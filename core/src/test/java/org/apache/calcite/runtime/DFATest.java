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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DFA} */
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
        equalTo(
            new DFA.MultiState(new Automaton.State(0), new Automaton.State(2)
        )));

    // Result should have three states
    // 0 -A-> 1 -A-> 2
    // 1 and 2 should be final
    assertThat(da.getTransitions().size(), equalTo(2));
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
        new DFA(automaton);

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
        new DFA(automaton);

    // Result should have two transitions
    // 0 -A-> 1 -B-> 2 (which again goes to 2 on a "B")
    // 1 should be final
    assertThat(da.getTransitions().size(), equalTo(3));
    assertThat(da.getEndStates().size(), equalTo(2));
  }

  @Test public void convertAutomaton4() {
    final Pattern.PatternBuilder builder = Pattern.builder();
    final Pattern pattern = builder
        .symbol("A")
        .symbol("B").optional().seq()
        .symbol("A").seq()
        .build();
    final Automaton automaton = pattern.toAutomaton();

    final DFA da =
        new DFA(automaton);

    // Result should have four transitions and one end state
    assertThat(da.getTransitions().size(), equalTo(4));
    assertThat(da.getEndStates().size(), equalTo(1));
  }


}

// End DFATest.java
