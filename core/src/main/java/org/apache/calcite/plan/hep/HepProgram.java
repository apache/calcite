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
package org.apache.calcite.plan.hep;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.linq4j.Nullness.castToInitialized;

/**
 * HepProgram specifies the order in which rules should be attempted by
 * {@link HepPlanner}. Use {@link HepProgramBuilder} to create a new
 * instance of HepProgram.
 *
 * <p>Note that the structure of a program is immutable, but the planner uses it
 * as read/write during planning, so a program can only be in use by a single
 * planner at a time.
 */
public class HepProgram extends HepInstruction {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Symbolic constant for matching until no more matches occur.
   */
  public static final int MATCH_UNTIL_FIXPOINT = Integer.MAX_VALUE;

  //~ Instance fields --------------------------------------------------------

  final ImmutableList<HepInstruction> instructions;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new empty HepProgram. The program has an initial match order of
   * {@link org.apache.calcite.plan.hep.HepMatchOrder#DEPTH_FIRST}, and an initial
   * match limit of {@link #MATCH_UNTIL_FIXPOINT}.
   */
  HepProgram(List<HepInstruction> instructions) {
    this.instructions = ImmutableList.copyOf(instructions);
  }

  public static HepProgramBuilder builder() {
    return new HepProgramBuilder();
  }

  //~ Methods ----------------------------------------------------------------

  @Override State prepare(PrepareContext px) {
    return new State(px, instructions);
  }

  /** State for a {@link HepProgram} instruction. */
  class State extends HepState {
    final ImmutableList<HepState> instructionStates;
    int matchLimit = MATCH_UNTIL_FIXPOINT;
    HepMatchOrder matchOrder = HepMatchOrder.DEPTH_FIRST;
    HepInstruction.EndGroup.@Nullable State group;

    State(PrepareContext px, List<HepInstruction> instructions) {
      super(px);
      final PrepareContext px2 = px.withProgramState(castToInitialized(this));
      final List<HepState> states = new ArrayList<>();
      final Map<HepInstruction, Consumer<HepState>> actions = new HashMap<>();
      for (HepInstruction instruction : instructions) {
        final HepState state;
        if (instruction instanceof BeginGroup) {
          // The state of a BeginGroup instruction needs the state of the
          // corresponding EndGroup instruction, which we haven't seen yet.
          // Temporarily put a placeholder State into the list, and add an
          // action to replace that State. The action will be invoked when we
          // reach the EndGroup.
          final int i = states.size();
          actions.put(((BeginGroup) instruction).endGroup, state2 ->
              states.set(i,
                  instruction.prepare(
                      px2.withEndGroupState((EndGroup.State) state2))));
          state = castNonNull(null);
        } else {
          state = instruction.prepare(px2);
          if (actions.containsKey(instruction)) {
            actions.get(instruction).accept(state);
          }
        }
        states.add(state);
      }
      this.instructionStates = ImmutableList.copyOf(states);
    }

    @Override void init() {
      matchLimit = MATCH_UNTIL_FIXPOINT;
      matchOrder = HepMatchOrder.DEPTH_FIRST;
      group = null;
    }

    @Override void execute() {
      planner.executeProgram(HepProgram.this, this);
    }

    boolean skippingGroup() {
      if (group != null) {
        // Skip if we've already collected the ruleset.
        return !group.collecting;
      } else {
        // Not grouping.
        return false;
      }
    }
  }
}
