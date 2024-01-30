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

import org.apache.calcite.plan.RelOptRule;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * HepInstruction represents one instruction in a HepProgram. The actual
 * instruction set is defined here via inner classes; if these grow too big,
 * they should be moved out to top-level classes.
 */
abstract class HepInstruction {
  //~ Methods ----------------------------------------------------------------

  /** Creates runtime state for this instruction.
   *
   * <p>The state is mutable, knows how to execute the instruction, and is
   * discarded after this execution. See {@link HepState}.
   *
   * @param px Preparation context; the state should copy from the context
   * all information that it will need to execute
   *
   * @return Initialized state
   */
  abstract HepState prepare(PrepareContext px);

  //~ Inner Classes ----------------------------------------------------------

  /** Instruction that executes all rules of a given class. */
  static class RuleClass extends HepInstruction {
    final Class<? extends RelOptRule> ruleClass;

    <R extends RelOptRule> RuleClass(Class<R> ruleClass) {
      this.ruleClass = requireNonNull(ruleClass, "ruleClass");
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link RuleClass} instruction. */
    class State extends HepState {
      /** Actual rule set instantiated during planning by filtering all the
       * planner's rules through {@link #ruleClass}. */
      @Nullable Set<RelOptRule> ruleSet;

      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeRuleClass(RuleClass.this, this);
      }
    }
  }

  /** Instruction that executes all rules in a given collection. */
  static class RuleCollection extends HepInstruction {
    /** Collection of rules to apply. */
    final List<RelOptRule> rules;

    RuleCollection(Collection<RelOptRule> rules) {
      this.rules = ImmutableList.copyOf(rules);
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link RuleCollection} instruction. */
    class State extends HepState {
      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeRuleCollection(RuleCollection.this, this);
      }
    }
  }

  /** Instruction that executes converter rules. */
  static class ConverterRules extends HepInstruction {
    final boolean guaranteed;

    ConverterRules(boolean guaranteed) {
      this.guaranteed = guaranteed;
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link ConverterRules} instruction. */
    class State extends HepState {
      /** Actual rule set instantiated during planning by filtering all the
       * planner's rules, looking for the desired converters. */
      @MonotonicNonNull Set<RelOptRule> ruleSet;

      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeConverterRules(ConverterRules.this, this);
      }
    }
  }

  /** Instruction that finds common relational sub-expressions. */
  static class CommonRelSubExprRules extends HepInstruction {
    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link CommonRelSubExprRules} instruction. */
    class State extends HepState {
      @Nullable Set<RelOptRule> ruleSet;

      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeCommonRelSubExprRules(CommonRelSubExprRules.this, this);
      }
    }
  }

  /** Instruction that executes a given rule. */
  static class RuleInstance extends HepInstruction {
    /** Explicitly specified rule. */
    final RelOptRule rule;

    RuleInstance(RelOptRule rule) {
      this.rule = requireNonNull(rule, "rule");
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link RuleInstance} instruction. */
    class State extends HepState {
      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeRuleInstance(RuleInstance.this, this);
      }
    }
  }

  /** Instruction that executes a rule that is looked up by description. */
  static class RuleLookup extends HepInstruction {
    /** Description to look for. */
    final String ruleDescription;

    RuleLookup(String ruleDescription) {
      this.ruleDescription = requireNonNull(ruleDescription, "ruleDescription");
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link RuleLookup} instruction. */
    class State extends HepState {
      /** Rule looked up by planner from description. */
      @Nullable RelOptRule rule;

      State(PrepareContext px) {
        super(px);
      }

      @Override void init() {
        // Look up anew each run.
        rule = null;
      }

      @Override void execute() {
        planner.executeRuleLookup(RuleLookup.this, this);
      }
    }
  }

  /** Instruction that sets match order. */
  static class MatchOrder extends HepInstruction {
    final HepMatchOrder order;

    MatchOrder(HepMatchOrder order) {
      this.order = requireNonNull(order, "order");
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link MatchOrder} instruction. */
    class State extends HepState {
      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeMatchOrder(MatchOrder.this, this);
      }
    }
  }

  /** Instruction that sets match limit. */
  static class MatchLimit extends HepInstruction {
    final int limit;

    MatchLimit(int limit) {
      this.limit = limit;
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link MatchLimit} instruction. */
    class State extends HepState {
      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeMatchLimit(MatchLimit.this, this);
      }
    }
  }

  /** Instruction that executes a sub-program. */
  static class SubProgram extends HepInstruction {
    final HepProgram subProgram;

    SubProgram(HepProgram subProgram) {
      this.subProgram = requireNonNull(subProgram, "subProgram");
    }

    @Override HepProgram.State prepare(PrepareContext px) {
      return subProgram.prepare(px);
    }

    /** State for a {@link SubProgram} instruction. */
    class State extends HepState {
      final HepProgram.State subProgramState;

      State(PrepareContext px) {
        super(px);
        subProgramState = subProgram.prepare(px);
      }

      @Override void init() {
        subProgramState.init();
      }

      @Override void execute() {
        planner.executeSubProgram(SubProgram.this, this);
      }
    }
  }

  /** Instruction that begins a group. */
  static class BeginGroup extends HepInstruction {
    final EndGroup endGroup;

    BeginGroup(EndGroup endGroup) {
      this.endGroup = requireNonNull(endGroup, "endGroup");
    }

    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link BeginGroup} instruction. */
    class State extends HepState {
      final HepInstruction.EndGroup.State endGroup;

      State(PrepareContext px) {
        super(px);
        this.endGroup = requireNonNull(px.endGroupState, "endGroupState");
      }

      @Override void execute() {
        planner.executeBeginGroup(BeginGroup.this, this);
      }
    }
  }

  /** Placeholder instruction that marks the beginning of a group under
   * construction. */
  static class Placeholder extends HepInstruction {
    @Override HepState prepare(PrepareContext px) {
      throw new UnsupportedOperationException();
    }
  }

  /** Instruction that ends a group. */
  static class EndGroup extends HepInstruction {
    @Override State prepare(PrepareContext px) {
      return new State(px);
    }

    /** State for a {@link EndGroup} instruction. */
    class State extends HepState {
      /** Actual rule set instantiated during planning by collecting grouped
       * rules. */
      final Set<RelOptRule> ruleSet = new HashSet<>();

      boolean collecting = true;

      State(PrepareContext px) {
        super(px);
      }

      @Override void execute() {
        planner.executeEndGroup(EndGroup.this, this);
      }

      @Override void init() {
        collecting = true;
      }
    }
  }

  /** All the information that might be necessary to initialize {@link HepState}
   * for a particular instruction. */
  static class PrepareContext {
    final HepPlanner planner;
    final HepProgram.State programState;
    final EndGroup.State endGroupState;

    private PrepareContext(HepPlanner planner,
        HepProgram.State programState, EndGroup.State endGroupState) {
      this.planner = planner;
      this.programState = programState;
      this.endGroupState = endGroupState;
    }

    static PrepareContext create(HepPlanner planner) {
      return new PrepareContext(planner, castNonNull(null), castNonNull(null));
    }

    PrepareContext withProgramState(HepProgram.State programState) {
      return new PrepareContext(planner, programState, endGroupState);
    }

    PrepareContext withEndGroupState(EndGroup.State endGroupState) {
      return new PrepareContext(planner, programState, endGroupState);
    }
  }
}
