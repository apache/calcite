/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.tools;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * Utilities for creating {@link Program}s.
 */
public class Programs {
  private static final Function<RuleSet, Program> RULE_SET_TO_PROGRAM =
      new Function<RuleSet, Program>() {
        public Program apply(RuleSet ruleSet) {
          return of(ruleSet);
        }
      };

  // private constructor for utility class
  private Programs() {}

  /** Creates a program that executes a rule set. */
  public static Program of(RuleSet ruleSet) {
    return new RuleSetProgram(ruleSet);
  }

  /** Creates a list of programs based on an array of rule sets. */
  public static List<Program> listOf(RuleSet... ruleSets) {
    return Lists.transform(Arrays.asList(ruleSets), RULE_SET_TO_PROGRAM);
  }

  /** Creates a list of programs based on a list of rule sets. */
  public static List<Program> listOf(List<RuleSet> ruleSets) {
    return Lists.transform(ruleSets, RULE_SET_TO_PROGRAM);
  }

  /** Creates a program from a list of rules. */
  public static Program ofRules(RelOptRule... rules) {
    return of(RuleSets.ofList(rules));
  }

  /** Creates a program from a list of rules. */
  public static Program ofRules(Collection<RelOptRule> rules) {
    return of(RuleSets.ofList(rules));
  }

  /** Creates a program that executes a sequence of programs. */
  public static Program sequence(Program... programs) {
    return new SequenceProgram(ImmutableList.copyOf(programs));
  }

  /** Creates a program that executes a list of rules in a HEP planner. */
  public static Program hep(ImmutableList<RelOptRule> rules, boolean noDag,
      RelMetadataProvider metadataProvider) {
    final HepProgramBuilder builder = HepProgram.builder();
    for (RelOptRule rule : rules) {
      builder.addRuleInstance(rule);
    }
    return of(builder.build(), noDag, metadataProvider);
  }

  /** Creates a program that executes a {@link HepProgram}. */
  public static Program of(final HepProgram hepProgram, final boolean noDag,
      final RelMetadataProvider metadataProvider) {
    return new Program() {
      public RelNode run(RelOptPlanner planner, RelNode rel,
          RelTraitSet requiredOutputTraits) {
        final HepPlanner hepPlanner = new HepPlanner(hepProgram,
            null, noDag, null, RelOptCostImpl.FACTORY);

        if (metadataProvider != null) {
          List<RelMetadataProvider> list = Lists.newArrayList();
          list.add(metadataProvider);
          hepPlanner.registerMetadataProviders(list);
          RelMetadataProvider plannerChain =
              ChainedRelMetadataProvider.of(list);
          rel.getCluster().setMetadataProvider(plannerChain);
        }

        hepPlanner.setRoot(rel);
        return hepPlanner.findBestExp();
      }
    };
  }

  /** Creates a program that invokes heuristic join-order optimization
   * (via {@link org.eigenbase.rel.rules.ConvertMultiJoinRule},
   * {@link org.eigenbase.rel.rules.MultiJoinRel} and
   * {@link org.eigenbase.rel.rules.LoptOptimizeJoinRule})
   * if there are 6 or more joins (7 or more relations). */
  public static Program heuristicJoinOrder(final Collection<RelOptRule> rules,
      final boolean bushy) {
    return new Program() {
      public RelNode run(RelOptPlanner planner, RelNode rel,
          RelTraitSet requiredOutputTraits) {
        final int joinCount = RelOptUtil.countJoins(rel);
        final Program program;
        if (joinCount < 6) {
          program = ofRules(rules);
        } else {
          // Create a program that gathers together joins as a MultiJoinRel.
          final HepProgram hep = new HepProgramBuilder()
              .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
              .addMatchOrder(HepMatchOrder.BOTTOM_UP)
              .addRuleInstance(ConvertMultiJoinRule.INSTANCE)
              .build();
          final Program program1 = of(hep, false, null);

          // Create a program that contains a rule to expand a MultiJoinRel
          // into heuristically ordered joins.
          // We use the rule set passed in, but remove SwapJoinRule and
          // PushJoinThroughJoinRule, because they cause exhaustive search.
          final List<RelOptRule> list = new ArrayList<RelOptRule>(rules);
          list.removeAll(
              ImmutableList.of(SwapJoinRule.INSTANCE,
                  CommutativeJoinRule.INSTANCE,
                  PushJoinThroughJoinRule.LEFT,
                  PushJoinThroughJoinRule.RIGHT));
          list.add(bushy
              ? OptimizeBushyJoinRule.INSTANCE
              : LoptOptimizeJoinRule.INSTANCE);
          final Program program2 = ofRules(list);

          program = sequence(program1, program2);
        }
        return program.run(planner, rel, requiredOutputTraits);
      }
    };
  }

  /** Program backed by a {@link RuleSet}. */
  static class RuleSetProgram implements Program {
    final RuleSet ruleSet;

    private RuleSetProgram(RuleSet ruleSet) {
      this.ruleSet = ruleSet;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits) {
      planner.clear();
      for (RelOptRule rule : ruleSet) {
        planner.addRule(rule);
      }
      if (!rel.getTraitSet().equals(requiredOutputTraits)) {
        rel = planner.changeTraits(rel, requiredOutputTraits);
      }
      planner.setRoot(rel);
      return planner.findBestExp();

    }
  }

  /** Program that runs sub-programs, sending the output of the previous as
   * input to the next. */
  private static class SequenceProgram implements Program {
    private final ImmutableList<Program> programs;

    SequenceProgram(ImmutableList<Program> programs) {
      this.programs = programs;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits) {
      for (Program program : programs) {
        rel = program.run(planner, rel, requiredOutputTraits);
      }
      return rel;
    }
  }
}

// End Programs.java
