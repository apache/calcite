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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.NoneLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests trait derivation in the top-down optimizer
 * ({@link TopDownRuleDriver}) for physical nodes whose own traits satisfy
 * none of the required subsets.
 */
class TopDownDeriveTest {
  /** Physical convention whose enforcer is {@link ExchangeRel}. */
  private static final Convention DIST =
      new Convention.Impl("DIST", RelNode.class) {
        @Override public @Nullable RelNode enforce(RelNode input,
            RelTraitSet required) {
          return new ExchangeRel(input.getCluster(), input,
              required.getDistribution());
        }
      };

  private static final RelDistribution HASH_0 =
      RelDistributions.hash(ImmutableList.of(0));

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4542">[CALCITE-4542]
   * Suboptimal plan is chosen when TopDownRuleDriver is enabled</a>.
   *
   * <p>The physical rel is created with distribution ANY and is required to
   * be SINGLETON. Its pass-through alternative needs an exchange of all the
   * input rows, while the alternative derived from the hash-distributed
   * input only needs an exchange of the (few) output rows. */
  @Test void testDeriveRelThatPassesThrough() {
    final String expected = ""
        + "ExchangeRel(distribution=[single])\n"
        + "  ScaleRel(distribution=[hash[0]])\n"
        + "    HashLeafRel(label=[t])\n";
    assertThat(plan(0.01, true), is(expected));
  }

  /** As {@link #testDeriveRelThatPassesThrough()}, but the rel cannot pass
   * the requirement through; the derived alternative is the only plan. */
  @Test void testDeriveRelThatDoesNotPassThrough() {
    final String expected = ""
        + "ExchangeRel(distribution=[single])\n"
        + "  ScaleRel(distribution=[hash[0]])\n"
        + "    HashLeafRel(label=[t])\n";
    assertThat(plan(0.01, false), is(expected));
  }

  /** The rel increases the number of rows, so the pass-through alternative,
   * with the exchange below the rel, must still win. */
  @Test void testPassThroughStillChosen() {
    final String expected = ""
        + "ScaleRel(distribution=[single])\n"
        + "  ExchangeRel(distribution=[single])\n"
        + "    HashLeafRel(label=[t])\n";
    assertThat(plan(100, true), is(expected));
  }

  private static String plan(double rowFactor, boolean passThrough) {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.setTopDownOpt(true);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    planner.addRule(
        ConverterRule.Config.INSTANCE
            .withConversion(NoneLeafRel.class, Convention.NONE, DIST,
                "HashLeafRule")
            .withRuleFactory(LeafRule::new)
            .toRule());
    // The slow alternative is implemented first, so that the subset with
    // distribution ANY is optimized before the ScaleRel appears in it.
    planner.addRule(
        ConverterRule.Config.INSTANCE
            .withConversion(NoneSingleRel.class, Convention.NONE, DIST,
                "SlowScaleRule")
            .withRuleFactory(c -> new ScaleRule(c, rowFactor, passThrough, true))
            .toRule());
    planner.addRule(
        ConverterRule.Config.INSTANCE
            .withConversion(NoneSingleRel.class, Convention.NONE, DIST,
                "ScaleRule")
            .withRuleFactory(c -> new ScaleRule(c, rowFactor, passThrough, false))
            .toRule());

    final RelOptCluster cluster = newCluster(planner);
    final RelNode rel =
        new NoneSingleRel(cluster, new NoneLeafRel(cluster, "t"));
    planner.setRoot(
        planner.changeTraits(rel,
            rel.getTraitSet().replace(DIST)
                .replace(RelDistributions.SINGLETON)));
    return RelOptUtil.toString(planner.findBestExp());
  }

  /** Converts {@link NoneLeafRel} to {@link HashLeafRel}. */
  private static class LeafRule extends ConverterRule {
    LeafRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      return new HashLeafRel(rel.getCluster(), ((NoneLeafRel) rel).label);
    }
  }

  /** Converts {@link NoneSingleRel} to {@link ScaleRel} with distribution
   * ANY. */
  private static class ScaleRule extends ConverterRule {
    private final double rowFactor;
    private final boolean passThrough;
    private final boolean slow;

    ScaleRule(Config config, double rowFactor, boolean passThrough,
        boolean slow) {
      super(config);
      this.rowFactor = rowFactor;
      this.passThrough = passThrough;
      this.slow = slow;
    }

    @Override public RelNode convert(RelNode rel) {
      final RelTraitSet traitSet = rel.getTraitSet().replace(DIST);
      return new ScaleRel(rel.getCluster(), traitSet,
          RelOptRule.convert(((NoneSingleRel) rel).getInput(), traitSet),
          rowFactor, passThrough, slow);
    }
  }

  /** Leaf that delivers {@code hash[0]} distribution; cost is its row
   * count. */
  private static class HashLeafRel extends TestLeafRel {
    HashLeafRel(RelOptCluster cluster, String label) {
      super(cluster, cluster.traitSetOf(DIST).replace(HASH_0), label);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return 100;
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(100, 0, 0);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return this;
    }
  }

  /** Physical rel that multiplies the number of input rows by
   * {@code rowFactor}. It requires its input to have its own distribution,
   * and cannot be executed with distribution ANY. A slow rel is expensive
   * and never derives traits. */
  private static class ScaleRel extends TestSingleRel implements PhysicalNode {
    private final double rowFactor;
    private final boolean passThrough;
    private final boolean slow;

    ScaleRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        double rowFactor, boolean passThrough, boolean slow) {
      super(cluster, traitSet, input);
      this.rowFactor = rowFactor;
      this.passThrough = passThrough;
      this.slow = slow;
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return mq.getRowCount(getInput()) * rowFactor;
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      if (getTraitSet().getDistribution() == RelDistributions.ANY) {
        return planner.getCostFactory().makeInfiniteCost();
      }
      return slow
          ? planner.getCostFactory().makeCost(1000, 0, 0)
          : planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new ScaleRel(getCluster(), traitSet, sole(inputs), rowFactor,
          passThrough, slow);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw)
          .item("distribution", getTraitSet().getDistribution())
          .itemIf("slow", true, slow);
    }

    @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>>
        passThroughTraits(RelTraitSet required) {
      if (!passThrough) {
        return null;
      }
      return Pair.of(required, ImmutableList.of(required));
    }

    @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        RelTraitSet childTraits, int childId) {
      throw new UnsupportedOperationException();
    }

    @Override public DeriveMode getDeriveMode() {
      return DeriveMode.OMAKASE;
    }

    @Override public List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
      final List<RelNode> rels = new ArrayList<>();
      if (slow) {
        return rels;
      }
      for (RelTraitSet traits : inputTraits.get(0)) {
        final RelDistribution distribution = traits.getDistribution();
        if (distribution != null && distribution != RelDistributions.ANY) {
          rels.add(
              copy(getTraitSet().replace(distribution),
                  ImmutableList.of(RelOptRule.convert(getInput(), traits))));
        }
      }
      return rels;
    }
  }

  /** Enforcer that changes distribution; cost is the number of rows it
   * moves. */
  private static class ExchangeRel extends TestSingleRel {
    ExchangeRel(RelOptCluster cluster, RelNode input,
        RelDistribution distribution) {
      super(cluster, input.getTraitSet().replace(distribution), input);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory()
          .makeCost(mq.getRowCount(getInput()), 0, 0);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new ExchangeRel(getCluster(), sole(inputs),
          traitSet.getDistribution());
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw)
          .item("distribution", getTraitSet().getDistribution());
    }
  }
}
