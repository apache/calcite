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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

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
import static org.apache.calcite.test.Matchers.isLinux;

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

  /** Plan where the exchange is above the rel, whose distribution is derived
   * from its input. */
  private static final String DERIVED_PLAN = ""
      + "ExchangeRel(distribution=[single])\n"
      + "  ScaleRel(distribution=[hash[0]])\n"
      + "    HashLeafRel(label=[t])\n";

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4542">[CALCITE-4542]
   * Suboptimal plan is chosen when TopDownRuleDriver is enabled</a>.
   *
   * <p>The physical rel is created with distribution ANY and is required to
   * be SINGLETON. Its pass-through alternative needs an exchange of all the
   * input rows, while the alternative derived from the hash-distributed
   * input only needs an exchange of the (few) output rows. */
  @Test void testDeriveRelThatPassesThrough() {
    assertThat(plan(1, 0.01, true), isLinux(DERIVED_PLAN));
  }

  /** As {@link #testDeriveRelThatPassesThrough()}, but the rel cannot pass
   * the requirement through; the derived alternative is the only plan. */
  @Test void testDeriveRelThatDoesNotPassThrough() {
    assertThat(plan(1, 0.01, false), isLinux(DERIVED_PLAN));
  }

  /** As {@link #testDeriveRelThatPassesThrough()}, for a rel with two
   * inputs. */
  @Test void testDeriveRelWithTwoInputs() {
    final String expected = ""
        + "ExchangeRel(distribution=[single])\n"
        + "  ScaleRel(distribution=[hash[0]])\n"
        + "    HashLeafRel(label=[t])\n"
        + "    HashLeafRel(label=[u])\n";
    assertThat(plan(2, 0.01, true), isLinux(expected));
  }

  /** The rel increases the number of rows, so the pass-through alternative,
   * with the exchange below the rel, must still win. */
  @Test void testPassThroughStillChosen() {
    final String expected = ""
        + "ScaleRel(distribution=[single])\n"
        + "  ExchangeRel(distribution=[single])\n"
        + "    HashLeafRel(label=[t])\n";
    assertThat(plan(1, 100, true), isLinux(expected));
  }

  /** Plans a rel with {@code inputCount} leaf inputs that must be SINGLETON.
   *
   * <p>The rule that creates the slow alternative is registered first, so
   * that the subset with distribution ANY is already optimized when the
   * {@link ScaleRel} is added to it. The bug only shows up in that order. */
  private static String plan(int inputCount, double rowFactor,
      boolean passThrough) {
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
    final Class<? extends RelNode> logicalClass =
        inputCount == 1 ? NoneSingleRel.class : NoneBiRel.class;
    for (boolean slow : new boolean[] {true, false}) {
      planner.addRule(
          ConverterRule.Config.INSTANCE
              .withConversion(logicalClass, Convention.NONE, DIST,
                  slow ? "SlowScaleRule" : "ScaleRule")
              .withRuleFactory(c ->
                  new ScaleRule(c, rowFactor, passThrough, slow))
              .toRule());
    }

    final RelOptCluster cluster = newCluster(planner);
    final RelNode leaf = new NoneLeafRel(cluster, "t");
    final RelNode rel = inputCount == 1
        ? new NoneSingleRel(cluster, leaf)
        : new NoneBiRel(cluster, leaf, new NoneLeafRel(cluster, "u"));
    planner.setRoot(
        planner.changeTraits(rel,
            rel.getTraitSet().replace(DIST)
                .replace(RelDistributions.SINGLETON)));
    return RelOptUtil.toString(planner.findBestExp());
  }

  /** Relational expression with two inputs and convention NONE. */
  private static class NoneBiRel extends BiRel {
    NoneBiRel(RelOptCluster cluster, RelNode left, RelNode right) {
      super(cluster, cluster.traitSetOf(Convention.NONE), left, right);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    @Override protected RelDataType deriveRowType() {
      return getLeft().getRowType();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoneBiRel(getCluster(), inputs.get(0), inputs.get(1));
    }
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

  /** Converts {@link NoneSingleRel} or {@link NoneBiRel} to {@link ScaleRel}
   * with distribution ANY. */
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
          Util.transform(rel.getInputs(),
              input -> RelOptRule.convert(input, traitSet)),
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

  /** Physical rel that returns the number of rows of its first input
   * multiplied by {@code rowFactor}. It requires all its inputs to have its
   * own distribution, and cannot be executed with distribution ANY. A slow
   * rel is expensive and never derives traits. */
  private static class ScaleRel extends AbstractRelNode implements PhysicalNode {
    private final List<RelNode> inputs;
    private final double rowFactor;
    private final boolean passThrough;
    private final boolean slow;

    ScaleRel(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs,
        double rowFactor, boolean passThrough, boolean slow) {
      super(cluster, traitSet);
      this.inputs = new ArrayList<>(inputs);
      this.rowFactor = rowFactor;
      this.passThrough = passThrough;
      this.slow = slow;
    }

    @Override public List<RelNode> getInputs() {
      return inputs;
    }

    @Override public void replaceInput(int ordinalInParent, RelNode p) {
      inputs.set(ordinalInParent, p);
      recomputeDigest();
    }

    @Override protected RelDataType deriveRowType() {
      return inputs.get(0).getRowType();
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return mq.getRowCount(inputs.get(0)) * rowFactor;
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
      return new ScaleRel(getCluster(), traitSet, inputs, rowFactor,
          passThrough, slow);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      super.explainTerms(pw);
      for (Ord<RelNode> input : Ord.zip(inputs)) {
        pw.input("input#" + input.i, input.e);
      }
      return pw.item("distribution", getTraitSet().getDistribution())
          .itemIf("slow", true, slow);
    }

    @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>>
        passThroughTraits(RelTraitSet required) {
      if (!passThrough) {
        return null;
      }
      return Pair.of(required, Util.transform(inputs, input -> required));
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
          final RelTraitSet traitSet = getTraitSet().replace(distribution);
          rels.add(
              copy(traitSet,
                  Util.transform(inputs,
                      input -> RelOptRule.convert(input, traitSet))));
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
