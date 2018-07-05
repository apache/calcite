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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.junit.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.TestLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.rel.RelDistributionTraitDef}.
 */
public class TraitConversionTest {

  private static final ConvertRelDistributionTraitDef NEW_TRAIT_DEF_INSTANCE =
      new ConvertRelDistributionTraitDef();
  private static final SimpleDistribution SIMPLE_DISTRIBUTION_ANY =
      new SimpleDistribution("ANY");
  private static final SimpleDistribution SIMPLE_DISTRIBUTION_RANDOM =
      new SimpleDistribution("RANDOM");
  private static final SimpleDistribution SIMPLE_DISTRIBUTION_SINGLETON =
      new SimpleDistribution("SINGLETON");

  @Test public void testTraitConversion() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(NEW_TRAIT_DEF_INSTANCE);

    planner.addRule(new RandomSingleTraitRule());
    planner.addRule(new SingleLeafTraitRule());
    planner.addRule(ExpandConversionRule.INSTANCE);

    final RelOptCluster cluster = newCluster(planner);
    final NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    final NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    final RelNode convertedRel =
        planner.changeTraits(singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    final RelNode result = planner.chooseDelegate().findBestExp();

    assertTrue(result instanceof RandomSingleRel);
    assertTrue(result.getTraitSet().contains(PHYS_CALLING_CONVENTION));
    assertTrue(result.getTraitSet().contains(SIMPLE_DISTRIBUTION_RANDOM));

    final RelNode input = result.getInput(0);
    assertTrue(input instanceof BridgeRel);
    assertTrue(input.getTraitSet().contains(PHYS_CALLING_CONVENTION));
    assertTrue(input.getTraitSet().contains(SIMPLE_DISTRIBUTION_RANDOM));

    final RelNode input2 = input.getInput(0);
    assertTrue(input2 instanceof SingletonLeafRel);
    assertTrue(input2.getTraitSet().contains(PHYS_CALLING_CONVENTION));
    assertTrue(input2.getTraitSet().contains(SIMPLE_DISTRIBUTION_SINGLETON));
  }

  /** Converts a {@link NoneSingleRel} (none convention, distribution any)
   * to {@link RandomSingleRel} (physical convention, distribution random). */
  private static class RandomSingleTraitRule extends RelOptRule {
    RandomSingleTraitRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel single = call.rel(0);
      RelNode input = single.getInput();
      RelNode physInput =
          convert(input,
              single.getTraitSet()
                  .replace(PHYS_CALLING_CONVENTION)
                  .plus(SIMPLE_DISTRIBUTION_RANDOM));
      call.transformTo(
          new RandomSingleRel(
              single.getCluster(),
              physInput));
    }
  }

  /** Rel with physical convention and random distribution. */
  private static class RandomSingleRel extends TestSingleRel {
    RandomSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION)
              .plus(SIMPLE_DISTRIBUTION_RANDOM), input);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new RandomSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Converts {@link NoneLeafRel} (none convention, any distribution) to
   * {@link SingletonLeafRel} (physical convention, singleton distribution). */
  private static class SingleLeafTraitRule extends RelOptRule {
    SingleLeafTraitRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
          new SingletonLeafRel(leafRel.getCluster(), leafRel.label));
    }
  }

  /** Rel with singleton distribution, physical convention. */
  private static class SingletonLeafRel extends TestLeafRel {
    SingletonLeafRel(RelOptCluster cluster, String label) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION)
              .plus(SIMPLE_DISTRIBUTION_SINGLETON), label);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SingletonLeafRel(getCluster(), label);
    }
  }

  /** Bridges the {@link SimpleDistribution}, difference between
   * {@link SingletonLeafRel} and {@link RandomSingleRel}. */
  private static class BridgeRel extends TestSingleRel {
    BridgeRel(RelOptCluster cluster, RelNode input) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION)
              .plus(SIMPLE_DISTRIBUTION_RANDOM), input);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new BridgeRel(getCluster(), sole(inputs));
    }
  }

  /** Dummy distribution for test (simplified version of RelDistribution). */
  private static class SimpleDistribution implements RelTrait {
    private final String name;

    SimpleDistribution(String name) {
      this.name = name;
    }

    @Override public String toString() {
      return name;
    }

    @Override public RelTraitDef getTraitDef() {
      return NEW_TRAIT_DEF_INSTANCE;
    }

    @Override public boolean satisfies(RelTrait trait) {
      return trait == this || trait == SIMPLE_DISTRIBUTION_ANY;

    }

    @Override public void register(RelOptPlanner planner) {}
  }

  /**
   * Dummy distribution trait def for test (handles conversion of SimpleDistribution)
   */
  private static class ConvertRelDistributionTraitDef
      extends RelTraitDef<SimpleDistribution> {

    @Override public Class<SimpleDistribution> getTraitClass() {
      return SimpleDistribution.class;
    }

    @Override public String toString() {
      return getSimpleName();
    }

    @Override public String getSimpleName() {
      return "ConvertRelDistributionTraitDef";
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel,
        SimpleDistribution toTrait, boolean allowInfiniteCostConverters) {
      if (toTrait == SIMPLE_DISTRIBUTION_ANY) {
        return rel;
      }

      return new BridgeRel(rel.getCluster(), rel);
    }

    @Override public boolean canConvert(RelOptPlanner planner,
        SimpleDistribution fromTrait, SimpleDistribution toTrait) {
      return (fromTrait == toTrait)
          || (toTrait == SIMPLE_DISTRIBUTION_ANY)
          || (fromTrait == SIMPLE_DISTRIBUTION_SINGLETON
          && toTrait == SIMPLE_DISTRIBUTION_RANDOM);

    }

    @Override public SimpleDistribution getDefault() {
      return SIMPLE_DISTRIBUTION_ANY;
    }
  }

  /** Any distribution and none convention. */
  private static class NoneLeafRel extends TestLeafRel {
    NoneLeafRel(RelOptCluster cluster, String label) {
      super(cluster, cluster.traitSetOf(Convention.NONE), label);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, SIMPLE_DISTRIBUTION_ANY);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** Rel with any distribution and none convention. */
  private static class NoneSingleRel extends TestSingleRel {
    NoneSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster, cluster.traitSetOf(Convention.NONE), input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, SIMPLE_DISTRIBUTION_ANY);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }
}

// End TraitConversionTest.java
