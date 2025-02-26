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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.TestLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.rel.RelCollationTraitDef}.
 */
class CollationConversionTest {
  private static final TestRelCollationImpl LEAF_COLLATION =
      new TestRelCollationImpl(
          ImmutableList.of(new RelFieldCollation(0, Direction.CLUSTERED)));

  private static final TestRelCollationImpl ROOT_COLLATION =
      new TestRelCollationImpl(ImmutableList.of(new RelFieldCollation(0)));

  private static final TestRelCollationTraitDef COLLATION_TRAIT_DEF =
      new TestRelCollationTraitDef();

  @Test void testCollationConversion() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(COLLATION_TRAIT_DEF);

    planner.addRule(SingleNodeRule.INSTANCE);
    planner.addRule(LeafTraitRule.INSTANCE);
    planner.addRule(ExpandConversionRule.INSTANCE);
    planner.setTopDownOpt(false);

    final RelOptCluster cluster = newCluster(planner);
    final NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    final NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    final RelNode convertedRel =
        planner.changeTraits(singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(ROOT_COLLATION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertThat(result, instanceOf(RootSingleRel.class));
    assertTrue(result.getTraitSet().contains(ROOT_COLLATION));
    assertTrue(result.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    final RelNode input = result.getInput(0);
    assertThat(input, instanceOf(PhysicalSort.class));
    assertTrue(result.getTraitSet().contains(ROOT_COLLATION));
    assertTrue(input.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    final RelNode input2 = input.getInput(0);
    assertThat(input2, instanceOf(LeafRel.class));
    assertTrue(input2.getTraitSet().contains(LEAF_COLLATION));
    assertTrue(input.getTraitSet().contains(PHYS_CALLING_CONVENTION));
  }

  /** Converts a NoneSingleRel to RootSingleRel. */
  public static class SingleNodeRule
      extends RelRule<SingleNodeRule.Config> {
    static final SingleNodeRule INSTANCE = ImmutableSingleNodeRuleConfig.builder()
        .withOperandSupplier(b -> b.operand(NoneSingleRel.class).anyInputs())
        .build()
        .toRule();

    protected SingleNodeRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneSingleRel single = call.rel(0);
      RelNode input = single.getInput();
      RelNode physInput =
          convert(input,
              single.getTraitSet()
                  .replace(PHYS_CALLING_CONVENTION)
                  .plus(ROOT_COLLATION));
      call.transformTo(
          new RootSingleRel(
              single.getCluster(),
              physInput));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableSingleNodeRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default SingleNodeRule toRule() {
        return new SingleNodeRule(this);
      }
    }
  }

  /** Root node with physical convention and ROOT_COLLATION trait. */
  private static class RootSingleRel extends TestSingleRel {
    RootSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(ROOT_COLLATION),
          input);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new RootSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Converts a {@link NoneLeafRel} (with none convention) to {@link LeafRel}
   * (with physical convention). */
  public static class LeafTraitRule
      extends RelRule<LeafTraitRule.Config> {
    static final LeafTraitRule INSTANCE = ImmutableLeafTraitRuleConfig.builder()
        .withOperandSupplier(b -> b.operand(NoneLeafRel.class).anyInputs())
        .build()
        .toRule();

    LeafTraitRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(new LeafRel(leafRel.getCluster(), leafRel.label));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableLeafTraitRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default LeafTraitRule toRule() {
        return new LeafTraitRule(this);
      }
    }
  }

  /** Leaf node with physical convention and LEAF_COLLATION trait. */
  private static class LeafRel extends TestLeafRel {
    LeafRel(RelOptCluster cluster, String label) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(LEAF_COLLATION),
          label);
    }

    public @Nullable RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new LeafRel(getCluster(), label);
    }
  }

  /** Leaf node with none convention and LEAF_COLLATION trait. */
  private static class NoneLeafRel extends TestLeafRel {
    NoneLeafRel(RelOptCluster cluster, String label) {
      super(cluster, cluster.traitSetOf(Convention.NONE).plus(LEAF_COLLATION),
          label);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, LEAF_COLLATION);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** A single-input node with none convention and LEAF_COLLATION trait. */
  private static class NoneSingleRel extends TestSingleRel {
    NoneSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster, cluster.traitSetOf(Convention.NONE).plus(LEAF_COLLATION),
          input);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, LEAF_COLLATION);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Dummy collation trait implementation for the test. */
  private static class TestRelCollationImpl extends RelCollationImpl {
    TestRelCollationImpl(ImmutableList<RelFieldCollation> fieldCollations) {
      super(fieldCollations);
    }

    @Override public RelTraitDef getTraitDef() {
      return COLLATION_TRAIT_DEF;
    }
  }

  /** Dummy collation trait def implementation for the test (uses
   * {@link PhysicalSort} below). */
  private static class TestRelCollationTraitDef
      extends RelTraitDef<RelCollation> {
    public Class<RelCollation> getTraitClass() {
      return RelCollation.class;
    }

    public String getSimpleName() {
      return "testsort";
    }

    @Override public boolean multiple() {
      return true;
    }

    public RelCollation getDefault() {
      return LEAF_COLLATION;
    }

    public @Nullable RelNode convert(RelOptPlanner planner, RelNode rel,
        RelCollation toCollation, boolean allowInfiniteCostConverters) {
      if (toCollation.getFieldCollations().isEmpty()) {
        // An empty sort doesn't make sense.
        return null;
      }

      return new PhysicalSort(rel.getCluster(),
          rel.getTraitSet().replace(toCollation), rel, toCollation, null, null);
    }

    public boolean canConvert(RelOptPlanner planner, RelCollation fromTrait,
        RelCollation toTrait) {
      return true;
    }
  }

  /** Physical sort node (not logical). */
  private static class PhysicalSort extends Sort {
    PhysicalSort(RelOptCluster cluster, RelTraitSet traits, RelNode input,
        RelCollation collation, @Nullable RexNode offset,
        @Nullable RexNode fetch) {
      super(cluster, traits, input, collation, offset, fetch);
    }

    public Sort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, @Nullable RexNode offset,
        @Nullable RexNode fetch) {
      return new PhysicalSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }
  }
}
