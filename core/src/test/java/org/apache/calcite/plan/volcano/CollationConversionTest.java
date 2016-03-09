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

import org.junit.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.TestLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.rel.RelCollationTraitDef}.
 */
public class CollationConversionTest {
  private static final TestRelCollationImpl LEAF_COLLATION =
      new TestRelCollationImpl(
          ImmutableList.of(new RelFieldCollation(0, Direction.CLUSTERED)));

  private static final TestRelCollationImpl ROOT_COLLATION =
      new TestRelCollationImpl(ImmutableList.of(new RelFieldCollation(0)));

  private static final TestRelCollationTraitDef COLLATION_TRAIT_DEF =
      new TestRelCollationTraitDef();

  @Test public void testCollationConversion() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(COLLATION_TRAIT_DEF);

    planner.addRule(new SingleNodeRule());
    planner.addRule(new LeafTraitRule());
    planner.addRule(ExpandConversionRule.INSTANCE);

    final RelOptCluster cluster = newCluster(planner);
    final NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    final NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    final RelNode convertedRel =
        planner.changeTraits(singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(ROOT_COLLATION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof RootSingleRel);
    assertTrue(result.getTraitSet().contains(ROOT_COLLATION));
    assertTrue(result.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    final RelNode input = result.getInput(0);
    assertTrue(input instanceof PhysicalSort);
    assertTrue(result.getTraitSet().contains(ROOT_COLLATION));
    assertTrue(input.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    final RelNode input2 = input.getInput(0);
    assertTrue(input2 instanceof LeafRel);
    assertTrue(input2.getTraitSet().contains(LEAF_COLLATION));
    assertTrue(input.getTraitSet().contains(PHYS_CALLING_CONVENTION));
  }

  /** Converts a NoneSingleRel to RootSingleRel. */
  private class SingleNodeRule extends RelOptRule {
    SingleNodeRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
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
  }

  /** Root node with physical convention and ROOT_COLLATION trait. */
  private class RootSingleRel extends TestSingleRel {
    RootSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(ROOT_COLLATION),
          input);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new RootSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Converts a {@link NoneLeafRel} (with none convention) to {@link LeafRel}
   * (with physical convention). */
  private class LeafTraitRule extends RelOptRule {
    LeafTraitRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(new LeafRel(leafRel.getCluster(), leafRel.label));
    }
  }

  /** Leaf node with physical convention and LEAF_COLLATION trait. */
  private class LeafRel extends TestLeafRel {
    LeafRel(RelOptCluster cluster, String label) {
      super(cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(LEAF_COLLATION),
          label);
    }

    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new LeafRel(getCluster(), label);
    }
  }

  /** Leaf node with none convention and LEAF_COLLATION trait. */
  private class NoneLeafRel extends TestLeafRel {
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

    public RelNode convert(RelOptPlanner planner, RelNode rel,
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
        RelCollation collation, RexNode offset, RexNode fetch) {
      super(cluster, traits, input, collation, offset, fetch);
    }

    public Sort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset, RexNode fetch) {
      return new PhysicalSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }
  }
}

// End CollationConversionTest.java
