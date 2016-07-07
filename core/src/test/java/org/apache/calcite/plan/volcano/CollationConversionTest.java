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

import static org.apache.calcite.plan.volcano.PlannerTestCommon.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.TestLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.newCluster;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.rel.RelCollationTraitDef}.
 */
public class CollationConversionTest {

  public TestRelCollationImpl leafCollation =
      new TestRelCollationImpl(ImmutableList.of(new RelFieldCollation(0, Direction.CLUSTERED)));

  public TestRelCollationImpl rootCollation =
      new TestRelCollationImpl(ImmutableList.of(new RelFieldCollation(0)));

  public TestRelCollationTraitDef collationTraitDef = new TestRelCollationTraitDef();

  @Test public void testCollationConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(collationTraitDef);

    planner.addRule(new SingleNodeRule());
    planner.addRule(new LeafTraitRule());
    planner.addRule(ExpandConversionRule.INSTANCE);

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    RelNode convertedRel =
        planner.changeTraits(singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(rootCollation));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof RootSingleRel);
    assertTrue(result.getTraitSet().contains(rootCollation));
    assertTrue(result.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    RelNode child = result.getInput(0);
    assertTrue(child instanceof PhysicalSort);
    assertTrue(result.getTraitSet().contains(rootCollation));
    assertTrue(child.getTraitSet().contains(PHYS_CALLING_CONVENTION));

    RelNode grandChild = child.getInput(0);
    assertTrue(grandChild instanceof LeafRel);
    assertTrue(grandChild.getTraitSet().contains(leafCollation));
    assertTrue(child.getTraitSet().contains(PHYS_CALLING_CONVENTION));
  }

  /**
   * Converts a NoneSingleRel to RootSingleRel
   */
  class SingleNodeRule extends RelOptRule {
    SingleNodeRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      RelNode childRel = singleRel.getInput();
      RelNode physInput =
          convert(childRel,
              singleRel.getTraitSet()
                  .replace(PHYS_CALLING_CONVENTION)
                  .plus(rootCollation));
      call.transformTo(
          new RootSingleRel(
              singleRel.getCluster(),
              physInput));
    }
  }

  /**
   * RootSingleRel: Root node with physical convention and rootCollation trait.
   */
  class RootSingleRel extends TestSingleRel {
    RootSingleRel(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(rootCollation),
          child);
    }

    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new RootSingleRel(getCluster(), sole(inputs));
    }
  }

  /**
   * Converts a NoneLeafRel (with none convention) to LeafRel (with physical convention)
   */
  class LeafTraitRule extends RelOptRule {
    LeafTraitRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(new LeafRel(leafRel.getCluster(), leafRel.getLabel()));
    }
  }

  /**
   * SingletonLeafRel:  leaf node with physical convention and leafCollation trait
   */
  class LeafRel extends TestLeafRel {
    LeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(leafCollation),
          label);
    }

    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new LeafRel(getCluster(), getLabel());
    }
  }

  /**
   * NoneLeafRel:  leaf node with none convention and leafCollation trait
   */
  class NoneLeafRel extends TestLeafRel {
    protected NoneLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE).plus(leafCollation),
          label);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, leafCollation);
      assert inputs.isEmpty();
      return this;
    }
  }

  /**
   * NoneSingleRel:  a single relNode with none convention and leafCollation trait
   */
  class NoneSingleRel extends TestSingleRel {
    protected NoneSingleRel(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE).plus(leafCollation),
          child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, leafCollation);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }

  /**
   * Dummy collation trait implementation for the test
   */
  public class TestRelCollationImpl extends RelCollationImpl {

    public TestRelCollationImpl(ImmutableList<RelFieldCollation> fieldCollations) {
      super(fieldCollations);
    }

    @Override public RelTraitDef getTraitDef() {
      return collationTraitDef;
    }
  }

  /**
   * Dummy collation trait def implementation for the test (uses the PhysicalSort below)
   */
  public class TestRelCollationTraitDef extends RelTraitDef<RelCollation> {
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
      return leafCollation;
    }

    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        RelCollation toCollation,
        boolean allowInfiniteCostConverters) {
      if (toCollation.getFieldCollations().isEmpty()) {
        // An empty sort doesn't make sense.
        return null;
      }

      final Sort sort = new PhysicalSort(rel.getCluster(),
          rel.getTraitSet().replace(toCollation), rel, toCollation, null, null);
      return sort;
    }

    public boolean canConvert(
        RelOptPlanner planner, RelCollation fromTrait, RelCollation toTrait) {
      return true;
    }
  }

  /**
   * Physical Sort RelNode (not logical)
   */
  public class PhysicalSort extends Sort {
    public PhysicalSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch) {
      super(cluster, traits, child, collation, offset, fetch);

    }

    @Override public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch) {
      return new PhysicalSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }
  }
}
// End CollationConversionTest.java
