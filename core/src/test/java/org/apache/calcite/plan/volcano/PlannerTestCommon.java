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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;

/**
 * Common set of classes and utility methods for the tests in this package
 */
public class PlannerTestCommon {

  private PlannerTestCommon() {}

  /**
   * Private calling convention representing a physical implementation.
   */
  public static final Convention PHYS_CALLING_CONVENTION
    = new Convention.Impl("PHYS", RelNode.class) {
      @Override public boolean canConvertConvention(Convention toConvention) {
        return true;
      }

      @Override public boolean useAbstractConvertersForConversion(
          RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
      }
    };

  public static RelOptCluster newCluster(VolcanoPlanner planner) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  /** Leaf relational expression. */
  abstract static class TestLeafRel extends AbstractRelNode {
    private String label;

    protected TestLeafRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        String label) {
      super(cluster, traits);
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    // implement RelNode
    protected RelDataType deriveRowType() {
      final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
      return typeFactory.builder()
          .add("this", typeFactory.createJavaType(Void.TYPE))
          .build();
    }

    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("label", label);
    }
  }

  /** Relational expression with one input. */
  abstract static class TestSingleRel extends SingleRel {
    protected TestSingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child) {
      super(cluster, traits, child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    // implement RelNode
    protected RelDataType deriveRowType() {
      return getInput().getRowType();
    }
  }

  /** Relational expression with one input and convention NONE. */
  static class NoneSingleRel extends TestSingleRel {
    protected NoneSingleRel(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Relational expression with zero inputs and convention NONE. */
  static class NoneLeafRel extends TestLeafRel {
    protected NoneLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          label);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** Relational expression with zero inputs and convention PHYS. */
  static class PhysLeafRel extends TestLeafRel {
    PhysLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION),
          label);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(PHYS_CALLING_CONVENTION);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** Relational expression with one input and convention PHYS. */
  static class PhysSingleRel extends TestSingleRel {
    PhysSingleRel(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION),
          child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(PHYS_CALLING_CONVENTION);
      return new PhysSingleRel(
          getCluster(),
          sole(inputs));
    }
  }

  /** Planner rule that converts {@link NoneLeafRel} to PHYS
   * convention. */
  static class PhysLeafRule extends RelOptRule {
    PhysLeafRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
          new PhysLeafRel(
              leafRel.getCluster(),
              leafRel.getLabel()));
    }
  }

  /** Planner rule that matches a {@link NoneSingleRel} and succeeds. */
  static class GoodSingleRule extends RelOptRule {
    GoodSingleRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      RelNode childRel = singleRel.getInput();
      RelNode physInput =
          convert(
              childRel,
              singleRel.getTraitSet().replace(PHYS_CALLING_CONVENTION));
      call.transformTo(
          new PhysSingleRel(
              singleRel.getCluster(),
              physInput));
    }
  }
}
// End PlannerTestCommon.java
