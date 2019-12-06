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
import org.apache.calcite.rel.BiRel;
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
 * Common classes and utility methods for Volcano planner tests.
 */
class PlannerTests {

  private PlannerTests() {}

  /**
   * Private calling convention representing a physical implementation.
   */
  static final Convention PHYS_CALLING_CONVENTION =
      new Convention.Impl("PHYS", RelNode.class) {
        @Override public boolean canConvertConvention(Convention toConvention) {
          return true;
        }

        @Override public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
          return true;
        }
      };

  static final Convention PHYS_CALLING_CONVENTION_2 =
      new Convention.Impl("PHYS_2", RelNode.class) {
        @Override public boolean canConvertConvention(Convention toConvention) {
          return true;
        }

        @Override public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
          return true;
        }
      };

  static RelOptCluster newCluster(VolcanoPlanner planner) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  /** Leaf relational expression. */
  abstract static class TestLeafRel extends AbstractRelNode {
    final String label;

    TestLeafRel(RelOptCluster cluster, RelTraitSet traits, String label) {
      super(cluster, traits);
      this.label = label;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    @Override protected RelDataType deriveRowType() {
      final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
      return typeFactory.builder()
          .add("this", typeFactory.createJavaType(Void.TYPE))
          .build();
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("label", label);
    }
  }

  /** Relational expression with one input. */
  abstract static class TestSingleRel extends SingleRel {
    TestSingleRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, traits, input);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    @Override protected RelDataType deriveRowType() {
      return getInput().getRowType();
    }
  }

  /** Relational expression with one input and convention NONE. */
  static class NoneSingleRel extends TestSingleRel {
    NoneSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster, cluster.traitSetOf(Convention.NONE), input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Relational expression with two inputs and convention PHYS. */
  static class PhysBiRel extends BiRel {
    PhysBiRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
        RelNode right) {
      super(cluster, traitSet, left, right);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.size() == 2;
      return new PhysBiRel(getCluster(), traitSet, inputs.get(0),
          inputs.get(1));
    }

    @Override protected RelDataType deriveRowType() {
      return getLeft().getRowType();
    }
  }

  /** Relational expression with zero inputs and convention NONE. */
  static class NoneLeafRel extends TestLeafRel {
    NoneLeafRel(RelOptCluster cluster, String label) {
      super(cluster, cluster.traitSetOf(Convention.NONE), label);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** Relational expression with zero inputs and convention PHYS. */
  static class PhysLeafRel extends TestLeafRel {
    Convention convention;

    PhysLeafRel(RelOptCluster cluster, String label) {
      this(cluster, PHYS_CALLING_CONVENTION, label);
    }

    PhysLeafRel(RelOptCluster cluster, Convention convention, String label) {
      super(cluster, cluster.traitSetOf(convention), label);
      this.convention = convention;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(convention);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** Relational expression with one input and convention PHYS. */
  static class PhysSingleRel extends TestSingleRel {
    PhysSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION), input);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(PHYS_CALLING_CONVENTION);
      return new PhysSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Planner rule that converts {@link NoneLeafRel} to PHYS convention. */
  static class PhysLeafRule extends RelOptRule {
    PhysLeafRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
          new PhysLeafRel(leafRel.getCluster(), leafRel.label));
    }
  }

  /** Planner rule that matches a {@link NoneSingleRel} and succeeds. */
  static class GoodSingleRule extends RelOptRule {
    GoodSingleRule() {
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
              single.getTraitSet().replace(PHYS_CALLING_CONVENTION));
      call.transformTo(
          new PhysSingleRel(single.getCluster(), physInput));
    }
  }

  /**
   * Planner rule that matches a parent with two children and asserts that they
   * are not the same.
   */
  static class AssertOperandsDifferentRule extends RelOptRule {
    AssertOperandsDifferentRule() {
      super(
          operand(PhysBiRel.class,
              operand(PhysLeafRel.class, any()),
              operand(PhysLeafRel.class, any())));
    }

    public void onMatch(RelOptRuleCall call) {
      PhysLeafRel left = call.rel(1);
      PhysLeafRel right = call.rel(2);

      assert left != right : left + " should be different from " + right;
    }
  }
}

// End PlannerTests.java
