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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTrait;
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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

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

        @Override public RelNode enforce(final RelNode input,
            final RelTraitSet required) {
          return null;
        }
      };

  static final Convention PHYS_CALLING_CONVENTION_2 =
      new Convention.Impl("PHYS_2", RelNode.class) {
      };

  static final Convention PHYS_CALLING_CONVENTION_3 =
      new Convention.Impl("PHYS_3", RelNode.class) {
        @Override public boolean satisfies(RelTrait trait) {
          if (trait.equals(PHYS_CALLING_CONVENTION)) {
            return true;
          }
          return super.satisfies(trait);
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

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
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

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
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
      assert traitSet.contains(Convention.NONE);
      return new NoneSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Relational expression with two inputs and convention PHYS. */
  static class PhysBiRel extends BiRel {
    PhysBiRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
        RelNode right) {
      super(cluster, traitSet, left, right);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
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
    final Convention convention;

    PhysLeafRel(RelOptCluster cluster, String label) {
      this(cluster, PHYS_CALLING_CONVENTION, label);
    }

    PhysLeafRel(RelOptCluster cluster, Convention convention, String label) {
      super(cluster, cluster.traitSetOf(convention), label);
      this.convention = convention;
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
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

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.contains(PHYS_CALLING_CONVENTION);
      return new PhysSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Planner rule that converts {@link NoneLeafRel} to PHYS convention. */
  public static class PhysLeafRule extends RelRule<PhysLeafRule.Config> {
    static final PhysLeafRule INSTANCE =
        ImmutableTraitPhysLeafRuleConfig.builder()
            .withOperandSupplier(b -> b.operand(NoneLeafRel.class).anyInputs())
            .build()
            .toRule();

    protected PhysLeafRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
          new PhysLeafRel(leafRel.getCluster(), leafRel.label));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableTraitPhysLeafRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysLeafRule toRule() {
        return new PhysLeafRule(this);
      }
    }
  }

  /** Planner rule that converts {@link NoneLeafRel} to PHYS convention with different type. */
  public static class MockPhysLeafRule extends RelRule<MockPhysLeafRule.Config> {
    static final MockPhysLeafRule INSTANCE =
        ImmutableMockPhysLeafRuleConfig.builder()
            .withOperandSupplier(b -> b.operand(NoneLeafRel.class).anyInputs())
            .build()
            .toRule();

    /** Relational expression with zero inputs and convention PHYS. */
    public static class MockPhysLeafRel extends PhysLeafRel {
      MockPhysLeafRel(RelOptCluster cluster, String label) {
        super(cluster, PHYS_CALLING_CONVENTION, label);
      }

      @Override protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        return typeFactory.builder()
            .add("this", typeFactory.createJavaType(Integer.class))
            .build();
      }
    }

    protected MockPhysLeafRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);

      // It would throw exception.
      call.transformTo(
          new MockPhysLeafRel(leafRel.getCluster(), leafRel.label));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableMockPhysLeafRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default MockPhysLeafRule toRule() {
        return new MockPhysLeafRule(this);
      }
    }
  }

  /** Planner rule that matches a {@link NoneSingleRel} and succeeds. */
  public static class GoodSingleRule
      extends RelRule<GoodSingleRule.Config> {
    static final GoodSingleRule INSTANCE =
        ImmutableGoodSingleRuleConfig.builder()
            .withOperandSupplier(b ->
                b.operand(NoneSingleRel.class).anyInputs())
            .build()
            .toRule();

    protected GoodSingleRule(Config config) {
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
              single.getTraitSet().replace(PHYS_CALLING_CONVENTION));
      call.transformTo(
          new PhysSingleRel(single.getCluster(), physInput));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableGoodSingleRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default GoodSingleRule toRule() {
        return new GoodSingleRule(this);
      }
    }
  }

  /**
   * Planner rule that matches a parent with two children and asserts that they
   * are not the same.
   */
  public static class AssertOperandsDifferentRule
      extends RelRule<AssertOperandsDifferentRule.Config> {
    public static final AssertOperandsDifferentRule INSTANCE =
        ImmutableAssertOperandsDifferentRuleConfig.builder().build().withOperandSupplier(b0 ->
                b0.operand(PhysBiRel.class).inputs(
                    b1 -> b1.operand(PhysLeafRel.class).anyInputs(),
                    b2 -> b2.operand(PhysLeafRel.class).anyInputs()))
            .toRule();

    protected AssertOperandsDifferentRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      PhysLeafRel left = call.rel(1);
      PhysLeafRel right = call.rel(2);

      assert left != right : left + " should be different from " + right;
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableAssertOperandsDifferentRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default AssertOperandsDifferentRule toRule() {
        return new AssertOperandsDifferentRule(this);
      }
    }
  }
}
