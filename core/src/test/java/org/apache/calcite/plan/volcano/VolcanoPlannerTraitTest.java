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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for handling of traits by {@link VolcanoPlanner}.
 */
class VolcanoPlannerTraitTest {
  /**
   * Private calling convention representing a generic "physical" calling
   * convention.
   */
  private static final Convention PHYS_CALLING_CONVENTION =
      new Convention.Impl(
          "PHYS",
          RelNode.class);

  /**
   * Private trait definition for an alternate type of traits.
   */
  private static final AltTraitDef ALT_TRAIT_DEF = new AltTraitDef();

  /**
   * Private alternate trait.
   */
  private static final AltTrait ALT_EMPTY_TRAIT =
      new AltTrait(ALT_TRAIT_DEF, "ALT_EMPTY");

  /**
   * Private alternate trait.
   */
  private static final AltTrait ALT_TRAIT =
      new AltTrait(ALT_TRAIT_DEF, "ALT");

  /**
   * Private alternate trait.
   */
  private static final AltTrait ALT_TRAIT2 =
      new AltTrait(ALT_TRAIT_DEF, "ALT2");

  /**
   * Ordinal count for alternate traits (so they can implement equals() and
   * avoid being canonized into the same trait).
   */
  private static int altTraitOrdinal = 0;

  @Disabled
  @Test void testDoubleConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(ALT_TRAIT_DEF);

    planner.addRule(PhysToIteratorConverterRule.INSTANCE);
    planner.addRule(
        AltTraitConverterRule.create(ALT_TRAIT, ALT_TRAIT2,
            "AltToAlt2ConverterRule"));
    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(IterSingleRule.INSTANCE);

    RelOptCluster cluster = newCluster(planner);

    NoneLeafRel noneLeafRel =
        RelOptUtil.addTrait(
            new NoneLeafRel(cluster, "noneLeafRel"), ALT_TRAIT);

    NoneSingleRel noneRel =
        RelOptUtil.addTrait(
            new NoneSingleRel(cluster, noneLeafRel), ALT_TRAIT2);

    RelNode convertedRel =
        planner.changeTraits(noneRel,
            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replace(ALT_TRAIT2));

    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    assertThat(result, instanceOf(IterSingleRel.class));
    assertThat(result.getTraitSet().getTrait(ConventionTraitDef.INSTANCE),
        is(EnumerableConvention.INSTANCE));
    assertThat(result.getTraitSet().getTrait(ALT_TRAIT_DEF), is(ALT_TRAIT2));

    RelNode child = result.getInputs().get(0);
    assertTrue(
        (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

    child = child.getInputs().get(0);
    assertTrue(
        (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

    child = child.getInputs().get(0);
    assertThat(child, instanceOf(PhysLeafRel.class));
  }

  @Test void testRuleMatchAfterConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(ALT_TRAIT_DEF);

    planner.addRule(PhysToIteratorConverterRule.INSTANCE);
    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(IterSingleRule.INSTANCE);
    planner.addRule(IterSinglePhysMergeRule.INSTANCE);

    RelOptCluster cluster = newCluster(planner);

    NoneLeafRel noneLeafRel =
        RelOptUtil.addTrait(
            new NoneLeafRel(cluster, "noneLeafRel"), ALT_TRAIT);

    NoneSingleRel noneRel =
        RelOptUtil.addTrait(
            new NoneSingleRel(cluster, noneLeafRel), ALT_EMPTY_TRAIT);

    RelNode convertedRel =
        planner.changeTraits(noneRel,
            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replace(ALT_EMPTY_TRAIT));

    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    assertThat(result, instanceOf(IterMergedRel.class));
  }

  @Disabled
  @Test void testTraitPropagation() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(ALT_TRAIT_DEF);

    planner.addRule(PhysToIteratorConverterRule.INSTANCE);
    planner.addRule(
        AltTraitConverterRule.create(ALT_TRAIT, ALT_TRAIT2,
            "AltToAlt2ConverterRule"));
    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(IterSingleRule2.INSTANCE);

    RelOptCluster cluster = newCluster(planner);

    NoneLeafRel noneLeafRel =
        RelOptUtil.addTrait(
            new NoneLeafRel(cluster, "noneLeafRel"), ALT_TRAIT);

    NoneSingleRel noneRel =
        RelOptUtil.addTrait(
            new NoneSingleRel(cluster, noneLeafRel), ALT_TRAIT2);

    RelNode convertedRel =
        planner.changeTraits(noneRel,
            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replace(ALT_TRAIT2));

    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    assertThat(result, instanceOf(IterSingleRel.class));
    assertThat(result.getTraitSet().getTrait(ConventionTraitDef.INSTANCE),
        is(EnumerableConvention.INSTANCE));
    assertThat(result.getTraitSet().getTrait(ALT_TRAIT_DEF), is(ALT_TRAIT2));

    RelNode child = result.getInputs().get(0);
    assertThat(child, instanceOf(IterSingleRel.class));
    assertThat(child.getTraitSet().getTrait(ConventionTraitDef.INSTANCE),
        is(EnumerableConvention.INSTANCE));
    assertThat(child.getTraitSet().getTrait(ALT_TRAIT_DEF), is(ALT_TRAIT2));

    child = child.getInputs().get(0);
    assertTrue(
        (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

    child = child.getInputs().get(0);
    assertTrue(
        (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

    child = child.getInputs().get(0);
    assertTrue(child instanceof PhysLeafRel);
  }

  @Test void testPlanWithNoneConvention() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);
    NoneTinyLeafRel leaf = new NoneTinyLeafRel(cluster, "noneLeafRel");
    planner.setRoot(leaf);
    RelOptCost cost = planner.getCost(leaf, cluster.getMetadataQuery());

    assertTrue(cost.isInfinite());

    planner.setNoneConventionHasInfiniteCost(false);
    cost = planner.getCost(leaf, cluster.getMetadataQuery());
    assertFalse(cost.isInfinite());
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Implementation of {@link RelTrait} for testing. */
  private static class AltTrait implements RelTrait {
    private final AltTraitDef traitDef;
    private final int ordinal;
    private final String description;

    private AltTrait(AltTraitDef traitDef, String description) {
      this.traitDef = traitDef;
      this.description = description;
      this.ordinal = altTraitOrdinal++;
    }

    public void register(RelOptPlanner planner) {}

    public RelTraitDef getTraitDef() {
      return traitDef;
    }

    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof AltTrait)) {
        return false;
      }
      AltTrait that = (AltTrait) other;
      return this.ordinal == that.ordinal;
    }

    public int hashCode() {
      return ordinal;
    }

    public boolean satisfies(RelTrait trait) {
      return trait.equals(ALT_EMPTY_TRAIT) || equals(trait);
    }

    public String toString() {
      return description;
    }
  }

  /** Definition of {@link AltTrait}. */
  private static class AltTraitDef extends RelTraitDef<AltTrait> {
    private final Multimap<RelTrait, Pair<RelTrait, ConverterRule>> conversionMap =
        HashMultimap.create();

    public Class<AltTrait> getTraitClass() {
      return AltTrait.class;
    }

    public String getSimpleName() {
      return "alt_phys";
    }

    public AltTrait getDefault() {
      return ALT_TRAIT;
    }

    public @Nullable RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        AltTrait toTrait,
        boolean allowInfiniteCostConverters) {
      RelTrait fromTrait = rel.getTraitSet().getTrait(this);

      if (conversionMap.containsKey(fromTrait)) {
        final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        for (Pair<RelTrait, ConverterRule> traitAndRule
            : conversionMap.get(fromTrait)) {
          RelTrait trait = traitAndRule.left;
          ConverterRule rule = traitAndRule.right;

          if (trait == toTrait) {
            RelNode converted = rule.convert(rel);
            if ((converted != null)
                && (!planner.getCost(converted, mq).isInfinite()
                || allowInfiniteCostConverters)) {
              return converted;
            }
          }
        }
      }

      return null;
    }

    public boolean canConvert(
        RelOptPlanner planner,
        AltTrait fromTrait,
        AltTrait toTrait) {
      if (conversionMap.containsKey(fromTrait)) {
        for (Pair<RelTrait, ConverterRule> traitAndRule
            : conversionMap.get(fromTrait)) {
          if (traitAndRule.left == toTrait) {
            return true;
          }
        }
      }

      return false;
    }

    public void registerConverterRule(
        RelOptPlanner planner,
        ConverterRule converterRule) {
      if (!converterRule.isGuaranteed()) {
        return;
      }

      RelTrait fromTrait = converterRule.getInTrait();
      RelTrait toTrait = converterRule.getOutTrait();

      conversionMap.put(fromTrait, Pair.of(toTrait, converterRule));
    }
  }

  /** A relational expression with zero inputs. */
  private abstract static class TestLeafRel extends AbstractRelNode {
    private final String label;

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
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
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
      return super.explainTerms(pw)
          .item("label", label);
    }
  }

  /** A relational expression with zero inputs, of NONE convention. */
  private static class NoneLeafRel extends TestLeafRel {
    protected NoneLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          label);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoneLeafRel(getCluster(), getLabel());
    }
  }

  /** Relational expression with zero inputs, of PHYS convention. */
  private static class PhysLeafRel extends TestLeafRel {
    PhysLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(PHYS_CALLING_CONVENTION),
          label);
    }

    // implement RelNode
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    // TODO: SWZ Implement clone?
  }

  /** Relational expression with one input. */
  private abstract static class TestSingleRel extends SingleRel {
    protected TestSingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child) {
      super(cluster, traits, child);
    }

    // implement RelNode
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    // implement RelNode
    protected RelDataType deriveRowType() {
      return getInput().getRowType();
    }

    // TODO: SWZ Implement clone?
  }

  /** Relational expression with one input, of NONE convention. */
  private static class NoneSingleRel extends TestSingleRel {
    protected NoneSingleRel(
        RelOptCluster cluster,
        RelNode child) {
      this(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          child);
    }

    protected NoneSingleRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child) {
      super(cluster, traitSet, child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoneSingleRel(
          getCluster(),
          traitSet,
          sole(inputs));
    }
  }


  /** A mix-in interface to extend {@link RelNode}, for testing. */
  interface FooRel extends EnumerableRel {
  }

  /** Relational expression with one input, that implements the {@link FooRel}
   * mix-in interface. */
  private static class IterSingleRel extends TestSingleRel implements FooRel {
    IterSingleRel(RelOptCluster cluster, RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(EnumerableConvention.INSTANCE),
          child);
    }

    // implement RelNode
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(EnumerableConvention.INSTANCE);
      return new IterSingleRel(
          getCluster(),
          sole(inputs));
    }

    @Override public Result implement(EnumerableRelImplementor implementor,
        Prefer pref) {
      return null;
    }
  }

  /** Relational expression with zero inputs, of the PHYS convention. */
  public static class PhysLeafRule extends RelRule<PhysLeafRule.Config> {
    static final PhysLeafRule INSTANCE = ImmutablePhysLeafRuleConfig.builder().build()
        .withOperandSupplier(b ->
            b.operand(NoneLeafRel.class).anyInputs())
        .as(Config.class)
        .toRule();

    PhysLeafRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
          new PhysLeafRel(
              leafRel.getCluster(),
              leafRel.getLabel()));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysLeafRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysLeafRule toRule() {
        return new PhysLeafRule(this);
      }
    }
  }

  /** Relational expression with zero input, of NONE convention, and tiny cost. */
  private static class NoneTinyLeafRel extends TestLeafRel {
    protected NoneTinyLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          label);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoneTinyLeafRel(getCluster(), getLabel());
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }
  }

  /** Planner rule to convert a {@link NoneSingleRel} to ENUMERABLE
   * convention. */
  public static class IterSingleRule
      extends RelRule<IterSingleRule.Config> {
    static final IterSingleRule INSTANCE = ImmutableIterSingleRuleConfig.builder()
        .build()
        .withOperandSupplier(b ->
            b.operand(NoneSingleRel.class).anyInputs())
        .as(Config.class)
        .toRule();

    IterSingleRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return EnumerableConvention.INSTANCE;
    }

    @Override public RelTrait getOutTrait() {
      return getOutConvention();
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneSingleRel rel = call.rel(0);

      RelNode converted =
          convert(
              rel.getInput(0),
              rel.getTraitSet().replace(getOutTrait()));

      call.transformTo(
          new IterSingleRel(
              rel.getCluster(),
              converted));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableIterSingleRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default IterSingleRule toRule() {
        return new IterSingleRule(this);
      }
    }
  }

  /** Another planner rule to convert a {@link NoneSingleRel} to ENUMERABLE
   * convention. */
  public static class IterSingleRule2
      extends RelRule<IterSingleRule2.Config> {
    static final IterSingleRule2 INSTANCE = ImmutableIterSingleRule2Config.builder().build()
        .withOperandSupplier(b ->
            b.operand(NoneSingleRel.class).anyInputs())
        .as(Config.class)
        .toRule();

    IterSingleRule2(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return EnumerableConvention.INSTANCE;
    }

    @Override public RelTrait getOutTrait() {
      return getOutConvention();
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneSingleRel rel = call.rel(0);

      RelNode converted =
          convert(
              rel.getInput(0),
              rel.getTraitSet().replace(getOutTrait()));

      IterSingleRel child =
          new IterSingleRel(
              rel.getCluster(),
              converted);

      call.transformTo(
          new IterSingleRel(
              rel.getCluster(),
              child));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableIterSingleRule2Config")
    public interface Config extends RelRule.Config {
      @Override default IterSingleRule2 toRule() {
        return new IterSingleRule2(this);
      }
    }
  }

  /** Planner rule that converts between {@link AltTrait}s. */
  private static class AltTraitConverterRule extends ConverterRule {
    static AltTraitConverterRule create(AltTrait fromTrait, AltTrait toTrait,
        String description) {
      return Config.INSTANCE
          .withConversion(RelNode.class, fromTrait, toTrait, description)
          .withRuleFactory(AltTraitConverterRule::new)
          .toRule(AltTraitConverterRule.class);
    }

    private final RelTrait toTrait;

    AltTraitConverterRule(Config config) {
      super(config);
      this.toTrait = config.outTrait();
    }

    @Override public RelNode convert(RelNode rel) {
      return new AltTraitConverter(
          rel.getCluster(),
          rel,
          toTrait);
    }

    public boolean isGuaranteed() {
      return true;
    }
  }

  /** Relational expression that converts between {@link AltTrait} values. */
  private static class AltTraitConverter extends ConverterImpl {
    private final RelTrait toTrait;

    private AltTraitConverter(
        RelOptCluster cluster,
        RelNode child,
        RelTrait toTrait) {
      super(
          cluster,
          toTrait.getTraitDef(),
          child.getTraitSet().replace(toTrait),
          child);

      this.toTrait = toTrait;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new AltTraitConverter(
          getCluster(),
          sole(inputs),
          toTrait);
    }
  }

  /** Planner rule that converts from PHYS to ENUMERABLE convention. */
  private static class PhysToIteratorConverterRule extends ConverterRule {
    static final PhysToIteratorConverterRule INSTANCE = Config.INSTANCE
        .withConversion(RelNode.class, PHYS_CALLING_CONVENTION,
            EnumerableConvention.INSTANCE, "PhysToIteratorRule")
        .withRuleFactory(PhysToIteratorConverterRule::new)
        .toRule(PhysToIteratorConverterRule.class);

    PhysToIteratorConverterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      return new PhysToIteratorConverter(
          rel.getCluster(),
          rel);
    }
  }

  /** Planner rule that converts PHYS to ENUMERABLE convention. */
  private static class PhysToIteratorConverter extends ConverterImpl {
    PhysToIteratorConverter(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          ConventionTraitDef.INSTANCE,
          child.getTraitSet().replace(EnumerableConvention.INSTANCE),
          child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new PhysToIteratorConverter(
          getCluster(),
          sole(inputs));
    }
  }

  /** Planner rule that converts an {@link IterSingleRel} on a
   * {@link PhysToIteratorConverter} into a {@link IterMergedRel}. */
  public static class IterSinglePhysMergeRule
      extends RelRule<IterSinglePhysMergeRule.Config> {
    static final IterSinglePhysMergeRule INSTANCE =
        ImmutableIterSinglePhysMergeRuleConfig.builder().build()
            .withOperandSupplier(b0 ->
                b0.operand(IterSingleRel.class).oneInput(b1 ->
                    b1.operand(PhysToIteratorConverter.class).anyInputs()))
            .as(Config.class)
            .toRule();

    protected IterSinglePhysMergeRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      IterSingleRel singleRel = call.rel(0);
      call.transformTo(
          new IterMergedRel(singleRel.getCluster(),  null));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableIterSinglePhysMergeRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default IterSinglePhysMergeRule toRule() {
        return new IterSinglePhysMergeRule(this);
      }
    }
  }

  /** Relational expression with no inputs, that implements the {@link FooRel}
   * mix-in interface. */
  private static class IterMergedRel extends TestLeafRel implements FooRel {
    IterMergedRel(RelOptCluster cluster, String label) {
      super(
          cluster,
          cluster.traitSetOf(EnumerableConvention.INSTANCE),
          label);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeZeroCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(EnumerableConvention.INSTANCE);
      assert inputs.isEmpty();
      return new IterMergedRel(getCluster(), this.getLabel());
    }

    @Override public Result implement(EnumerableRelImplementor implementor,
        Prefer pref) {
      return null;
    }
  }
}
