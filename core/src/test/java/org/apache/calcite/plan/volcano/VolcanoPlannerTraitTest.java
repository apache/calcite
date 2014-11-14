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
package org.eigenbase.relopt.volcano;

import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for handling of traits by {@link VolcanoPlanner}.
 */
public class VolcanoPlannerTraitTest {
  //~ Static fields/initializers ---------------------------------------------

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

  //~ Constructors -----------------------------------------------------------

  public VolcanoPlannerTraitTest() {
  }

  //~ Methods ----------------------------------------------------------------

  @Ignore
  @Test public void testDoubleConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(ALT_TRAIT_DEF);

    planner.addRule(new PhysToIteratorConverterRule());
    planner.addRule(
        new AltTraitConverterRule(
            ALT_TRAIT,
            ALT_TRAIT2,
            "AltToAlt2ConverterRule"));
    planner.addRule(new PhysLeafRule());
    planner.addRule(new IterSingleRule());

    RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

    NoneLeafRel noneLeafRel =
        RelOptUtil.addTrait(
            new NoneLeafRel(cluster, "noneLeafRel"), ALT_TRAIT);

    NoneSingleRel noneRel =
        RelOptUtil.addTrait(
            new NoneSingleRel(cluster, noneLeafRel),
            ALT_TRAIT2);

    RelNode convertedRel =
        planner.changeTraits(
            noneRel,
            cluster.traitSetOf(EnumerableConvention.INSTANCE, ALT_TRAIT2));

    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    assertTrue(result instanceof IterSingleRel);
    assertEquals(
        EnumerableConvention.INSTANCE,
        result.getTraitSet().getTrait(ConventionTraitDef.INSTANCE));
    assertEquals(
        ALT_TRAIT2,
        result.getTraitSet().getTrait(ALT_TRAIT_DEF));

    RelNode child = result.getInputs().get(0);
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

  @Ignore
  @Test public void testTraitPropagation() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(ALT_TRAIT_DEF);

    planner.addRule(new PhysToIteratorConverterRule());
    planner.addRule(
        new AltTraitConverterRule(
            ALT_TRAIT,
            ALT_TRAIT2,
            "AltToAlt2ConverterRule"));
    planner.addRule(new PhysLeafRule());
    planner.addRule(new IterSingleRule2());

    RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

    NoneLeafRel noneLeafRel =
        RelOptUtil.addTrait(
            new NoneLeafRel(cluster, "noneLeafRel"),
            ALT_TRAIT);

    NoneSingleRel noneRel =
        RelOptUtil.addTrait(
            new NoneSingleRel(cluster, noneLeafRel),
            ALT_TRAIT2);

    RelNode convertedRel =
        planner.changeTraits(
            noneRel,
            cluster.traitSetOf(
                EnumerableConvention.INSTANCE, ALT_TRAIT2));

    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    assertTrue(result instanceof IterSingleRel);
    assertEquals(
        EnumerableConvention.INSTANCE,
        result.getTraitSet().getTrait(ConventionTraitDef.INSTANCE));
    assertEquals(
        ALT_TRAIT2,
        result.getTraitSet().getTrait(ALT_TRAIT_DEF));

    RelNode child = result.getInputs().get(0);
    assertTrue(child instanceof IterSingleRel);
    assertEquals(
        EnumerableConvention.INSTANCE,
        child.getTraitSet().getTrait(ConventionTraitDef.INSTANCE));
    assertEquals(
        ALT_TRAIT2,
        child.getTraitSet().getTrait(ALT_TRAIT_DEF));

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

  //~ Inner Classes ----------------------------------------------------------

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

    public boolean subsumes(RelTrait trait) {
      return equals(trait);
    }

    public String toString() {
      return description;
    }
  }

  private static class AltTraitDef extends RelTraitDef<AltTrait> {
    private Multimap<RelTrait, Pair<RelTrait, ConverterRule>> conversionMap =
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

    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        AltTrait toTrait,
        boolean allowInfiniteCostConverters) {
      RelTrait fromTrait = rel.getTraitSet().getTrait(this);

      if (conversionMap.containsKey(fromTrait)) {
        for (Pair<RelTrait, ConverterRule> traitAndRule
            : conversionMap.get(fromTrait)) {
          RelTrait trait = traitAndRule.left;
          ConverterRule rule = traitAndRule.right;

          if (trait == toTrait) {
            RelNode converted = rule.convert(rel);
            if ((converted != null)
                && (!planner.getCost(converted).isInfinite()
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

      conversionMap.put(fromTrait,
          new Pair<RelTrait, ConverterRule>(toTrait, converterRule));
    }
  }

  private abstract static class TestLeafRel extends AbstractRelNode {
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
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
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

  private static class NoneLeafRel extends TestLeafRel {
    protected NoneLeafRel(
        RelOptCluster cluster,
        String label) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          label);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoneLeafRel(getCluster(), getLabel());
    }
  }

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
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return planner.getCostFactory().makeTinyCost();
    }

    // TODO: SWZ Implement clone?
  }

  private abstract static class TestSingleRel extends SingleRel {
    protected TestSingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child) {
      super(cluster, traits, child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    // implement RelNode
    protected RelDataType deriveRowType() {
      return getChild().getRowType();
    }

    // TODO: SWZ Implement clone?
  }

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


  interface FooRel {
    String implement(FooRelImplementor implementor);
  }

  interface FooRelImplementor {
  }

  private static class IterSingleRel extends TestSingleRel implements FooRel {
    public IterSingleRel(RelOptCluster cluster, RelNode child) {
      super(
          cluster,
          cluster.traitSetOf(EnumerableConvention.INSTANCE),
          child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(EnumerableConvention.INSTANCE);
      return new IterSingleRel(
          getCluster(),
          sole(inputs));
    }

    public String implement(FooRelImplementor implementor) {
      return null;
    }
  }

  private static class PhysLeafRule extends RelOptRule {
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

  private static class IterSingleRule extends RelOptRule {
    IterSingleRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return EnumerableConvention.INSTANCE;
    }

    public RelTrait getOutTrait() {
      return getOutConvention();
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
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
  }

  private static class IterSingleRule2 extends RelOptRule {
    IterSingleRule2() {
      super(operand(NoneSingleRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return EnumerableConvention.INSTANCE;
    }

    public RelTrait getOutTrait() {
      return getOutConvention();
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
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
  }

  private static class AltTraitConverterRule extends ConverterRule {
    private final RelTrait toTrait;

    private AltTraitConverterRule(
        AltTrait fromTrait,
        AltTrait toTrait,
        String description) {
      super(
          RelNode.class,
          fromTrait,
          toTrait,
          description);

      this.toTrait = toTrait;
    }

    public RelNode convert(RelNode rel) {
      return new AltTraitConverter(
          rel.getCluster(),
          rel,
          toTrait);
    }

    public boolean isGuaranteed() {
      return true;
    }
  }

  private static class AltTraitConverter extends ConverterRelImpl {
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

  private static class PhysToIteratorConverterRule extends ConverterRule {
    public PhysToIteratorConverterRule() {
      super(
          RelNode.class,
          PHYS_CALLING_CONVENTION,
          EnumerableConvention.INSTANCE,
          "PhysToIteratorRule");
    }

    public RelNode convert(RelNode rel) {
      return new PhysToIteratorConverter(
          rel.getCluster(),
          rel);
    }
  }

  private static class PhysToIteratorConverter extends ConverterRelImpl {
    public PhysToIteratorConverter(
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
}

// End VolcanoPlannerTraitTest.java
