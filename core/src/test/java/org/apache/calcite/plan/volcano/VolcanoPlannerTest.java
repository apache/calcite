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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.GoodSingleRule;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysLeafRule;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link VolcanoPlanner the optimizer}.
 */
public class VolcanoPlannerTest {

  public VolcanoPlannerTest() {
  }

  //~ Methods ----------------------------------------------------------------
  /**
   * Tests transformation of a leaf from NONE to PHYS.
   */
  @Test public void testTransformLeaf() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    RelNode convertedRel =
        planner.changeTraits(
            leafRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysLeafRel);
  }

  /**
   * Tests transformation of a single+leaf from NONE to PHYS.
   */
  @Test public void testTransformSingleGood() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());
    planner.addRule(new GoodSingleRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysSingleRel);
  }

  /**
   * Tests a rule that is fired once per subset (whereas most rules are fired
   * once per rel in a set or rel in a subset)
   */
  @Test public void testSubsetRule() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());
    planner.addRule(new GoodSingleRule());
    final List<String> buf = new ArrayList<>();
    planner.addRule(new SubsetRule(buf));

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysSingleRel);
    assertThat(sort(buf),
        equalTo(
            sort(
                "NoneSingleRel:Subset#0.NONE",
                "PhysSingleRel:Subset#0.NONE",
                "PhysSingleRel:Subset#0.PHYS")));
  }

  private static <E extends Comparable> List<E> sort(List<E> list) {
    final List<E> list2 = new ArrayList<>(list);
    Collections.sort(list2);
    return list2;
  }

  private static <E extends Comparable> List<E> sort(E... es) {
    return sort(Arrays.asList(es));
  }

  /**
   * Tests transformation of a single+leaf from NONE to PHYS. In the past,
   * this one didn't work due to the definition of ReformedSingleRule.
   */
  @Ignore // broken, because ReformedSingleRule matches child traits strictly
  @Test public void testTransformSingleReformed() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());
    planner.addRule(new ReformedSingleRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysSingleRel);
  }

  private void removeTrivialProject(boolean useRule) {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.ambitious = true;

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    if (useRule) {
      planner.addRule(ProjectRemoveRule.INSTANCE);
    }

    planner.addRule(new PhysLeafRule());
    planner.addRule(new GoodSingleRule());
    planner.addRule(new PhysProjectRule());

    planner.addRule(
        new ConverterRule(
            RelNode.class,
            PHYS_CALLING_CONVENTION,
            EnumerableConvention.INSTANCE,
            "PhysToIteratorRule") {
          public RelNode convert(RelNode rel) {
            return new PhysToIteratorConverter(
                rel.getCluster(),
                rel);
          }
        });

    RelOptCluster cluster = newCluster(planner);
    PhysLeafRel leafRel =
        new PhysLeafRel(
            cluster,
            "a");
    final RelBuilder relBuilder =
        RelFactories.LOGICAL_BUILDER.create(leafRel.getCluster(), null);
    RelNode projectRel =
        relBuilder.push(leafRel)
            .project(relBuilder.alias(relBuilder.field(0), "this"))
            .build();
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            projectRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(EnumerableConvention.INSTANCE));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysToIteratorConverter);
  }

  // NOTE:  this used to fail but now works
  @Test public void testWithRemoveTrivialProject() {
    removeTrivialProject(true);
  }

  // NOTE:  this always worked; it's here as contrast to
  // testWithRemoveTrivialProject()
  @Test public void testWithoutRemoveTrivialProject() {
    removeTrivialProject(false);
  }

  /**
   * Previously, this didn't work because ReformedRemoveSingleRule uses a
   * pattern which spans calling conventions.
   */
  @Ignore // broken, because ReformedSingleRule matches child traits strictly
  @Test public void testRemoveSingleReformed() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.ambitious = true;
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());
    planner.addRule(new ReformedRemoveSingleRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysLeafRel);
    PhysLeafRel resultLeaf = (PhysLeafRel) result;
    assertEquals(
        "c",
        resultLeaf.label);
  }

  /**
   * This always worked (in contrast to testRemoveSingleReformed) because it
   * uses a completely-physical pattern (requiring GoodSingleRule to fire
   * first).
   */
  @Test public void testRemoveSingleGood() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.ambitious = true;
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());
    planner.addRule(new GoodSingleRule());
    planner.addRule(new GoodRemoveSingleRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysLeafRel);
    PhysLeafRel resultLeaf = (PhysLeafRel) result;
    assertEquals(
        "c",
        resultLeaf.label);
  }

  /**
   * Tests whether planner correctly notifies listeners of events.
   */
  @Ignore
  @Test public void testListener() {
    TestListener listener = new TestListener();

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addListener(listener);

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new PhysLeafRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    RelNode convertedRel =
        planner.changeTraits(
            leafRel,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof PhysLeafRel);

    List<RelOptListener.RelEvent> eventList = listener.getEventList();

    // add node
    checkEvent(
        eventList,
        0,
        RelOptListener.RelEquivalenceEvent.class,
        leafRel,
        null);

    // internal subset
    checkEvent(
        eventList,
        1,
        RelOptListener.RelEquivalenceEvent.class,
        null,
        null);

    // before rule
    checkEvent(
        eventList,
        2,
        RelOptListener.RuleAttemptedEvent.class,
        leafRel,
        PhysLeafRule.class);

    // before rule
    checkEvent(
        eventList,
        3,
        RelOptListener.RuleProductionEvent.class,
        result,
        PhysLeafRule.class);

    // result of rule
    checkEvent(
        eventList,
        4,
        RelOptListener.RelEquivalenceEvent.class,
        result,
        null);

    // after rule
    checkEvent(
        eventList,
        5,
        RelOptListener.RuleProductionEvent.class,
        result,
        PhysLeafRule.class);

    // after rule
    checkEvent(
        eventList,
        6,
        RelOptListener.RuleAttemptedEvent.class,
        leafRel,
        PhysLeafRule.class);

    // choose plan
    checkEvent(
        eventList,
        7,
        RelOptListener.RelChosenEvent.class,
        result,
        null);

    // finish choosing plan
    checkEvent(
        eventList,
        8,
        RelOptListener.RelChosenEvent.class,
        null,
        null);
  }

  private void checkEvent(
      List<RelOptListener.RelEvent> eventList,
      int iEvent,
      Class expectedEventClass,
      RelNode expectedRel,
      Class<? extends RelOptRule> expectedRuleClass) {
    assertTrue(iEvent < eventList.size());
    RelOptListener.RelEvent event = eventList.get(iEvent);
    assertSame(
        expectedEventClass,
        event.getClass());
    if (expectedRel != null) {
      assertSame(
          expectedRel,
          event.getRel());
    }
    if (expectedRuleClass != null) {
      RelOptListener.RuleEvent ruleEvent =
          (RelOptListener.RuleEvent) event;
      assertSame(
          expectedRuleClass,
          ruleEvent.getRuleCall().getRule().getClass());
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Converter from PHYS to ENUMERABLE convention. */
  class PhysToIteratorConverter extends ConverterImpl {
    PhysToIteratorConverter(
        RelOptCluster cluster,
        RelNode child) {
      super(
          cluster,
          ConventionTraitDef.INSTANCE,
          cluster.traitSetOf(EnumerableConvention.INSTANCE),
          child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(EnumerableConvention.INSTANCE);
      return new PhysToIteratorConverter(
          getCluster(),
          sole(inputs));
    }
  }

  /** Rule that matches a {@link RelSubset}. */
  private static class SubsetRule extends RelOptRule {
    private final List<String> buf;

    SubsetRule(List<String> buf) {
      super(operand(TestSingleRel.class, operand(RelSubset.class, any())));
      this.buf = buf;
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      // Do not transform to anything; just log the calls.
      TestSingleRel singleRel = call.rel(0);
      RelSubset childRel = call.rel(1);
      assertThat(call.rels.length, equalTo(2));
      buf.add(singleRel.getClass().getSimpleName() + ":"
          + childRel.getDigest());
    }
  }

  // NOTE: Previously, ReformedSingleRule didn't work because it explicitly
  // specifies PhysLeafRel rather than RelNode for the single input.  Since
  // the PhysLeafRel is in a different subset from the original NoneLeafRel,
  // ReformedSingleRule never saw it.  (GoodSingleRule saw the NoneLeafRel
  // instead and fires off of that; later the NoneLeafRel gets converted into
  // a PhysLeafRel).  Now Volcano supports rules which match across subsets.

  /** Planner rule that matches a {@link NoneSingleRel} whose input is
   * a {@link PhysLeafRel} in a different subset. */
  private static class ReformedSingleRule extends RelOptRule {
    ReformedSingleRule() {
      super(
          operand(
              NoneSingleRel.class,
              operand(PhysLeafRel.class, any())));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      RelNode childRel = call.rel(1);
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

  /** Planner rule that converts a {@link LogicalProject} to PHYS convention. */
  private static class PhysProjectRule extends RelOptRule {
    PhysProjectRule() {
      super(operand(LogicalProject.class, any()));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      RelNode childRel = project.getInput();
      call.transformTo(
          new PhysLeafRel(
              childRel.getCluster(),
              "b"));
    }
  }

  /** Planner rule that successfully removes a {@link PhysSingleRel}. */
  private static class GoodRemoveSingleRule extends RelOptRule {
    GoodRemoveSingleRule() {
      super(
          operand(
              PhysSingleRel.class,
              operand(PhysLeafRel.class, any())));
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      PhysSingleRel singleRel = call.rel(0);
      PhysLeafRel leafRel = call.rel(1);
      call.transformTo(
          new PhysLeafRel(
              singleRel.getCluster(),
              "c"));
    }
  }

  /** Planner rule that removes a {@link NoneSingleRel}. */
  private static class ReformedRemoveSingleRule extends RelOptRule {
    ReformedRemoveSingleRule() {
      super(
          operand(
              NoneSingleRel.class,
              operand(PhysLeafRel.class, any())));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      PhysLeafRel leafRel = call.rel(1);
      call.transformTo(
          new PhysLeafRel(
              singleRel.getCluster(),
              "c"));
    }
  }

  /** Implementation of {@link RelOptListener}. */
  private static class TestListener implements RelOptListener {
    private List<RelEvent> eventList;

    TestListener() {
      eventList = new ArrayList<>();
    }

    List<RelEvent> getEventList() {
      return eventList;
    }

    private void recordEvent(RelEvent event) {
      eventList.add(event);
    }

    public void relChosen(RelChosenEvent event) {
      recordEvent(event);
    }

    public void relDiscarded(RelDiscardedEvent event) {
      // Volcano is quite a pack rat--it never discards anything!
      throw new AssertionError(event);
    }

    public void relEquivalenceFound(RelEquivalenceEvent event) {
      if (!event.isPhysical()) {
        return;
      }
      recordEvent(event);
    }

    public void ruleAttempted(RuleAttemptedEvent event) {
      recordEvent(event);
    }

    public void ruleProductionSucceeded(RuleProductionEvent event) {
      recordEvent(event);
    }
  }
}

// End VolcanoPlannerTest.java
