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
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.immutables.value.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.AssertOperandsDifferentRule;
import static org.apache.calcite.plan.volcano.PlannerTests.GoodSingleRule;
import static org.apache.calcite.plan.volcano.PlannerTests.MockPhysLeafRule;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION_2;
import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION_3;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysBiRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysLeafRule;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;
import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link VolcanoPlanner the optimizer}.
 */
class VolcanoPlannerTest {

  //~ Methods ----------------------------------------------------------------
  /**
   * Tests transformation of a leaf from NONE to PHYS.
   */
  @Test void testTransformLeaf() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysLeafRel.class));
  }

  /**
   * Tests transformation of a single+leaf from NONE to PHYS.
   */
  @Test void testTransformSingleGood() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysSingleRel.class));
  }

  @Test void testMemoizeInputRelNodes() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);

    // The rule that triggers the assert rule
    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);

    // Leaf RelNode
    NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    RelNode leafPhy = planner
        .changeTraits(leafRel, cluster.traitSetOf(PHYS_CALLING_CONVENTION));

    // RelNode with leaf RelNode as single input
    NoneSingleRel singleRel = new NoneSingleRel(cluster, leafPhy);
    RelNode singlePhy = planner
        .changeTraits(singleRel, cluster.traitSetOf(PHYS_CALLING_CONVENTION));

    // Binary RelNode with identical input on either side
    PhysBiRel parent =
        new PhysBiRel(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION),
            singlePhy, singlePhy);
    planner.setRoot(parent);

    RelNode result = planner.chooseDelegate().findBestExp();

    // Expect inputs to remain identical
    assertThat(result.getInput(1), is(result.getInput(0)));
  }

  @Test void testPlanToDot() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

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

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    RelDotWriter planWriter = new RelDotWriter(pw, SqlExplainLevel.NO_ATTRIBUTES, false);
    planner.getRoot().explain(planWriter);
    String planStr = sw.toString();

    assertThat(
        planStr, isLinux("digraph {\n"
            + "\"NoneLeafRel\\n\" -> \"NoneSingleRel\\n\" [label=\"0\"]\n"
            + "}\n"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3118">[CALCITE-3118]
   * VolcanoRuleCall should look at RelSubset rather than RelSet
   * when checking child ordinal of a parent operand</a>. */
  @Test void testMatchedOperandsDifferent() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);

    // The rule that triggers the assert rule
    planner.addRule(PhysLeafRule.INSTANCE);

    // The rule asserting that the matched operands are different
    planner.addRule(AssertOperandsDifferentRule.INSTANCE);

    // Construct two children in the same set and a parent RelNode
    NoneLeafRel leftRel = new NoneLeafRel(cluster, "a");
    RelNode leftPhy = planner
        .changeTraits(leftRel, cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    PhysLeafRel rightPhy =
        new PhysLeafRel(cluster, PHYS_CALLING_CONVENTION_2, "b");

    PhysBiRel parent =
        new PhysBiRel(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION),
            leftPhy, rightPhy);
    planner.setRoot(parent);

    // Make sure both RelNodes are in the same set, but different subset
    planner.ensureRegistered(leftPhy, rightPhy);

    planner.chooseDelegate().findBestExp();
  }

  /**
   * A pattern that matches a three input union with third child matching for
   * a PhysLeafRel node.
   */
  public static class ThreeInputsUnionRule
      extends RelRule<ThreeInputsUnionRule.Config> {
    static final ThreeInputsUnionRule INSTANCE = ImmutableThreeInputsUnionRuleConfig.builder()
        .build()
        .withOperandSupplier(b0 ->
            b0.operand(EnumerableUnion.class).inputs(
                b1 -> b1.operand(PhysBiRel.class).anyInputs(),
                b2 -> b2.operand(PhysBiRel.class).anyInputs(),
                b3 -> b3.operand(PhysLeafRel.class).anyInputs()))
        .as(Config.class)
        .toRule();

    ThreeInputsUnionRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableThreeInputsUnionRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default ThreeInputsUnionRule toRule() {
        return new ThreeInputsUnionRule(this);
      }
    }
  }

  @Test void testMultiInputsParentOpMatching() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);

    // The trigger rule that generates PhysLeafRel from NoneLeafRel
    planner.addRule(PhysLeafRule.INSTANCE);

    // The rule with third child op matching PhysLeafRel, which should not be
    // matched at all
    planner.addRule(ThreeInputsUnionRule.INSTANCE);

    // Construct a union with only two children
    NoneLeafRel leftRel = new NoneLeafRel(cluster, "b");
    RelNode leftPhy = planner
        .changeTraits(leftRel, cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    PhysLeafRel rightPhy =
        new PhysLeafRel(cluster, PHYS_CALLING_CONVENTION, "b");

    planner.setRoot(
        new EnumerableUnion(cluster,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION),
            Arrays.asList(leftPhy, rightPhy), false));

    planner.chooseDelegate().findBestExp();
  }

  /**
   * Tests a rule that is fired once per subset. (Whereas most rules are fired
   * once per rel in a set or rel in a subset.)
   */
  @Test void testSubsetRule() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);
    List<String> buf = new ArrayList<>();
    SubsetRule.Config config = SubsetRule.config(buf);
    planner.addRule(config.toRule());
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
    planner.changeTraits(leafRel,
        cluster.traitSetOf(PHYS_CALLING_CONVENTION)
        .plus(RelCollations.of(0)));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();

    buf = config.buf();
    assertThat(result, instanceOf(PhysSingleRel.class));
    assertThat(sort(buf),
        equalTo(
            sort(
                "NoneSingleRel:RelSubset#0.NONE.[]",
                "PhysSingleRel:RelSubset#0.PHYS.[0]",
                "PhysSingleRel:RelSubset#0.PHYS.[]")));
  }

  @Test void testTypeMismatch() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRule(MockPhysLeafRule.INSTANCE);

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

    RuntimeException ex =
        assertThrows(RuntimeException.class, () ->
            planner.chooseDelegate().findBestExp(),
            "Should throw exception fail since the type mismatches after "
                + "applying rule.");

    Throwable exception = ExceptionUtils.getRootCause(ex);
    assertThat(exception, instanceOf(IllegalArgumentException.class));
    assertThat(
        exception.getMessage(), isLinux("Type mismatch:\n"
            + "rel rowtype: RecordType(JavaType(class java.lang.Integer) this) NOT NULL\n"
            + "equiv rowtype: RecordType(JavaType(void) NOT NULL this) NOT NULL\n"
            + "Difference:\n"
            + "this: JavaType(class java.lang.Integer) -> JavaType(void) NOT NULL\n"));
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
   * Tests that VolcanoPlanner should fire rule match from subsets after a
   * RelSet merge. The rules matching for a RelSubset should be able to fire
   * on the subsets that are merged into the RelSets.
   */
  @Test void testSetMergeMatchSubsetRule() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);
    planner.addRule(PhysSingleInputSetMergeRule.INSTANCE);
    List<String> buf = new ArrayList<>();
    PhysSingleSubsetRule.Config config = PhysSingleSubsetRule.config(buf);
    planner.addRule(config.toRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    RelNode convertedRel = planner
        .changeTraits(singleRel, cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    buf = config.buf();
    assertThat(result, instanceOf(PhysSingleRel.class));
    assertThat(sort(buf),
        equalTo(
            sort("PhysSingleRel:RelSubset#0.PHYS.[]",
            "PhysSingleRel:RelSubset#0.PHYS_3.[]")));
  }

  /**
   * Tests transformation of a single+leaf from NONE to PHYS. In the past,
   * this one didn't work due to the definition of ReformedSingleRule.
   */
  @Disabled // broken, because ReformedSingleRule matches child traits strictly
  @Test void testTransformSingleReformed() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(ReformedSingleRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysSingleRel.class));
  }

  private void removeTrivialProject(boolean useRule) {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    if (useRule) {
      planner.addRule(CoreRules.PROJECT_REMOVE);
    }

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);
    planner.addRule(PhysProjectRule.INSTANCE);

    planner.addRule(PhysToIteratorRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysToIteratorConverter.class));
  }

  // NOTE:  this used to fail but now works
  @Test void testWithRemoveTrivialProject() {
    removeTrivialProject(true);
  }

  // NOTE:  this always worked; it's here as contrast to
  // testWithRemoveTrivialProject()
  @Test void testWithoutRemoveTrivialProject() {
    removeTrivialProject(false);
  }

  /**
   * Previously, this didn't work because ReformedRemoveSingleRule uses a
   * pattern which spans calling conventions.
   */
  @Disabled // broken, because ReformedSingleRule matches child traits strictly
  @Test void testRemoveSingleReformed() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(ReformedRemoveSingleRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysLeafRel.class));
    PhysLeafRel resultLeaf = (PhysLeafRel) result;
    assertThat(resultLeaf.label, is("c"));
  }

  /**
   * This always worked (in contrast to testRemoveSingleReformed) because it
   * uses a completely-physical pattern (requiring GoodSingleRule to fire
   * first).
   */
  @Test void testRemoveSingleGood() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);
    planner.addRule(GoodSingleRule.INSTANCE);
    planner.addRule(GoodRemoveSingleRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysLeafRel.class));
    PhysLeafRel resultLeaf = (PhysLeafRel) result;
    assertThat(resultLeaf.label, is("c"));
  }

  @Test void testMergeJoin() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    // Below two lines are important for the planner to use collation trait and generate merge join
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.registerAbstractRelationalRules();

    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);

    RelOptCluster cluster = newCluster(planner);

    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan = relBuilder
        .values(new String[]{"id", "name"}, "2", "a", "1", "b")
        .values(new String[]{"id", "name"}, "1", "x", "2", "y")
        .join(JoinRelType.INNER, "id")
        .build();

    RelTraitSet desiredTraits =
        cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode newRoot = planner.changeTraits(logicalPlan, desiredTraits);
    planner.setRoot(newRoot);

    RelNode bestExp = planner.findBestExp();

    final String plan = ""
        + "EnumerableMergeJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
        + "    EnumerableValues(tuples=[[{ '2', 'a' }, { '1', 'b' }]])\n"
        + "  EnumerableValues(tuples=[[{ '1', 'x' }, { '2', 'y' }]])\n";
    assertThat("Merge join + sort is expected", RelOptUtil.toString(bestExp),
        isLinux(plan));
  }

  @Test void testPruneNode() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    planner.setRoot(leafRel);

    // prune the node
    planner.prune(leafRel);

    // verify that the rule match cannot be popped,
    // as the related node has been pruned
    RuleQueue ruleQueue = planner.ruleDriver.getRuleQueue();
    while (true) {
      VolcanoRuleMatch ruleMatch;
      if (ruleQueue instanceof IterativeRuleQueue) {
        ruleMatch = ((IterativeRuleQueue) ruleQueue).popMatch();
      } else {
        ruleMatch = ((TopDownRuleQueue) ruleQueue).popMatch(Pair.of(leafRel, null));
      }
      if (ruleMatch == null) {
        break;
      }
      assertNotSame(leafRel, ruleMatch.rels[0]);
    }
  }

  /**
   * Tests whether planner correctly notifies listeners of events.
   */
  @Disabled
  @Test void testListener() {
    TestListener listener = new TestListener();

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addListener(listener);

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(PhysLeafRule.INSTANCE);

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
    assertThat(result, instanceOf(PhysLeafRel.class));

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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4514">[CALCITE-4514]
   * Fine tune the merge order of two RelSets</a>. When the merging RelSets,
   * if they are both parents of each other (that is, there is a 1-cycle), we
   * should merge the less popular/smaller/younger set into the more
   * popular/bigger/older one. */
  @Test void testSetMergeWithCycle() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);

    NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    NoneSingleRel singleRelA = new NoneSingleRel(cluster, leafRel);
    NoneSingleRel singleRelB = new NoneSingleRel(cluster, singleRelA);

    planner.setRoot(singleRelA);
    RelSet setA = planner.ensureRegistered(singleRelA, null).getSet();
    RelSet setB = planner.ensureRegistered(leafRel, null).getSet();

    // Create the relSet dependency cycle, so that both sets are parent of each
    // other.
    planner.ensureRegistered(singleRelB, leafRel);

    // trigger the set merge
    planner.ensureRegistered(singleRelB, singleRelA);

    // setA and setB have the same popularity (parentRels.size()).
    // Since setB is larger than setA, setA should be merged into setB.
    assertThat(setA.equivalentSet, sameInstance(setB));
  }

  /**
   * Test whether {@link RelSubset#getParents()} can work correctly,
   * after adding break to the inner loop.
   */
  @Test void testGetParents() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode joinRel = relBuilder
        .values(new String[]{"id", "name"}, "2", "a", "1", "b")
        .values(new String[]{"id", "name"}, "1", "x", "2", "y")
        .join(JoinRelType.INNER, "id")
        .build();
    RelSubset joinSubset = planner.register(joinRel, null);
    RelSubset leftRelSubset = planner.getSubset(joinRel.getInput(0));
    assertNotNull(leftRelSubset);
    RelNode leftParentRel = leftRelSubset.getParents().iterator().next();
    assertThat(joinSubset.getRelList().get(0), is(leftParentRel));
  }

  /**
   * Test whether {@link RelSubset#getParentSubsets(VolcanoPlanner)} can work correctly,
   * after adding break to the inner loop.
   */
  @Test void testGetParentSubsets() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = newCluster(planner);
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode joinRel = relBuilder
        .values(new String[]{"id", "name"}, "2", "a", "1", "b")
        .values(new String[]{"id", "name"}, "1", "x", "2", "y")
        .join(JoinRelType.INNER, "id")
        .build();
    RelSubset joinSubset = planner.register(joinRel, null);
    RelSubset leftRelSubset = planner.getSubset(joinRel.getInput(0));
    assertNotNull(leftRelSubset);
    RelNode leftParentSubset = leftRelSubset.getParentSubsets(planner).iterator().next();
    assertThat(joinSubset, is(leftParentSubset));
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
  static class PhysToIteratorConverter extends ConverterImpl {
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
  public static class SubsetRule extends RelRule<SubsetRule.Config> {
    static Config config(List<String> buf) {
      return ModifiableSubsetRuleConfig.create()
          .withOperandSupplier(b0 ->
              b0.operand(TestSingleRel.class).oneInput(b1 ->
                  b1.operand(RelSubset.class).anyInputs()))
          .as(Config.class)
          .withBuf(buf);
    }

    protected SubsetRule(Config config) {
      super(config);
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      // Do not transform to anything; just log the calls.
      TestSingleRel singleRel = call.rel(0);
      RelSubset childRel = call.rel(1);
      assertThat(call.rels, arrayWithSize(2));
      config.addBuf(singleRel.getClass().getSimpleName() + ":"
          + childRel.getDigest());
    }

    /** Rule configuration. */
    @Value.Modifiable
    @Value.Style(set = "with*", typeModifiable = "ModifiableSubsetRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default SubsetRule toRule() {
        return new SubsetRule(this);
      }

      List<String> buf();

      /** Sets {@link #buf()}. */
      Config withBuf(Iterable<String> buf);

      Config addBuf(String element);

    }
  }

  /** Rule that matches a PhysSingle on a RelSubset. */
  public static class PhysSingleSubsetRule
      extends RelRule<PhysSingleSubsetRule.Config> {
    static Config config(List<String> buf) {
      return ModifiablePhysSingleSubsetRuleConfig.create()
          .withOperandSupplier(b0 ->
              b0.operand(PhysSingleRel.class).oneInput(b1 ->
                  b1.operand(RelSubset.class).anyInputs()))
          .as(Config.class)
          .withBuf(buf);
    }

    protected PhysSingleSubsetRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      PhysSingleRel singleRel = call.rel(0);
      RelSubset subset = call.rel(1);
      config.addBuf(singleRel.getClass().getSimpleName() + ":"
          + subset.getDigest());
    }

    /** Rule configuration. */
    @Value.Modifiable
    @Value.Style(set = "with*", typeModifiable = "ModifiablePhysSingleSubsetRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysSingleSubsetRule toRule() {
        return new PhysSingleSubsetRule(this);
      }

      List<String> buf();

      /** Sets {@link #buf()}. */
      Config withBuf(Iterable<String> buf);

      Config addBuf(String element);
    }
  }

  /** Creates an artificial RelSet merge in the PhysSingleRel's input RelSet. */
  public static class PhysSingleInputSetMergeRule
      extends RelRule<PhysSingleInputSetMergeRule.Config> {
    static final PhysSingleInputSetMergeRule INSTANCE =
        ImmutablePhysSingleInputSetMergeRuleConfig.builder().build()
            .withOperandSupplier(b0 ->
                b0.operand(PhysSingleRel.class).oneInput(b1 ->
                    b1.operand(PhysLeafRel.class)
                        .trait(PHYS_CALLING_CONVENTION).anyInputs()))
            .as(Config.class)
            .toRule();

    protected PhysSingleInputSetMergeRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      PhysSingleRel singleRel = call.rel(0);
      PhysLeafRel input = call.rel(1);
      RelNode newInput =
          new PhysLeafRel(input.getCluster(), PHYS_CALLING_CONVENTION_3, "a");

      VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
      // Register into a new RelSet first
      planner.ensureRegistered(newInput, null);
      // Merge into the old RelSet
      planner.ensureRegistered(newInput, input);
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysSingleInputSetMergeRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysSingleInputSetMergeRule toRule() {
        return new PhysSingleInputSetMergeRule(this);
      }
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
  public static class ReformedSingleRule
      extends RelRule<ReformedSingleRule.Config> {
    static final ReformedSingleRule INSTANCE =
        ImmutableReformedSingleRuleConfig.builder().build()
            .withOperandSupplier(b0 ->
                b0.operand(NoneSingleRel.class).oneInput(b1 ->
                    b1.operand(PhysLeafRel.class).anyInputs()))
            .as(Config.class)
            .toRule();

    protected ReformedSingleRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
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

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableReformedSingleRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default ReformedSingleRule toRule() {
        return new ReformedSingleRule(this);
      }
    }
  }

  /** Planner rule that converts a {@link LogicalProject} to PHYS convention. */
  public static class PhysProjectRule
      extends RelRule<PhysProjectRule.Config> {
    static final PhysProjectRule INSTANCE =
        ImmutablePhysProjectRuleConfig.builder().build()
            .withOperandSupplier(b ->
                b.operand(LogicalProject.class).anyInputs())
            .as(Config.class)
            .toRule();

    PhysProjectRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      RelNode childRel = project.getInput();
      call.transformTo(
          new PhysLeafRel(
              childRel.getCluster(),
              "b"));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysProjectRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysProjectRule toRule() {
        return new PhysProjectRule(this);
      }
    }
  }

  /** Planner rule that successfully removes a {@link PhysSingleRel}. */
  public static class GoodRemoveSingleRule
      extends RelRule<GoodRemoveSingleRule.Config> {
    static final GoodRemoveSingleRule INSTANCE =
        ImmutableGoodRemoveSingleRuleConfig.builder().build()
            .withOperandSupplier(b0 ->
                b0.operand(PhysSingleRel.class).oneInput(b1 ->
                    b1.operand(PhysLeafRel.class).anyInputs()))
            .as(Config.class)
            .toRule();


    protected GoodRemoveSingleRule(Config config) {
      super(config);
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      PhysSingleRel singleRel = call.rel(0);
      PhysLeafRel leafRel = call.rel(1);
      call.transformTo(
          new PhysLeafRel(
              singleRel.getCluster(),
              "c"));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableGoodRemoveSingleRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default GoodRemoveSingleRule toRule() {
        return new GoodRemoveSingleRule(this);
      }
    }
  }

  /** Planner rule that removes a {@link NoneSingleRel}. */
  public static class ReformedRemoveSingleRule
      extends RelRule<ReformedRemoveSingleRule.Config> {
    static final ReformedRemoveSingleRule INSTANCE =
        ImmutableReformedRemoveSingleRuleConfig.builder().build()
            .withOperandSupplier(b0 ->
                b0.operand(NoneSingleRel.class).oneInput(b1 ->
                    b1.operand(PhysLeafRel.class).anyInputs()))
            .as(Config.class)
            .toRule();

    protected ReformedRemoveSingleRule(Config config) {
      super(config);
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      PhysLeafRel leafRel = call.rel(1);
      call.transformTo(
          new PhysLeafRel(
              singleRel.getCluster(),
              "c"));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutableReformedRemoveSingleRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default ReformedRemoveSingleRule toRule() {
        return new ReformedRemoveSingleRule(this);
      }
    }
  }

  /** Implementation of {@link RelOptListener}. */
  private static class TestListener implements RelOptListener {
    private final List<RelEvent> eventList;

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

  /** Rule that converts a physical RelNode to an iterator. */
  private static class PhysToIteratorRule extends ConverterRule {
    static final PhysToIteratorRule INSTANCE = Config.INSTANCE
        .withConversion(RelNode.class, PlannerTests.PHYS_CALLING_CONVENTION,
            EnumerableConvention.INSTANCE, "PhysToIteratorRule")
        .withRuleFactory(PhysToIteratorRule::new)
        .toRule(PhysToIteratorRule.class);

    PhysToIteratorRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      return new PhysToIteratorConverter(
          rel.getCluster(),
          rel);
    }
  }
}
