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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.GoodSingleRule;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.NoneSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTests.PhysSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link VolcanoPlanner}
 */
public class ComboRuleTest {

  @Test public void testCombo() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRule(new ComboRule());
    planner.addRule(new AddIntermediateNodeRule());
    planner.addRule(new GoodSingleRule());

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel = new NoneLeafRel(cluster, "a");
    NoneSingleRel singleRel = new NoneSingleRel(cluster, leafRel);
    NoneSingleRel singleRel2 = new NoneSingleRel(cluster, singleRel);
    RelNode convertedRel =
        planner.changeTraits(singleRel2,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof IntermediateNode);
  }

  /** Intermediate node, the cost decreases as it is pushed up the tree
   * (more inputs it has, cheaper it gets). */
  private static class IntermediateNode extends TestSingleRel {
    final int nodesBelowCount;

    IntermediateNode(RelOptCluster cluster, RelNode input, int nodesBelowCount) {
      super(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION), input);
      this.nodesBelowCount = nodesBelowCount;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(100, 100, 100)
          .multiplyBy(1.0 / nodesBelowCount);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(PHYS_CALLING_CONVENTION);
      return new IntermediateNode(getCluster(), sole(inputs), nodesBelowCount);
    }
  }

  /** Rule that adds an intermediate node above the {@link PhysLeafRel}. */
  private static class AddIntermediateNodeRule extends RelOptRule {
    AddIntermediateNodeRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leaf = call.rel(0);

      RelNode physLeaf = new PhysLeafRel(leaf.getCluster(), leaf.label);
      RelNode intermediateNode = new IntermediateNode(physLeaf.getCluster(), physLeaf, 1);

      call.transformTo(intermediateNode);
    }
  }

  /** Matches {@link PhysSingleRel}-{@link IntermediateNode}-Any
   * and converts to {@link IntermediateNode}-{@link PhysSingleRel}-Any. */
  private static class ComboRule extends RelOptRule {
    ComboRule() {
      super(createOperand());
    }

    private static RelOptRuleOperand createOperand() {
      RelOptRuleOperand input = operand(RelNode.class, any());
      input = operand(IntermediateNode.class, some(input));
      input = operand(PhysSingleRel.class, some(input));
      return input;
    }

    @Override public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    @Override public boolean matches(RelOptRuleCall call) {
      if (call.rels.length < 3) {
        return false;
      }

      if (call.rel(0) instanceof PhysSingleRel
          && call.rel(1) instanceof IntermediateNode
          && call.rel(2) instanceof RelNode) {
        return true;
      }
      return false;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      List<RelNode> newInputs = ImmutableList.of(call.rel(2));
      IntermediateNode oldInter = call.rel(1);
      RelNode physRel = call.rel(0).copy(call.rel(0).getTraitSet(), newInputs);
      RelNode converted = new IntermediateNode(physRel.getCluster(), physRel,
          oldInter.nodesBelowCount + 1);
      call.transformTo(converted);
    }
  }
}

// End ComboRuleTest.java
