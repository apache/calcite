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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTestCommon.GoodSingleRule;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.NoneLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.NoneSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.PhysLeafRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.PhysSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.TestSingleRel;
import static org.apache.calcite.plan.volcano.PlannerTestCommon.newCluster;

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
    NoneLeafRel leafRel =
        new NoneLeafRel(
            cluster,
            "a");
    NoneSingleRel singleRel =
        new NoneSingleRel(
            cluster,
            leafRel);
    NoneSingleRel singleRel2 =
        new NoneSingleRel(
            cluster,
            singleRel);
    RelNode convertedRel =
        planner.changeTraits(
            singleRel2,
            cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
    assertTrue(result instanceof IntermediateNode);
  }

  /**
   * Intermediate node, the cost decreases as it is pushed up the tree
   * (more children it has, cheaper it gets)
   */
  private static class IntermediateNode extends TestSingleRel {

    private final int numNodesBelow;

    IntermediateNode(RelOptCluster cluster, RelNode child, int numNodesBelow) {
      super(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION), child);
      this.numNodesBelow = numNodesBelow;
    }

    public int getNumNodesBelow() { return numNodesBelow; }

    public RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(100, 100, 100).multiplyBy(1.0 / numNodesBelow);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(PHYS_CALLING_CONVENTION);
      return new IntermediateNode(getCluster(), sole(inputs), numNodesBelow);
    }
  }

  /**
   * Rule that adds an intermediate node above the PhysLeafRel
   */
  private static class AddIntermediateNodeRule extends RelOptRule {
    AddIntermediateNodeRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leaf = call.rel(0);

      RelNode physLeaf = new PhysLeafRel(leaf.getCluster(), leaf.getLabel());
      RelNode intermediateNode = new IntermediateNode(physLeaf.getCluster(), physLeaf, 1);

      call.transformTo(intermediateNode);
    }
  }

  /**
   * Matches PhysSingleRel-IntermediateNode-Any and converts to Intermediate-PhysSingleRel-Any
   */
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

    public Convention getOutConvention() {
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
      List<RelNode> newInputs = new ArrayList<>(1);
      newInputs.add(call.rel(2));
      IntermediateNode oldInter = call.rel(1);
      RelNode physRel = call.rel(0).copy(call.rel(0).getTraitSet(), newInputs);
      RelNode converted = new IntermediateNode(
          physRel.getCluster(),
          physRel,
          oldInter.getNumNodesBelow() + 1);
      call.transformTo(converted);
    }
  }
}
// End ComboRuleTest.java
