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
package org.apache.calcite.rel.rules;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/** Rule that re-orders a {@link Join} tree using dphyp algorithm.
 *
 * @see CoreRules#HYPER_GRAPH_OPTIMIZE */
@Value.Enclosing
@Experimental
public class DphypJoinReorderRule
    extends RelRule<DphypJoinReorderRule.Config>
    implements TransformationRule {

  protected DphypJoinReorderRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    HyperGraph hyperGraph = call.rel(0);
    RelBuilder relBuilder = call.builder();

    // enumerate by Dphyp
    DpHyp dpHyp = new DpHyp(hyperGraph, relBuilder, call.getMetadataQuery());
    dpHyp.startEnumerateJoin();
    RelNode orderedJoin = dpHyp.getBestPlan();
    if (orderedJoin == null) {
      return;
    }

    // permute field to origin order
    List<String> oriNames = hyperGraph.getRowType().getFieldNames();
    List<String> newNames = orderedJoin.getRowType().getFieldNames();
    List<RexNode> projects = new ArrayList<>();
    RexBuilder rexBuilder = hyperGraph.getCluster().getRexBuilder();
    for (String oriName : oriNames) {
      projects.add(rexBuilder.makeInputRef(orderedJoin, newNames.indexOf(oriName)));
    }

    RelNode result = call.builder()
        .push(orderedJoin)
        .project(projects)
        .build();
    call.transformTo(result);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableDphypJoinReorderRule.Config.of()
        .withOperandSupplier(b1 ->
            b1.operand(HyperGraph.class).anyInputs());

    @Override default DphypJoinReorderRule toRule() {
      return new DphypJoinReorderRule(this);
    }
  }
}
