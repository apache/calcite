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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import static org.apache.calcite.plan.cascades.CascadesTestUtils.CASCADES_TEST_CONVENTION;
import static org.apache.calcite.plan.cascades.CascadesTestUtils.deriveInputsDistribution;

/**
 *
 */
public class CascadesTestNestedLoopsJoinRule extends ImplementationRule<LogicalJoin> {

  public static final CascadesTestNestedLoopsJoinRule CASCADES_NL_JOIN_RULE  =
      new CascadesTestNestedLoopsJoinRule();

  public CascadesTestNestedLoopsJoinRule() {
    super(LogicalJoin.class,
        join -> true,
        Convention.NONE,
        CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER,
        "CascadesNestedLoopsJoinRule");
  }

  @Override public void implement(LogicalJoin join, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    // NL join preserves collation of the left child (outer table).
    RelTraitSet leftRequestedTraits = requestedTraits;
    RelTraitSet rightRequestedTraits = requestedTraits
        .replace(RelCollationTraitDef.INSTANCE.getDefault());

    Pair<RelDistribution, RelDistribution> childrenDistribution =
        deriveInputsDistribution(join);

    CascadesTestNestedLoopsJoin nlJoin = new CascadesTestNestedLoopsJoin(join.getCluster(),
        requestedTraits,
        ImmutableList.of(),
        convert(join.getLeft(), leftRequestedTraits.plus(childrenDistribution.left)),
        convert(join.getRight(), rightRequestedTraits.plus(childrenDistribution.right)),
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());

    call.transformTo(nlJoin);
  }
}
