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
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;


/**
 *
 */
public class CascadesTestHashAggregateRule extends ImplementationRule<LogicalAggregate> {

  public static final CascadesTestHashAggregateRule CASCADES_HASH_AGGREGATE_RULE =
      new CascadesTestHashAggregateRule();

  public CascadesTestHashAggregateRule() {
    super(LogicalAggregate.class,
        t -> true,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER, "CascadesHashAggregateRule");
  }

  @Override public void implement(LogicalAggregate agg, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    // We don't care about the sortedness of hash agg.
    requestedTraits = requestedTraits.replace(RelCollationTraitDef.INSTANCE.getDefault());

    implementAggregate(agg, requestedTraits, call);
  }

  private static void implementAggregate(LogicalAggregate agg, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    if (agg.getGroupSet().isEmpty()) {
      // For empty grouping move everything to a single node.
      requestedTraits = requestedTraits
          .replace(RelDistributions.SINGLETON);

      CascadesTestHashAggregate result = new CascadesTestHashAggregate(
          agg.getCluster(),
          requestedTraits,
          ImmutableList.of(),
          convert(agg.getInput(), requestedTraits),
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());

      call.transformTo(result);

      return;
    }

    ImmutableBitSet groupings = agg.getGroupSet();
    RelDistribution distr = agg.getCluster().getMetadataQuery().distribution(agg.getInput());

    assert distr.getKeys().size() <= 1
        : "Only distributions with keys size <= 1 are supported in prototype: " + distr;

    boolean collocated = false; // Whether we can do an aggregation without redistribution.
    if (distr.getType() == RelDistribution.Type.SINGLETON) {
      collocated = true;
    } else if (distr.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      int distrKey = distr.getKeys().get(0);
      collocated = groupings.get(distrKey);
    }

    if (!collocated) {
      List<Integer> newHashKey = groupings.asList().subList(0, 1);
      requestedTraits = requestedTraits.plus(RelDistributions.hash(newHashKey));
    } else {
      requestedTraits = requestedTraits.plus(distr);
    }

    CascadesTestHashAggregate result = new CascadesTestHashAggregate(
        agg.getCluster(),
        requestedTraits,
        ImmutableList.of(),
        convert(agg.getInput(), requestedTraits),
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList());

    call.transformTo(result);
  }
}
