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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CascadesTestStreamAggregateRule extends ImplementationRule<LogicalAggregate> {

  public static final CascadesTestStreamAggregateRule CASCADES_STREAM_AGGREGATE_RULE =
      new CascadesTestStreamAggregateRule();

  public CascadesTestStreamAggregateRule() {
    super(LogicalAggregate.class,
        t -> true,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER, "CascadesStreamAggregateRule");
  }

  @Override public void implement(LogicalAggregate agg, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    List<RelFieldCollation> fldCollations = new ArrayList<>(agg.getGroupSet().size());
    for (int i : agg.getGroupSet()) {
      fldCollations.add(new RelFieldCollation(i));
    }
    RelCollation collation = RelCollations.of(fldCollations);
    requestedTraits = requestedTraits.replace(collation);

    implementAggregate(agg, requestedTraits, call);

    if (fldCollations.size() > 1) {
      Collections.reverse(fldCollations);
      collation = RelCollations.of(fldCollations);
      requestedTraits = requestedTraits.replace(collation);

      implementAggregate(agg, requestedTraits, call);
    }
  }


  private static void implementAggregate(LogicalAggregate agg, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    if (agg.getGroupSet().isEmpty()) {
      // For empty grouping move everything to a single node.
      requestedTraits = requestedTraits
          .replace(RelDistributions.SINGLETON);

      CascadesTestStreamAggregate result = new CascadesTestStreamAggregate(
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
    RelDistribution distr = RelDistributionTraitDef.INSTANCE.getDefault();
    try {
      distr = agg.getCluster().getMetadataQuery().distribution(agg.getInput());
    } catch (Exception e) {
      System.out.println("distr MD error:" + e); // TODO Fix project distribution mappings.
    }

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

    CascadesTestStreamAggregate result = new CascadesTestStreamAggregate(
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
