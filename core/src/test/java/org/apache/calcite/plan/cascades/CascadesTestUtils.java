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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.plan.RelOptRules.ABSTRACT_RELATIONAL_RULES;
import static org.apache.calcite.plan.RelOptRules.ABSTRACT_RULES;
import static org.apache.calcite.plan.RelOptRules.BASE_RULES;
import static org.apache.calcite.plan.cascades.rel.CascadesTestExchangeRule.CASCADES_EXCHANGE_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestFilterRule.CASCADES_FILTER_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestHashAggregateRule.CASCADES_HASH_AGGREGATE_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestHashJoinRule.CASCADES_HASH_JOIN_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestMergeJoinRule.CASCADES_MERGE_JOIN_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestNestedLoopsJoinRule.CASCADES_NL_JOIN_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestProjectRule.CASCADES_PROJECT_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestSortRule.CASCADES_SORT_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestStreamAggregateRule.CASCADES_STREAM_AGGREGATE_RULE;
import static org.apache.calcite.plan.cascades.rel.CascadesTestTableScanRule.CASCADES_TABLE_SCAN_RULE;
import static org.apache.calcite.rel.core.RelFactories.LOGICAL_BUILDER;

/**
 *
 */
public class CascadesTestUtils {

  public static final Convention CASCADES_TEST_CONVENTION =
      new Convention.Impl("PHYS", RelNode.class) {
        @Override public boolean canConvertConvention(Convention toConvention) {
          return true;
        }

        @Override public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
          return false;
        }
      };

  public static final Collection<RelOptRule> LOGICAL_RULES = ImmutableList.<RelOptRule>builder()
      .add(
          new FilterProjectTransposeRule(LogicalFilter.class, LogicalProject.class, true, true,
          LOGICAL_BUILDER))
      .add(ProjectFilterTransposeRule.INSTANCE)
      .add(new JoinAssociateRule(LogicalJoin.class, LOGICAL_BUILDER))
      .add(JoinCommuteRule.INSTANCE)
      .addAll(BASE_RULES)
      .addAll(ABSTRACT_RULES)
      .addAll(ABSTRACT_RELATIONAL_RULES)
      .build();

  public static final Collection<RelOptRule> PHYSICAL_RULES = ImmutableList.<RelOptRule>builder()
      .add(CASCADES_EXCHANGE_RULE)
      .add(CASCADES_SORT_RULE)
      .add(CASCADES_FILTER_RULE)
      .add(CASCADES_PROJECT_RULE)
      .add(CASCADES_TABLE_SCAN_RULE)
      .add(CASCADES_HASH_JOIN_RULE)
      .add(CASCADES_NL_JOIN_RULE)
      .add(CASCADES_MERGE_JOIN_RULE)
      .add(CASCADES_HASH_AGGREGATE_RULE)
      .add(CASCADES_STREAM_AGGREGATE_RULE)
      .build();

  private CascadesTestUtils() {
  }

  // Schema ====================

  static RelOptCluster newCluster(RelOptPlanner planner) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  public static Pair<RelDistribution, RelDistribution> deriveInputsDistribution(LogicalJoin join) {
    JoinInfo info = join.analyzeCondition();

    if (info.pairs().isEmpty()) {
      return Pair.of(RelDistributionTraitDef.INSTANCE.getDefault(),
          RelDistributionTraitDef.INSTANCE.getDefault());
    }

    RelDistribution leftDistribution =
        join.getCluster().getMetadataQuery().distribution(join.getLeft());
    RelDistribution rightDistribution =
        join.getCluster().getMetadataQuery().distribution(join.getRight());
    // Consider only one-key distributions for simplicity
    assert leftDistribution.getKeys().size() <= 1
        : "Distribution is not supported in prototype: " + leftDistribution;
    assert rightDistribution.getKeys().size() <= 1
        : "Distribution is not supported in prototype:  " + rightDistribution;

    List<Integer> leftDistributionKeys = leftDistribution.getKeys();
    List<IntPair> joinKeys = info.pairs();
    IntPair resultDistributionKey = null;
    for (IntPair pair : joinKeys) {
      if (leftDistributionKeys.contains(pair.source)) {
        resultDistributionKey = pair;
        break;
      }
    }
    if (resultDistributionKey == null) {
      resultDistributionKey = joinKeys.get(0);
    }
    RelDistribution desiredLeftDistribution =
        RelDistributions.hash(Collections.singleton(resultDistributionKey.source));
    RelDistribution desiredRightDistribution =
        RelDistributions.hash(Collections.singleton(resultDistributionKey.target));

    return Pair.of(desiredLeftDistribution, desiredRightDistribution);
  }
}
