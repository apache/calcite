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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rel.rules.AggregateJoinTransposeRule.isAggregateSupported;

/**
 * Planner rule that pulls an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * from below a {@link org.apache.calcite.rel.core.Join} to above it.
 *
 * <p>Before
 * <pre><code>
 * SELECT s.sales
 * FROM (SELECT ss_sold_date_sk, SUM(ss_sales_price) AS sales
 *       FROM store_sales
 *       GROUP BY ss_sold_date_sk) s
 * JOIN date_dim d
 *   ON s.ss_sold_date_sk = d.d_date_sk
 * WHERE d.d_year = 2000
 * </code></pre>
 *
 * <p>After
 * <pre><code>
 * SELECT SUM(ss_sales_price) AS sales
 * FROM store_sales s
 * JOIN date_dim d
 *   ON s.ss_sold_date_sk = d.d_date_sk
 * WHERE d.d_year = 2000
 * GROUP BY s.ss_sold_date_sk
 * </code></pre>
 *
 * <p>This rule implements the simplest form of group-by pull up transformation
 * described in the following papers:
 *
 * <ul>
 * <li>Weipeng P. Yan, and Per-Ake Larson. "Interchanging the order of grouping and join". Technical
 * Report CS 95-09, Dept. of Computer Science, University of Waterloo, Canada, 1995.</li>
 * <li>Weipeng P. Yan, and Per-Ake Larson. "Eager Aggregation and Lazy Aggregation." Proceedings
 * of the 21th International Conference on Very Large Data Bases. 1995.</li>
 * </ul>
 *
 * <p>The papers contain additional variants ("lazy" aggregation) not currently
 * implemented.
 *
 * @see CoreRules#JOIN_AGGREGATE_TRANSPOSE
 */
@Value.Enclosing
public class JoinAggregateTransposeRule
    extends RelRule<JoinAggregateTransposeRule.Config>
    implements TransformationRule {

  protected JoinAggregateTransposeRule(Config config) {
    super(config);
  }

  @Override public final boolean matches(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final Aggregate left = call.rel(1);
    final RelNode right = call.rel(2);
    final JoinInfo info = join.analyzeCondition();
    final RelMetadataQuery mq = call.getMetadataQuery();

    // Only handle INNER equijoins with simple aggregates for now.
    // Join keys on the agg side must reference only group-by columns
    // (ensures row elimination removes whole groups, not partial)
    ImmutableBitSet groupOutput = ImmutableBitSet.range(left.getGroupCount());
    return join.getJoinType() == JoinRelType.INNER
        && info.isEqui()
        // We could potentially relax the check for the supported functions
        // in this rule. I opted to keep things more constrained for now
        // in case we decide to extend this rule for lazy aggregation.
        && isAggregateSupported(left, true)
        && groupOutput.contains(info.leftSet())
        // The right side must be unique on its join keys (no row duplication)
        && Boolean.TRUE.equals(mq.areColumnsUnique(right, info.rightSet()));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final Aggregate left = call.rel(1);
    final RelNode aggInput = left.getInput();
    final RelNode right = join.getRight();

    // Build the transformation
    final int rawFieldCount = aggInput.getRowType().getFieldCount();
    final int leftFields = left.getRowType().getFieldCount();
    final int rightFields = right.getRowType().getFieldCount();
    final List<Integer> groupList = left.getGroupSet().toList();

    // Remap join condition: replace references to left output columns
    // with references to raw aggInput columns in the new join layout.
    // Old join: [agg output (leftFields) | other (rightFields)]
    // New join: [aggInput (rawFieldCount) | other (rightFields)]
    final int oldJoinWidth = join.getRowType().getFieldCount();
    final int newJoinWidth = rawFieldCount + rightFields;

    final Mappings.TargetMapping condMapping =
        Mappings.create(MappingType.FUNCTION, oldJoinWidth, newJoinWidth);
    // Agg output positions 0..groupCount-1 -> raw aggInput column positions
    for (int i = 0; i < groupList.size(); i++) {
      condMapping.set(i, groupList.get(i));
    }
    // Other-side columns shift: from leftFields+j to rawFieldCount+j
    for (int j = 0; j < rightFields; j++) {
      condMapping.set(leftFields + j, rawFieldCount + j);
    }
    final RexNode newCondition = RexUtil.apply(condMapping, join.getCondition());

    // Build new join
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(aggInput).push(right);
    relBuilder.join(JoinRelType.INNER, newCondition);

    // Build new left above the join.
    // New group-by set: original group columns (at their raw positions in
    // aggInput) plus all other-side columns (to preserve them).
    final ImmutableBitSet.Builder newGroupSetBuilder = ImmutableBitSet.builder();
    for (int col : groupList) {
      newGroupSetBuilder.set(col);
    }
    for (int j = 0; j < rightFields; j++) {
      newGroupSetBuilder.set(rawFieldCount + j);
    }
    final ImmutableBitSet newGroupSet = newGroupSetBuilder.build();

    relBuilder.aggregate(relBuilder.groupKey(newGroupSet), left.getAggCallList());

    // Add project to restore original join output column order.
    // Original output: [group(left_cols), agg_calls, right_cols]
    // New output: [group(left_cols, right_cols), agg_calls]

    // Create a mapping between the input (source) and the output (target)
    // columns of the new aggregate. For example:
    //
    // Aggregate: Aggregate(group=[{7, 9, 10}])
    // Mapping: { 7 -> 0, 9 -> 1, 10 -> 2 }
    final Mappings.TargetMapping newGroupMap = Mappings.target(newGroupSet.toList(), newJoinWidth);

    final List<RexNode> projects = new ArrayList<>();
    // Group-by columns of original left
    for (int col : groupList) {
      int pos = newGroupMap.getTarget(col);
      projects.add(relBuilder.field(pos));
    }
    // Aggregate call results
    int aggCallBase = newGroupSet.cardinality();
    for (int k = 0; k < left.getAggCallList().size(); k++) {
      projects.add(relBuilder.field(aggCallBase + k));
    }
    // Right-side columns
    for (int j = 0; j < rightFields; j++) {
      int pos = newGroupMap.getTarget(rawFieldCount + j);
      projects.add(relBuilder.field(pos));
    }

    relBuilder.project(projects, join.getRowType().getFieldNames());

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinAggregateTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(Aggregate.class).anyInputs(),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .withDescription("JoinAggregateTransposeRule");

    @Override default JoinAggregateTransposeRule toRule() {
      return new JoinAggregateTransposeRule(this);
    }
  }
}
