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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Intersect}
 * (<code>all</code> = <code>false</code>)
 * into a group of operators composed of
 * {@link org.apache.calcite.rel.core.Union} {@link org.apache.calcite.rel.core.Aggregate}, etc.
 * <p> Rewrite: (GB-Union All-GB)-GB-UDTF (on all attributes)
 * Example: R1 Intersect All R2
 * R3 = GB(R1 on all attributes + count(*) as c) union all GB(R2 on all attributes + count(*) as c)
 * R4 = GB(R3 on all attributes + count(c) as cnt  + min(c) as m)
 * Note that we do not need min(c) in intersect distinct
 * R5 = Fil ( cnt == #branch )
 * If it is intersect all then
 * R6 = UDTF (R5) which will explode the tuples based on min(c).
 * R7 = Proj(R6 on all attributes)
 * Else
 * R6 = Proj(R5 on all attributes)
 * @see org.apache.calcite.rel.rules.UnionToDistinctRule
 */
public class IntersectToDistinctRule extends RelOptRule {
  public static final IntersectToDistinctRule INSTANCE =
          new IntersectToDistinctRule(LogicalIntersect.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a IntersectToDistinctRule.
   */
  public IntersectToDistinctRule(Class<? extends Intersect> intersectClazz,
                                 RelBuilderFactory relBuilderFactory) {
    super(operand(intersectClazz, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    if (intersect.all) {
      return; // nothing to do
    }
    final RelDataTypeFactory typeFactory = intersect.getCluster().getTypeFactory();
    final int inputCount = intersect.getInputs().size();
    final RexBuilder rexBuilder = intersect.getCluster().getRexBuilder();
    final RelDataType bigintType = typeFactory.createSqlType(
            org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    final RelBuilder unionBuilder = call.builder();

    // 1st level GB: create a GB (col0, col1, count() as c) for each branch
    for (int index = 0; index < inputCount; index++) {
      RelNode input = intersect.getInputs().get(index);
      final List<Integer> groupSetPositions = Lists.newArrayList();
      for (int cInd = 0; cInd < input.getRowType().getFieldList().size(); cInd++) {
        groupSetPositions.add(cInd);
      }

      final RelBuilder relBuilder = call.builder();

      // groupSetPosition includes all the positions
      final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

      List<AggregateCall> aggregateCalls = Lists.newArrayList();

      final List<Integer> argList = Lists.newArrayList();
      AggregateCall aggregateCall = AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              argList,
              -1,
              bigintType,
              null);

      aggregateCalls.add(aggregateCall);

      relBuilder.push(input);
      relBuilder.aggregate(
              relBuilder.groupKey(groupSet, false, null), aggregateCalls);
      unionBuilder.push(relBuilder.build());
    }

    // create a union above all the branches
    unionBuilder.union(true, inputCount);
    RelNode union = unionBuilder.build();

    // 2nd level GB: create a GB (col0, col1, count(c)) for each branch
    final List<Integer> groupSetPositions = Lists.newArrayList();
    // the index of c is union.getRowType().getFieldList().size() - 1
    for (int index = 0; index < union.getRowType().getFieldList().size() - 1; index++) {
      groupSetPositions.add(index);
    }
    final List<Integer> argList = Lists.newArrayList();
    AggregateCall aggregateCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            argList,
            -1,
            bigintType,
            null);
    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    aggregateCalls.add(aggregateCall);

    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(union);
    relBuilder.aggregate(
            relBuilder.groupKey(groupSet, false, null), aggregateCalls);
    RelNode aggregateRel = relBuilder.build();

    // add a filter count(c) = #branches
    int countInd = union.getRowType().getFieldList().size() - 1;
    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    RexInputRef ref = rexBuilder.makeInputRef(aggregateRel, countInd);
    RexLiteral literal = rexBuilder.makeBigintLiteral(new BigDecimal(inputCount));
    childRexNodeLst.add(ref);
    childRexNodeLst.add(literal);
    RexNode factoredFilterExpr = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, childRexNodeLst);

    relBuilder.push(aggregateRel);
    relBuilder.filter(factoredFilterExpr);
    RelNode filterRel = relBuilder.build();

    List<RexNode> originalInputRefs = Lists.transform(filterRel.getRowType().getFieldList(),
            new Function<RelDataTypeField, RexNode>() {
        @Override public RexNode apply(RelDataTypeField input) {
          return new RexInputRef(input.getIndex(), input.getType());
        }
      });
    List<RexNode> copyInputRefs = new ArrayList<>();
    for (int i = 0; i < originalInputRefs.size() - 1; i++) {
      copyInputRefs.add(originalInputRefs.get(i));
    }
    relBuilder.push(filterRel);
    relBuilder.project(copyInputRefs);

    // the schema for intersect distinct is like this
    // R3 on all attributes + count(c) as cnt
    // finally add a project to project out the last column
    call.transformTo(relBuilder.build());
  }
}

// End IntersectToDistinctRule.java
