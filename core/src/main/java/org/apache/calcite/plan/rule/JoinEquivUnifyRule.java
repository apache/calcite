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
package org.apache.calcite.plan.rule;

import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.mutable.MutableCalc;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.rule.JoinRexUtils.addJoinDerivePredicate;
import static org.apache.calcite.plan.rule.JoinRexUtils.extractJoinContext;
import static org.apache.calcite.plan.rule.JoinRexUtils.isJoinRewritable;
import static org.apache.calcite.plan.rule.JoinRexUtils.isSupportedPredict;
import static org.apache.calcite.plan.rule.JoinRexUtils.tryCastRexInputRef;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.fieldCnt;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.tryMergeParentCalcAndGenResult;

public class JoinEquivUnifyRule
        extends SubstitutionVisitor.AbstractUnifyRule
{
    public static final JoinEquivUnifyRule INSTANCE =
            new JoinEquivUnifyRule();
    public JoinEquivUnifyRule()
    {
        super(
                operand(MutableJoin.class, query(0), query(1)),
                operand(MutableJoin.class, target(0), target(1)), 2);
    }
    @Override
    protected SubstitutionVisitor.@Nullable UnifyRuleCall match(SubstitutionVisitor visitor, MutableRel query, MutableRel target)
    {
        return super.match(visitor, query, target);
    }
  @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(SubstitutionVisitor.UnifyRuleCall call)
  {
    MutableJoin query = (MutableJoin) call.query;
    MutableJoin target = (MutableJoin) call.target;
    MutableJoin newQuery = MutableJoin.of(query.rowType, query.getLeft(), query.getRight(), query.condition, target.joinType, query.variablesSet);
    if (!newQuery.equals(target)) {
      return null;
    }
    RexBuilder rexBuilder = call.getCluster().getRexBuilder();

    List<Pair<RexInputRef, RexInputRef>> joinConditionColumnPairs = new ArrayList<>();
    if (!isSupportedPredict(target.condition, joinConditionColumnPairs, fieldCnt(target.getLeft()))) {
      return null;
    }

    JoinContext joinContext = extractJoinContext(query, target, joinConditionColumnPairs, fieldCnt(target.getLeft()), rexBuilder);
    if (joinContext == null || !isJoinRewritable(joinContext, fieldCnt(target.getLeft()))) {
      return null;
    }

    final List<RexNode> compenProjs = new ArrayList<>();
    for (int i = 0; i < query.rowType.getFieldCount(); i++) {
      RexInputRef rexInputRef = rexBuilder.makeInputRef(query.rowType.getFieldList().get(i).getValue(), i);
      compenProjs.add(tryCastRexInputRef(rexInputRef, joinContext, rexBuilder));
    }

    final RexNode compenCond =
        addJoinDerivePredicate(rexBuilder, joinContext, null);

    RexProgram compensatingRexProgram = RexProgram.create(target.rowType, compenProjs, compenCond, query.rowType, rexBuilder);
    final MutableCalc compensatingCalc =
        MutableCalc.of(target, compensatingRexProgram);
    return tryMergeParentCalcAndGenResult(call, compensatingCalc);
  }
}
