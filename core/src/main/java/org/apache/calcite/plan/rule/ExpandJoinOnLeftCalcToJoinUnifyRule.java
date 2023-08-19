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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.mutable.MutableCalc;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.SubstitutionVisitor.explainCalc;
import static org.apache.calcite.plan.SubstitutionVisitor.splitFilter;
import static org.apache.calcite.plan.rule.JoinRexUtils.addJoinDerivePredicate;
import static org.apache.calcite.plan.rule.JoinRexUtils.extractJoinContext;
import static org.apache.calcite.plan.rule.JoinRexUtils.isJoinRewritable;
import static org.apache.calcite.plan.rule.JoinRexUtils.isSupportedPredict;
import static org.apache.calcite.plan.rule.JoinRexUtils.tryCastRexInputRef;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.canPullUpFilterUnderJoin;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.fieldCnt;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.referenceByMapping;
import static org.apache.calcite.plan.rule.SubstitutionRuleUtils.tryMergeParentCalcAndGenResult;

/**
 * @author suibianwanwan
 */
public class ExpandJoinOnLeftCalcToJoinUnifyRule
    extends SubstitutionVisitor.AbstractUnifyRule {
  public static final ExpandJoinOnLeftCalcToJoinUnifyRule INSTANCE =
      new ExpandJoinOnLeftCalcToJoinUnifyRule();

  public ExpandJoinOnLeftCalcToJoinUnifyRule() {
    super(
        operand(MutableJoin.class, operand(MutableCalc.class, query(0)), query(1)),
        operand(MutableJoin.class, target(0), target(1)), 2);
  }

  @Override
  protected SubstitutionVisitor.@Nullable UnifyRuleCall match(SubstitutionVisitor visitor,
      MutableRel query, MutableRel target) {
    return super.match(visitor, query, target);
  }

  @Override
  protected SubstitutionVisitor.@Nullable UnifyResult apply(SubstitutionVisitor.UnifyRuleCall call) {
    final MutableJoin query = (MutableJoin) call.query;
    final MutableCalc qInput0 = (MutableCalc) query.getLeft();
    final MutableRel qInput1 = query.getRight();
    final Pair<RexNode, List<RexNode>> qInput0Explained = explainCalc(qInput0);
    final RexNode qInput0Cond = qInput0Explained.left;
    final List<RexNode> qInput0Projs = qInput0Explained.right;

    final MutableJoin target = (MutableJoin) call.target;

    final RexBuilder rexBuilder = call.getCluster().getRexBuilder();
    JoinRelType joinRelType = query.joinType;
    // Check if filter under join can be pulled up.
    if (!canPullUpFilterUnderJoin(joinRelType, qInput0Cond, null)) {
      return null;
    }
    // Try pulling up MutableCalc only when Join condition references mapping.
    final List<RexNode> identityProjects =
        rexBuilder.identityProjects(qInput1.rowType);
    if (!referenceByMapping(query.condition, qInput0Projs, identityProjects)) {
      return null;
    }

    List<Pair<RexInputRef, RexInputRef>> joinColumnPairs = new ArrayList<>();
    if (!isSupportedPredict(target.condition, joinColumnPairs, fieldCnt(target.getLeft()))) {
      return null;
    }

    JoinContext joinContext = extractJoinContext(query, target, joinColumnPairs,
        fieldCnt(target.getLeft()), rexBuilder);
    if (joinContext == null || !isJoinRewritable(joinContext, fieldCnt(target.getLeft()))) {
      return null;
    }

    final RexNode newQueryJoinCond = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        final int idx = inputRef.getIndex();
        if (idx < fieldCnt(qInput0)) {
          final int newIdx = ((RexInputRef) qInput0Projs.get(idx)).getIndex();
          return new RexInputRef(newIdx, inputRef.getType());
        } else {
          int newIdx = idx - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
          return new RexInputRef(newIdx, inputRef.getType());
        }
      }
    }.apply(query.condition);
    final RexNode splitted =
        splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
    // MutableJoin matches only when the conditions are analyzed to be same.
    if (splitted != null && splitted.isAlwaysTrue()) {
      final List<RexNode> compenProjs = new ArrayList<>();
      for (int i = 0; i < fieldCnt(query); i++) {
        RexNode rexInputRef;
        if (i < fieldCnt(qInput0)) {
          rexInputRef = rexBuilder.copy(qInput0Projs.get(i));
        } else {
          final int newIdx = i - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
          rexInputRef = new RexInputRef(newIdx, query.rowType.getFieldList().get(i).getType());
        }
        compenProjs.add(tryCastRexInputRef(rexInputRef, joinContext, rexBuilder));
      }

      final RexNode compenCond =
          addJoinDerivePredicate(rexBuilder, joinContext, qInput0Cond);

      final RexProgram compenRexProgram = RexProgram.create(target.rowType, compenProjs,
          compenCond, query.rowType, rexBuilder);
      final MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
      return tryMergeParentCalcAndGenResult(call, compenCalc);
    }
    return null;
  }
}
