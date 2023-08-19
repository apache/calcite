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
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubstitutionRuleUtils
{
    private SubstitutionRuleUtils()
    {
    }

    protected static RexShuttle getRexShuttle(List<RexNode> rexNodes)
    {
        final Map<RexNode, Integer> map = new HashMap<>();
        for (int i = 0; i < rexNodes.size(); i++) {
            final RexNode rexNode = rexNodes.get(i);
            if (map.containsKey(rexNode)) {
                continue;
            }

            map.put(rexNode, i);
        }

        return new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref)
            {
                final Integer integer = map.get(ref);
                if (integer != null) {
                    return new RexInputRef(integer, ref.getType());
                }
                throw MatchFailed.INSTANCE;
            }

            @Override public RexNode visitCall(RexCall call)
            {
                final Integer integer = map.get(call);
                if (integer != null) {
                    return new RexInputRef(integer, call.getType());
                }
                return super.visitCall(call);
            }

            @Override public RexNode visitLiteral(RexLiteral literal)
            {
                final Integer integer = map.get(literal);
                if (integer != null) {
                    return new RexInputRef(integer, literal.getType());
                }
                return super.visitLiteral(literal);
            }
        };
    }

    protected static SubstitutionVisitor.UnifyResult tryMergeParentCalcAndGenResult(
            SubstitutionVisitor.UnifyRuleCall call, MutableRel child)
    {
        final MutableRel parent = call.query.getParent();
        if (child instanceof MutableCalc && parent instanceof MutableCalc) {
            final MutableCalc mergedCalc = mergeCalc(call.getCluster().getRexBuilder(),
                    (MutableCalc) parent, (MutableCalc) child);
            if (mergedCalc != null) {
                // Note that property of stopTrying in the result is false
                // and this query node deserves further matching iterations.
                return call.create(parent).result(mergedCalc, false);
            }
        }
        return call.result(child, false);
    }

    protected static @Nullable MutableCalc mergeCalc(
            RexBuilder rexBuilder, MutableCalc topCalc, MutableCalc bottomCalc)
    {
        RexProgram topProgram = topCalc.program;
        if (RexOver.containsOver(topProgram)) {
            return null;
        }

        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(
                        topCalc.program,
                        bottomCalc.program,
                        rexBuilder);
        assert mergedProgram.getOutputRowType()
                == topProgram.getOutputRowType();
        return MutableCalc.of(bottomCalc.getInput(), mergedProgram);
    }

    protected static class MatchFailed
            extends ControlFlowException
    {
        @SuppressWarnings("ThrowableInstanceNeverThrown")
        public static final MatchFailed INSTANCE = new MatchFailed();
    }

    protected static int fieldCnt(MutableRel rel)
    {
        return rel.rowType.getFieldCount();
    }

    protected static boolean canPullUpFilterUnderJoin(JoinRelType joinType,
                                                      @Nullable RexNode leftFilterRexNode, @Nullable RexNode rightFilterRexNode)
    {
        if (joinType == JoinRelType.INNER) {
            return true;
        }
        if (joinType == JoinRelType.LEFT
                && (rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())) {
            return true;
        }
        if (joinType == JoinRelType.RIGHT
                && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue())) {
            return true;
        }
        if (joinType == JoinRelType.FULL
                && ((rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())
                && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue()))) {
            return true;
        }
        return false;
    }

    protected static boolean referenceByMapping(
            RexNode joinCondition, List<RexNode>... projectsOfInputs)
    {
        List<RexNode> projects = new ArrayList<>();
        for (List<RexNode> projectsOfInput : projectsOfInputs) {
            projects.addAll(projectsOfInput);
        }

        try {
            RexVisitor rexVisitor = new RexVisitorImpl<Void>(true) {
                @Override public Void visitInputRef(RexInputRef inputRef)
                {
                    if (!(projects.get(inputRef.getIndex()) instanceof RexInputRef)) {
                        throw Util.FoundOne.NULL;
                    }
                    return super.visitInputRef(inputRef);
                }
            };
            joinCondition.accept(rexVisitor);
        }
        catch (Util.FoundOne e) {
            return false;
        }
        return true;
    }
}
