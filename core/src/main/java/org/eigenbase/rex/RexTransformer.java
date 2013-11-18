/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rex;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;

/**
 * Takes a tree of {@link RexNode} objects and transforms it into another in one
 * sense equivalent tree. Nodes in tree will be modified and hence tree will not
 * remain unchanged.
 *
 * <p>NOTE: You must validate the tree of RexNodes before using this class.
 *
 * @author wael
 * @version $Id$
 * @since Mar 8, 2004
 */
public class RexTransformer
{
    //~ Instance fields --------------------------------------------------------

    private RexNode root;
    private final RexBuilder rexBuilder;
    private int isParentsCount;
    private final Set<SqlOperator> transformableOperators =
        new HashSet<SqlOperator>();

    //~ Constructors -----------------------------------------------------------

    public RexTransformer(
        RexNode root,
        RexBuilder rexBuilder)
    {
        this.root = root;
        this.rexBuilder = rexBuilder;
        isParentsCount = 0;

        transformableOperators.add(SqlStdOperatorTable.andOperator);

        /** NOTE the OR operator is NOT missing.
         * see {@link org.eigenbase.test.RexTransformerTest} */
        transformableOperators.add(SqlStdOperatorTable.equalsOperator);
        transformableOperators.add(SqlStdOperatorTable.notEqualsOperator);
        transformableOperators.add(SqlStdOperatorTable.greaterThanOperator);
        transformableOperators.add(
            SqlStdOperatorTable.greaterThanOrEqualOperator);
        transformableOperators.add(SqlStdOperatorTable.lessThanOperator);
        transformableOperators.add(SqlStdOperatorTable.lessThanOrEqualOperator);
    }

    //~ Methods ----------------------------------------------------------------

    private boolean isBoolean(RexNode node)
    {
        RelDataType type = node.getType();
        return SqlTypeUtil.inBooleanFamily(type);
    }

    private boolean isNullable(RexNode node)
    {
        return node.getType().isNullable();
    }

    private boolean isTransformable(RexNode node)
    {
        if (0 == isParentsCount) {
            return false;
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            return !transformableOperators.contains(
                call.getOperator())
                && isNullable(node);
        }
        return isNullable(node);
    }

    public RexNode transformNullSemantics()
    {
        root = transformNullSemantics(root);
        return root;
    }

    private RexNode transformNullSemantics(RexNode node)
    {
        assert (isParentsCount >= 0) : "Cannot be negative";
        if (!isBoolean(node)) {
            return node;
        }

        Boolean directlyUnderIs = null;
        if (node.isA(SqlKind.IS_TRUE)) {
            directlyUnderIs = Boolean.TRUE;
            isParentsCount++;
        } else if (node.isA(SqlKind.IS_FALSE)) {
            directlyUnderIs = Boolean.FALSE;
            isParentsCount++;
        }

        // Special case when we have a Literal, Parameter or Identifier directly
        // as an operand to IS TRUE or IS FALSE.
        if (null != directlyUnderIs) {
            RexCall call = (RexCall) node;
            assert isParentsCount > 0 : "Stack should not be empty";
            assert 1 == call.operands.size();
            RexNode operand = call.operands.get(0);
            if (((operand instanceof RexLiteral)
                    || (operand instanceof RexInputRef)
                    || (operand instanceof RexDynamicParam)))
            {
                if (isNullable(node)) {
                    RexNode notNullNode =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.isNotNullOperator,
                            operand);
                    RexNode boolNode =
                        rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode eqNode =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            operand,
                            boolNode);
                    RexNode andBoolNode =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            notNullNode,
                            eqNode);

                    return andBoolNode;
                } else {
                    RexNode boolNode =
                        rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode andBoolNode =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            node,
                            boolNode);
                    return andBoolNode;
                }
            }

            // else continue as normal
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            // Transform children (if any) before transforming node itself.
            final ArrayList<RexNode> operands = new ArrayList<RexNode>();
            for (RexNode operand : call.operands) {
                operands.add(transformNullSemantics(operand));
            }

            if (null != directlyUnderIs) {
                isParentsCount--;
                directlyUnderIs = null;
                return operands.get(0);
            }

            if (transformableOperators.contains(call.getOperator())) {
                assert 2 == operands.size();

                final RexNode isNotNullOne;
                if (isTransformable(operands.get(0))) {
                    isNotNullOne =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.isNotNullOperator,
                            operands.get(0));
                } else {
                    isNotNullOne = null;
                }

                final RexNode isNotNullTwo;
                if (isTransformable(operands.get(1))) {
                    isNotNullTwo =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.isNotNullOperator,
                            operands.get(1));
                } else {
                    isNotNullTwo = null;
                }

                RexNode intoFinalAnd = null;
                if ((null != isNotNullOne) && (null != isNotNullTwo)) {
                    intoFinalAnd =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            isNotNullOne,
                            isNotNullTwo);
                } else if (null != isNotNullOne) {
                    intoFinalAnd = isNotNullOne;
                } else if (null != isNotNullTwo) {
                    intoFinalAnd = isNotNullTwo;
                }

                if (null != intoFinalAnd) {
                    RexNode andNullAndCheckNode =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            intoFinalAnd,
                            call.clone(call.getType(), operands));
                    return andNullAndCheckNode;
                }

                // if come here no need to do anything
            }

            if (!operands.equals(call.operands)) {
                return call.clone(call.getType(), operands);
            }
        }

        return node;
    }
}

// End RexTransformer.java
