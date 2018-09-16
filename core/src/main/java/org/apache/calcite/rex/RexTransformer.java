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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Takes a tree of {@link RexNode} objects and transforms it into another in one
 * sense equivalent tree. Nodes in tree will be modified and hence tree will not
 * remain unchanged.
 *
 * <p>NOTE: You must validate the tree of RexNodes before using this class.
 */
public class RexTransformer {
  //~ Instance fields --------------------------------------------------------

  private RexNode root;
  private final RexBuilder rexBuilder;
  private int isParentsCount;
  private final Set<SqlOperator> transformableOperators = new HashSet<>();

  //~ Constructors -----------------------------------------------------------

  public RexTransformer(
      RexNode root,
      RexBuilder rexBuilder) {
    this.root = root;
    this.rexBuilder = rexBuilder;
    isParentsCount = 0;

    transformableOperators.add(SqlStdOperatorTable.AND);

    /** NOTE the OR operator is NOT missing.
     * see {@link org.apache.calcite.test.RexTransformerTest} */
    transformableOperators.add(SqlStdOperatorTable.EQUALS);
    transformableOperators.add(SqlStdOperatorTable.NOT_EQUALS);
    transformableOperators.add(SqlStdOperatorTable.GREATER_THAN);
    transformableOperators.add(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    transformableOperators.add(SqlStdOperatorTable.LESS_THAN);
    transformableOperators.add(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
  }

  //~ Methods ----------------------------------------------------------------

  private boolean isBoolean(RexNode node) {
    RelDataType type = node.getType();
    return SqlTypeUtil.inBooleanFamily(type);
  }

  private boolean isNullable(RexNode node) {
    return node.getType().isNullable();
  }

  private boolean isTransformable(RexNode node) {
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

  public RexNode transformNullSemantics() {
    root = transformNullSemantics(root);
    return root;
  }

  private RexNode transformNullSemantics(RexNode node) {
    assert isParentsCount >= 0 : "Cannot be negative";
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
      if (operand instanceof RexLiteral
          || operand instanceof RexInputRef
          || operand instanceof RexDynamicParam) {
        if (isNullable(node)) {
          RexNode notNullNode =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.IS_NOT_NULL,
                  operand);
          RexNode boolNode =
              rexBuilder.makeLiteral(
                  directlyUnderIs.booleanValue());
          RexNode eqNode =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.EQUALS,
                  operand,
                  boolNode);
          RexNode andBoolNode =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  notNullNode,
                  eqNode);

          return andBoolNode;
        } else {
          RexNode boolNode =
              rexBuilder.makeLiteral(
                  directlyUnderIs.booleanValue());
          RexNode andBoolNode =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.EQUALS,
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
      final ArrayList<RexNode> operands = new ArrayList<>();
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
                  SqlStdOperatorTable.IS_NOT_NULL,
                  operands.get(0));
        } else {
          isNotNullOne = null;
        }

        final RexNode isNotNullTwo;
        if (isTransformable(operands.get(1))) {
          isNotNullTwo =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.IS_NOT_NULL,
                  operands.get(1));
        } else {
          isNotNullTwo = null;
        }

        RexNode intoFinalAnd = null;
        if ((null != isNotNullOne) && (null != isNotNullTwo)) {
          intoFinalAnd =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
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
                  SqlStdOperatorTable.AND,
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
