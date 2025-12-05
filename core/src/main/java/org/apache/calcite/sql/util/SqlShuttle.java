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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic implementation of {@link SqlVisitor} which returns each leaf node
 * unchanged.
 *
 * <p>This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface and have {@link SqlNode} as the return type. The
 * derived class can override whichever methods it chooses.
 */
public class SqlShuttle extends SqlBasicVisitor<@Nullable SqlNode> {
  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable SqlNode visit(SqlLiteral literal) {
    return literal;
  }

  @Override public @Nullable SqlNode visit(SqlIdentifier id) {
    return id;
  }

  @Override public @Nullable SqlNode visit(SqlDataTypeSpec type) {
    return type;
  }

  @Override public @Nullable SqlNode visit(SqlDynamicParam param) {
    return param;
  }

  @Override public @Nullable SqlNode visit(SqlIntervalQualifier intervalQualifier) {
    return intervalQualifier;
  }

  @Override public @Nullable SqlNode visit(final SqlCall call) {
    // Handler creates a new copy of 'call' only if one or more operands
    // change.
    CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, false);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }

  @Override public @Nullable SqlNode visit(SqlNodeList nodeList) {
    boolean update = false;
    final List<@Nullable SqlNode> newList = new ArrayList<>(nodeList.size());
    for (SqlNode operand : nodeList) {
      SqlNode clonedOperand;
      if (operand == null) {
        clonedOperand = null;
      } else {
        clonedOperand = operand.accept(this);
        if (clonedOperand != operand) {
          update = true;
        }
      }
      newList.add(clonedOperand);
    }
    if (update) {
      return SqlNodeList.of(nodeList.getParserPosition(), newList);
    } else {
      return nodeList;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Implementation of
   * {@link org.apache.calcite.sql.util.SqlBasicVisitor.ArgHandler}
   * that deep-copies {@link SqlCall}s and their operands.
   */
  protected class CallCopyingArgHandler implements ArgHandler<@Nullable SqlNode> {
    boolean update;
    final @Nullable SqlNode[] clonedOperands;
    private final SqlCall call;
    private final boolean alwaysCopy;

    public CallCopyingArgHandler(SqlCall call, boolean alwaysCopy) {
      this.call = call;
      this.update = false;
      final List<@Nullable SqlNode> operands = (List<@Nullable SqlNode>) call.getOperandList();
      this.clonedOperands = operands.toArray(new SqlNode[0]);
      this.alwaysCopy = alwaysCopy;
    }

    @Override public SqlNode result() {
      if (update || alwaysCopy) {
        return call.getOperator().createCall(
            call.getFunctionQuantifier(),
            call.getParserPosition(),
            clonedOperands);
      } else {
        return call;
      }
    }

    @Override public @Nullable SqlNode visitChild(
        SqlVisitor<@Nullable SqlNode> visitor,
        SqlNode expr,
        int i,
        @Nullable SqlNode operand) {
      if (operand == null) {
        return null;
      }
      SqlNode newOperand = operand.accept(SqlShuttle.this);
      if (newOperand != operand) {
        update = true;
      }
      clonedOperands[i] = newOperand;
      return newOperand;
    }
  }
}
