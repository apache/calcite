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
public class SqlShuttle extends SqlBasicVisitor<SqlNode> {
  //~ Methods ----------------------------------------------------------------

  public SqlNode visit(SqlLiteral literal) {
    return literal;
  }

  public SqlNode visit(SqlIdentifier id) {
    return id;
  }

  public SqlNode visit(SqlDataTypeSpec type) {
    return type;
  }

  public SqlNode visit(SqlDynamicParam param) {
    return param;
  }

  public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
    return intervalQualifier;
  }

  public SqlNode visit(final SqlCall call) {
    // Handler creates a new copy of 'call' only if one or more operands
    // change.
    ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, false);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }

  public SqlNode visit(SqlNodeList nodeList) {
    boolean update = false;
    List<SqlNode> exprs = nodeList.getList();
    int exprCount = exprs.size();
    List<SqlNode> newList = new ArrayList<>(exprCount);
    for (SqlNode operand : exprs) {
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
      return new SqlNodeList(newList, nodeList.getParserPosition());
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
  protected class CallCopyingArgHandler implements ArgHandler<SqlNode> {
    boolean update;
    SqlNode[] clonedOperands;
    private final SqlCall call;
    private final boolean alwaysCopy;

    public CallCopyingArgHandler(SqlCall call, boolean alwaysCopy) {
      this.call = call;
      this.update = false;
      final List<SqlNode> operands = call.getOperandList();
      this.clonedOperands = operands.toArray(new SqlNode[0]);
      this.alwaysCopy = alwaysCopy;
    }

    public SqlNode result() {
      if (update || alwaysCopy) {
        return call.getOperator().createCall(
            call.getFunctionQuantifier(),
            call.getParserPosition(),
            clonedOperands);
      } else {
        return call;
      }
    }

    public SqlNode visitChild(
        SqlVisitor<SqlNode> visitor,
        SqlNode expr,
        int i,
        SqlNode operand) {
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

// End SqlShuttle.java
