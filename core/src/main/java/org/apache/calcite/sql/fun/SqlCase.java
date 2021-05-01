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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.UnmodifiableArrayList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A <code>SqlCase</code> is a node of a parse tree which represents a case
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlCase extends SqlCall {
  @Nullable SqlNode value;
  SqlNodeList whenList;
  SqlNodeList thenList;
  @Nullable SqlNode elseExpr;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlCase expression.
   *
   * @param pos Parser position
   * @param value The value (null for boolean case)
   * @param whenList List of all WHEN expressions
   * @param thenList List of all THEN expressions
   * @param elseExpr The implicit or explicit ELSE expression
   */
  public SqlCase(SqlParserPos pos, @Nullable SqlNode value, SqlNodeList whenList,
      SqlNodeList thenList, @Nullable SqlNode elseExpr) {
    super(pos);
    this.value = value;
    this.whenList = whenList;
    this.thenList = thenList;
    this.elseExpr = elseExpr;
  }

  /**
   * Creates a call to the switched form of the CASE operator. For example:
   *
   * <blockquote><code>CASE value<br>
   * WHEN whenList[0] THEN thenList[0]<br>
   * WHEN whenList[1] THEN thenList[1]<br>
   * ...<br>
   * ELSE elseClause<br>
   * END</code></blockquote>
   */
  public static SqlCase createSwitched(SqlParserPos pos, @Nullable SqlNode value,
      SqlNodeList whenList, SqlNodeList thenList, @Nullable SqlNode elseClause) {
    if (null != value) {
      for (int i = 0; i < whenList.size(); i++) {
        SqlNode e = whenList.get(i);
        final SqlCall call;
        if (e instanceof SqlNodeList) {
          call = SqlStdOperatorTable.IN.createCall(pos, value, e);
        } else {
          call = SqlStdOperatorTable.EQUALS.createCall(pos, value, e);
        }
        whenList.set(i, call);
      }
    }

    if (null == elseClause) {
      elseClause = SqlLiteral.createNull(pos);
    }

    return new SqlCase(pos, null, whenList, thenList, elseClause);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.CASE;
  }

  @Override public SqlOperator getOperator() {
    return SqlStdOperatorTable.CASE;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return UnmodifiableArrayList.of(value, whenList, thenList, elseExpr);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      value = operand;
      break;
    case 1:
      whenList = (SqlNodeList) operand;
      break;
    case 2:
      thenList = (SqlNodeList) operand;
      break;
    case 3:
      elseExpr = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public @Nullable SqlNode getValueOperand() {
    return value;
  }

  public SqlNodeList getWhenOperands() {
    return whenList;
  }

  public SqlNodeList getThenOperands() {
    return thenList;
  }

  public @Nullable SqlNode getElseOperand() {
    return elseExpr;
  }
}
