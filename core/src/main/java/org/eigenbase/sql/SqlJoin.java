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
package org.eigenbase.sql;

import java.util.Arrays;
import java.util.List;

import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * Parse tree node representing a {@code JOIN} clause.
 */
public class SqlJoin extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  SqlNode left;

  /**
   * Operand says whether this is a natural join. Must be constant TRUE or
   * FALSE.
   */
  SqlLiteral natural;

  /**
   * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
   * SqlJoinOperator.JoinType}.
   */
  SqlLiteral joinType;
  SqlNode right;

  /**
   * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
   * SqlJoinOperator.ConditionType}.
   */
  SqlLiteral conditionType;
  SqlNode condition;

  //~ Constructors -----------------------------------------------------------

  public SqlJoin(SqlParserPos pos, SqlNode left, SqlLiteral natural,
      SqlLiteral joinType, SqlNode right, SqlLiteral conditionType,
      SqlNode condition) {
    super(pos);
    this.left = left;
    this.natural = natural;
    this.joinType = joinType;
    this.right = right;
    this.conditionType = conditionType;
    this.condition = condition;

    assert natural.getTypeName() == SqlTypeName.BOOLEAN;
    assert conditionType != null;
    assert SqlLiteral.symbolValue(conditionType)
        instanceof SqlJoinOperator.ConditionType;
    assert joinType != null;
    assert SqlLiteral.symbolValue(joinType) instanceof SqlJoinOperator.JoinType;

  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return SqlStdOperatorTable.JOIN;
  }

  @Override public SqlKind getKind() {
    return SqlKind.JOIN;
  }

  public List<SqlNode> getOperandList() {
    return Arrays.asList(left, natural, joinType, right, conditionType,
        condition);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      left = operand;
      break;
    case 1:
      natural = (SqlLiteral) operand;
      break;
    case 2:
      joinType = (SqlLiteral) operand;
      break;
    case 3:
      right = operand;
      break;
    case 4:
      conditionType = (SqlLiteral) operand;
      break;
    case 5:
      condition = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public final SqlNode getCondition() {
    return condition;
  }

  /** Returns a {@link SqlJoinOperator.ConditionType}, never null. */
  public final SqlJoinOperator.ConditionType getConditionType() {
    return (SqlJoinOperator.ConditionType)
        SqlLiteral.symbolValue(conditionType);
  }

  public SqlLiteral getConditionTypeNode() {
    return conditionType;
  }

  /** Returns a {@link SqlJoinOperator.JoinType}, never null. */
  public final SqlJoinOperator.JoinType getJoinType() {
    return (SqlJoinOperator.JoinType) SqlLiteral.symbolValue(joinType);
  }

  public SqlLiteral getJoinTypeNode() {
    return joinType;
  }

  public final SqlNode getLeft() {
    return left;
  }

  public void setLeft(SqlNode left) {
    this.left = left;
  }

  public final boolean isNatural() {
    return SqlLiteral.booleanValue(natural);
  }

  public final SqlLiteral isNaturalNode() {
    return natural;
  }

  public final SqlNode getRight() {
    return right;
  }

  public void setRight(SqlNode right) {
    this.right = right;
  }
}

// End SqlJoin.java
