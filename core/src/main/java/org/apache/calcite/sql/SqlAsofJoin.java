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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Parse tree node representing a {@code ASOF JOIN} clause.
 */
public class SqlAsofJoin extends SqlJoin {
  SqlNode matchCondition;
  static final SqlAsofJoinOperator ASOF_OPERATOR =
      new SqlAsofJoinOperator("ASOF-JOIN", 16);

  //~ Constructors -----------------------------------------------------------

  public SqlAsofJoin(SqlParserPos pos, SqlNode left, SqlLiteral natural,
                     SqlLiteral joinType, SqlNode right, SqlLiteral conditionType,
                     @Nullable SqlNode condition, SqlNode matchCondition) {
    super(pos, left, natural, joinType, right, conditionType, condition);
    this.matchCondition = matchCondition;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(left, natural, joinType, right,
        conditionType, condition, matchCondition);
  }

  @Override public SqlOperator getOperator() {
    return ASOF_OPERATOR;
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
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
    case 6:
      matchCondition = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * The match condition of the ASOF JOIN.
   *
   * @return The match condition of the ASOF join.
   */
  public final SqlNode getMatchCondition() {
    return matchCondition;
  }

  /**
   * Describes the syntax of the SQL ASOF JOIN operator.
   */
  public static class SqlAsofJoinOperator extends SqlOperator {
    //~ Constructors -----------------------------------------------------------

    private SqlAsofJoinOperator(String name, int prec) {
      super(name, SqlKind.JOIN, prec, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @SuppressWarnings("argument.type.incompatible")
    @Override public SqlCall createCall(
        @Nullable SqlLiteral functionQualifier,
        SqlParserPos pos,
        @Nullable SqlNode... operands) {
      assert functionQualifier == null;
      return new SqlAsofJoin(pos, operands[0], (SqlLiteral) operands[1],
          (SqlLiteral) operands[2], operands[3], (SqlLiteral) operands[4],
          operands[5], operands[6]);
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlAsofJoin join = (SqlAsofJoin) call;

      join.left.unparse(
          writer,
          leftPrec,
          getLeftPrec());
      if (join.getJoinType() == JoinType.LEFT_ASOF) {
        writer.sep("LEFT ASOF JOIN");
      } else {
        writer.sep("ASOF JOIN");
      }
      join.right.unparse(writer, getRightPrec(), rightPrec);
      SqlNode matchCondition = join.matchCondition;
      writer.keyword("MATCH_CONDITION");
      matchCondition.unparse(writer, 0, 0);

      SqlNode joinCondition = join.condition;
      if (joinCondition != null) {
        switch (join.getConditionType()) {
        case ON:
          writer.keyword("ON");
          joinCondition.unparse(writer, leftPrec, rightPrec);
          break;

        default:
          throw Util.unexpected(join.getConditionType());
        }
      }
    }
  }

  @Override public SqlString toSqlString(UnaryOperator<SqlWriterConfig> transform) {
    SqlNode selectWrapper =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            SqlNodeList.SINGLETON_STAR, this, null, null, null,
            SqlNodeList.EMPTY, null, null, null, null, SqlNodeList.EMPTY);
    return selectWrapper.toSqlString(transform);
  }
}
