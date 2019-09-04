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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Standard implementation of {@link RexSqlConvertletTable}.
 */
public class RexSqlStandardConvertletTable
    extends RexSqlReflectiveConvertletTable {
  //~ Constructors -----------------------------------------------------------

  public RexSqlStandardConvertletTable() {
    super();

    // Register convertlets

    registerEquivOp(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    registerEquivOp(SqlStdOperatorTable.GREATER_THAN);
    registerEquivOp(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    registerEquivOp(SqlStdOperatorTable.LESS_THAN);
    registerEquivOp(SqlStdOperatorTable.EQUALS);
    registerEquivOp(SqlStdOperatorTable.NOT_EQUALS);
    registerEquivOp(SqlStdOperatorTable.AND);
    registerEquivOp(SqlStdOperatorTable.OR);
    registerEquivOp(SqlStdOperatorTable.NOT_IN);
    registerEquivOp(SqlStdOperatorTable.IN);
    registerEquivOp(SqlStdOperatorTable.LIKE);
    registerEquivOp(SqlStdOperatorTable.NOT_LIKE);
    registerEquivOp(SqlStdOperatorTable.SIMILAR_TO);
    registerEquivOp(SqlStdOperatorTable.NOT_SIMILAR_TO);
    registerEquivOp(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
    registerEquivOp(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE);
    registerEquivOp(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);
    registerEquivOp(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
    registerEquivOp(SqlStdOperatorTable.PLUS);
    registerEquivOp(SqlStdOperatorTable.MINUS);
    registerEquivOp(SqlStdOperatorTable.MULTIPLY);
    registerEquivOp(SqlStdOperatorTable.DIVIDE);

    registerEquivOp(SqlStdOperatorTable.NOT);

    registerEquivOp(SqlStdOperatorTable.IS_NOT_NULL);
    registerEquivOp(SqlStdOperatorTable.IS_NULL);

    registerEquivOp(SqlStdOperatorTable.IS_NOT_TRUE);
    registerEquivOp(SqlStdOperatorTable.IS_TRUE);

    registerEquivOp(SqlStdOperatorTable.IS_NOT_FALSE);
    registerEquivOp(SqlStdOperatorTable.IS_FALSE);

    registerEquivOp(SqlStdOperatorTable.IS_NOT_UNKNOWN);
    registerEquivOp(SqlStdOperatorTable.IS_UNKNOWN);

    registerEquivOp(SqlStdOperatorTable.UNARY_MINUS);
    registerEquivOp(SqlStdOperatorTable.UNARY_PLUS);

    registerCaseOp(SqlStdOperatorTable.CASE);
    registerEquivOp(SqlStdOperatorTable.CONCAT);

    registerEquivOp(SqlStdOperatorTable.BETWEEN);
    registerEquivOp(SqlStdOperatorTable.SYMMETRIC_BETWEEN);

    registerEquivOp(SqlStdOperatorTable.NOT_BETWEEN);
    registerEquivOp(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN);

    registerEquivOp(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);
    registerEquivOp(SqlStdOperatorTable.IS_DISTINCT_FROM);

    registerEquivOp(SqlStdOperatorTable.MINUS_DATE);
    registerEquivOp(SqlStdOperatorTable.EXTRACT);

    registerEquivOp(SqlStdOperatorTable.SUBSTRING);
    registerEquivOp(SqlStdOperatorTable.CONVERT);
    registerEquivOp(SqlStdOperatorTable.TRANSLATE);
    registerEquivOp(SqlStdOperatorTable.OVERLAY);
    registerEquivOp(SqlStdOperatorTable.TRIM);
    registerEquivOp(SqlLibraryOperators.TRANSLATE3);
    registerEquivOp(SqlStdOperatorTable.POSITION);
    registerEquivOp(SqlStdOperatorTable.CHAR_LENGTH);
    registerEquivOp(SqlStdOperatorTable.CHARACTER_LENGTH);
    registerEquivOp(SqlStdOperatorTable.UPPER);
    registerEquivOp(SqlStdOperatorTable.LOWER);
    registerEquivOp(SqlStdOperatorTable.INITCAP);

    registerEquivOp(SqlStdOperatorTable.POWER);
    registerEquivOp(SqlStdOperatorTable.SQRT);
    registerEquivOp(SqlStdOperatorTable.MOD);
    registerEquivOp(SqlStdOperatorTable.LN);
    registerEquivOp(SqlStdOperatorTable.LOG10);
    registerEquivOp(SqlStdOperatorTable.ABS);
    registerEquivOp(SqlStdOperatorTable.EXP);
    registerEquivOp(SqlStdOperatorTable.FLOOR);
    registerEquivOp(SqlStdOperatorTable.CEIL);

    registerEquivOp(SqlStdOperatorTable.NULLIF);
    registerEquivOp(SqlStdOperatorTable.COALESCE);

    registerTypeAppendOp(SqlStdOperatorTable.CAST);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a call to an operator into a {@link SqlCall} to the same
   * operator.
   *
   * <p>Called automatically via reflection.
   *
   * @param converter Converter
   * @param call      Call
   * @return Sql call
   */
  public SqlNode convertCall(
      RexToSqlNodeConverter converter,
      RexCall call) {
    if (get(call) == null) {
      return null;
    }

    final SqlOperator op = call.getOperator();
    final List<RexNode> operands = call.getOperands();

    final SqlNode[] exprs = convertExpressionList(converter, operands);
    if (exprs == null) {
      return null;
    }
    return new SqlBasicCall(
        op,
        exprs,
        SqlParserPos.ZERO);
  }

  private SqlNode[] convertExpressionList(
      RexToSqlNodeConverter converter,
      List<RexNode> nodes) {
    final SqlNode[] exprs = new SqlNode[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      RexNode node = nodes.get(i);
      exprs[i] = converter.convertNode(node);
      if (exprs[i] == null) {
        return null;
      }
    }
    return exprs;
  }

  /**
   * Creates and registers a convertlet for an operator in which
   * the SQL and Rex representations are structurally equivalent.
   *
   * @param op operator instance
   */
  protected void registerEquivOp(SqlOperator op) {
    registerOp(op, new EquivConvertlet(op));
  }

  /**
   * Creates and registers a convertlet for an operator in which
   * the SQL representation needs the result type appended
   * as an extra argument (e.g. CAST).
   *
   * @param op operator instance
   */
  private void registerTypeAppendOp(final SqlOperator op) {
    registerOp(
        op, (converter, call) -> {
          SqlNode[] operands =
              convertExpressionList(converter, call.operands);
          if (operands == null) {
            return null;
          }
          List<SqlNode> operandList =
              new ArrayList<>(Arrays.asList(operands));
          SqlDataTypeSpec typeSpec =
              SqlTypeUtil.convertTypeToSpec(call.getType());
          operandList.add(typeSpec);
          return new SqlBasicCall(
              op,
              operandList.toArray(new SqlNode[0]),
              SqlParserPos.ZERO);
        });
  }

  /**
   * Creates and registers a convertlet for the CASE operator,
   * which takes different forms for SQL vs Rex.
   *
   * @param op instance of CASE operator
   */
  private void registerCaseOp(final SqlOperator op) {
    registerOp(
        op, (converter, call) -> {
          assert op instanceof SqlCaseOperator;
          SqlNode[] operands =
              convertExpressionList(converter, call.operands);
          if (operands == null) {
            return null;
          }
          SqlNodeList whenList = new SqlNodeList(SqlParserPos.ZERO);
          SqlNodeList thenList = new SqlNodeList(SqlParserPos.ZERO);
          int i = 0;
          while (i < operands.length - 1) {
            whenList.add(operands[i]);
            ++i;
            thenList.add(operands[i]);
            ++i;
          }
          SqlNode elseExpr = operands[i];
          return op.createCall(null, SqlParserPos.ZERO, null, whenList, thenList, elseExpr);
        });
  }

  /** Convertlet that converts a {@link SqlCall} to a {@link RexCall} of the
   * same operator. */
  private class EquivConvertlet implements RexSqlConvertlet {
    private final SqlOperator op;

    EquivConvertlet(SqlOperator op) {
      this.op = op;
    }

    public SqlNode convertCall(RexToSqlNodeConverter converter, RexCall call) {
      SqlNode[] operands = convertExpressionList(converter, call.operands);
      if (operands == null) {
        return null;
      }
      return new SqlBasicCall(op, operands, SqlParserPos.ZERO);
    }
  }
}

// End RexSqlStandardConvertletTable.java
