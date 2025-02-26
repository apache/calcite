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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.UnmodifiableArrayList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static org.apache.calcite.sql.SqlPivot.stripList;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlLambda</code> is a node of a parse tree which
 * represents a lambda expression.
 */
public class SqlLambda extends SqlCall {

  public static final SqlOperator OPERATOR = new SqlLambdaOperator();

  SqlNodeList parameters;
  SqlNode expression;

  public SqlLambda(SqlParserPos pos, SqlNodeList parameters,
      SqlNode expression) {
    super(pos);
    this.parameters = parameters;
    this.expression = expression;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.LAMBDA;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return UnmodifiableArrayList.of(parameters, expression);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      parameters = requireNonNull((SqlNodeList) operand, "parameters");
      break;
    case 1:
      expression = requireNonNull(operand, "operand");
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (parameters.size() != 1) {
      writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA, stripList(parameters));
    } else {
      parameters.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword(OPERATOR.getName());
    expression.unparse(writer, leftPrec, rightPrec);
  }

  public SqlNodeList getParameters() {
    return parameters;
  }

  public SqlNode getExpression() {
    return expression;
  }

  /**
   * The {@code SqlLambdaOperator} represents a lambda expression.
   * The syntax :
   * {@code IDENTIFIER -> EXPRESSION} or {@code (IDENTIFIER, IDENTIFIER, ...) -> EXPRESSION}.
   */
  private static class SqlLambdaOperator extends SqlSpecialOperator {

    SqlLambdaOperator() {
      super("->", SqlKind.LAMBDA);
    }

    @Override public RelDataType deriveType(
        SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
      final SqlLambda lambdaExpr = (SqlLambda) call;
      final SqlLambdaScope lambdaScope = (SqlLambdaScope) scope;
      final List<String> paramNames = lambdaExpr.getParameters().stream()
          .map(SqlNode::toString)
          .collect(toImmutableList());
      final List<RelDataType> paramTypes = lambdaScope.getParameterTypes().values()
          .stream()
          .collect(toImmutableList());
      final RelDataType paramRowType =
          validator.getTypeFactory().createStructType(paramTypes, paramNames);
      final RelDataType returnType = validator.getValidatedNodeType(lambdaExpr.getExpression());
      return validator.getTypeFactory().createFunctionSqlType(paramRowType, returnType);
    }
  }
}
