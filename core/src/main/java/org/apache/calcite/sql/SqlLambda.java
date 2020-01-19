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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.UnmodifiableArrayList;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * A <code>SqlLambda</code> is a node of a parse tree which represents a lambda statement. It
 * warrants * its own node type just because we have a lot of methods to put somewhere.
 */
public class SqlLambda extends SqlCall {

  SqlNodeList parameters;
  SqlNode expression;

  //~ Constructors -----------------------------------------------------------


  public SqlLambda(SqlParserPos pos, SqlNodeList parameters,
      SqlNode expression) {
    super(pos);
    this.parameters = parameters;
    this.expression = expression;
  }

  /**
   * Creates a call to the lambda operator
   *
   * <br>LAMBDA<br>
   */
  public static SqlLambda createLambda(SqlParserPos pos, SqlNodeList parameters,
      SqlNode expression) {
    return new SqlLambda(pos, parameters, expression);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.LAMBDA;
  }

  public SqlOperator getOperator() {
    return SqlStdOperatorTable.LAMBDA;
  }

  public List<SqlNode> getOperandList() {
    return UnmodifiableArrayList.of(parameters, expression);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      parameters = (SqlNodeList) operand;
      break;
    case 1:
      expression = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public SqlNodeList getParameters() {
    return parameters;
  }

  public SqlNode getExpression() {
    return expression;
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    Map<String, RelDataType> varMap = ImmutableMap.of("A", intType);
    // validator.validateParameterizedExpression(expression, varMap);
  }
}
