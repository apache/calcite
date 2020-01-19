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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class SqlLambdaOperator extends SqlOperator {

  public static final SqlLambdaOperator INSTANCE = new SqlLambdaOperator();

  //~ Instance fields --------------------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlLambdaOperator.
   */
  SqlLambdaOperator() {
    super(
        "LAMBDA",
        SqlKind.LAMBDA,
        32,
        false,
        ReturnTypes.ARG0_NULLABLE,
        InferTypes.RETURN_TYPE,
        OperandTypes.VARIADIC);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.any();
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return true;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
    writer.sep(getName());

    call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
    writer.endList(frame);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    SqlLambda lambda = (SqlLambda) call;
   // lambda.expression.validateExpr(validator, operandScope);
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    assert functionQualifier == null;
    assert operands.length == 2;
    return new SqlLambda(pos, (SqlNodeList) operands[0], operands[1]);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return validateOperands(validator, scope, call);
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return opBinding.getTypeFactory().createSqlType(SqlTypeName.LAMBDA);
  }
}
