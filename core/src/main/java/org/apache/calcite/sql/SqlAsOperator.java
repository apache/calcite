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
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * The <code>AS</code> operator associates an expression with an alias.
 */
public class SqlAsOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AS operator.
   */
  public SqlAsOperator() {
    this(
        "AS",
        SqlKind.AS,
        20,
        true,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY_ANY);
  }

  protected SqlAsOperator(String name, SqlKind kind, int prec,
      boolean leftAssoc, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(name, kind, prec, leftAssoc, returnTypeInference,
        operandTypeInference, operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() >= 2;
    final SqlWriter.Frame frame =
        writer.startList(
            SqlWriter.FrameTypeEnum.SIMPLE);
    call.operand(0).unparse(writer, leftPrec, getLeftPrec());
    final boolean needsSpace = true;
    writer.setNeedWhitespace(needsSpace);
    if (writer.getDialect().allowsAs()) {
      writer.sep("AS");
      writer.setNeedWhitespace(needsSpace);
    }
    call.operand(1).unparse(writer, getRightPrec(), rightPrec);
    if (call.operandCount() > 2) {
      final SqlWriter.Frame frame1 =
          writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
      for (SqlNode operand : Util.skip(call.getOperandList(), 2)) {
        writer.sep(",", false);
        operand.unparse(writer, 0, 0);
      }
      writer.endList(frame1);
    }
    writer.endList(frame);
  }

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    // The base method validates all operands. We override because
    // we don't want to validate the identifier.
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 2;
    assert operands.get(1) instanceof SqlIdentifier;
    operands.get(0).validateExpr(validator, scope);
    SqlIdentifier id = (SqlIdentifier) operands.get(1);
    if (!id.isSimple()) {
      throw validator.newValidationError(id,
          RESOURCE.aliasMustBeSimpleIdentifier());
    }
  }

  public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (onlyExpressions) {
      // Do not visit operands[1] -- it is not an expression.
      argHandler.visitChild(visitor, call, 0, call.operand(0));
    } else {
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // special case for AS:  never try to derive type for alias
    RelDataType nodeType =
        validator.deriveType(scope, call.operand(0));
    assert nodeType != null;
    return validateOperands(validator, scope, call);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return call.getOperandMonotonicity(0);
  }
}

// End SqlAsOperator.java
