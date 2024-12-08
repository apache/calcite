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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * The <code>CONVERT</code> function, which converts a string from one character
 * set to another.
 *
 * <p>Also the base class for the {@code CONVERT(string USING transcoding)}
 * and {@code TRANSLATE(string USING transcoding)} functions (see
 * {@link SqlTranslateFunction}).
 *
 * <p>The SQL syntax is
 *
 * <blockquote><pre>
 *   {@code CONVERT(characterString, sourceCharset, destCharset)}
 * </pre></blockquote>
 */
public class SqlConvertFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  protected SqlConvertFunction(String name) {
    this(name, SqlKind.CONVERT, ReturnTypes.ARG0, null, null,
        SqlFunctionCategory.STRING);
  }

  protected SqlConvertFunction(String name, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, category);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    // The base method validates all operands. We override because
    // we don't want to validate the Charset as identifier.
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 3;
    operands.get(0).validateExpr(validator, scope);
    // validate if the Charsets are legal.
    assert operands.get(1) instanceof SqlIdentifier;
    final String src_charset = operands.get(1).toString();
    SqlUtil.getCharset(src_charset);
    assert operands.get(2) instanceof SqlIdentifier;
    final String dest_charset = operands.get(2).toString();
    SqlUtil.getCharset(dest_charset);
    super.validateQuantifier(validator, call);
  }

  @Override public <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call,
      boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (onlyExpressions) {
      // Both operand[1] and operand[2] are not an expression, but Charset
      // identifier
      argHandler.visitChild(visitor, call, 0, call.operand(0));
    } else {
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType ret = opBinding.getOperandType(0);
    if (SqlTypeUtil.isNull(ret)) {
      return ret;
    }
    final String descCharsetName;
    if (opBinding instanceof SqlCallBinding) {
      descCharsetName = ((SqlCallBinding) opBinding).getCall().operand(2).toString();
    } else {
      descCharsetName = ((RexCallBinding) opBinding).getStringLiteralOperand(2);
    }
    assert descCharsetName != null;
    Charset descCharset = SqlUtil.getCharset(descCharsetName);
    return typeFactory.createTypeWithCharsetAndCollation(ret, descCharset, getCollation(ret));
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // don't need to derive type for Charsets
    RelDataType nodeType =
        validator.deriveType(scope, call.operand(0));
    requireNonNull(nodeType, "nodeType");
    RelDataType ret = validateOperands(validator, scope, call);
    if (SqlTypeUtil.isNull(ret)) {
      return ret;
    }
    // descCharset should be the returning type Charset of CONVERT
    Charset descCharset = SqlUtil.getCharset(call.operand(2).toString());
    return validator.getTypeFactory()
        .createTypeWithCharsetAndCollation(ret, descCharset, getCollation(ret));
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // type of operand[0] should be Character or NULL
    final RelDataType t = callBinding.getOperandType(0);
    if (SqlTypeUtil.isNull(t)) {
      // convert(null, src_charset, dest_charset) is supported
      return true;
    }
    if (!SqlTypeUtil.inCharFamily(t)) {
      if (throwOnFailure) {
        throw callBinding.newValidationError(
            RESOURCE.unsupportedTypeInConvertFunc(t.getFullTypeString(),
                "CONVERT", "CHARACTER"));
      }
      return false;
    }
    return true;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (SqlNode node : call.getOperandList()) {
      writer.sep(",");
      node.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  @Override public String getSignatureTemplate(final int operandsCount) {
    //noinspection SwitchStatementWithTooFewBranches
    switch (operandsCount) {
    case 3:
      return "{0}({1}, {2}, {3})";
    default:
      throw new IllegalStateException("operandsCount should be 3, got "
          + operandsCount);
    }
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(3);
  }
}
