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
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.nio.charset.Charset;
import java.util.List;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;

import static java.util.Objects.requireNonNull;

/** Oracle's "CONVERT(charValue, destCharsetName[, srcCharsetName])" function.
 *
 * <p>It has a slight different semantics to standard SQL's
 * {@link SqlStdOperatorTable#CONVERT} function on operands' order, and default
 * charset will be used if the {@code srcCharsetName} is not specified.
 *
 * <p>Returns null if {@code charValue} is null or empty. */
public class SqlOracleConvertFunction extends SqlConvertFunction {
    //~ Constructors -----------------------------------------------------------

  public SqlOracleConvertFunction(String name) {
    super(name, SqlKind.CONVERT_ORACLE, ReturnTypes.ARG0, null, null,
        SqlFunctionCategory.STRING);
  }

    //~ Methods ----------------------------------------------------------------

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    final List<SqlNode> operands = call.getOperandList();
    operands.get(0).validateExpr(validator, scope);
    // validate if the Charsets are legal.
    assert operands.get(1) instanceof SqlIdentifier;
    final String src_charset = operands.get(1).toString();
    SqlUtil.getCharset(src_charset);
    if (operands.size() == 3) {
      assert operands.get(2) instanceof SqlIdentifier;
      final String dest_charset = operands.get(2).toString();
      SqlUtil.getCharset(dest_charset);
    }
    super.validateQuantifier(validator, call);
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    final RelDataType ret = opBinding.getOperandType(0);
    if (SqlTypeUtil.isNull(ret)) {
      return ret;
    }
    final String descCharsetName;
    if (opBinding instanceof SqlCallBinding) {
      descCharsetName = ((SqlCallBinding) opBinding).getCall().operand(1).toString();
    } else {
      descCharsetName = ((RexCallBinding) opBinding).getStringLiteralOperand(1);
    }
    assert descCharsetName != null;
    Charset descCharset = SqlUtil.getCharset(descCharsetName);
    return opBinding
        .getTypeFactory().createTypeWithCharsetAndCollation(ret, descCharset, getCollation(ret));
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    RelDataType nodeType =
        validator.deriveType(scope, call.operand(0));
    requireNonNull(nodeType, "nodeType");
    RelDataType ret = validateOperands(validator, scope, call);
    if (SqlTypeUtil.isNull(ret)) {
      return ret;
    }
    Charset descCharset = SqlUtil.getCharset(call.operand(1).toString());
    return validator.getTypeFactory()
        .createTypeWithCharsetAndCollation(ret, descCharset, getCollation(ret));
  }

  @Override public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0}({1}, {2})";
    case 3:
      return "{0}({1}, {2}, {3})";
    default:
      throw new IllegalStateException("operandsCount should be 2 or 3, got "
          + operandsCount);
    }
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }
}
