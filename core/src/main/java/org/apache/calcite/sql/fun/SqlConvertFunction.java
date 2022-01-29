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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Common base for the <code>CONVERT</code> function.
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>CONVERT(<i>character String</i>, <i>sourceCharset</i>,
 * <i>destCharset</i>)</code>
 * </blockquote>
 */
public class SqlConvertFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  protected SqlConvertFunction(String name) {
    super(
        name,
        SqlKind.CONVERT,
        ReturnTypes.ARG0,
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void validateCall(
          SqlCall call,
          SqlValidator validator,
          SqlValidatorScope scope,
          SqlValidatorScope operandScope) {
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

  @Override public <R> void acceptCall(
          SqlVisitor<R> visitor,
          SqlCall call,
          boolean onlyExpressions,
          SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (onlyExpressions) {
      // both operand[1] and operand[2] are not an expression, but Charset identifier
      argHandler.visitChild(visitor, call, 0, call.operand(0));
    } else {
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @Override public RelDataType deriveType(
          SqlValidator validator,
          SqlValidatorScope scope,
          SqlCall call) {
    // special case for CONVERT: don't need to derive type for Charsets
    RelDataType nodeType =
            validator.deriveType(scope, call.operand(0));
    assert nodeType != null;
    return validateOperands(validator, scope, call);
  }

  @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding,
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

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (SqlNode node : call.getOperandList()) {
      writer.sep(",");
      node.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  @Override public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 3:
      return "{0}({1}, {2}, {3})";
    default:
      break;
    }
    throw new IllegalStateException("operandsCount should be 3, got " + operandsCount);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(3);
  }
}
