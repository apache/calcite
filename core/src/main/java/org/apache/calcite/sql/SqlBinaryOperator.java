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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.nio.charset.Charset;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator extends SqlOperator {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlBinaryOperator.
   *
   * @param name                 Name of operator
   * @param kind                 Kind
   * @param prec                 Precedence
   * @param leftAssoc            Left-associativity
   * @param returnTypeInference  Strategy to infer return type
   * @param operandTypeInference Strategy to infer operand types
   * @param operandTypeChecker   Validator for operand types
   */
  public SqlBinaryOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        leftPrec(prec, leftAssoc),
        rightPrec(prec, leftAssoc),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.BINARY;
  }

  public String getSignatureTemplate(final int operandsCount) {
    Util.discard(operandsCount);

    // op0 opname op1
    return "{1} {0} {2}";
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns true for most operators but false for the '.' operator;
   * consider
   *
   * <blockquote>
   * <pre>x.y + 5 * 6</pre>
   * </blockquote>
   */
  @Override boolean needsSpace() {
    return !getName().equals(".");
  }

  protected RelDataType adjustType(
      SqlValidator validator,
      final SqlCall call,
      RelDataType type) {
    return convertType(validator, call, type);
  }

  private RelDataType convertType(SqlValidator validator, SqlCall call, RelDataType type) {
    RelDataType operandType0 =
        validator.getValidatedNodeType(call.operand(0));
    RelDataType operandType1 =
        validator.getValidatedNodeType(call.operand(1));
    if (SqlTypeUtil.inCharFamily(operandType0)
        && SqlTypeUtil.inCharFamily(operandType1)) {
      Charset cs0 = operandType0.getCharset();
      Charset cs1 = operandType1.getCharset();
      assert (null != cs0) && (null != cs1)
          : "An implicit or explicit charset should have been set";
      if (!cs0.equals(cs1)) {
        throw validator.newValidationError(call,
            RESOURCE.incompatibleCharset(getName(), cs0.name(), cs1.name()));
      }

      SqlCollation collation0 = operandType0.getCollation();
      SqlCollation collation1 = operandType1.getCollation();
      assert (null != collation0) && (null != collation1)
          : "An implicit or explicit collation should have been set";

      // Validation will occur inside getCoercibilityDyadicOperator...
      SqlCollation resultCol =
          SqlCollation.getCoercibilityDyadicOperator(
              collation0,
              collation1);

      if (SqlTypeUtil.inCharFamily(type)) {
        type =
            validator.getTypeFactory()
                .createTypeWithCharsetAndCollation(
                    type,
                    type.getCharset(),
                    resultCol);
      }
    }
    return type;
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    RelDataType type = super.deriveType(validator, scope, call);
    return convertType(validator, call, type);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    if (getName().equals("/")) {
      final SqlMonotonicity mono0 = call.getOperandMonotonicity(0);
      final SqlMonotonicity mono1 = call.getOperandMonotonicity(1);
      if (mono0 == null || mono1 == null) {
        return null;
      }
      if (mono1 == SqlMonotonicity.CONSTANT) {
        if (call.isOperandLiteral(1, false)) {
          switch (call.getOperandLiteralValue(1, BigDecimal.class).signum()) {
          case -1:

            // mono / -ve constant --> reverse mono, unstrict
            return mono0.reverse().unstrict();
          case 0:

            // mono / zero --> constant (infinity!)
            return SqlMonotonicity.CONSTANT;
          default:

            // mono / +ve constant * mono1 --> mono, unstrict
            return mono0.unstrict();
          }
        }
      }
    }

    return super.getMonotonicity(call);
  }

  @Override public boolean validRexOperands(int count, Litmus litmus) {
    if (count != 2) {
      // Special exception for AND and OR.
      if ((this == SqlStdOperatorTable.AND
          || this == SqlStdOperatorTable.OR)
          && count > 2) {
        return true;
      }
      return litmus.fail("wrong operand count {} for {}", count, this);
    }
    return litmus.succeed();
  }
}
