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
package org.eigenbase.sql;

import java.math.*;

import java.nio.charset.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import static org.eigenbase.util.Static.RESOURCE;

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
   * Returns whether this operator should be surrounded by space when
   * unparsed.
   *
   * <p>Returns true for most operators but false for the '.' operator;
   * consider
   *
   * <blockquote>
   * <pre>x.y + 5 * 6</pre>
   * </blockquote>
   *
   * @return whether this operator should be surrounded by space
   */
  boolean needsSpace() {
    return !getName().equals(".");
  }

  protected RelDataType adjustType(
      SqlValidator validator,
      final SqlCall call,
      RelDataType type) {
    RelDataType operandType1 =
        validator.getValidatedNodeType(call.operand(0));
    RelDataType operandType2 =
        validator.getValidatedNodeType(call.operand(1));
    if (SqlTypeUtil.inCharFamily(operandType1)
        && SqlTypeUtil.inCharFamily(operandType2)) {
      Charset cs1 = operandType1.getCharset();
      Charset cs2 = operandType2.getCharset();
      assert (null != cs1) && (null != cs2)
          : "An implicit or explicit charset should have been set";
      if (!cs1.equals(cs2)) {
        throw validator.newValidationError(call,
            RESOURCE.incompatibleCharset(getName(), cs1.name(), cs2.name()));
      }

      SqlCollation col1 = operandType1.getCollation();
      SqlCollation col2 = operandType2.getCollation();
      assert (null != col1) && (null != col2)
          : "An implicit or explicit collation should have been set";

      // validation will occur inside getCoercibilityDyadicOperator...
      SqlCollation resultCol =
          SqlCollation.getCoercibilityDyadicOperator(
              col1,
              col2);

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

    RelDataType operandType1 =
        validator.getValidatedNodeType(call.operand(0));
    RelDataType operandType2 =
        validator.getValidatedNodeType(call.operand(1));
    if (SqlTypeUtil.inCharFamily(operandType1)
        && SqlTypeUtil.inCharFamily(operandType2)) {
      Charset cs1 = operandType1.getCharset();
      Charset cs2 = operandType2.getCharset();
      assert (null != cs1) && (null != cs2)
          : "An implicit or explicit charset should have been set";
      if (!cs1.equals(cs2)) {
        throw validator.newValidationError(call,
            RESOURCE.incompatibleCharset(getName(), cs1.name(), cs2.name()));
      }

      SqlCollation col1 = operandType1.getCollation();
      SqlCollation col2 = operandType2.getCollation();
      assert (null != col1) && (null != col2)
          : "An implicit or explicit collation should have been set";

      // validation will occur inside getCoercibilityDyadicOperator...
      SqlCollation resultCol =
          SqlCollation.getCoercibilityDyadicOperator(
              col1,
              col2);

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

  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    if (getName().equals("/")) {
      final SqlNode operand0 = call.operand(0);
      final SqlNode operand1 = call.operand(1);
      final SqlMonotonicity mono0 = operand0.getMonotonicity(scope);
      final SqlMonotonicity mono1 = operand1.getMonotonicity(scope);
      if (mono1 == SqlMonotonicity.CONSTANT) {
        if (operand1 instanceof SqlLiteral) {
          SqlLiteral literal = (SqlLiteral) operand1;
          switch (
              literal.bigDecimalValue().compareTo(
                  BigDecimal.ZERO)) {
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

    return super.getMonotonicity(call, scope);
  }

  @Override
  public boolean validRexOperands(int count, boolean fail) {
    if (count != 2) {
      // Special exception for AND and OR.
      if ((this == SqlStdOperatorTable.AND
          || this == SqlStdOperatorTable.OR)
          && count > 2) {
        return true;
      }
      assert !fail : "wrong operand count " + count + " for " + this;
      return false;
    }
    return true;
  }
}

// End SqlBinaryOperator.java
