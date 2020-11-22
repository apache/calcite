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
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCharset;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;

/**
 * A unary operator.
 */
public class SqlPrefixOperator extends SqlOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlPrefixOperator(
      String name,
      SqlKind kind,
      int prec,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        leftPrec(prec, true),
        rightPrec(prec, true),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.PREFIX;
  }

  @Override public @Nullable String getSignatureTemplate(final int operandsCount) {
    Util.discard(operandsCount);
    return "{0}{1}";
  }

  @Override protected RelDataType adjustType(
      SqlValidator validator,
      SqlCall call,
      RelDataType type) {
    if (SqlTypeUtil.inCharFamily(type)) {
      // Determine coercibility and resulting collation name of
      // unary operator if needed.
      RelDataType operandType =
          validator.getValidatedNodeType(call.operand(0));
      if (null == operandType) {
        throw new AssertionError("operand's type should have been derived");
      }
      if (SqlTypeUtil.inCharFamily(operandType)) {
        SqlCollation collation = getCollation(operandType);
        type =
            validator.getTypeFactory()
                .createTypeWithCharsetAndCollation(
                    type,
                    getCharset(type),
                    collation);
      }
    }
    return type;
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    if (getName().equals("-")) {
      return call.getOperandMonotonicity(0).reverse();
    }

    return super.getMonotonicity(call);
  }

  @Override public boolean validRexOperands(int count, Litmus litmus) {
    if (count != 1) {
      return litmus.fail("wrong operand count {} for {}", count, this);
    }
    return litmus.succeed();
  }
}
