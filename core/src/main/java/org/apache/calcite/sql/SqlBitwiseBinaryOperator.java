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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>SqlBitwiseBinaryOperator</code> is a binary bitwise operator.
 */
public class SqlBitwiseBinaryOperator extends SqlBinaryOperator {

  public SqlBitwiseBinaryOperator(
      String name,
      SqlKind kind,
      int prec,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        true,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        operandTypeChecker);
  }

  /**
   * Validates the bitwise binary operator.
   * If we look up the bitwise OR "|" in the operator table by name, we may find two operators:
   * {@link SqlLibraryOperators#BITWISE_OR} and {@link SqlStdOperatorTable#PATTERN_ALTER},
   * both of which are instances of {@link SqlBinaryOperator}. It will throw an exception if we
   * don't override this method, because the {@link SqlStdOperatorTable#PATTERN_ALTER} don't have
   * an operand type checker.
   */
  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      assert nodeType != null;
    }

    final SqlOperatorTable opTab = validator.getOperatorTable();
    final List<SqlOperator> sqlOperators = new ArrayList<>();
    opTab.lookupOperatorOverloads(getNameAsId(), null, getSyntax(),
        sqlOperators, validator.getCatalogReader().nameMatcher());

    assert sqlOperators.contains(this);
    return call.getOperator().validateOperands(validator, scope, call);
  }
}
