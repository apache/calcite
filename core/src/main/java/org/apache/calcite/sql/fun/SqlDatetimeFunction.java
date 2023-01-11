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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

/**
 * <p>The Google BigQuery {@code DATETIME} function returns a Calcite {@code TIMESTAMP}
 * and can be invoked in one of three ways.</p>
 *
 * <ul>
 *   <li>DATETIME(year, month, day, hour, minute, second)</li>
 *   <li>DATETIME(date_expression[, time_expression])</li>
 *   <li>DATETIME(timestamp_expression[, time_zone])</li>
 * </ul>
 *
 * <p>For a BigQuery function that returns a Calcite {@code TIMESTAMP WITH LOCAL TIME ZONE},
 * see {@link SqlTimestampFunction}.</p>
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp">Documentation</a>
 */
public class SqlDatetimeFunction extends SqlFunction {

  SqlDatetimeFunction() {
    super("DATETIME", SqlKind.OTHER_FUNCTION, SqlDatetimeFunction::deduceReturnType, null,
        new DatetimeOperandTypeChecker(), SqlFunctionCategory.TIMEDATE);
  }

  private static RelDataType deduceReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP), areAnyOperandsNullable(opBinding));
  }

  private static boolean areAnyOperandsNullable(SqlOperatorBinding opBinding) {
    final int operandCount = opBinding.getOperandCount();
    for (int i = 0; i < operandCount; i++) {
      if (opBinding.getOperandType(i).isNullable()) {
        return true;
      }
    }
    return false;
  }

  /** Operand type checker for {@link SqlDatetimeFunction}. */
  private static class DatetimeOperandTypeChecker implements SqlOperandTypeChecker {

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      switch (callBinding.getOperandType(0).getSqlTypeName()) {
      case INTEGER:
        // Must be DATETIME(year, month, day, hour, minute, second).
        return callBinding.getOperandCount() == 6
            && callBinding.getOperandType(1).getSqlTypeName() == SqlTypeName.INTEGER
            && callBinding.getOperandType(2).getSqlTypeName() == SqlTypeName.INTEGER
            && callBinding.getOperandType(3).getSqlTypeName() == SqlTypeName.INTEGER
            && callBinding.getOperandType(4).getSqlTypeName() == SqlTypeName.INTEGER
            && callBinding.getOperandType(5).getSqlTypeName() == SqlTypeName.INTEGER;
      case DATE:
        // Must be DATETIME(date_expression[, time_expression]).
        return callBinding.getOperandCount() == 1
            || (callBinding.getOperandCount() == 2
                && callBinding.getOperandType(1).getSqlTypeName() == SqlTypeName.TIME);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Must be DATETIME(timestamp_expression[, time_zone]).
        return callBinding.getOperandCount() == 1
            || (callBinding.getOperandCount() == 2
                && SqlTypeName.CHAR_TYPES
                    .contains(callBinding.getOperandType(1).getSqlTypeName()));
      default:
        return false;
      }
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return OPERAND_COUNT_RANGE;
    }

    private static final SqlOperandCountRange OPERAND_COUNT_RANGE = new SqlOperandCountRange() {
      @Override public boolean isValidCount(int count) {
        return count == 1 || count == 2 || count == 6;
      }

      @Override public int getMin() {
        return 1;
      }

      @Override public int getMax() {
        return 6;
      }
    };

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return String.format(
          Locale.ROOT,
          "%s(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER) | %s(DATE[, TIME]) | %s(TIMESTAMP[, STRING])",
          opName, opName, opName);
    }

    @Override public boolean isOptional(int i) {
      return i > 1; // Only the first parameter is required.
    }
  }
}
