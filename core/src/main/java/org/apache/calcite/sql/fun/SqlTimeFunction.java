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
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

/**
 * <p>The Google BigQuery {@code TIME} function returns a time object
 * and can be invoked in one of three ways:</p>
 *
 * <ul>
 *   <li>TIME(hour, minute, second)</li>
 *   <li>TIME(timestamp, [time_zone])</li>
 *   <li>TIME(datetime)</li>
 * </ul>
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time">Documentation</a>
 */
public class SqlTimeFunction extends SqlFunction {

  SqlTimeFunction() {
    super("TIME", SqlKind.OTHER_FUNCTION, SqlTimeFunction::deduceReturnType, null,
        new TimeOperandTypeChecker(), SqlFunctionCategory.TIMEDATE);
  }

  private static RelDataType deduceReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    // The time object is nullable if any of its operands are nullable.
    final boolean isNullable = opBinding.getOperandType(0).isNullable()
        || (opBinding.getOperandCount() > 1 && opBinding.getOperandType(1).isNullable())
        || (opBinding.getOperandCount() > 2 && opBinding.getOperandType(2).isNullable());
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIME), isNullable);
  }

  /** Operand type checker for {@link SqlTimeFunction}. */
  private static class TimeOperandTypeChecker implements SqlOperandTypeChecker {

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      switch (callBinding.getOperandType(0).getSqlTypeName()) {
      case INTEGER:
        // Must be TIME(hour, minute, second) where all operands are integers.
        return callBinding.getOperandCount() == 3
            && callBinding.getOperandType(1).getSqlTypeName() == SqlTypeName.INTEGER
            && callBinding.getOperandType(2).getSqlTypeName() == SqlTypeName.INTEGER;
      case TIMESTAMP:
        // Must be DATE(timestamp[, time_zone]).
        return callBinding.getOperandCount() == 1
            || (callBinding.getOperandCount() == 2
                && SqlTypeName.CHAR_TYPES
                    .contains(callBinding.getOperandType(1).getSqlTypeName()));
      default:
        // TODO: Support TIME(datetime).
        return false;
      }
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.between(1, 3); // Takes 1, 2, or 3 parameters.
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return String.format(
          Locale.ROOT,
          "%s(INTEGER, INTEGER, INTEGER) | %s(TIMESTAMP[, STRING]) | %s(DATETIME)",
          opName, opName, opName);
    }

    @Override public boolean isOptional(int i) {
      return i > 1; // Only the first parameter is required.
    }
  }
}
