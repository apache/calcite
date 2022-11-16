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
 * The Google BigQuery {@code TIMESTAMP} function returns a timestamp object
 * and can be invoked in 3 ways:
 *     TIMESTAMP(string_expression[, time_zone])
 *     TIMESTAMP(date_expression[, time_zone])
 *     TIMESTAMP(datetime_expression[, time_zone])
 *
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp
 */
public class SqlTimestampFunction extends SqlFunction {

  SqlTimestampFunction() {
    super("TIMESTAMP", SqlKind.OTHER_FUNCTION, SqlTimestampFunction::deduceReturnType, null,
        new TimestampOperandTypeChecker(), SqlFunctionCategory.TIMEDATE);
  }

  private static RelDataType deduceReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    // The timestamp object is nullable if any of its operands are nullable.
    final boolean isNullable = opBinding.getOperandType(0).isNullable()
        || (opBinding.getOperandCount() > 1 && opBinding.getOperandType(1).isNullable());
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP), isNullable);
  }

  /** Operand type checker for {@link SqlTimestampFunction}. */
  private static class TimestampOperandTypeChecker implements SqlOperandTypeChecker {

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      switch (callBinding.getOperandType(0).getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
      case DATE:
        // No matter what the first operand is, the second is an optional string.
        return callBinding.getOperandCount() == 1
            || (callBinding.getOperandCount() == 2
                && SqlTypeName.CHAR_TYPES
                    .contains(callBinding.getOperandType(1).getSqlTypeName()));
      default:
        // TODO: Support TIMESTAMP(datetime_expression[, time_zone]).
        return false;
      }
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.between(1, 2); // Takes 1 or 2 parameters.
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return String.format(
          Locale.ROOT,
          "%s(STRING[, STRING]) | %s(DATE[, STRING]) | %s(DATETIME[, STRING])",
          opName, opName, opName);
    }

    @Override public boolean isOptional(int i) {
      return i > 1; // Only the first parameter is required.
    }
  }
}
