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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;

/**
 * <p> The Google BigQuery style datetime formatting functions. This is a generic type representing
 * one of the following:</p>
 *
 * <ul>
 *   <li>{@code FORMAT_TIME(format_string, time_object)}
 *   <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time">ref</a></li>
 *   <li>{@code FORMAT_DATE(format_string, date_expr)}
 *   <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#format_date">ref</a></li>
 *   <li>{@code FORMAT_TIMESTAMP(format_string, timestamp[, time_zone])}
 *  <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp">ref</a></li>
 *  <li>{@code FORMAT_DATETIME(format_string, timestamp[, time_zone])}
 *  <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime">ref</a></li>
 * </ul>
 */
public class SqlBigQueryFormatDatetimeFunction extends SqlFunction {

  SqlBigQueryFormatDatetimeFunction(String name, SqlTypeName type) {
    super(name, SqlKind.valueOf(name), ReturnTypes.VARCHAR_2000_NULLABLE, null,
        new FormatDatetimeOperandTypeChecker(type), SqlFunctionCategory.TIMEDATE);
  }

  /**
   * Operand type checker for {@code SqlBigQueryFormatDatetimeFunctions}.
   */
  private static class FormatDatetimeOperandTypeChecker implements SqlOperandTypeChecker {

    private SqlTypeName typeName;

    FormatDatetimeOperandTypeChecker(SqlTypeName typeName) {
      this.typeName = typeName;
    }

    private boolean isCharType(SqlCallBinding callBinding, int ordinal) {
      return callBinding.getOperandType(ordinal).getSqlTypeName().getFamily()
          == SqlTypeFamily.CHARACTER;
    }

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      int opCount = callBinding.getOperandCount();
      boolean validOpCount = (this.typeName == TIME || this.typeName == DATE)
          ? opCount == 2
          // TIMESTAMP and DATETIME allow a third optional timezone operand
          : opCount == 2 || (opCount == 3 && isCharType(callBinding, 3));
      boolean valid = validOpCount
          && isCharType(callBinding, 0)
          && callBinding.getOperandType(1).getSqlTypeName() == this.typeName;
      if (!valid && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return valid;
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      switch (this.typeName) {
      case TIME:
      case DATE:
        return SqlOperandCountRanges.of(2);
      default:
        // Optional third timezone operand for DATETIME and TIMESTAMP types
        return SqlOperandCountRanges.between(2, 3);
      }
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      switch (this.typeName) {
      case TIME:
      case DATE:
        return String.format(Locale.ROOT, "%s(STRING, %s)", opName, this.typeName.getName());
      default:
        return String.format(Locale.ROOT, "%s(STRING, %s [, STRING])", opName,
            this.typeName.getName());
      }
    }
  }
}
