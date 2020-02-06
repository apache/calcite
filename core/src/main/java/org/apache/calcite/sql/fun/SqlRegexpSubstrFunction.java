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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

/**
 * The REGEXP_SUBSTR(source_string, regex_pattern, [, pos, occurrence, match_type]) extracts a
 * substring from source_string that matches a regular expression specified by regex_pattern.
 */
public class SqlRegexpSubstrFunction extends SqlFunction {

  public SqlRegexpSubstrFunction() {
    super(
        "REGEXP_SUBSTR",
        SqlKind.REGEXP_SUBSTR,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 5);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    final int operandCount = callBinding.getOperandCount();
    for (int i = 0; i < 2; i++) {
      if (!OperandTypes.STRING.checkSingleOperandType(
          callBinding, callBinding.operand(i), 0, throwOnFailure)) {
        return false;
      }
    }
    for (int i = 2; i < operandCount; i++) {
      if (i == 2 && !OperandTypes.INTEGER.checkSingleOperandType(
          callBinding, callBinding.operand(i), 0, throwOnFailure)) {
        return false;
      }
      if (i == 3 && !OperandTypes.INTEGER.checkSingleOperandType(
          callBinding, callBinding.operand(i), 0, throwOnFailure)) {
        return false;
      }
      if (i == 4 && !OperandTypes.STRING.checkSingleOperandType(
          callBinding, callBinding.operand(i), 0, throwOnFailure)) {
        return false;
      }
    }
    return true;
  }
}
