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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.ArrayList;
import java.util.List;

/**
 * The PostgreSQL
 * REGEXP_REPLACE(source_string, pattern, replacement [, pos, [, occurrence]] [, match_type])
 * searches for a regular expression pattern and replaces every occurrence of the pattern
 * with the specified string. It differs from the standard REGEXP_REPLACE in that there is
 * no type inference for position or occurrence parameters and allows the match_type parameter
 * to be used as the 3rd, 4th, or 5th parameters instead.
 */
public class SqlPgRegexpReplaceFunction extends SqlRegexpReplaceFunction {

  @Override public String getAllowedSignatures(String opNameToUse) {
    return opNameToUse + "(VARCHAR, VARCHAR, VARCHAR [, INTEGER [, INTEGER]] [, VARCHAR])";
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final int operandCount = callBinding.getOperandCount();
    assert operandCount >= 3;
    if (operandCount == 3) {
      return OperandTypes.STRING_STRING_STRING
          .checkOperandTypes(callBinding, throwOnFailure);
    }
    final List<SqlTypeFamily> families = new ArrayList<>();
    families.add(SqlTypeFamily.STRING);
    families.add(SqlTypeFamily.STRING);
    families.add(SqlTypeFamily.STRING);
    for (int i = 3; i < operandCount; i++) {
      // The argument type at index 3 and 4 can be either integer or string.
      // Index 3 can either be the start pos or the flags.
      // Index 4 can either be the end pos of the flags.
      // If the flags get used at index 3 or 4, there can be no more arguments, since index 5
      // can only be flags.
      if (i == 3) {
        if (SqlTypeFamily.STRING.contains(callBinding.getOperandType(i))) {
          families.add(SqlTypeFamily.STRING);
          break;
        }
        families.add(SqlTypeFamily.INTEGER);
      } else if (i == 4) {
        if (SqlTypeFamily.STRING.contains(callBinding.getOperandType(i))) {
          families.add(SqlTypeFamily.STRING);
          break;
        }
        families.add(SqlTypeFamily.INTEGER);
      } else if (i == 5) {
        families.add(SqlTypeFamily.STRING);
      }
    }

    if (throwOnFailure && operandCount != families.size()) {
      throw callBinding.newValidationSignatureError();
    }
    return OperandTypes.family(families.toArray(new SqlTypeFamily[0]))
        .checkOperandTypes(callBinding, throwOnFailure);
  }
}
