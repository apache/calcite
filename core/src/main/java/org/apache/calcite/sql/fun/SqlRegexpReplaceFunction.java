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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import java.util.ArrayList;
import java.util.List;

/**
 * The REGEXP_REPLACE(source_string, pattern, replacement [, pos, occurrence, match_type])
 * searches for a regular expression pattern and replaces every occurrence of the pattern
 * with the specified string.
 * */
public class SqlRegexpReplaceFunction extends SqlFunction {

  public SqlRegexpReplaceFunction() {
    super("REGEXP_REPLACE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
            SqlTypeTransforms.TO_NULLABLE),
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(3, 6);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
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
      if (i == 3) {
        families.add(SqlTypeFamily.INTEGER);
      }
      if (i == 4) {
        families.add(SqlTypeFamily.INTEGER);
      }
      if (i == 5) {
        families.add(SqlTypeFamily.STRING);
      }
    }
    return OperandTypes.family(families.toArray(new SqlTypeFamily[0]))
        .checkOperandTypes(callBinding, throwOnFailure);
  }
}

// End SqlRegexpReplaceFunction.java
