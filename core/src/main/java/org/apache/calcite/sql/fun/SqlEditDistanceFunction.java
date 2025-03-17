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

import java.util.ArrayList;
import java.util.List;

/**
 * The EDIT_DISTANCE(string1, string2 [, ci, cd, cs, ct ])
 * measures the similarity between two strings.
 * */
public class SqlEditDistanceFunction extends SqlFunction {

  public SqlEditDistanceFunction() {
    super("EDIT_DISTANCE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null, null, SqlFunctionCategory.NUMERIC);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 6);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final int operandCount = callBinding.getOperandCount();
    assert operandCount >= 2;
    if (operandCount == 2) {
      return OperandTypes.STRING_STRING
          .checkOperandTypes(callBinding, throwOnFailure);
    }
    final List<SqlTypeFamily> families = new ArrayList<>();
    families.add(SqlTypeFamily.STRING);
    families.add(SqlTypeFamily.STRING);
    for (int i = 2; i < operandCount; i++) {
      families.add(SqlTypeFamily.INTEGER);
    }
    return OperandTypes.family(families.toArray(new SqlTypeFamily[0]))
        .checkOperandTypes(callBinding, throwOnFailure);
  }
}
