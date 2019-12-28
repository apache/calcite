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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Locale;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * The <code>JSON_SET</code> function.
 */
public class SqlJsonSetFunction extends SqlFunction {
  public SqlJsonSetFunction() {
    super("JSON_SET",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.cascade(ReturnTypes.VARCHAR_2000,
            SqlTypeTransforms.FORCE_NULLABLE),
        null,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.from(3);
  }

  @Override protected void checkOperandCount(SqlValidator validator,
      SqlOperandTypeChecker argType, SqlCall call) {
    assert (call.operandCount() >= 3) && (call.operandCount() % 2 == 1);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final int count = callBinding.getOperandCount();
    for (int i = 1; i < count; i += 2) {
      RelDataType nameType = callBinding.getOperandType(i);
      if (!SqlTypeUtil.inCharFamily(nameType)) {
        if (throwOnFailure) {
          throw callBinding.newError(RESOURCE.expectedCharacter());
        }
        return false;
      }
      if (nameType.isNullable()) {
        if (throwOnFailure) {
          throw callBinding.newError(
              RESOURCE.argumentMustNotBeNull(
                  callBinding.operand(i).toString()));
        }
        return false;
      }
    }
    return true;
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    assert operandsCount % 2 == 1;
    StringBuilder sb = new StringBuilder();
    sb.append("{0}(");
    for (int i = 1; i < operandsCount; i++) {
      sb.append(String.format(Locale.ROOT, "{%d} ", i + 1));
    }
    sb.append("{1})");
    return sb.toString();
  }
}
