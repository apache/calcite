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
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * SqlSessionTableFunction implements an operator for per-key sessionization. It allows
 * four parameters:
 * 1. a table.
 * 2. a descriptor to provide a watermarked column name from the input table.
 * 3. a descriptor to provide a column as key, on which sessionization will be applied.
 * 4. an interval parameter to specify a inactive activity gap to break sessions.
 */
public class SqlSessionTableFunction extends SqlWindowTableFunction {
  public SqlSessionTableFunction() {
    super(SqlKind.SESSION.name());
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(4);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode operand0 = callBinding.operand(0);
    final SqlValidator validator = callBinding.getValidator();
    final RelDataType type = validator.getValidatedNodeType(operand0);
    if (type.getSqlTypeName() != SqlTypeName.ROW) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    final SqlNode operand1 = callBinding.operand(1);
    if (operand1.getKind() != SqlKind.DESCRIPTOR) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    validateColumnNames(validator, type.getFieldNames(), ((SqlCall) operand1).getOperandList());
    final SqlNode operand2 = callBinding.operand(2);
    if (operand2.getKind() != SqlKind.DESCRIPTOR) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    validateColumnNames(validator, type.getFieldNames(), ((SqlCall) operand2).getOperandList());
    final RelDataType type3 = validator.getValidatedNodeType(callBinding.operand(3));
    if (!SqlTypeUtil.isInterval(type3)) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    return true;
  }

  @Override public String getAllowedSignatures(String opNameToUse) {
    return getName() + "(TABLE table_name, DESCRIPTOR(col), "
        + "DESCRIPTOR(col), datetime interval)";
  }
}
