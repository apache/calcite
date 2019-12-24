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
 * SqlTumbleTableFunction implements an operator for tumbling. It allows three parameters:
 * 1. a table.
 * 2. a descriptor to provide a watermarked column name from the input table.
 * 3. an interval parameter to specify the length of window size.
 */
public class SqlTumbleTableFunction extends SqlWindowTableFunction {
  public SqlTumbleTableFunction() {
    super(SqlKind.TUMBLE.name());
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(3);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // There should only be three operands, and number of operands are checked before
    // this call.
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
    final RelDataType type2 = validator.getValidatedNodeType(callBinding.operand(2));
    if (!SqlTypeUtil.isInterval(type2)) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    return true;
  }

  @Override public String getAllowedSignatures(String opNameToUse) {
    return getName() + "(TABLE table_name, DESCRIPTOR(col1, col2 ...), datetime interval)";
  }
}
