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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

import com.google.common.collect.ImmutableList;

/**
 * SqlSessionTableFunction implements an operator for per-key sessionization. It allows
 * four parameters:
 *
 * <ol>
 *   <li>table as data source</li>
 *   <li>a descriptor to provide a watermarked column name from the input table</li>
 *   <li>a descriptor to provide a column as key, on which sessionization will be applied</li>
 *   <li>an interval parameter to specify a inactive activity gap to break sessions</li>
 * </ol>
 */
public class SqlSessionTableFunction extends SqlWindowTableFunction {
  public SqlSessionTableFunction() {
    super(SqlKind.SESSION.name(), new OperandMetadataImpl());
  }

  /** Operand type checker for SESSION. */
  private static class OperandMetadataImpl extends AbstractOperandMetadata {
    OperandMetadataImpl() {
      super(ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_KEY, PARAM_SIZE),
          4);
    }

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
        boolean throwOnFailure) {
      final SqlValidator validator = callBinding.getValidator();
      if (!checkTableAndDescriptorOperands(callBinding, 2)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      final RelDataType type3 = validator.getValidatedNodeType(callBinding.operand(3));
      if (!SqlTypeUtil.isInterval(type3)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      return true;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(TABLE table_name, DESCRIPTOR(timecol), "
          + "DESCRIPTOR(key), datetime interval)";
    }
  }
}
