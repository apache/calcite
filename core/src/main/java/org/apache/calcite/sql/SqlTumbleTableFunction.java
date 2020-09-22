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

import com.google.common.collect.ImmutableList;

/**
 * SqlTumbleTableFunction implements an operator for tumbling.
 *
 * <p>It allows three parameters:
 *
 * <ol>
 *   <li>a table</li>
 *   <li>a descriptor to provide a watermarked column name from the input table</li>
 *   <li>an interval parameter to specify the length of window size</li>
 * </ol>
 */
public class SqlTumbleTableFunction extends SqlWindowTableFunction {
  public SqlTumbleTableFunction() {
    super(SqlKind.TUMBLE.name(), new OperandMetadataImpl());
  }

  /** Operand type checker for TUMBLE. */
  private static class OperandMetadataImpl extends AbstractOperandMetadata {
    OperandMetadataImpl() {
      super(
          ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_SIZE, PARAM_OFFSET),
          3);
    }

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
        boolean throwOnFailure) {
      // There should only be three operands, and number of operands are checked before
      // this call.
      if (!checkTableAndDescriptorOperands(callBinding, 1)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      if (!checkTimeColumnDescriptorOperand(callBinding, 1)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      if (!checkIntervalOperands(callBinding, 2)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      return true;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(TABLE table_name, DESCRIPTOR(col1, col2 ...), datetime interval"
          + "[, datetime interval])";
    }
  }
}
