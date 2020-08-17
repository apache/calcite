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

import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

import com.google.common.collect.ImmutableList;

import java.util.List;

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
    super(SqlKind.TUMBLE.name(), OperandTypeCheckerImpl.INSTANCE);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(3, 4);
  }

  @Override public List<String> getParamNames() {
    return ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_SIZE, PARAM_OFFSET);
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /** Operand type checker for SESSION. */
  private static class OperandTypeCheckerImpl implements SqlOperandTypeChecker {
    static final OperandTypeCheckerImpl INSTANCE = new OperandTypeCheckerImpl();

    @Override public boolean checkOperandTypes(
        SqlCallBinding callBinding, boolean throwOnFailure) {
      // There should only be three operands, and number of operands are checked before
      // this call.
      if (!validateTableWithFollowingDescriptors(callBinding, 1)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      if (!validateTailingIntervals(callBinding, 2)) {
        return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
      }
      return true;
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.of(4);
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(TABLE table_name, DESCRIPTOR(col1, col2 ...), datetime interval"
          + "[, datetime interval])";
    }

    @Override public Consistency getConsistency() {
      return Consistency.NONE;
    }

    @Override public boolean isOptional(int i) {
      return i == 3;
    }
  }
}
