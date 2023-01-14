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
package org.apache.calcite.sql.type;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

import java.util.function.Predicate;

/**
 * Parameter type-checking strategy whether the operand must be an interval.
 */
public class IntervalOperandTypeChecker implements SqlSingleOperandTypeChecker {

  private final Predicate<SqlIntervalQualifier> predicate;

  IntervalOperandTypeChecker(Predicate<SqlIntervalQualifier> predicate) {
    this.predicate = predicate;
  }

  @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
      SqlNode operand, int iFormalOperand, boolean throwOnFailure) {
    if (operand instanceof SqlIntervalQualifier) {
      final SqlIntervalQualifier interval = (SqlIntervalQualifier) operand;
      if (predicate.test(interval)) {
        return true;
      }
      if (throwOnFailure) {
        final String name =
            Util.first(interval.timeFrameName, interval.timeUnitRange.name());
        throw callBinding.getValidator().newValidationError(operand,
            Static.RESOURCE.invalidTimeFrame(name));
      }
    }
    return false;
  }

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<INTERVAL>";
  }
}
