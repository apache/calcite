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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Static;

import com.google.common.collect.ImmutableSet;

/**
 * Parameter type-checking strategy whether the operand must be an interval.
 */
public class IntervalOperandTypeChecker implements SqlSingleOperandTypeChecker {

  private final ImmutableSet<TimeUnitRange> unitSet;

  IntervalOperandTypeChecker(ImmutableSet<TimeUnitRange> unitSet) {
    this.unitSet = unitSet;
  }

  @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
      SqlNode node, int iFormalOperand, boolean throwOnFailure) {
    final SqlNode operand = callBinding.operand(iFormalOperand);
    if (operand instanceof SqlIntervalQualifier) {
      final SqlIntervalQualifier interval = (SqlIntervalQualifier) operand;
      if (unitSet.contains(interval.timeUnitRange)) {
        return true;
      }
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(operand,
            Static.RESOURCE.invalidTimeFrame(interval.timeUnitRange.name()));
      }
    }
    return false;
  }

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<INTERVAL>";
  }
}
