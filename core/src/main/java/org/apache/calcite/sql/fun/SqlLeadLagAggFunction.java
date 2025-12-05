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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransform;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <code>LEAD</code> and <code>LAG</code> aggregate functions
 * return the value of given expression evaluated at given offset.
 */
public class SqlLeadLagAggFunction extends SqlAggFunction {
  private static final SqlSingleOperandTypeChecker OPERAND_TYPES =
      OperandTypes.ANY
          .or(OperandTypes.ANY_NUMERIC)
          .or(OperandTypes.ANY_NUMERIC_ANY
              // "same" only checks if two types are comparable
              .and(OperandTypes.same(3, 0, 2)));

  // A version of least restrictive which only looks at operands 0 and 2,
  // if the latter exists.
  private static final SqlReturnTypeInference ARG03 = opBinding -> {
    List<RelDataType> toCheck = new ArrayList<>();
    toCheck.add(opBinding.getOperandType(0));
    if (opBinding.getOperandCount() >= 3) {
      RelDataType op2 = opBinding.getOperandType(2);
      toCheck.add(op2);
    }
    // If any operand is in the CHAR type family, return VARCHAR.
    for (RelDataType type : toCheck) {
      if (type.getFamily() == SqlTypeFamily.CHARACTER) {
        return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      }
    }
    return opBinding.getTypeFactory().leastRestrictive(toCheck);
  };

  private static final SqlReturnTypeInference RETURN_TYPE =
      ARG03.andThen(SqlLeadLagAggFunction::transformType);

  public SqlLeadLagAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        RETURN_TYPE,
        null,
        OPERAND_TYPES,
        SqlFunctionCategory.NUMERIC,
        false,
        true,
        Optionality.FORBIDDEN);
    checkArgument(kind == SqlKind.LEAD || kind == SqlKind.LAG);
  }

  @Deprecated // to be removed before 2.0
  public SqlLeadLagAggFunction(boolean isLead) {
    this(isLead ? SqlKind.LEAD : SqlKind.LAG);
  }

  // Result is NOT NULL if NOT NULL default value is provided
  private static RelDataType transformType(SqlOperatorBinding binding,
      RelDataType type) {
    SqlTypeTransform transform =
        binding.getOperandCount() < 3 || binding.getOperandType(2).isNullable()
            ? SqlTypeTransforms.FORCE_NULLABLE
            : SqlTypeTransforms.TO_NOT_NULLABLE;
    return transform.transformType(binding, type);
  }

  @Override public boolean allowsFraming() {
    return false;
  }

  @Override public boolean allowsNullTreatment() {
    return true;
  }

}
