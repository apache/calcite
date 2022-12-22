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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.BodoTZInfo;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for functions such as CURRENT_TIMESTAMP that return
 * TZ_Aware data dependent on the current TZ_INFO session parameter.
 */
public class SqlAbstractTZSessionFunction extends SqlFunction {

  //~ Static fields/initializers ---------------------------------------------
  private static final SqlOperandTypeChecker OTC_CUSTOM =
      OperandTypes.or(
          OperandTypes.POSITIVE_INTEGER_LITERAL, OperandTypes.NILADIC);

  /**
   * Creates a new SqlFunction for a call to a built-in function.
   *
   * @param name                 Name of built-in function
   */
  public SqlAbstractTZSessionFunction(String name) {
    super(name, SqlKind.OTHER_FUNCTION, null, null, OTC_CUSTOM, SqlFunctionCategory.TIMEDATE);
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_ID;
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // TODO: Support using precision information in TZAwareSqlType
    // We keep the error check here.
    int precision = 0;
    if (opBinding.getOperandCount() == 1) {
      RelDataType type = opBinding.getOperandType(0);
      if (SqlTypeUtil.isNumeric(type)) {
        precision = getOperandLiteralValueOrThrow(opBinding, 0, Integer.class);
      }
    }
    assert precision >= 0;
    if (precision > SqlTypeName.MAX_DATETIME_PRECISION) {
      throw opBinding.newError(
          RESOURCE.argumentMustBeValidPrecision(
              opBinding.getOperator().getName(), 0,
              SqlTypeName.MAX_DATETIME_PRECISION));
    }
    BodoTZInfo tzInfo = opBinding.getTypeFactory().getTypeSystem().getDefaultTZInfo();
    return opBinding.getTypeFactory().createTZAwareSqlType(tzInfo);
  }


  // All TZ-Aware Timestamp functions are increasing. Not strictly increasing.
  // This is based on this Class replacing other uses of SqlAbstractionTimeFunction,
  // which is also increasing.
  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.INCREASING;
  }

  // Plans referencing context variables should never be cached
  @Override public boolean isDynamicFunction() {
    return true;
  }
}
