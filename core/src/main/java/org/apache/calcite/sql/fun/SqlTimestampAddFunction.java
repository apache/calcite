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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.calcite.util.Util.first;

/**
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a
 * datetime (TIMESTAMP, TIME or DATE).
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPADD(<i>timestamp interval</i>, <i>quantity</i>,
 * <i>datetime</i>)</code>
 * </blockquote>
 *
 * <p>The interval time unit can one of the following literals:<ul>
 * <li>NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)
 * <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)
 * <li>SECOND (and synonym SQL_TSI_SECOND)
 * <li>MINUTE (and synonym  SQL_TSI_MINUTE)
 * <li>HOUR (and synonym  SQL_TSI_HOUR)
 * <li>DAY (and synonym SQL_TSI_DAY)
 * <li>WEEK (and synonym  SQL_TSI_WEEK)
 * <li>MONTH (and synonym SQL_TSI_MONTH)
 * <li>QUARTER (and synonym SQL_TSI_QUARTER)
 * <li>YEAR (and synonym  SQL_TSI_YEAR)
 * </ul>
 *
 * <p>Returns modified datetime.
 */
public class SqlTimestampAddFunction extends SqlFunction {

  private static final Map<TimeUnit, Integer> FRAC_SECOND_PRECISION_MAP =
      ImmutableMap.of(TimeUnit.MILLISECOND, 3,
          TimeUnit.MICROSECOND, 6,
          TimeUnit.NANOSECOND, 9);

  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding ->
          deduceType(opBinding.getTypeFactory(),
              opBinding.getOperandLiteralValue(0, TimeUnit.class),
              opBinding.getOperandType(2));

  @Deprecated // to be removed before 2.0
  public static RelDataType deduceType(RelDataTypeFactory typeFactory,
      @Nullable TimeUnit timeUnit, RelDataType operandType1,
      RelDataType operandType2) {
    final RelDataType type = deduceType(typeFactory, timeUnit, operandType2);
    return typeFactory.createTypeWithNullability(type,
        operandType1.isNullable() || operandType2.isNullable());
  }

  static RelDataType deduceType(RelDataTypeFactory typeFactory,
      @Nullable TimeUnit timeUnit, RelDataType datetimeType) {
    final TimeUnit timeUnit2 = first(timeUnit, TimeUnit.EPOCH);
    SqlTypeName typeName = datetimeType.getSqlTypeName();
    switch (timeUnit2) {
    case MICROSECOND:
    case MILLISECOND:
    case NANOSECOND:
      return typeFactory.createSqlType(typeName,
          Math.max(FRAC_SECOND_PRECISION_MAP.getOrDefault(timeUnit2, 0),
              datetimeType.getPrecision()));
    case HOUR:
    case MINUTE:
    case SECOND:
      if (datetimeType.getFamily() == SqlTypeFamily.TIME) {
        return datetimeType;
      } else if (datetimeType.getFamily() == SqlTypeFamily.TIMESTAMP) {
        return
            typeFactory.createSqlType(typeName,
                datetimeType.getPrecision());
      } else {
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      }
    default:
      return datetimeType;
    }
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);

    // This is either a time unit or a time frame:
    //
    //  * In "TIMESTAMPADD(YEAR, 2, x)" operand 0 is a SqlIntervalQualifier
    //    with startUnit = YEAR and timeFrameName = null.
    //
    //  * In "TIMESTAMPADD(MINUTE15, 2, x) operand 0 is a SqlIntervalQualifier
    //    with startUnit = EPOCH and timeFrameName = 'MINUTE15'.
    //
    // If the latter, check that timeFrameName is valid.
    SqlIntervalQualifier op0 = call.operand(0);
    validator.validateTimeFrame(op0);
    if (op0.timeFrameName == null
        && op0.timeUnitRange.startUnit.multiplier == null) {
      // Not all time frames can be used in date arithmetic, e.g., DOW
      throw validator.newValidationError(op0,
          RESOURCE.invalidTimeFrameInOperation(
              op0.timeUnitRange.toString(), call.getOperator().getName()));
    }
  }

  /** Creates a SqlTimestampAddFunction. */
  SqlTimestampAddFunction(String name) {
    super(name, SqlKind.TIMESTAMP_ADD,
        RETURN_TYPE_INFERENCE.andThen(SqlTypeTransforms.TO_NULLABLE), null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
  }
}
