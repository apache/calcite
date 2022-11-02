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
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * The <code>TIMESTAMPDIFF</code> function, which calculates the difference
 * between two timestamps.
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPDIFF(<i>timestamp interval</i>, <i>timestamp</i>,
 * <i>timestamp</i>)</code>
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
 * <p>Returns difference between two timestamps in indicated timestamp
 * interval.
 */
class SqlTimestampDiffFunction extends SqlFunction {
  private static RelDataType inferReturnType2(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    TimeUnit timeUnit = opBinding.getOperandLiteralValue(0, TimeUnit.class);
    SqlTypeName sqlTypeName =
        timeUnit == TimeUnit.NANOSECOND
            ? SqlTypeName.BIGINT
            : SqlTypeName.INTEGER;
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(sqlTypeName),
        opBinding.getOperandType(1).isNullable()
            || opBinding.getOperandType(2).isNullable());
  }

  /** Creates a SqlTimestampDiffFunction. */
  SqlTimestampDiffFunction(String name) {
    super(name, SqlKind.TIMESTAMP_DIFF,
        SqlTimestampDiffFunction::inferReturnType2, null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATETIME,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
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
    validator.validateTimeFrame(
        (SqlIntervalQualifier) call.getOperandList().get(0));
  }
}
