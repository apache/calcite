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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * The <code>DATE_TRUNC</code> function.
 */
public class SqlDateTruncFunction extends SqlFunction {
  public SqlDateTruncFunction() {
    super("DATE_TRUNC",
        SqlKind.FLOOR,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.sequence(
            "'DATE_TRUNC(<DATE>, <DATETIME_INTERVAL>)'",
            OperandTypes.DATE,
            OperandTypes.ANY
        ),
        SqlFunctionCategory.TIMEDATE);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);

    // This is either a time unit or a time frame:
    //
    //  * In "DATE_TRUNC(date, YEAR)" operand 0 is a SqlIntervalQualifier with
    //    startUnit = YEAR and timeFrameName = null.
    //
    //  * In "DATE_TRUNC(date, MINUTE15)" operand 1 is a SqlIntervalQualifier with
    //    startUnit = EPOCH and timeFrameName = 'MINUTE15'.
    //
    // If the latter, check that timeFrameName is valid.
    validator.validateTimeFrame(
        (SqlIntervalQualifier) call.getOperandList().get(1));
  }
}
