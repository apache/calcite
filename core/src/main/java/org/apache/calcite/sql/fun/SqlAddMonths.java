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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Represents a SQL function for adding months to a date or timestamp value.
 * This function is typically used to add a specified number of months to a given date or timestamp.
 * The behavior of this function can be adjusted to handle edge cases such as the last day of the
 * month.
 */
public class SqlAddMonths extends SqlFunction {
  public final boolean isLastDay;

  /**
   * Constructs a new instance of SqlAddMonths.
   *
   * @param isLastDay Indicates whether the function should adjust the result to the last day of
   *                  the month.
   *                  By default, this is set to false.
   */
  public SqlAddMonths(
      boolean isLastDay) {
    super(
        "ADD_MONTHS",
        SqlKind.PLUS,
        ReturnTypes.ARG0,
        null,
        OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER),
        SqlFunctionCategory.TIMEDATE);
    this.isLastDay = isLastDay;
  }

  /**
   * Checks if the function is configured to adjust the result to the last day of the month.
   *
   * @return true if the function is configured to adjust the result to the last day of the
   * month, otherwise false.
   */
  public boolean isLastDay() {
    return isLastDay;
  }
}
