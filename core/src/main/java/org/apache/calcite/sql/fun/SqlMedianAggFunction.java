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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

/**
 * <code>MEDIAN</code> aggregate function
 * Takes a numeric value and returns the middle value or an interpolated value that
 * would be the middle value after the values are sorted. Nulls are ignored in the calculation.
 */
public class SqlMedianAggFunction extends SqlAggFunction {
  public SqlMedianAggFunction(SqlKind sqlKind, SqlReturnTypeInference sqlReturnTypeInference) {
    super("MEDIAN", null, sqlKind, sqlReturnTypeInference,
        null, OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC, false, false, Optionality.OPTIONAL);
  }
}
