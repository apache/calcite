/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.sql.fun;

import java.util.List;

import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.type.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>Sum0</code> is an aggregator which returns the sum of the values which
 * go into it like <code>Sum</code>. It differs in that when no non null values
 * are applied zero is returned instead of null. Can be used along with <code>
 * Count</code> to implement <code>Sum</code>.
 */
public class SqlSumEmptyIsZeroAggFunction extends SqlAggFunction {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  public SqlSumEmptyIsZeroAggFunction(RelDataType type) {
    super(
        "$SUM0",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC);
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  public RelDataType getType() {
    return type;
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }
}

// End SqlSumEmptyIsZeroAggFunction.java
