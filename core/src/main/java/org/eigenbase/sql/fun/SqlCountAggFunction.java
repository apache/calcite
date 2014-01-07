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
package org.eigenbase.sql.fun;

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

import com.google.common.collect.ImmutableList;

/**
 * Definition of the SQL <code>COUNT</code> aggregation function.
 *
 * <p><code>COUNT</code> is an aggregator which returns the number of rows which
 * have gone into it. With one argument (or more), it returns the number of rows
 * for which that argument (or all) is not <code>null</code>.
 */
public class SqlCountAggFunction extends SqlAggFunction {
  //~ Static fields/initializers ---------------------------------------------

  public static final RelDataType type = null; // TODO:

  //~ Constructors -----------------------------------------------------------

  public SqlCountAggFunction() {
    super(
        "COUNT",
        SqlKind.OTHER_FUNCTION,
        SqlTypeStrategies.rtiBigint,
        null,
        SqlTypeStrategies.otcAny,
        SqlFunctionCategory.Numeric);
  }

  //~ Methods ----------------------------------------------------------------

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.BIGINT);
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Check for COUNT(*) function.  If it is we don't
    // want to try and derive the "*"
    if (call.isCountStar()) {
      return validator.getTypeFactory().createSqlType(
          SqlTypeName.BIGINT);
    }
    return super.deriveType(validator, scope, call);
  }
}

// End SqlCountAggFunction.java
