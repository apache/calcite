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

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

/**
 * Definition of the "FLOOR" builtin SQL function.
 */
public class SqlFloorFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlFloorFunction() {
    super(
        "FLOOR",
        SqlKind.OTHER_FUNCTION,
        SqlTypeStrategies.rtiFirstArgType,
        null,
        SqlTypeStrategies.otcNumeric,
        SqlFunctionCategory.NUMERIC);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    // Monotonic iff its first argument is, but not strict.
    SqlNode node = (SqlNode) call.operands[0];
    return scope.getMonotonicity(node).unstrict();
  }
}

// End SqlFloorFunction.java
