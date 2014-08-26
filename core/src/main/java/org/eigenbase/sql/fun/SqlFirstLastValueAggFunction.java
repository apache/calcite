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
package org.eigenbase.sql.fun;

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>FIRST_VALUE</code> and <code>LAST_VALUE</code> aggregate functions
 * return the first or the last value in a list of values that are input to the
 * function.
 */
public class SqlFirstLastValueAggFunction extends SqlAggFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlFirstLastValueAggFunction(boolean firstFlag) {
    super(
        firstFlag ? "FIRST_VALUE" : "LAST_VALUE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean requiresOrder() {
    // Allow the user to shoot herself into the foot by using first_value
    // and/or last_value without order by. This will result in undefined
    // behaviour, however lots of databases allow that.
    return false;
  }

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.ANY);
  }
}

// End SqlFirstLastValueAggFunction.java
