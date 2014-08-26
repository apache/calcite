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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;

/**
 * <code>NTILE</code> aggregate function
 * return the value of given expression evaluated at given offset.
 */
public class SqlNtileAggFunction extends SqlAggFunction {
  public SqlNtileAggFunction() {
    super(
        "NTILE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.POSITIVE_INTEGER_LITERAL,
        SqlFunctionCategory.NUMERIC);
  }

  @Override public boolean requiresOrder() {
    return true;
  }

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    throw new UnsupportedOperationException("remove before optiq-0.9");
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    throw new UnsupportedOperationException("remove before optiq-0.9");
  }
}

// End SqlNtileAggFunction.java
