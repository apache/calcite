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
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * The <code>RAND_INTEGER</code> function. There are two overloads:
 *
 * <ul>
 *   <li>RAND_INTEGER(bound) returns a random integer between 0 and bound - 1
 *   <li>RAND_INTEGER(seed, bound) returns a random integer between 0 and
 *   bound - 1, initializing the random number generator with seed on first call
 * </ul>
 */
public class SqlRandIntegerFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlRandIntegerFunction() {
    super("RAND_INTEGER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.NUMERIC.or(OperandTypes.NUMERIC_NUMERIC),
        SqlFunctionCategory.NUMERIC);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  // Plans referencing context variables should never be cached
  @Override public boolean isDynamicFunction() {
    return true;
  }
}
