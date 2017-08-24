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

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * The {@code GROUPING_ID} function.
 *
 * <p>This function is not defined in the SQL standard; our implementation is
 * consistent with Oracle.
 *
 * <p>Some examples are in {@code agg.iq}.
 *
 * @deprecated Now that {@code GROUPING} has the same functionality,
 * this function is deprecated.
 */
@Deprecated // to be removed before 2.0
class SqlGroupingIdFunction extends SqlAbstractGroupFunction {
  SqlGroupingIdFunction() {
    //noinspection deprecation
    super("GROUPING_ID", SqlKind.GROUPING_ID, ReturnTypes.BIGINT, null,
        OperandTypes.ONE_OR_MORE, SqlFunctionCategory.SYSTEM);
  }
}

// End SqlGroupingIdFunction.java
