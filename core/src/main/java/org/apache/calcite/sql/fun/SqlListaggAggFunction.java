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
 * <code>LISTAGG</code> aggregate function
 * returns the concatenation of its group rows.
 */
class SqlListaggAggFunction extends SqlAggFunction {
  SqlListaggAggFunction(SqlKind kind,
      SqlReturnTypeInference returnTypeInference) {
    super(kind.name(), null, kind, returnTypeInference,
        null, OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING),
        SqlFunctionCategory.SYSTEM, false, false, Optionality.OPTIONAL);
  }
}
