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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Operator that indicates that an argument to a function call is to take its
 * default value.
 *
 * <p>Not an expression; just a holder to represent syntax until the validator
 * has chance to resolve arguments.
 */
class SqlDefaultOperator extends SqlSpecialOperator {
  SqlDefaultOperator() {
    super("DEFAULT", SqlKind.DEFAULT, 100, true,
        ReturnTypes.explicit(SqlTypeName.ANY), InferTypes.RETURN_TYPE,
        OperandTypes.NILADIC);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    writer.keyword(getName());
  }
}

// End SqlDefaultOperator.java
