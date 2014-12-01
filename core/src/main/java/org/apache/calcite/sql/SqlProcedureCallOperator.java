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
package org.apache.calcite.sql;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Collections;

/**
 * SqlProcedureCallOperator represents the CALL statement. It takes a single
 * operand which is the real SqlCall.
 */
public class SqlProcedureCallOperator extends SqlPrefixOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlProcedureCallOperator() {
    super("CALL", SqlKind.PROCEDURE_CALL, 0, null, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  // override SqlOperator
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    // for now, rewrite "CALL f(x)" to "SELECT f(x) FROM VALUES(0)"
    // TODO jvs 18-Jan-2005:  rewrite to SELECT * FROM TABLE f(x)
    // once we support function calls as tables
    return new SqlSelect(SqlParserPos.ZERO,
        null,
        new SqlNodeList(
            Collections.singletonList(call.operand(0)),
            SqlParserPos.ZERO),
        SqlStdOperatorTable.VALUES.createCall(
            SqlParserPos.ZERO,
            SqlStdOperatorTable.ROW.createCall(
                SqlParserPos.ZERO,
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO))),
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }
}

// End SqlProcedureCallOperator.java
