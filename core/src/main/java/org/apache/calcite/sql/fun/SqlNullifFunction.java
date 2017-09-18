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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

/**
 * The <code>NULLIF</code> function.
 */
public class SqlNullifFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlNullifFunction() {
    // NOTE jvs 26-July-2006:  We fill in the type strategies here,
    // but normally they are not used because the validator invokes
    // rewriteCall to convert NULLIF into CASE early.  However,
    // validator rewrite can optionally be disabled, in which case these
    // strategies are used.
    super("NULLIF",
        SqlKind.NULLIF,
        ReturnTypes.ARG0_FORCE_NULLABLE,
        null,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  // override SqlOperator
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    SqlParserPos pos = call.getParserPosition();

    checkOperandCount(
        validator,
        getOperandTypeChecker(),
        call);
    assert operands.size() == 2;

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);
    whenList.add(operands.get(1));
    thenList.add(SqlLiteral.createNull(SqlParserPos.ZERO));
    return SqlCase.createSwitched(pos, operands.get(0), whenList, thenList,
        SqlNode.clone(operands.get(0)));
  }
}

// End SqlNullifFunction.java
