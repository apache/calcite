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
package org.apache.calcite.util;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.apache.commons.lang3.StringUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.LPAD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RPAD;

/**
 * Handle rpad and ldap formatting
 */
public class PaddingFunctionUtil {

  private PaddingFunctionUtil() {
  }

  public static void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    SqlFunction sqlFunction = call.getOperator().getName().equals(RPAD.getName()) ? RPAD : LPAD;
    if (((SqlBasicCall) call).operands.length == 2) {
      SqlCharStringLiteral blankLiteral = SqlLiteral.createCharString(StringUtils.SPACE,
          SqlParserPos.ZERO);
      SqlCall paddingFunctionCall = sqlFunction.createCall(SqlParserPos.ZERO, call.operand(0),
          call.operand(1), blankLiteral);
      sqlFunction.unparse(writer, paddingFunctionCall, leftPrec, rightPrec);
    } else {
      sqlFunction.unparse(writer, call, leftPrec, rightPrec);
    }
  }
}
// End PaddingFunctionUtil.java
