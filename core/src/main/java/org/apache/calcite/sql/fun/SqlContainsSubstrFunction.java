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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Definition of the "CONTAINS_SUBSTR(expression, string [, json_scope =&gt;
 * json_scope_value ])" function; returns whether string exists as a
 * substring in expression.
 */
public class SqlContainsSubstrFunction extends SqlFunction {
  public SqlContainsSubstrFunction() {
    super("CONTAINS_SUBSTR", SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE, null,
        OperandTypes.ANY_STRING_OPTIONAL_STRING,
        SqlFunctionCategory.STRING);
  }

  /**
   * The parser only accepts the optional third operand with the named
   * syntax "json_scope =&gt; json_scope_value", both in Calcite and in
   * BigQuery, so {@code unparse} must emit "JSON_SCOPE =&gt;" before it.
   */
  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    writer.sep(",");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",");
    call.operand(1).unparse(writer, 0, 0);
    if (call.operandCount() == 3) {
      writer.sep(",");
      writer.keyword("JSON_SCOPE");
      writer.keyword("=>");
      call.operand(2).unparse(writer, 0, 0);
    }
    writer.endFunCall(frame);
  }
}
