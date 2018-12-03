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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;

/**
 * The <code>JSON_OBJECTAGG</code> aggregation function.
 */
public class SqlJsonObjectAggAggFunction extends SqlAggFunction {
  private final SqlJsonConstructorNullClause nullClause;

  public SqlJsonObjectAggAggFunction(String name,
      SqlJsonConstructorNullClause nullClause) {
    super(name, null, SqlKind.JSON_OBJECTAGG, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM, false, false, Optionality.FORBIDDEN);
    this.nullClause = nullClause;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame = writer.startFunCall("JSON_OBJECTAGG");
    writer.keyword("KEY");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.keyword("VALUE");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    switch (nullClause) {
    case ABSENT_ON_NULL:
      writer.keyword("ABSENT ON NULL");
      break;
    case NULL_ON_NULL:
      writer.keyword("NULL ON NULL");
      break;
    default:
      throw new IllegalStateException("unreachable code");
    }
    writer.endFunCall(frame);
  }

  private <E extends Enum<E>> E getEnumValue(SqlNode operand) {
    return (E) ((SqlLiteral) operand).getValue();
  }
}

// End SqlJsonObjectAggAggFunction.java
