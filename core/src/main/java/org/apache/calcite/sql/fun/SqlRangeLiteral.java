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

import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Big Query operator for RANGE literal.
 */
public class SqlRangeLiteral extends SqlFunction {

  public SqlRangeLiteral() {
    super("RANGE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME),
        SqlFunctionCategory.SYSTEM);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    unparseTypeName(writer, call.getOperandList().get(0));
    String rangeLiteral = "'[" + extractLiteralValue(call.operand(0))
        + ", " + extractLiteralValue(call.operand(1)) + ")'";
    writer.literal(rangeLiteral);
  }

  private static String extractLiteralValue(SqlNode node) {
    if (SqlUtil.isCallTo(node, SqlStdOperatorTable.CAST)) {
      return extractLiteralValue(((SqlCall) node).getOperandList().get(0));
    }
    if (node instanceof SqlCharStringLiteral) {
      return ((SqlCharStringLiteral) node).getValueAs(String.class);
    }
    return ((SqlAbstractDateTimeLiteral) node).toFormattedString();
  }

  private static void unparseTypeName(SqlWriter writer, SqlNode node) {
    writer.keyword("RANGE<");
    writer.setNeedWhitespace(false);
    if (SqlUtil.isCallTo(node, SqlStdOperatorTable.CAST)) {
      ((SqlCall) node).getOperandList().get(1).unparse(writer, 0, 0);
    } else {
      switch (((SqlAbstractDateTimeLiteral) node).getTypeName()) {
      case DATE:
        writer.keyword("DATE");
        break;
      case TIMESTAMP:
        writer.keyword("DATETIME");
        break;
      default:
        writer.keyword("TIMESTAMP");
      }
    }
    writer.setNeedWhitespace(false);
    writer.literal(">");
  }
}
