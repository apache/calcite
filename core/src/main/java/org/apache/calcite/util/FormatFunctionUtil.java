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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.commons.lang3.StringUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_VARCHAR;

/**
 * Format Function Support
 */
public class FormatFunctionUtil {

  public SqlCall fetchSqlCallForFormat(SqlCall call) {
    SqlCall sqlCall = null;
    switch (call.getOperandList().size()) {
    case 1:
      if (call.operand(0).toString().equalsIgnoreCase("null")) {
        SqlNode[] extractNodeOperands = new SqlNode[]{
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
                SqlParserPos.ZERO)
        };
        sqlCall = new SqlBasicCall(TO_VARCHAR, extractNodeOperands, SqlParserPos.ZERO);
      }
      break;
    case 2:
      SqlNode[] sqlNode;
      if (call.operand(1) instanceof SqlIdentifier) {
        sqlNode = handleColumnOperand(call);
      } else {
        sqlNode = handleLiteralOperand(call);
      }
      sqlCall = new SqlBasicCall(TO_VARCHAR, sqlNode, SqlParserPos.ZERO);
      break;
    default:
      throw new IllegalArgumentException("more than 2 argument for format is not supported.");
    }
    return sqlCall;
  }

  private SqlNode[] handleLiteralOperand(SqlCall call) {
    String modifiedOperand;
    SqlNode[] sqlNode;
    if (call.operand(1).toString().contains(".")) {
      modifiedOperand = call.operand(1).toString()
          .replaceAll("[0-9]", "9")
          .replaceAll("'", "");
    } else {
      int firstOperand = Integer.valueOf(call.operand(0).toString()
          .replaceAll("[^0-9]", "")) - 1;
      modifiedOperand = StringUtils.repeat("9", firstOperand);
    }
    sqlNode = new SqlNode[]{
        SqlLiteral.createExactNumeric(
            call.operand(1).toString().replaceAll("'", ""),
            SqlParserPos.ZERO),
        SqlLiteral.createCharString(modifiedOperand.trim(),
            SqlParserPos.ZERO)};
    return sqlNode;
  }

  private SqlNode[] handleColumnOperand(SqlCall call) {
    String modifiedOperand;
    SqlNode[] sqlNode;
    if (call.operand(0).toString().contains(".")) {
      modifiedOperand = call.operand(0).toString()
          .replaceAll("%|f|'", "");
      String[] modifiedOperandArry = modifiedOperand.split("\\.");
      int patternRepeatNumber = Integer.valueOf(modifiedOperandArry[0]) - 1;
      if (Integer.valueOf(modifiedOperandArry[1]) != 0) {
        patternRepeatNumber = patternRepeatNumber - 1 - Integer.valueOf(modifiedOperandArry[1]);
      }
      modifiedOperand = StringUtils.repeat("9", patternRepeatNumber);
      int decimalValue = Integer.valueOf(modifiedOperandArry[1]);
      modifiedOperand += "." + StringUtils.repeat("0", decimalValue);
    } else {
      int intValue = Integer.valueOf(call.operand(0).toString()
          .replaceAll("[^0-9]", ""));
      modifiedOperand = StringUtils.repeat("9", intValue - 1);
    }
    sqlNode = new SqlNode[]{
        call.operand(1),
        SqlLiteral.createCharString(modifiedOperand.trim(),
            SqlParserPos.ZERO)};
    return sqlNode;
  }
}

// End FormatFunctionUtil.java
