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
 * Format Function Support.
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
      SqlNode[] sqlNode = new SqlNode[]{
          call.operand(1),
          SqlLiteral.createCharString(call.operand(1).toString(),
              SqlParserPos.ZERO)};
      SqlNode sqlNode1 = call.operand(1);
      while (sqlNode1 instanceof SqlBasicCall) {
        sqlNode1 = ((SqlBasicCall) sqlNode1).operand(0);
      }
      if (sqlNode1 instanceof SqlIdentifier) {
        sqlNode = handleColumnOperand(call);
      } else if (sqlNode1 instanceof SqlLiteral) {
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
    } else if (call.operand(0).toString().contains("d")) {
      modifiedOperand = getModifiedLiteralOperandForInteger(call);
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
    String modifiedOperandForSF = null;
    SqlNode[] sqlNode;
    if (call.operand(0).toString().contains(".")) {
      modifiedOperand = call.operand(0).toString()
          .replaceAll("%|f|'", "");
      String[] modifiedOperandArry = modifiedOperand.split("\\.");
      if (StringUtils.isNotBlank(modifiedOperandArry[0])) {
        modifiedOperand = getModifiedOperandForDecimal(modifiedOperandForSF, modifiedOperandArry);
      } else {
        modifiedOperand = "TM9";
      }
    } else if (call.operand(0).toString().contains("d")) {
      modifiedOperand = getModifiedOperandForInteger(call);
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

  private String getModifiedOperandForDecimal(String modifiedOperandForSF,
        String[] modifiedOperandArry) {
    int patternRepeatNumber;
    String modifiedOperand;
    patternRepeatNumber = Integer.valueOf(modifiedOperandArry[0]) - 1;
    if (modifiedOperandArry[1].contains("E")) {
      modifiedOperandForSF = getModifiedOperandForFloat(modifiedOperandArry);
    } else if (Integer.valueOf(modifiedOperandArry[1]) != 0) {
      patternRepeatNumber = patternRepeatNumber - 1 - Integer.valueOf(modifiedOperandArry[1]);
    }
    modifiedOperand = StringUtils.repeat("9", patternRepeatNumber);
    int decimalValue = Integer.valueOf(modifiedOperandArry[1]);
    modifiedOperand += "." + StringUtils.repeat("0", decimalValue);
    if (null != modifiedOperandForSF) {
      modifiedOperand = modifiedOperandForSF;
    }
    return modifiedOperand;
  }

  private String getModifiedLiteralOperandForInteger(SqlCall call) {
    int firstOperand = Integer.valueOf(call.operand(0).toString()
            .replaceAll("[^0-9]", "")) - 1;
    String modifiedString = call.operand(0).toString()
            .replaceAll("[^0-9]", "");
    String modifiedOperand;
    if (modifiedString.charAt(0) == '0') {
      modifiedOperand = "FM" + StringUtils.repeat("0", firstOperand + 1);
    } else {
      modifiedOperand = StringUtils.repeat("9", firstOperand);
    }
    return  modifiedOperand;
  }

  private String getModifiedOperandForInteger(SqlCall call) {
    String modifiedOperand = call.operand(0).toString()
            .replaceAll("[^0-9]", "");
    String[] modifiedOperandArry = modifiedOperand.split(",");
    int patternRepeatNumber = Integer.valueOf(modifiedOperandArry[0]);
    if (modifiedOperand.charAt(0) == '0') {
      modifiedOperand = "FM" + StringUtils.repeat("0", patternRepeatNumber);
    } else {
      modifiedOperand = StringUtils.repeat("9", patternRepeatNumber - 1);
    }
    return modifiedOperand;
  }

  private String getModifiedOperandForFloat(String[] modifiedOperandArry) {
    modifiedOperandArry[1] = modifiedOperandArry[1].replaceAll("E", "");
    int secondValue = Integer.valueOf(modifiedOperandArry[1]);
    int firstValue = Integer.valueOf(modifiedOperandArry[0]);
    String modifiedOperand = StringUtils.repeat("0", firstValue) + "d"
            + StringUtils.repeat("0", secondValue) + StringUtils.repeat("E", 5);
    return modifiedOperand;
  }
}
