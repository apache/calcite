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
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This class is specific to BigQuery, Hive and Spark.
 */
public class ToNumberUtils {

  private static final SqlSetOperator CONV =
    new SqlSetOperator("CONV", SqlKind.CONV, 18, false);

  private ToNumberUtils() {
  }

  private static void handleCasting(
    SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
    SqlTypeName sqlTypeName) {
    SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), new SqlDataTypeSpec(new
      SqlBasicTypeNameSpec(sqlTypeName, SqlParserPos.ZERO),
      SqlParserPos.ZERO)};
    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
      extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
  }

  private static void handleNegativeValue(SqlCall call, String regEx) {
    String firstOperand = call.operand(0).toString().replaceAll(regEx, "");
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      firstOperand = "-" + firstOperand;
    }

    SqlNode[] sqlNode = new SqlNode[]{SqlLiteral.createCharString(firstOperand.trim(),
      SqlParserPos.ZERO)};
    call.setOperand(0, sqlNode[0]);
  }

  private static boolean handleNullOperand(
    SqlWriter writer, int leftPrec, int rightPrec) {
    SqlNode[] extractNodeOperands = new SqlNode[]{new SqlDataTypeSpec(new
      SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
      SqlParserPos.ZERO), new SqlDataTypeSpec(new
      SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO),
      SqlParserPos.ZERO)};

    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
      extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
    return true;
  }

  private static boolean isOperandNull(SqlCall call) {
    for (SqlNode sqlNode : call.getOperandList()) {
      if (sqlNode.toString().equalsIgnoreCase("null")) {
        return true;
      }
    }
    return false;
  }

  public static void unparseToNumbertoConv(
    SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    SqlNode[] sqlNode = new SqlNode[]{SqlLiteral.createExactNumeric("16",
      SqlParserPos.ZERO), SqlLiteral.createExactNumeric("10",
      SqlParserPos.ZERO)};

    List<SqlNode> operandList = new ArrayList<>();
    operandList.add(call.getOperandList().get(0));
    operandList.add(sqlNode[0]);
    operandList.add(sqlNode[1]);

    SqlCall sqlCall = call.getOperator().createCall(null, SqlParserPos.ZERO,
      operandList.toArray(new SqlNode[0]));
    SqlSyntax.FUNCTION.unparse(writer, CONV, sqlCall, leftPrec, rightPrec);
  }

  public static void unparseToNumber(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
    case 3:
      if (isOperandNull(call)) {
        handleNullOperand(writer, leftPrec, rightPrec);
      } else {
        if (call.operand(0) instanceof SqlCharStringLiteral) {
          String regEx1 = "[',A-Za-z]+";
          String firstOperand = call.operand(0).toString().replaceAll(regEx1, "");
          SqlNode[] sqlNode = new SqlNode[]{SqlLiteral.createCharString(firstOperand.trim(),
            SqlParserPos.ZERO)};
          call.setOperand(0, sqlNode[0]);
        }

        SqlTypeName sqlTypeName = call.operand(0).toString().contains(".")
          ? SqlTypeName.FLOAT : SqlTypeName.INTEGER;
        handleCasting(writer, call, leftPrec, rightPrec, sqlTypeName);
      }
      break;
    case 2:
      if (isOperandNull(call)) {
        handleNullOperand(writer, leftPrec, rightPrec);
      } else {
        if (Pattern.matches("^'[Xx]+'", call.operand(1).toString())) {
          SqlNode[] sqlNodes = new SqlNode[]{SqlLiteral.createCharString("0x",
            SqlParserPos.ZERO), call.operand(0)};
          SqlCall extractCall = new SqlBasicCall(SqlStdOperatorTable.CONCAT, sqlNodes,
            SqlParserPos.ZERO);
          call.setOperand(0, extractCall);
          handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.INTEGER);

        } else if (call.operand(0).toString().contains(".")) {
          String regEx = "[-',]+";
          handleNegativeValue(call, regEx);
          handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.FLOAT);
        } else {
          String regEx = "[-',$]+";
          if (call.operand(1).toString().contains("C")) {
            regEx = "[-',$A-Za-z]+";
          }

          handleNegativeValue(call, regEx);
          SqlTypeName sqlType = call.operand(0).toString().contains("E")
            && call.operand(1).toString().contains("E")
            ? SqlTypeName.DECIMAL : SqlTypeName.INTEGER;

          handleCasting(writer, call, leftPrec, rightPrec, sqlType);
        }
      }
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument Exception");
    }
  }

}
// End ToNumberUtils.java
