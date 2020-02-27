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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.regex.Pattern;


/**
 * This class is specific to BigQuery, Hive and Spark.
 */
public class ToNumberUtils {

  private ToNumberUtils() {
  }

  private static void handleCasting(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlTypeName sqlTypeName, SqlDialect sqlDialect) {
    SqlNode[] extractNodeOperands = new SqlNode[]{
        call.operand(0),
        sqlDialect.getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, sqlTypeName))
    };
    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
        extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
  }

  private static void handleNegativeValue(SqlCall call, String regEx) {
    String firstOperand = call.operand(0).toString().replaceAll(regEx, "");
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      firstOperand = "-" + firstOperand;
    }

    SqlNode[] sqlNode = new SqlNode[]{
        SqlLiteral.createCharString(firstOperand.trim(),
            SqlParserPos.ZERO)
    };
    call.setOperand(0, sqlNode[0]);
  }

  public static boolean handleIfOperandIsNull(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect sqlDialect) {
    if (call.operand(0).toString().equalsIgnoreCase("null")
        || (call.getOperandList().size() == 2 && call.operand(1).toString().equalsIgnoreCase
        ("null"))) {

      SqlNode[] extractNodeOperands = new SqlNode[]{
          new SqlDataTypeSpec(new
              SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
              SqlParserPos.ZERO),
          sqlDialect.getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER))
      };

      SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
          extractNodeOperands, SqlParserPos.ZERO);

      SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
      return true;
    }
    return false;
  }

  public static void handleToNumber(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
      if (handleIfOperandIsNull(writer, call, leftPrec, rightPrec, writer.getDialect())) {
        return;
      }
      if (call.operand(0) instanceof SqlCharStringLiteral && Pattern.matches("['.-[0-9]]+", call
          .operand(0).toString())) {
        String regEx1 = "[',]+";
        String firstOperand = call.operand(0).toString().replaceAll(regEx1, "");
        SqlNode[] sqlNode = new SqlNode[]{
            SqlLiteral.createCharString(firstOperand.trim(),
                SqlParserPos.ZERO)
        };
        call.setOperand(0, sqlNode[0]);
      }

      SqlTypeName sqlTypeName = call.operand(0).toString().contains(".")
          ? SqlTypeName.FLOAT : SqlTypeName.INTEGER;
      handleCasting(writer, call, leftPrec, rightPrec, sqlTypeName, writer.getDialect());

      break;
    case 2:
      if (handleIfOperandIsNull(writer, call, leftPrec, rightPrec, writer.getDialect())) {
        return;
      }
      if (Pattern.matches("^'[Xx]+'", call.operand(1).toString())) {

        if (!writer.getDialect().getConformance().toString().equals("BIG_QUERY")) {
          throw new UnsupportedOperationException();
        }
        SqlNode[] sqlNodes = new SqlNode[]{
            SqlLiteral.createCharString("0x",
                SqlParserPos.ZERO), call.operand(0)
        };
        SqlCall extractCall = new SqlBasicCall(SqlStdOperatorTable.CONCAT, sqlNodes,
            SqlParserPos.ZERO);
        call.setOperand(0, extractCall);
        handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.INTEGER, writer.getDialect());

      } else if (call.operand(0).toString().contains(".")) {
        String regEx = "[-',]+";
        handleNegativeValue(call, regEx);
        handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.FLOAT, writer.getDialect());
      } else {
        String regEx = "[-',$]+";
        if (call.operand(1).toString().contains("C")) {
          regEx = "[-',$A-Za-z]+";
        }

        handleNegativeValue(call, regEx);
        SqlTypeName sqlType = call.operand(0).toString().contains("E")
            && call.operand(1).toString().contains("E")
            ? SqlTypeName.DECIMAL : SqlTypeName.INTEGER;

        handleCasting(writer, call, leftPrec, rightPrec, sqlType, writer.getDialect());
      }
      break;
    case 3:
      if (call.operand(1).toString().replaceAll("[L']+", "").length()
          + call.operand(2).toString().split("=")[1].replaceAll("[']+", "").length()
          == call.operand(0).toString().replaceAll("[']+", "").length()) {

        int lengthOfFirstArgument = call.operand(0).toString().replaceAll("[']+", "").length()
            - call.operand(1).toString().replaceAll("[L']+", "").length();

        SqlNode[] sqlNodes = new SqlNode[]{
            SqlLiteral.createCharString(call.operand(0).toString().replaceAll("[']+", "")
                    .substring(lengthOfFirstArgument, call.operand(0)
                        .toString().replaceAll("[']+", "").length()),
                call.operand(1).getParserPosition())
        };

        call.setOperand(0, sqlNodes[0]);

        handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.INTEGER, writer.getDialect());
      }
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument Exception");
    }
  }

}
