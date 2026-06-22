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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * This class is specific to BigQuery, Hive, Spark and Snowflake.
 */
public class ToNumberUtils {

  private ToNumberUtils() {
  }

  private static String regExRemove = "[',$A-Za-z]+";

  public static void unparseToNumber(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {

    final int operandCount = call.getOperandList().size();
    if (operandCount < 1 || operandCount > 3) {
      throw new IllegalArgumentException("Unsupported number of operands: " + operandCount);
    }
    if (isOperandLiteral(call) && isOperandNull(call)) {
      handleNullOperand(writer, leftPrec, rightPrec, dialect);
      return;
    }
    if (operandCount == 1 || operandCount == 3) {
      handleSingleOrTripleOperands(call, writer, leftPrec, rightPrec, dialect);
    } else {
      handleDoubleOperands(call, writer, leftPrec, rightPrec, dialect);
    }
  }

  private static void handleSingleOrTripleOperands(
      SqlCall call, SqlWriter writer, int leftPrec, int rightPrec, SqlDialect dialect) {

    if (call.operand(0) instanceof SqlCharStringLiteral) {
      String cleanedOperand = call.operand(0).toString().replaceAll(regExRemove, "").trim();
      call.setOperand(0, SqlLiteral.createCharString(cleanedOperand, SqlParserPos.ZERO));
    }

    RelDataType targetType = resolveToNumberTargetType(call, dialect, null);
    handleCasting(writer, call, leftPrec, rightPrec, targetType, dialect, false);
  }

  private static void handleDoubleOperands(
      SqlCall call, SqlWriter writer, int leftPrec, int rightPrec, SqlDialect dialect) {

    // Check if the second operand represents a Hex format string
    if (Pattern.matches("^'[Xx]+'", call.operand(1).toString())) {
      SqlNode[] concatNodes = new SqlNode[] {
          SqlLiteral.createCharString("0x", SqlParserPos.ZERO),
          call.operand(0)
      };
      SqlCall concatCall = new SqlBasicCall(SqlStdOperatorTable.CONCAT, concatNodes, SqlParserPos.ZERO);
      call.setOperand(0, concatCall);

      RelDataType bigintType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
      handleCasting(writer, call, leftPrec, rightPrec, bigintType, dialect, true);
      return;
    }
    boolean scientificFormat = call.operand(0).toString().contains("E")
        && call.operand(1).toString().contains("E");
    if (!(call.operand(0) instanceof SqlIdentifier)) {
      modifyOperand(call);
    }
    RelDataType targetType = resolveToNumberTargetType(call, dialect, scientificFormat);
    handleCasting(writer, call, leftPrec, rightPrec, targetType, dialect, false);
  }

  public static void unparseToNumberSnowFlake(SqlWriter writer, SqlCall call,
                                              int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
    case 3:
      SqlNode[] extractNodeOperands;
      extractNodeOperands = prepareSqlNodes(call);
      parseToNumber(writer, leftPrec, rightPrec, extractNodeOperands);
      break;
    case 2:
      if (isFirstOperandCurrencyType(call)) {
        String secondOperand = call.operand(1).toString().replaceAll("[UL]", "\\$")
                .replace("'", "");
        extractNodeOperands =
                new SqlNode[]{call.operand(0), SqlLiteral.createCharString(secondOperand.trim(),
                        SqlParserPos.ZERO)};
        parseToNumber(writer, leftPrec, rightPrec, extractNodeOperands);

      } else if (isOperandNull(call)) {

        extractNodeOperands = new SqlNode[]{new SqlDataTypeSpec(new
                SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
                SqlParserPos.ZERO)};

        parseToNumber(writer, leftPrec, rightPrec, extractNodeOperands);

      } else if (isOperandTypeOfCurrencyOrContainSpace(call)) {

        extractNodeOperands = prepareSqlNodes(call);
        parseToNumber(writer, leftPrec, rightPrec, extractNodeOperands);

      } else if (call.operand(0).toString().contains(".")) {

        String firstOperand =
                removeSignFromLastOfStringAndAddInBeginning(call,
                        call.operand(0).toString().replaceAll("[',]", ""));
        int scale = firstOperand.split("\\.")[1].length();
        extractNodeOperands = new SqlNode[]{SqlLiteral
            .createCharString(firstOperand.trim(), SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric
            ("38", SqlParserPos.ZERO), SqlLiteral.createExactNumeric(scale + "",
            SqlParserPos.ZERO)};
        parseToNumber(writer, leftPrec, rightPrec, extractNodeOperands);

      }
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument Exception");
    }
  }

  /**
   * Resolves the SQL type used when unparsing {@code TO_NUMBER} to a {@code CAST}.
   *
   * <p>When {@link SqlDialect#castsToNumberViaBigNumeric()} is enabled, string literals use
   * finer-grained rules so that high-magnitude decimals can remain as {@code BIGNUMERIC} while
   * other literals still map to {@code INT64} or {@code FLOAT64} as before.
   */
  private static RelDataType resolveToNumberTargetType(
      SqlCall call, SqlDialect dialect, @Nullable Boolean scientificFormat) {
    if (dialect.castsToNumberViaBigNumeric()
        && call.operand(0) instanceof SqlCharStringLiteral) {
      if (Boolean.TRUE.equals(scientificFormat)) {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL);
      }
      final String content =
          Objects.requireNonNull(
                  ((SqlCharStringLiteral) call.operand(0)).getValueAs(NlsString.class))
              .getValue();
      return inferBigQueryLiteralNumericType(content);
    }
    final SqlTypeName sqlTypeName;
    if (scientificFormat != null && scientificFormat) {
      sqlTypeName = SqlTypeName.DECIMAL;
    } else if (call.operand(0).toString().contains(".")) {
      sqlTypeName = SqlTypeName.FLOAT;
    } else {
      sqlTypeName = SqlTypeName.BIGINT;
    }
    return new BasicSqlType(RelDataTypeSystem.DEFAULT, sqlTypeName);
  }

  /**
   * BigQuery-specific inference for {@code TO_NUMBER} string literals without a format model.
   *
   * <p>Heuristic: values whose absolute integer part is at least 200 are treated as exact
   * decimals ({@code BIGNUMERIC} after casting), values in {@code [100, 200)} round toward an
   * integer ({@code INT64}), and smaller magnitudes use {@code FLOAT64} when a fractional part is
   * present.
   */
  private static BasicSqlType inferBigQueryLiteralNumericType(String content) {
    if (!content.contains(".")) {
      return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
    }
    try {
      final BigDecimal bd = new BigDecimal(content);
      if (bd.stripTrailingZeros().scale() <= 0) {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
      }
      final BigDecimal intPart = bd.setScale(0, RoundingMode.DOWN);
      final BigDecimal absInt = intPart.abs();
      if (absInt.compareTo(BigDecimal.valueOf(200)) >= 0) {
        return new BasicSqlType(
            RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 38, bd.scale());
      }
      if (absInt.compareTo(BigDecimal.valueOf(100)) >= 0) {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
      }
      return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT);
    } catch (NumberFormatException e) {
      return new BasicSqlType(
          RelDataTypeSystem.DEFAULT,
          content.contains(".") ? SqlTypeName.FLOAT : SqlTypeName.BIGINT);
    }
  }

  private static SqlNode bigQueryBigNumericCastSpec() {
    final SqlParserPos pos = SqlParserPos.ZERO;
    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec("BIGNUMERIC", SqlTypeName.DECIMAL, pos), pos);
  }

  private static void handleCasting(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      RelDataType targetType, SqlDialect dialect, boolean skipBigNumericBridge) {
    final SqlTypeName typeName = targetType.getSqlTypeName();
    if (dialect.castsToNumberViaBigNumeric()
        && !skipBigNumericBridge
        && (typeName == SqlTypeName.BIGINT || typeName == SqlTypeName.FLOAT)) {
      final SqlParserPos pos = SqlParserPos.ZERO;
      final SqlNode innerCast =
          new SqlBasicCall(
              SqlStdOperatorTable.CAST,
              new SqlNode[]{call.operand(0), bigQueryBigNumericCastSpec()},
              pos);
      final SqlNode outerCastSpec = dialect.getCastSpec(targetType);
      final SqlCall outerCast =
          new SqlBasicCall(
              SqlStdOperatorTable.CAST, new SqlNode[]{innerCast, outerCastSpec}, pos);
      writer.getDialect().unparseCall(writer, outerCast, leftPrec, rightPrec);
      return;
    }
    SqlNode[] extractNodeOperands =
        new SqlNode[]{call.operand(0), dialect.getCastSpec(targetType)};
    SqlCall extractCallCast =
        new SqlBasicCall(SqlStdOperatorTable.CAST, extractNodeOperands, SqlParserPos.ZERO);
    writer.getDialect().unparseCall(writer, extractCallCast, leftPrec, rightPrec);
  }

  private static void modifyOperand(SqlCall call) {
    String regEx = "[',$]+";
    if (call.operand(1).toString().contains("C")) {
      regEx = "[',$A-Za-z]+";
    }

    String firstOperand =
            removeSignFromLastOfStringAndAddInBeginning(call,
                    call.operand(0).toString().replaceAll(regEx, ""));

    SqlNode[] sqlNode =
            new SqlNode[]{SqlLiteral.createCharString(firstOperand.trim(), SqlParserPos.ZERO)};
    call.setOperand(0, sqlNode[0]);
  }

  private static String removeSignFromLastOfStringAndAddInBeginning(SqlCall call,
                                                                    String firstOperand) {
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      if (call.operand(0).toString().contains("-")) {
        firstOperand = firstOperand.replaceAll("-", "");
        firstOperand = "-" + firstOperand;
      } else {
        firstOperand = firstOperand.replaceAll("\\+", "");
      }
    }
    return firstOperand;
  }

  private static boolean handleNullOperand(
      SqlWriter writer, int leftPrec, int rightPrec, SqlDialect dialect) {
    SqlNode[] extractNodeOperands =
      new SqlNode[]{new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.NULL,
        SqlParserPos.ZERO), SqlParserPos.ZERO),
        dialect.getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER))};

    SqlCall extractCallCast =
        new SqlBasicCall(SqlStdOperatorTable.CAST, extractNodeOperands, SqlParserPos.ZERO);

    writer.getDialect().unparseCall(writer, extractCallCast, leftPrec, rightPrec);
    return true;
  }

  private static boolean isOperandNull(SqlCall call) {
    for (SqlNode sqlNode : call.getOperandList()) {
      SqlLiteral literal = (SqlLiteral) sqlNode;
      if (literal.getValue() == null) {
        return true;
      }
    }
    return false;
  }

  public static void unparseToNumbertoConv(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {
    SqlNode[] sqlNode =
        new SqlNode[]{call.getOperandList().get(0), SqlLiteral.createExactNumeric("16",
                SqlParserPos.ZERO),
        SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO)};
    SqlCall extractCall =
        new SqlBasicCall(SqlLibraryOperators.CONV, sqlNode, SqlParserPos.ZERO);
    call.setOperand(0, extractCall);
    handleCasting(writer, call, leftPrec, rightPrec,
        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
        dialect, false);
  }

  private static boolean isOperandLiteral(SqlCall call) {
    return call.operand(0) instanceof SqlCharStringLiteral || call.operand(0)
            instanceof SqlLiteral;
  }

  private static boolean isFirstOperandCurrencyType(SqlCall call) {
    return call.operand(0).toString().contains("$") && (call.operand(1).toString().contains("L")
            || call.operand(1).toString().contains("U"));
  }

  private static boolean isOperandTypeOfCurrencyOrContainSpace(SqlCall call) {
    return call.operand(1).toString().contains("PR")
            || (call.operand(0).toString().contains("USD")
            && call.operand(1).toString().contains("C"));
  }

  public static boolean needsCustomUnparsing(SqlCall call) {
    if (((call.getOperandList().size() == 1 || call.getOperandList().size() == 3)
            && isOperandLiteral(call))
        || (call.getOperandList().size() == 2 && isOperandLiteral(call)
            && (isFirstOperandCurrencyType(call)
            || isOperandNull(call)
            || isOperandTypeOfCurrencyOrContainSpace(call)
            || call.operand(0).toString().contains(".")))) {
      return true;
    }
    return false;
  }

  private static SqlNode[] prepareSqlNodes(SqlCall call) {
    if (isOperandNull(call)) {
      SqlNode[] extractNodeOperands = new SqlNode[]{new SqlDataTypeSpec(new
              SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
              SqlParserPos.ZERO)};
      return extractNodeOperands;
    }
    String firstOperand = call.operand(0).toString().replaceAll(regExRemove, "");
    if (firstOperand.contains(".")) {
      int scale = firstOperand.split("\\.")[1].length();

      SqlNode[] extractNodeOperands = new SqlNode[]{SqlLiteral
          .createCharString(firstOperand.trim(), SqlParserPos.ZERO),
          SqlLiteral.createExactNumeric
          ("38", SqlParserPos.ZERO), SqlLiteral.createExactNumeric(scale + "",
          SqlParserPos.ZERO)};
      return extractNodeOperands;
    }
    SqlNode[] extractNodeOperands = new SqlNode[]{SqlLiteral
            .createCharString(firstOperand.trim(), SqlParserPos.ZERO)};
    return extractNodeOperands;
  }

  private static void parseToNumber(SqlWriter writer, int leftPrec, int rightPrec,
                                    SqlNode[] extractNodeOperands) {
    SqlCall extractCallCast =
            new SqlBasicCall(SqlStdOperatorTable.TO_NUMBER, extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.TO_NUMBER.unparse(writer, extractCallCast, leftPrec, rightPrec);
  }
}
