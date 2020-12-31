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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.FormatFunctionUtil;
import org.apache.calcite.util.ToNumberUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_DATE;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.TO_UPPER)
          .withConformance(SqlConformanceEnum.SNOWFLAKE));

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
      return getTargetFunctionForDateOperations(call);
    default:
      return super.getTargetFunc(call);
    }
  }

  private SqlOperator getTargetFunctionForDateOperations(RexCall call) {
    switch (call.getOperands().get(1).getType().getSqlTypeName()) {
    case INTERVAL_DAY:
      if (call.op.kind == SqlKind.MINUS) {
        return SqlLibraryOperators.DATE_SUB;
      }
      return SqlLibraryOperators.DATE_ADD;
    }
    return super.getTargetFunc(call);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final
  int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case TO_NUMBER:
      if (ToNumberUtils.needsCustomUnparsing(call)) {
        ToNumberUtils.unparseToNumberSnowFlake(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case CHAR_LENGTH:
      final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(lengthFrame);
      break;
    case FORMAT:
      FormatFunctionUtil ffu = new FormatFunctionUtil();
      SqlCall sqlCall = ffu.fetchSqlCallForFormat(call);
      super.unparseCall(writer, sqlCall, leftPrec, rightPrec);
      break;
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    case TRUNCATE:
    case IF:
    case OTHER_FUNCTION:
      unparseOtherFunction(writer, call, leftPrec, rightPrec);
      break;
    case DIVIDE_INTEGER:
      unparseDivideInteger(writer, call, leftPrec, rightPrec);
      break;
    case OVER:
      handleOverCall(writer, call, leftPrec, rightPrec);
      break;
    case TIMES:
      unparseIntervalTimes(writer, call, leftPrec, rightPrec);
      break;
    case PLUS:
      unparseTimestampAdd(writer, call, leftPrec, rightPrec);
      //unparseDateMinus(writer, call, leftPrec, rightPrec, "");
      break;
    case MINUS:
      unparseDateMinus(writer, call, leftPrec, rightPrec, "-");
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseDateMinus(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec, String sign) {
    if ("TIMESTAMP_SUB".equals(call.getOperator().getName())
        || "TIMESTAMP_ADD".equals(call.getOperator().getName())) {
      unparseTimestampInterval(writer, call, leftPrec, rightPrec, sign);
    } else if ("DATE_SUB".equals(call.getOperator().getName())
        || "DATE_ADD".equals(call.getOperator().getName())) {
      unparseDateSubInterval(writer, call, leftPrec, rightPrec, sign);
    } else {
      unparseMinusIntervalOperand(writer, call, leftPrec, rightPrec, sign);
    }
  }

  private void unparseMinusIntervalOperand(SqlWriter writer, SqlCall call,
       int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_DAY":
      case "INTERVAL_MONTH":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      case "INTERVAL_YEAR_MONTH":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split("-");
        unparseDateAddBasedonTimeUnit(writer, "YEAR", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MONTH", dayTimeSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      }
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseDateSubInterval(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_YEAR_MONTH":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split("-");
        unparseDateAddBasedonTimeUnit(writer, "YEAR", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MONTH", dayTimeSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_MONTH":
      case "INTERVAL_DAY":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      }
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseTimestampInterval(SqlWriter writer, SqlCall call,
          int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_DAY_SECOND":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split(" ");
        String[] timeSplit = dayTimeSplit[1].split(":");
        unparseDateAddBasedonTimeUnit(writer, "DAY", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "HOUR", timeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", timeSplit[1], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", timeSplit[2], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))))");
        break;
      case "INTERVAL_MINUTE_SECOND":
        String value1 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit1 = value1.split(":");
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", dayTimeSplit1[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", dayTimeSplit1[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_SECOND":
      case "INTERVAL_MINUTE":
      case "INTERVAL_HOUR":
      case "INTERVAL_DAY":
      case "INTERVAL_MONTH":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      case "INTERVAL_HOUR_SECOND":
        String hourToSecond = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] hourToSecondSplit = hourToSecond.split(":");
        unparseDateAddBasedonTimeUnit(writer, "HOUR", hourToSecondSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", hourToSecondSplit[1], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", hourToSecondSplit[2], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_DAY_HOUR":
        String dayToHour = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayToHourSplit = dayToHour.split(" ");
        unparseDateAddBasedonTimeUnit(writer, "DAY", dayToHourSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "HOUR", dayToHourSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      }
    } else if (call.operand(1) instanceof SqlBasicCall) {
      SqlCall node1 = (SqlBasicCall) call.operand(1);
      if (node1 instanceof SqlBasicCall) {
        SqlCall intervalNode = node1;
        if (node1.operand(0) instanceof SqlCall) {
          intervalNode = node1.operand(0);
        }
        if (intervalNode.operand(0) instanceof SqlIntervalLiteral) {
          unparseDateAddBasedonTimeUnit(writer,
              ((SqlIntervalLiteral) intervalNode.operand(0)).getTypeName().toString(),
              ((SqlIntervalLiteral) intervalNode.operand(0)).getValue().toString(), sign);
        }
        if (node1.operand(0) instanceof SqlCall
            && intervalNode.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateAddBasedonTimeUnit(writer,
              ((SqlIntervalLiteral) intervalNode.operand(1)).getTypeName().toString(),
              ((SqlIntervalLiteral) intervalNode.operand(1)).getValue().toString(), sign);
        }
        if (node1.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateTimeIntervalWithActualOperand(writer, node1,
              leftPrec, rightPrec, call.operand(0), sign);
          writer.print(")");
        }
        if (node1.operand(0) instanceof SqlCall) {
          writer.print(")");
        }
      } else {
        if (call.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateTimeIntervalWithActualOperand(writer, call,
              leftPrec, rightPrec, call.operand(0), sign);
        }
      }
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseDateTimeIntervalWithActualOperand(SqlWriter writer, SqlCall call,
          int leftPrec, int rightPrec, SqlNode operand, String sign) {
    final SqlWriter.Frame dateAddFrame = writer.startFunCall("DATEADD");
    writer.print(((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()
        .replace("INTERVAL_", ""));
    String intervalSign = String.valueOf(((SqlIntervalLiteral.IntervalValue)
        ((SqlIntervalLiteral) call.operand(1))
        .getValue()).getSign()).replace("1", "");
    if ("-".equals(intervalSign)) {
      sign = intervalSign;
    }
    writer.print(", " + sign);
    writer.print(((SqlIntervalLiteral) call.operand(1)).getValue().toString());
    writer.print(", ");
    operand.unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(dateAddFrame);
  }

  private void unparseDateAddBasedonTimeUnit(SqlWriter writer, String typeName, String value,
           String sign) {
    writer.print("DATEADD(");
    writer.print(typeName.replace("INTERVAL_", ""));
    writer.print(", " + sign);
    writer.print(value);
    writer.print(", ");
  }

  private void unparseTimestampAdd(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.operand(1) instanceof SqlBasicCall
        && ((SqlBasicCall) call.operand(1)).getOperandList().get(0) instanceof SqlIntervalLiteral
        && SqlKind.PLUS != ((SqlBasicCall) call.operand(1)).getOperator().getKind()) {
      unparseDateAddForInterval(writer, call, leftPrec, rightPrec);
    } else if ("DATE_ADD".equals(call.getOperator().toString())) {
      unparseDateMinus(writer, call, leftPrec, rightPrec, "");
//      super.unparseCall(writer, call, leftPrec, rightPrec);
    } else {
      unparseDateMinus(writer, call, leftPrec, rightPrec, "");
    }
  }

  private void unparseDateAddForInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    String timeUnit = ((SqlIntervalLiteral.IntervalValue)
        ((SqlIntervalLiteral) ((SqlBasicCall) call.operand(1)).operand(0)).getValue()).
        getIntervalQualifier().timeUnitRange.toString();
    SqlCall multipleCall = unparseMultipleInterval(call);
    SqlNode[] sqlNodes = new SqlNode[]{SqlLiteral.createSymbol(TimeUnit.valueOf(timeUnit),
        SqlParserPos.ZERO), multipleCall, call.operand(0)};
    unparseDateAdd(writer, leftPrec, rightPrec, sqlNodes);
  }

  private void unparseDateAdd(SqlWriter writer, int leftPrec, int rightPrec, SqlNode[] sqlNodes) {
    final SqlWriter.Frame dateAddFrame = writer.startFunCall("DATEADD");
    for (SqlNode operand : sqlNodes) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(dateAddFrame);
  }

  private void unparseIntervalTimes(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.operand(0) instanceof SqlIntervalLiteral) {
      SqlCall multipleCall = unparseMultipleInterval(call);
      multipleCall.unparse(writer, leftPrec, rightPrec);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private SqlCall unparseMultipleInterval(SqlCall call) {
    SqlNode[] timesNodes = null;
    if (call.operand(1) instanceof SqlBasicCall) {
      timesNodes = new SqlNode[] {
          SqlLiteral.createCharString(
              ((SqlIntervalLiteral) ((SqlBasicCall) call.operand(1)).operand(0)).
                  getValue().toString(), SqlParserPos.ZERO),
          ((SqlBasicCall) call.operand(1)).operand(1)
      };
    } else if (call.operand(0) instanceof SqlIntervalLiteral) {
      timesNodes = new SqlNode[] {
          SqlLiteral.createCharString(
              ((SqlIntervalLiteral) call.operand(0)).
                  getValue().toString(), SqlParserPos.ZERO), call.operand(1)
      };
    }
    return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, timesNodes,
        SqlParserPos.ZERO);
  }

  private void handleOverCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (checkWindowFunctionContainOrderBy(call)) {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    } else {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      unparseSqlWindow(call.operand(1), writer);
    }
  }

  private boolean checkWindowFunctionContainOrderBy(SqlCall call) {
    return !((SqlWindow) call.operand(1)).getOrderList().getList().isEmpty();
  }

  private void unparseSqlWindow(SqlWindow call, SqlWriter writer) {
    final SqlWindow window = call;
    writer.print("OVER ");
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.WINDOW, "(", ")");
    if (window.getRefName() != null) {
      window.getRefName().unparse(writer, 0, 0);
    }
    if (window.getPartitionList().size() > 0) {
      writer.sep("PARTITION BY");
      final SqlWriter.Frame partitionFrame = writer.startList("", "");
      window.getPartitionList().unparse(writer, 0, 0);
      writer.endList(partitionFrame);
    }
    if (window.getOrderList().size() > 0) {
      writer.sep("ORDER BY");
      final SqlWriter.Frame orderFrame = writer.startList("", "");
      window.getOrderList().unparse(writer, 0, 0);
      writer.endList(orderFrame);
    }
    writer.endList(frame);
  }

  private void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "TRUNCATE":
    case "ROUND":
      handleMathFunction(writer, call, leftPrec, rightPrec);
      break;
    case "FORMAT_DATE":
      final SqlWriter.Frame formatDate = writer.startFunCall("TO_VARCHAR");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.print(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(formatDate);
      break;
    case "LOG10":
      if (call.operand(0) instanceof SqlLiteral && "1".equals(call.operand(0).toString())) {
        writer.print(0);
      } else {
        final SqlWriter.Frame logFrame = writer.startFunCall("LOG");
        writer.print("10");
        writer.print(", ");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(logFrame);
      }
      break;
    case "IF":
      unparseIf(writer, call, leftPrec, rightPrec);
      break;
    case "STR_TO_DATE":
      SqlCall parseDateCall = TO_DATE.createCall(SqlParserPos.ZERO, call.operand(0),
          call.operand(1));
      unparseCall(writer, parseDateCall, leftPrec, rightPrec);
      break;
    case "INSTR":
      final SqlWriter.Frame regexpInstr = writer.startFunCall("REGEXP_INSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(regexpInstr);
      break;
    case "DATE_MOD":
      unparseDateModule(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * unparse function for math functions
   * SF can support precision and scale within specific range
   * handled precision range using 'case', 'when', 'then'
   */
  private void handleMathFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame mathFun = writer.startFunCall(call.getOperator().getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    if (call.getOperandList().size() > 1) {
      writer.print(",");
      if (call.operand(1) instanceof SqlNumericLiteral) {
        call.operand(1).unparse(writer, leftPrec, rightPrec);
      } else {
        writer.print("CASE WHEN ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("> 38 THEN 38 ");
        writer.print("WHEN ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("< -12 THEN -12 ");
        writer.print("ELSE ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("END");
      }
    }
    writer.endFunCall(mathFun);
  }

  /**
   * For usage of DATE_ADD,DATE_SUB function in Snowflake. It will unparse the SqlCall and write it
   * into Snowflake format. Below are few examples:
   * Example 1:
   * Input: select date + INTERVAL 1 DAY
   * It will write output query as: select (date + 1)
   * Example 2:
   * Input: select date + Store_id * INTERVAL 2 DAY
   * It will write output query as: select (date + Store_id * 2)
   *
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall : date + Store_id * INTERVAL 2 DAY
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate left precision
   */
  @Override public void unparseIntervalOperandsBasedFunctions(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep((SqlKind.PLUS == call.getKind()) ? "+" : "-");
    switch (call.operand(1).getKind()) {
    case LITERAL:
      unparseSqlIntervalLiteral(writer, call.operand(1), leftPrec, rightPrec);
      break;
    case TIMES:
      unparseExpressionIntervalCall(writer, call.operand(1), leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
    writer.endList(frame);
  }

  /**
   * Unparse the literal call from input query and write the INTERVAL part. Below is an example:
   * Input: INTERVAL 2 DAY
   * It will write this as: 2
   *
   * @param literal SqlIntervalLiteral :INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   */
  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    if (interval.getSign() == -1) {
      writer.print("(-");
      writer.literal(interval.getIntervalLiteral());
      writer.print(")");
    } else {
      writer.literal(interval.getIntervalLiteral());
    }
  }

  /**
   * Unparse the SqlBasic call and write INTERVAL with expression. Below are the examples:
   * Example 1:
   * Input: store_id * INTERVAL 1 DAY
   * It will write this as: store_id
   * Example 2:
   * Input: 10 * INTERVAL 2 DAY
   * It will write this as: 10 * 2
   *
   * @param call SqlCall : store_id * INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate right precision
   */
  private void unparseExpressionIntervalCall(
      SqlWriter writer, SqlBasicCall call, int leftPrec, int rightPrec) {
    SqlLiteral intervalLiteral = getIntervalLiteral(call);
    SqlNode identifier = getIdentifier(call);
    SqlIntervalLiteral.IntervalValue literalValue =
        (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    if (call.getKind() == SqlKind.TIMES) {
      identifier.unparse(writer, leftPrec, rightPrec);
      if (!literalValue.getIntervalLiteral().equals("1")) {
        writer.sep("*");
        writer.sep(literalValue.toString());
      }
    }
  }

  /**
   * Return the SqlLiteral from the SqlBasicCall.
   *
   * @param intervalOperand store_id * INTERVAL 1 DAY
   * @return SqlLiteral INTERVAL 1 DAY
   */
  private SqlLiteral getIntervalLiteral(SqlBasicCall intervalOperand) {
    if (intervalOperand.operand(1).getKind() == SqlKind.IDENTIFIER
        || (intervalOperand.operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(0);
    }
    return ((SqlBasicCall) intervalOperand).operand(1);
  }

    /**
     * For usage of TRIM, LTRIM and RTRIM in SnowFlake
     */
  private void unparseTrim(
      SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operand(0) instanceof SqlLiteral : call.operand(0);
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    SqlLiteral valueToTrim = call.operand(1);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    if (!valueToTrim.toValue().matches("\\s+")) {
      writer.literal(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(trimFrame);
  }

  /**
   * For usage of IFF() in snowflake
   */
  private void unparseIf(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame iffFrame = writer.startFunCall("IFF");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(iffFrame);
  }

  /**
   * Return the identifer from the SqlBasicCall.
   *
   * @param intervalOperand Store_id * INTERVAL 1 DAY
   * @return SqlIdentifier Store_id
   */
  private SqlNode getIdentifier(SqlBasicCall intervalOperand) {
    if (intervalOperand.operand(1).getKind() == SqlKind.IDENTIFIER
        || (intervalOperand.operand(1) instanceof SqlNumericLiteral)) {
      return intervalOperand.operand(1);
    }
    return intervalOperand.operand(0);
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    return ((SqlBasicCall) aggCall).operand(0);
  }

}

// End SnowflakeSqlDialect.java
