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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDateTimeFormat;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.CurrentTimestampHandler;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.CastCallBuilder;
import org.apache.calcite.util.PaddingFunctionUtil;
import org.apache.calcite.util.RelToSqlConverterUtil;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.ToNumberUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDDAYOFWEEK;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.AMPM;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFWEEK;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.DDMMYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.DDMMYYYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.FOURDIGITYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONFIVE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONFOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONSIX;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONTHREE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONTWO;
import static org.apache.calcite.sql.SqlDateTimeFormat.HOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.MINUTE;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYYYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MONTHNAME;
import static org.apache.calcite.sql.SqlDateTimeFormat.NUMERICMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.SECOND;
import static org.apache.calcite.sql.SqlDateTimeFormat.TIMEZONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWENTYFOURHOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWODIGITYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYMMDD;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FORMAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_UNIXTIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPLIT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

/**
 * A <code>SqlDialect</code> implementation for the APACHE SPARK database.
 */
public class SparkSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SparkSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SPARK)
          .withNullCollation(NullCollation.LOW));

  private static final SqlFunction SPARKSQL_SUBSTRING =
      new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  private static final Map<SqlDateTimeFormat, String> DATE_TIME_FORMAT_MAP =
      new HashMap<SqlDateTimeFormat, String>() {{
        put(DAYOFMONTH, "dd");
        put(DAYOFYEAR, "ddd");
        put(NUMERICMONTH, "MM");
        put(ABBREVIATEDMONTH, "MMM");
        put(MONTHNAME, "MMMM");
        put(TWODIGITYEAR, "yy");
        put(FOURDIGITYEAR, "yyyy");
        put(DDMMYYYY, "ddMMyyyy");
        put(DDMMYY, "ddMMyy");
        put(MMDDYYYY, "MMddyyyy");
        put(MMDDYY, "MMddyy");
        put(YYYYMMDD, "yyyyMMdd");
        put(YYMMDD, "yyMMdd");
        put(DAYOFWEEK, "EEEE");
        put(ABBREVIATEDDAYOFWEEK, "EEE");
        put(TWENTYFOURHOUR, "HH");
        put(HOUR, "hh");
        put(MINUTE, "mm");
        put(SECOND, "ss");
        put(FRACTIONONE, "s");
        put(FRACTIONTWO, "ss");
        put(FRACTIONTHREE, "sss");
        put(FRACTIONFOUR, "ssss");
        put(FRACTIONFIVE, "sssss");
        put(FRACTIONSIX, "ssssss");
        put(AMPM, "aa");
        put(TIMEZONE, "z");
      }};

  /**
   * Creates a SparkSqlDialect.
   */
  public SparkSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsAnalyticalFunctionInAggregate() {
    return false;
  }

  @Override public boolean supportsAnalyticalFunctionInGroupBy() {
    return false;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public JoinType emulateJoinTypeForCrossJoin() {
    return JoinType.CROSS;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
      switch (call.getOperands().get(1).getType().getSqlTypeName()) {
      case INTERVAL_DAY:
        if (call.op.kind == SqlKind.MINUS) {
          return SqlLibraryOperators.DATE_SUB;
        }
        return SqlLibraryOperators.DATE_ADD;
      case INTERVAL_MONTH:
        return SqlLibraryOperators.ADD_MONTHS;
      }
    default:
      return super.getTargetFunc(call);
    }
  }

  @Override public SqlNode getCastCall(
      SqlNode operandToCast, RelDataType castFrom, RelDataType castTo) {
    if (castTo.getSqlTypeName() == SqlTypeName.TIMESTAMP && castTo.getPrecision() > 0) {
      return new CastCallBuilder(this).makCastCallForTimestampWithPrecision(operandToCast,
          castTo.getPrecision());
    } else if (castTo.getSqlTypeName() == SqlTypeName.TIME) {
      if (castFrom.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        return new CastCallBuilder(this)
            .makCastCallForTimeWithPrecision(operandToCast, castTo.getPrecision());
      }
      return operandToCast;
    }
    return super.getCastCall(operandToCast, castFrom, castTo);
  }

  @Override public SqlNode getTimeLiteral(
      TimeString timeString, int precision, SqlParserPos pos) {
    return SqlLiteral.createCharString(timeString.toString(), SqlParserPos.ZERO);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call,
      final int leftPrec, final int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      SqlUtil.unparseFunctionSyntax(SPARKSQL_SUBSTRING, writer, call);
    } else {
      switch (call.getKind()) {

      case POSITION:
        final SqlWriter.Frame frame = writer.startFunCall("INSTR");
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        if (3 == call.operandCount()) {
          throw new RuntimeException("3rd operand Not Supported for Function INSTR in Hive");
        }
        writer.endFunCall(frame);
        break;
      case MOD:
        SqlOperator op = SqlStdOperatorTable.PERCENT_REMAINDER;
        SqlSyntax.BINARY.unparse(writer, op, call, leftPrec, rightPrec);
        break;
      case CHAR_LENGTH:
      case CHARACTER_LENGTH:
        final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(lengthFrame);
        break;
      case SUBSTRING:
        final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endFunCall(substringFrame);
        break;
      case EXTRACT:
        final SqlWriter.Frame extractFrame = writer.startFunCall(call.operand(0).toString());
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(extractFrame);
        break;
      case ARRAY_VALUE_CONSTRUCTOR:
        writer.keyword(call.getOperator().getName());
        final SqlWriter.Frame arrayFrame = writer.startList("(", ")");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(arrayFrame);
        break;
      case CONCAT:
        final SqlWriter.Frame concatFrame = writer.startFunCall("CONCAT");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endFunCall(concatFrame);
        break;
      case DIVIDE_INTEGER:
        unparseDivideInteger(writer, call, leftPrec, rightPrec);
        break;
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }

        final SqlLiteral timeUnitNode = call.operand(1);
        final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

        SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
            timeUnitNode.getParserPosition());
        SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
        break;
      case FORMAT:
        unparseFormat(writer, call, leftPrec, rightPrec);
        break;
      case TO_NUMBER:
        if (call.getOperandList().size() == 2 && Pattern.matches("^'[Xx]+'", call.operand(1)
                .toString())) {
          ToNumberUtils.unparseToNumbertoConv(writer, call, leftPrec, rightPrec);
          break;
        }
        ToNumberUtils.unparseToNumber(writer, call, leftPrec, rightPrec);
        break;
      case TRIM:
        unparseTrim(writer, call, leftPrec, rightPrec);
        break;
      case OTHER_FUNCTION:
        unparseOtherFunction(writer, call, leftPrec, rightPrec);
        break;
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
    return;
  }

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {
    switch (sqlKind) {
    case MINUS:
      final SqlWriter.Frame dateDiffFrame = writer.startFunCall("DATEDIFF");
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(dateDiffFrame);
      break;
    }
  }

  /**
   * For usage of DATE_ADD,DATE_SUB,ADD_MONTH function in SPARK. It will unparse the SqlCall and
   * write it into SPARK format, below are few examples:
   * Example 1:
   * Input: select date + INTERVAL 1 DAY
   * It will write the output query as: select DATE_ADD(date , 1)
   * Example 2:
   * Input: select date + Store_id * INTERVAL 2 MONTH
   * It will write the output query as: select ADD_MONTH(date , Store_id * 2)
   *
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall : date + Store_id * INTERVAL 2 MONTH
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate right precision
   */
  @Override public void unparseIntervalOperandsBasedFunctions(
      SqlWriter writer,
      SqlCall call, int leftPrec, int rightPrec) {
    switch (call.operand(1).getKind()) {
    case LITERAL:
    case TIMES:
      unparseIntervalOperandCall(call, writer, leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
  }

  private void unparseIntervalOperandCall(
      SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print(call.getOperator().toString());
    writer.print("(");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(",");
    SqlNode intervalValue = modifySqlNode(writer, call.operand(1));
    writer.print(intervalValue.toString().replace("`", ""));
    writer.print(")");
  }

  /**
   * Modify the SqlNode to expected output form.
   * If SqlNode Kind is Literal then it will return the literal value and for
   * the Kind TIMES it will modify it to expression if required else return the
   * identifer part.Below are few examples:
   *
   * For SqlKind LITERAL:
   * Input: INTERVAL 1 DAY
   * Output: 1
   *
   * For SqlKind TIMES:
   * Input: store_id * INTERVAL 2 DAY
   * Output: store_id * 2
   *
   * @param writer Target SqlWriter to write the call
   * @param intervalOperand SqlNode
   * @return Modified SqlNode
   */

  private SqlNode modifySqlNode(SqlWriter writer, SqlNode intervalOperand) {

    if (intervalOperand.getKind() == SqlKind.LITERAL) {
      return modifySqlNodeForLiteral(writer, intervalOperand);
    }
    return modifySqlNodeForExpression(writer, intervalOperand);
  }

  /**
   * Modify the SqlNode Expression call to desired output form.
   * Below are the few examples:
   * Example 1:
   * Input: store_id * INTERVAL 1 DAY
   * Output: store_id
   * Example 2:
   * Input: 10 * INTERVAL 2 DAY
   * Output: 10 * 2
   *
   * @param writer  Target SqlWriter to write the call
   * @param intervalOperand store_id * INTERVAL 2 DAY
   * @return Modified SqlNode store_id * 2
   */
  private SqlNode modifySqlNodeForExpression(SqlWriter writer, SqlNode intervalOperand) {
    SqlLiteral intervalLiteralValue = getIntervalLiteral(intervalOperand);
    SqlNode identifierValue = getIdentifier(intervalOperand);
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) intervalLiteralValue.getValue();
    writeNegativeLiteral(interval, writer);
    if (interval.getIntervalLiteral().equals("1")) {
      return identifierValue;
    }
    SqlNode intervalValue = new SqlIdentifier(interval.toString(),
        intervalOperand.getParserPosition());
    SqlNode[] sqlNodes = new SqlNode[]{identifierValue,
        intervalValue};
    return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, sqlNodes, SqlParserPos.ZERO);
  }

  /**
   * Modify the SqlNode Literal call to desired output form.
   * For example :
   * Input: INTERVAL 1 DAY
   * Output: 1
   * Input: INTERVAL -1 DAY
   * Output: -1
   *
   * @param writer Target SqlWriter to write the call
   * @param intervalOperand INTERVAL 1 DAY
   * @return Modified SqlNode 1
   */
  private SqlNode modifySqlNodeForLiteral(SqlWriter writer, SqlNode intervalOperand) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) intervalOperand).getValue();
    writeNegativeLiteral(interval, writer);
    return new SqlIdentifier(interval.toString(), intervalOperand.getParserPosition());
  }

  /**
   * Return the SqlLiteral from the SqlNode.
   *
   * @param intervalOperand store_id * INTERVAL 1 DAY
   * @return SqlLiteral INTERVAL 1 DAY
   */
  public SqlLiteral getIntervalLiteral(SqlNode intervalOperand) {
    if ((((SqlBasicCall) intervalOperand).operand(1).getKind() == SqlKind.IDENTIFIER)
        || (((SqlBasicCall) intervalOperand).operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(0);
    }
    return ((SqlBasicCall) intervalOperand).operand(1);
  }

  /**
   * Return the identifer from the SqlNode.
   *
   * @param intervalOperand Store_id * INTERVAL 1 DAY
   * @return SqlIdentifier Store_id
   */
  public SqlNode getIdentifier(SqlNode intervalOperand) {
    if (((SqlBasicCall) intervalOperand).operand(1).getKind() == SqlKind.IDENTIFIER
        || (((SqlBasicCall) intervalOperand).operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(1);
    }
    return ((SqlBasicCall) intervalOperand).operand(0);
  }

  private void writeNegativeLiteral(
      SqlIntervalLiteral.IntervalValue interval,
      SqlWriter writer) {
    if (interval.signum() == -1) {
      writer.print("-");
    }
  }

  /**
   * For usage of TRIM, LTRIM and RTRIM in Spark
   */
  private void unparseTrim(
      SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    SqlLiteral valueToBeTrim = call.operand(1);
    if (valueToBeTrim.toValue().matches("\\s+")) {
      handleTrimWithSpace(writer, call, leftPrec, rightPrec);
    } else {
      handleTrimWithChar(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * This method will handle the TRIM function if the value to be trimmed is space
   * Below is an example :
   * INPUT : SELECT TRIM(both ' ' from "ABC")
   * OUPUT : SELECT TRIM(ABC)
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate Right precision
   */
  private void handleTrimWithSpace(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
      break;
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(trimFrame);
  }

  /**
   * This method will handle the TRIM function if the value to be trimmed is not space
   * Below is an example :
   * INPUT : SELECT TRIM(both 'A' from "ABC")
   * OUPUT : SELECT REGEXP_REPLACE("ABC", '^(A)*', '')
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate Right precision
   */
  private void handleTrimWithChar(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlLiteral trimFlag = call.operand(0);
    SqlCharStringLiteral regexNode =
        RelToSqlConverterUtil.makeRegexNodeFromCall(call.operand(1), trimFlag);
    SqlCharStringLiteral blankLiteral = SqlLiteral.createCharString("",
        call.getParserPosition());
    SqlNode[] trimOperands = new SqlNode[]{call.operand(2), regexNode, blankLiteral};
    SqlCall regexReplaceCall = new SqlBasicCall(REGEXP_REPLACE, trimOperands, SqlParserPos.ZERO);
    REGEXP_REPLACE.unparse(writer, regexReplaceCall, leftPrec, rightPrec);
  }

  private void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "CURRENT_TIMESTAMP":
      if (((SqlBasicCall) call).getOperands().length > 0) {
        new CurrentTimestampHandler(this)
            .unparseCurrentTimestamp(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case "STRING_SPLIT":
      SqlCall splitCall = SPLIT.createCall(SqlParserPos.ZERO, call.getOperandList());
      unparseCall(writer, splitCall, leftPrec, rightPrec);
      break;
    case "FORMAT_TIMESTAMP":
    case "FORMAT_TIME":
    case "FORMAT_DATE":
      SqlCall dateFormatCall = DATE_FORMAT.createCall(SqlParserPos.ZERO, call.operand(1),
          creteDateTimeFormatSqlCharLiteral(call.operand(0).toString()));
      unparseCall(writer, dateFormatCall, leftPrec, rightPrec);
      break;
    case "STR_TO_DATE":
      unparseStrToDate(writer, call, leftPrec, rightPrec);
      break;
    case "RPAD":
    case "LPAD":
      PaddingFunctionUtil.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseStrToDate(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCall unixTimestampCall = UNIX_TIMESTAMP.createCall(SqlParserPos.ZERO, call.operand(0),
        creteDateTimeFormatSqlCharLiteral(call.operand(1).toString()));
    SqlCall fromUnixTimeCall = FROM_UNIXTIME.createCall(SqlParserPos.ZERO, unixTimestampCall,
        SqlLiteral.createCharString("yyyy-MM-dd", SqlParserPos.ZERO));
    SqlCall castToDateCall = CAST.createCall(SqlParserPos.ZERO, fromUnixTimeCall,
        getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE)));
    unparseCall(writer, castToDateCall, leftPrec, rightPrec);
  }

  private SqlCharStringLiteral creteDateTimeFormatSqlCharLiteral(String format) {
    String formatString = getDateTimeFormatString(unquoteStringLiteral(format),
        DATE_TIME_FORMAT_MAP);
    return SqlLiteral.createCharString(formatString, SqlParserPos.ZERO);
  }

  @Override protected String getDateTimeFormatString(
      String standardDateFormat, Map<SqlDateTimeFormat, String> dateTimeFormatMap) {
    return super.getDateTimeFormatString(standardDateFormat, dateTimeFormatMap);
  }
}

// End SparkSqlDialect.java
