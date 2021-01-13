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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDateTimeFormat;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.CurrentTimestampHandler;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.CastCallBuilder;
import org.apache.calcite.util.ToNumberUtils;
import org.apache.calcite.util.interval.BigQueryDateTimestampInterval;
import org.apache.calcite.util.interval.IntervalUtils;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDDAYOFWEEK;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATED_MONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATED_NAME_OF_DAY;
import static org.apache.calcite.sql.SqlDateTimeFormat.AMPM;
import static org.apache.calcite.sql.SqlDateTimeFormat.ANTE_MERIDIAN_INDICATOR;
import static org.apache.calcite.sql.SqlDateTimeFormat.ANTE_MERIDIAN_INDICATOR1;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFWEEK;
import static org.apache.calcite.sql.SqlDateTimeFormat.DAYOFYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.DDMMYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.DDMMYYYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.E3;
import static org.apache.calcite.sql.SqlDateTimeFormat.E4;
import static org.apache.calcite.sql.SqlDateTimeFormat.FOURDIGITYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONFIVE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONFOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONSIX;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONTHREE;
import static org.apache.calcite.sql.SqlDateTimeFormat.FRACTIONTWO;
import static org.apache.calcite.sql.SqlDateTimeFormat.HOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.HOUR_OF_DAY_12;
import static org.apache.calcite.sql.SqlDateTimeFormat.MILISECONDS_4;
import static org.apache.calcite.sql.SqlDateTimeFormat.MILLISECONDS_5;
import static org.apache.calcite.sql.SqlDateTimeFormat.MINUTE;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYYYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MONTHNAME;
import static org.apache.calcite.sql.SqlDateTimeFormat.MONTH_NAME;
import static org.apache.calcite.sql.SqlDateTimeFormat.NAME_OF_DAY;
import static org.apache.calcite.sql.SqlDateTimeFormat.NUMERICMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.POST_MERIDIAN_INDICATOR;
import static org.apache.calcite.sql.SqlDateTimeFormat.POST_MERIDIAN_INDICATOR1;
import static org.apache.calcite.sql.SqlDateTimeFormat.SECOND;
import static org.apache.calcite.sql.SqlDateTimeFormat.TIMEZONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWENTYFOURHOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWODIGITYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYMMDD;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMM;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FORMAT_TIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.IFNULL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PARSE_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PARSE_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT_ALL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SUBSTR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_SECONDS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;


/**
 * A <code>SqlDialect</code> implementation for Google BigQuery's "Standard SQL"
 * dialect.
 */
public class BigQuerySqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new BigQuerySqlDialect(
          EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
              .withLiteralQuoteString("'")
              .withLiteralEscapedQuoteString("\\'")
              .withIdentifierQuoteString("`")
              .withNullCollation(NullCollation.LOW)
              .withConformance(SqlConformanceEnum.BIG_QUERY));

  private static final List<String> RESERVED_KEYWORDS =
      ImmutableList.copyOf(
          Arrays.asList("ALL", "AND", "ANY", "ARRAY", "AS", "ASC",
              "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN", "BY", "CASE", "CAST",
              "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT",
              "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM",
              "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE",
              "FETCH", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING",
              "GROUPS", "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER",
              "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT",
              "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL", "NEW", "NO",
              "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER",
              "OVER", "PARTITION", "PRECEDING", "PROTO", "RANGE", "RECURSIVE",
              "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME",
              "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE",
              "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE",
              "WINDOW", "WITH", "WITHIN"));

  private static final Map<SqlDateTimeFormat, String> DATE_TIME_FORMAT_MAP =
      new HashMap<SqlDateTimeFormat, String>() {{
        put(DAYOFMONTH, "%d");
        put(DAYOFYEAR, "%j");
        put(NUMERICMONTH, "%m");
        put(ABBREVIATEDMONTH, "%b");
        put(MONTHNAME, "%B");
        put(TWODIGITYEAR, "%y");
        put(FOURDIGITYEAR, "%Y");
        put(DDMMYYYY, "%d%m%Y");
        put(DDMMYY, "%d%m%y");
        put(MMDDYYYY, "%m%d%Y");
        put(MMDDYY, "%m%d%y");
        put(YYYYMMDD, "%Y%m%d");
        put(YYMMDD, "%y%m%d");
        put(DAYOFWEEK, "%A");
        put(ABBREVIATEDDAYOFWEEK, "%a");
        put(TWENTYFOURHOUR, "%H");
        put(HOUR, "%I");
        put(MINUTE, "%M");
        put(SECOND, "%S");
        put(FRACTIONONE, "1S");
        put(FRACTIONTWO, "2S");
        put(FRACTIONTHREE, "3S");
        put(FRACTIONFOUR, "4S");
        put(FRACTIONFIVE, "5S");
        put(FRACTIONSIX, "6S");
        put(AMPM, "%p");
        put(TIMEZONE, "%Z");
        put(YYYYMM, "%Y%m");
        put(MMYY, "%m%y");
        put(MONTH_NAME, "%B");
        put(ABBREVIATED_MONTH, "%b");
        put(NAME_OF_DAY, "%A");
        put(ABBREVIATED_NAME_OF_DAY, "%a");
        put(HOUR_OF_DAY_12, "%l");
        put(POST_MERIDIAN_INDICATOR, "%p");
        put(POST_MERIDIAN_INDICATOR1, "%p");
        put(ANTE_MERIDIAN_INDICATOR, "%p");
        put(ANTE_MERIDIAN_INDICATOR1, "%p");
        put(MILLISECONDS_5, "*S");
        put(MILISECONDS_4, "*S");
        put(E4, "%A");
        put(E3, "%a");
      }};

  /** An unquoted BigQuery identifier must start with a letter and be followed
   * by zero or more letters, digits or _. */
  private static final Pattern IDENTIFIER_REGEX =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  /** Creates a BigQuerySqlDialect. */
  public BigQuerySqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public String quoteIdentifier(String val) {
    return quoteIdentifier(new StringBuilder(), val).toString();
  }

  @Override public boolean supportAggInGroupByClause() {
    return false;
  }

  @Override public boolean supportNestedAnalyticalFunctions() {
    return false;
  }

  @Override protected boolean identifierNeedsQuote(String val) {
    return !IDENTIFIER_REGEX.matcher(val).matches()
        || RESERVED_KEYWORDS.contains(val.toUpperCase(Locale.ROOT));
  }

  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
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

  @Override public boolean supportsColumnAliasInSort() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean castRequiredForStringOperand(RexCall node) {
    if (super.castRequiredForStringOperand(node)) {
      return true;
    }
    RexNode operand = node.getOperands().get(0);
    RelDataType castType = node.type;
    if (operand instanceof RexLiteral) {
      if (SqlTypeFamily.NUMERIC.contains(castType)) {
        return true;
      }
      return false;
    } else {
      return true;
    }
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.getOperator().kind) {
    case PLUS:
    case MINUS:
      switch (call.type.getSqlTypeName()) {
      case DATE:
        switch (call.getOperands().get(1).getType().getSqlTypeName()) {
        case INTEGER:
          RexNode rex2 = call.getOperands().get(1);
          SqlTypeName op1Type = call.getOperands().get(0).getType().getSqlTypeName();
          if (((RexLiteral) rex2).getValueAs(Integer.class) < 0) {
            if (op1Type == SqlTypeName.DATE) {
              return SqlLibraryOperators.DATE_SUB;
            } else {
              return SqlLibraryOperators.DATETIME_SUB;
            }
          }
          if (op1Type == SqlTypeName.DATE) {
            return SqlLibraryOperators.DATE_ADD;
          }
          return SqlLibraryOperators.DATETIME_ADD;
        case INTERVAL_DAY:
        case INTERVAL_MONTH:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.DATE_SUB;
          }
          return SqlLibraryOperators.DATE_ADD;
        default:
          return super.getTargetFunc(call);
        }
      case TIMESTAMP:
        if (call.getOperands().get(1).getType().getSqlTypeName() == SqlTypeName.INTEGER) {
          RexNode rex2 = call.getOperands().get(1);
          if (((RexLiteral) rex2).getValueAs(Integer.class) < 0) {
            return SqlLibraryOperators.DATETIME_SUB;
          }
          return SqlLibraryOperators.DATETIME_ADD;
        }
        if (call.op.kind == SqlKind.MINUS) {
          return SqlLibraryOperators.TIMESTAMP_SUB;
        }
        return SqlLibraryOperators.TIMESTAMP_ADD;
      default:
        return super.getTargetFunc(call);
      }
    case IS_NOT_TRUE:
      if (call.getOperands().get(0).getKind() == SqlKind.EQUALS) {
        return SqlStdOperatorTable.NOT_EQUALS;
      } else if (call.getOperands().get(0).getKind() == SqlKind.NOT_EQUALS) {
        return SqlStdOperatorTable.EQUALS;
      } else {
        return super.getTargetFunc(call);
      }
    case IS_TRUE:
      if (call.getOperands().get(0).getKind() == SqlKind.EQUALS) {
        return SqlStdOperatorTable.EQUALS;
      } else if (call.getOperands().get(0).getKind() == SqlKind.NOT_EQUALS) {
        return SqlStdOperatorTable.NOT_EQUALS;
      } else {
        return super.getTargetFunc(call);
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
    } else if (castTo.getSqlTypeName() == SqlTypeName.TIME && castTo.getPrecision() > 0) {
      return makCastCallForTimeWithPrecision(operandToCast, castTo.getPrecision());
    }
    return super.getCastCall(operandToCast, castFrom, castTo);
  }

  private SqlNode makCastCallForTimeWithPrecision(SqlNode operandToCast, int precision) {
    SqlParserPos pos = SqlParserPos.ZERO;
    SqlNode timeWithoutPrecision =
        getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIME));
    SqlCall castedTimeNode = CAST.createCall(pos, operandToCast, timeWithoutPrecision);
    SqlCharStringLiteral timeFormat = SqlLiteral.createCharString(String.format
        (Locale.ROOT, "%s%s%s", "HH24:MI:SS.S(", precision, ")"), pos);
    SqlCall formattedCall = FORMAT_TIME.createCall(pos, timeFormat, castedTimeNode);
    return CAST.createCall(pos, formattedCall, timeWithoutPrecision);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    switch (call.getKind()) {
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("STRPOS");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (3 == call.operandCount()) {
        throw new RuntimeException("3rd operand Not Supported for Function STRPOS in Big Query");
      }
      writer.endFunCall(frame);
      break;
    case UNION:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, UNION_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case EXCEPT:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, EXCEPT_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case INTERSECT:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, INTERSECT_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case CHARACTER_LENGTH:
    case CHAR_LENGTH:
      final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(lengthFrame);
      break;
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case TRUNCATE:
      final SqlWriter.Frame truncateFrame = writer.startFunCall("TRUNC");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(truncateFrame);
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
      final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
      unparseDivideInteger(writer, call, leftPrec, rightPrec);
      writer.sep("AS");
      writer.literal("INT64");
      writer.endFunCall(castFrame);
      break;
    case REGEXP_SUBSTR:
      unparseRegexSubstr(writer, call, leftPrec, rightPrec);
      break;
    case TO_NUMBER:
      ToNumberUtils.unparseToNumber(writer, call, leftPrec, rightPrec);
      break;
    case ASCII:
      SqlWriter.Frame toCodePointsFrame = writer.startFunCall("TO_CODE_POINTS");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(toCodePointsFrame);
      writer.literal("[OFFSET(0)]");
      break;
    case NVL:
      SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), call.operand(1)};
      SqlCall sqlCall = new SqlBasicCall(IFNULL, extractNodeOperands,
          SqlParserPos.ZERO);
      unparseCall(writer, sqlCall, leftPrec, rightPrec);
      break;
    case OTHER_FUNCTION:
    case OTHER:
      unparseOtherFunction(writer, call, leftPrec, rightPrec);
      break;
    case COLLECTION_TABLE:
      if (call.operandCount() > 1) {
        throw new RuntimeException("Table function supports only one argument in Big Query");
      }
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      SqlCollectionTableOperator operator = (SqlCollectionTableOperator) call.getOperator();
      if (operator.getAliasName() == null) {
        throw new RuntimeException("Table function must have alias in Big Query");
      }
      writer.sep("as " + operator.getAliasName());
      break;
    case PLUS:
      BigQueryDateTimestampInterval plusInterval = new BigQueryDateTimestampInterval();
      if (!plusInterval.handlePlusMinus(writer, call, leftPrec, rightPrec, "")) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case MINUS:
      BigQueryDateTimestampInterval minusInterval = new BigQueryDateTimestampInterval();
      if (!minusInterval.handlePlusMinus(writer, call, leftPrec, rightPrec, "-")) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    return ((SqlBasicCall) aggCall).operand(0);
  }

  /**
   * List of BigQuery Specific Operators needed to form Syntactically Correct SQL.
   */
  private static final SqlOperator UNION_DISTINCT = new SqlSetOperator(
      "UNION DISTINCT", SqlKind.UNION, 14, false);

  private static final SqlSetOperator EXCEPT_DISTINCT =
      new SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false);

  private static final SqlSetOperator INTERSECT_DISTINCT =
      new SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false);

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {
    switch (sqlKind) {
    case MINUS:
      final SqlWriter.Frame dateDiffFrame = writer.startFunCall("DATE_DIFF");
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      writer.literal("DAY");
      writer.endFunCall(dateDiffFrame);
      break;
    }
  }

  private void unparseRegexSubstr(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCall extractCall;
    switch (call.operandCount()) {
    case 3:
      extractCall = makeExtractSqlCall(call);
      REGEXP_EXTRACT.unparse(writer, extractCall, leftPrec, rightPrec);
      break;
    case 4:
    case 5:
      extractCall = makeExtractSqlCall(call);
      REGEXP_EXTRACT_ALL.unparse(writer, extractCall, leftPrec, rightPrec);
      writeOffset(writer, call);
      break;
    default:
      REGEXP_EXTRACT.unparse(writer, call, leftPrec, rightPrec);
    }
  }

  private void writeOffset(SqlWriter writer, SqlCall call) {
    int occurrenceNumber = Integer.parseInt(call.operand(3).toString()) - 1;
    writer.literal("[OFFSET(" + occurrenceNumber + ")]");
  }

  private SqlCall makeExtractSqlCall(SqlCall call) {
    SqlCall substringCall = makeSubstringSqlCall(call);
    call.setOperand(0, substringCall);
    if (call.operandCount() == 5 && call.operand(4).toString().equals("'i'")) {
      SqlCharStringLiteral regexNode = makeRegexNode(call);
      call.setOperand(1, regexNode);
    }
    SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), call.operand(1)};
    return new SqlBasicCall(REGEXP_EXTRACT, extractNodeOperands, SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral makeRegexNode(SqlCall call) {
    String regexStr = call.operand(1).toString();
    regexStr = regexStr.replace("\\", "\\\\");
    String regexLiteral = "(?i)".concat(regexStr.substring(1, regexStr.length() - 1));
    return SqlLiteral.createCharString(regexLiteral,
        call.operand(1).getParserPosition());
  }

  private SqlCall makeSubstringSqlCall(SqlCall call) {
    SqlNode[] sqlNodes = new SqlNode[]{call.operand(0), call.operand(2)};
    return new SqlBasicCall(SUBSTR, sqlNodes, SqlParserPos.ZERO);
  }


  /**
   * For usage of TRIM, LTRIM and RTRIM in BQ
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
      break;
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
   * For usage of DATE_ADD,DATE_SUB function in BQ. It will unparse the SqlCall and write it into BQ
   * format. Below are few examples:
   * Example 1:
   * Input: select date + INTERVAL 1 DAY
   * It will write output query as: select DATE_ADD(date , INTERVAL 1 DAY)
   * Example 2:
   * Input: select date + Store_id * INTERVAL 2 DAY
   * It will write output query as: select DATE_ADD(date , INTERVAL Store_id * 2 DAY)
   *
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall : date + Store_id * INTERVAL 2 DAY
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate left precision
   */
  @Override public void unparseIntervalOperandsBasedFunctions(
      SqlWriter writer,
      SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(call.getOperator().toString());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(",");
    switch (call.operand(1).getKind()) {
    case LITERAL:
      unparseSqlIntervalLiteral(writer, call.operand(1), leftPrec, rightPrec);
      break;
    case TIMES:
      unparseExpressionIntervalCall(call.operand(1), writer, leftPrec, rightPrec);
      break;
    case OTHER_FUNCTION:
      unparseOtherFunction(writer, call.operand(1), leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
    writer.endFunCall(frame);
  }

  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    literal = updateSqlIntervalLiteral(literal);
    SqlIntervalLiteral.IntervalValue intervalValue =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    writer.sep("INTERVAL");
    if (intervalValue.getSign() == -1) {
      writer.print("-");
    }
    writer.sep(intervalValue.getIntervalLiteral());
    unparseSqlIntervalQualifier(
        writer, intervalValue.getIntervalQualifier(), RelDataTypeSystem.DEFAULT);
  }

  private SqlIntervalLiteral updateSqlIntervalLiteral(SqlIntervalLiteral literal) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    switch (literal.getTypeName()) {
    case INTERVAL_HOUR_SECOND:
      long equivalentSecondValue = SqlParserUtil.intervalToMillis(interval.getIntervalLiteral(),
          interval.getIntervalQualifier()) / 1000;
      SqlIntervalQualifier qualifier = new SqlIntervalQualifier(TimeUnit.SECOND,
          RelDataType.PRECISION_NOT_SPECIFIED, TimeUnit.SECOND,
          RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO);
      return SqlLiteral.createInterval(interval.getSign(), Long.toString(equivalentSecondValue),
          qualifier, literal.getParserPosition());
    default:
      return literal;
    }
  }

  /**
   * Unparse the SqlBasic call and write INTERVAL with expression. Below are the examples:
   * Example 1:
   * Input: store_id * INTERVAL 1 DAY
   * It will write this as: INTERVAL store_id DAY
   * Example 2:
   * Input: 10 * INTERVAL 2 DAY
   * It will write this as: INTERVAL 10 * 2 DAY
   *
   * @param call SqlCall : store_id * INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate right precision
   */
  private void unparseExpressionIntervalCall(
      SqlBasicCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlLiteral intervalLiteral = getIntervalLiteral(call);
    SqlNode identifier = getIdentifier(call);
    SqlIntervalLiteral.IntervalValue literalValue =
        (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    writer.sep("INTERVAL");
    if (call.getKind() == SqlKind.TIMES) {
      if (!literalValue.getIntervalLiteral().equals("1")) {
        identifier.unparse(writer, leftPrec, rightPrec);
        writer.sep("*");
        writer.sep(literalValue.toString());
      } else {
        identifier.unparse(writer, leftPrec, rightPrec);
      }
      writer.print(literalValue.getIntervalQualifier().toString());
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
    case "CURRENT_USER":
    case "SESSION_USER":
      final SqlWriter.Frame sessionUserFunc = writer.startFunCall(SESSION_USER.getName());
      writer.endFunCall(sessionUserFunc);
      break;
    case "TIMESTAMPINTADD":
    case "TIMESTAMPINTSUB":
      unparseTimestampAddSub(writer, call, leftPrec, rightPrec);
      break;
    case "FORMAT_TIMESTAMP":
    case "FORMAT_TIME":
    case "FORMAT_DATE":
    case "FORMAT_DATETIME":
      SqlCall formatCall = call.getOperator().createCall(SqlParserPos.ZERO,
          creteDateTimeFormatSqlCharLiteral(call.operand(0).toString()), call.operand(1));
      super.unparseCall(writer, formatCall, leftPrec, rightPrec);
      break;
    case "STR_TO_DATE":
      SqlCall parseDateCall = PARSE_DATE.createCall(SqlParserPos.ZERO,
          creteDateTimeFormatSqlCharLiteral(call.operand(1).toString()), call.operand(0));
      unparseCall(writer, parseDateCall, leftPrec, rightPrec);
      break;
    case "TO_DATE":
      SqlCall parseToDateCall = PARSE_TIMESTAMP.createCall(SqlParserPos.ZERO,
              creteDateTimeFormatSqlCharLiteral(call.operand(1).toString()), call.operand(0));
      final SqlWriter.Frame timestampSecond = writer.startFunCall("DATE");
      unparseCall(writer, parseToDateCall, leftPrec, rightPrec);
      writer.endFunCall(timestampSecond);
      break;
    case "TO_TIMESTAMP":
      if (call.getOperandList().size() == 1) {
        SqlCall timestampSecondsCall = TIMESTAMP_SECONDS.createCall(SqlParserPos.ZERO,
                call.operand(0));
        unparseCall(writer, timestampSecondsCall, leftPrec, rightPrec);
        break;
      }
      SqlCall parseTimestampCall = PARSE_TIMESTAMP.createCall(SqlParserPos.ZERO,
              creteDateTimeFormatSqlCharLiteral(call.operand(1).toString()), call.operand(0));
      unparseCall(writer, parseTimestampCall, leftPrec, rightPrec);
      break;
    case "INSTR":
      final SqlWriter.Frame frame = writer.startFunCall("STRPOS");
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
      break;
    case "DATE_MOD":
      unparseDateModule(writer, call, leftPrec, rightPrec);
      break;
    case "TIMESTAMPINTMUL":
      unparseTimestampIntMul(writer, call, leftPrec, rightPrec);
      break;
    case "RAND_INTEGER":
      unparseRandomfunction(writer, call, leftPrec, rightPrec);
      break;
    case DateTimestampFormatUtil.WEEKNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.YEARNUMBER_OF_CALENDAR:
    case DateTimestampFormatUtil.MONTHNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.QUARTERNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.MONTHNUMBER_OF_QUARTER:
    case DateTimestampFormatUtil.WEEKNUMBER_OF_MONTH:
    case DateTimestampFormatUtil.WEEKNUMBER_OF_CALENDAR:
    case DateTimestampFormatUtil.DAYOCCURRENCE_OF_MONTH:
    case DateTimestampFormatUtil.DAYNUMBER_OF_CALENDAR:
    case DateTimestampFormatUtil.DAY_OF_YEAR:
      DateTimestampFormatUtil dateTimestampFormatUtil = new DateTimestampFormatUtil();
      dateTimestampFormatUtil.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    case "DATETIME_ADD":
    case "DATETIME_SUB":
      new IntervalUtils().unparse(writer, call, leftPrec, rightPrec, this);
      break;
    case "STRTOK":
      unparseStrtok(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseStrtok(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame splitFrame = writer.startFunCall("SPLIT");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(",");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(splitFrame);
    writer.print("[OFFSET (");
    int thirdOperandValue = Integer.valueOf(call.operand(2).toString()) - 1;
    writer.print(thirdOperandValue);
    writer.print(") ]");
  }

  private void unparseTimestampAddSub(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame timestampAdd = writer.startFunCall(getFunName(call));
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(",");
    writer.print("INTERVAL ");
    call.operand(call.getOperandList().size() - 1)
            .unparse(writer, leftPrec, rightPrec);
    writer.print("SECOND");
    writer.endFunCall(timestampAdd);
  }

  private String getFunName(SqlCall call) {
    String operatorName = call.getOperator().getName();
    return operatorName.equals("TIMESTAMPINTADD") ? "TIMESTAMP_ADD"
            : operatorName.equals("TIMESTAMPINTSUB") ? "TIMESTAMP_SUB"
            : operatorName;
  }

  private SqlCharStringLiteral creteDateTimeFormatSqlCharLiteral(String format) {
    String formatString = getDateTimeFormatString(unquoteStringLiteral(format),
        DATE_TIME_FORMAT_MAP);
    return SqlLiteral.createCharString(formatString, SqlParserPos.ZERO);
  }

  /**
   * unparse method for Random function
   */
  private void unparseRandomfunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCall randCall = RAND.createCall(SqlParserPos.ZERO);
    SqlCall upperLimitCall = PLUS.createCall(SqlParserPos.ZERO, MINUS.createCall
            (SqlParserPos.ZERO, call.operand(1), call.operand(0)), call.operand(0));
    SqlCall numberGenerator = MULTIPLY.createCall(SqlParserPos.ZERO, randCall, upperLimitCall);
    SqlCall floorDoubleValue = FLOOR.createCall(SqlParserPos.ZERO, numberGenerator);
    SqlCall plusNode = PLUS.createCall(SqlParserPos.ZERO, floorDoubleValue, call.operand(0));
    unparseCall(writer, plusNode, leftPrec, rightPrec);
  }

  @Override protected String getDateTimeFormatString(
      String standardDateFormat, Map<SqlDateTimeFormat, String> dateTimeFormatMap) {
    String dateTimeFormat = super.getDateTimeFormatString(standardDateFormat, dateTimeFormatMap);
    return dateTimeFormat
        .replace("%Y-%m-%d", "%F")
        .replace("%S.", "%E");
  }

  private void unparseTimestampIntMul(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.operand(0) instanceof SqlBasicCall) {
      handleSqlBasicCallForTimestampMulti(writer, call);
    } else {
      SqlIntervalLiteral intervalLiteralValue = call.operand(0);
      SqlIntervalLiteral.IntervalValue literalValue =
              (SqlIntervalLiteral.IntervalValue) intervalLiteralValue.getValue();
      String secondOperand = "";
      if (call.operand(1) instanceof SqlIdentifier) {
        SqlIdentifier sqlIdentifier = call.operand(1);
        secondOperand = sqlIdentifier.toString() + "*"
                + (Integer.valueOf(literalValue.toString()) + "");
      } else if (call.operand(1) instanceof SqlNumericLiteral) {
        SqlNumericLiteral sqlNumericLiteral = call.operand(1);
        secondOperand = Integer.parseInt(sqlNumericLiteral.toString())
                * (Integer.parseInt(literalValue.toString())) + "";
      }
      writer.sep("INTERVAL");
      writer.sep(secondOperand);
      writer.print(literalValue.getIntervalQualifier().toString());
    }
  }

  void handleSqlBasicCallForTimestampMulti(SqlWriter writer, SqlCall call) {
    String firstOperand = String.valueOf((SqlBasicCall) call.getOperandList().get(0));
    firstOperand = firstOperand.replaceAll("TIME(0)", "TIME");
    SqlIntervalLiteral intervalLiteralValue = (SqlIntervalLiteral) call.getOperandList().get(1);
    SqlIntervalLiteral.IntervalValue literalValue =
            (SqlIntervalLiteral.IntervalValue) intervalLiteralValue.getValue();
    String secondOperand = literalValue.toString() + " * " + firstOperand;
    writer.sep("INTERVAL");
    writer.sep(secondOperand);
    writer.print(literalValue.toString());
  }

}

// End BigQuerySqlDialect.java
