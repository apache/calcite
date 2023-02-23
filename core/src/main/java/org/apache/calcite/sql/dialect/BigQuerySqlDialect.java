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
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateTimeFormat;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.CurrentTimestampHandler;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.CastCallBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.ToNumberUtils;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.interval.BigQueryDateTimestampInterval;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDDAYOFWEEK;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATEDMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATED_MONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.ABBREVIATED_NAME_OF_DAY;
import static org.apache.calcite.sql.SqlDateTimeFormat.AMPM;
import static org.apache.calcite.sql.SqlDateTimeFormat.ANTE_MERIDIAN_INDICATOR;
import static org.apache.calcite.sql.SqlDateTimeFormat.ANTE_MERIDIAN_INDICATOR_WITH_DOT;
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
import static org.apache.calcite.sql.SqlDateTimeFormat.HOURMINSEC;
import static org.apache.calcite.sql.SqlDateTimeFormat.HOUR_OF_DAY_12;
import static org.apache.calcite.sql.SqlDateTimeFormat.MILLISECONDS_4;
import static org.apache.calcite.sql.SqlDateTimeFormat.MILLISECONDS_5;
import static org.apache.calcite.sql.SqlDateTimeFormat.MINUTE;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMDDYYYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MMYY;
import static org.apache.calcite.sql.SqlDateTimeFormat.MONTHNAME;
import static org.apache.calcite.sql.SqlDateTimeFormat.MONTH_NAME;
import static org.apache.calcite.sql.SqlDateTimeFormat.NAME_OF_DAY;
import static org.apache.calcite.sql.SqlDateTimeFormat.NUMERICMONTH;
import static org.apache.calcite.sql.SqlDateTimeFormat.NUMERIC_TIME_ZONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.POST_MERIDIAN_INDICATOR;
import static org.apache.calcite.sql.SqlDateTimeFormat.POST_MERIDIAN_INDICATOR_WITH_DOT;
import static org.apache.calcite.sql.SqlDateTimeFormat.QUARTER;
import static org.apache.calcite.sql.SqlDateTimeFormat.SECOND;
import static org.apache.calcite.sql.SqlDateTimeFormat.SECONDS_PRECISION;
import static org.apache.calcite.sql.SqlDateTimeFormat.SEC_FROM_MIDNIGHT;
import static org.apache.calcite.sql.SqlDateTimeFormat.TIMEOFDAY;
import static org.apache.calcite.sql.SqlDateTimeFormat.TIMEZONE;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWENTYFOURHOUR;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWENTYFOURHOURMIN;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWENTYFOURHOURMINSEC;
import static org.apache.calcite.sql.SqlDateTimeFormat.TWODIGITYEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.U;
import static org.apache.calcite.sql.SqlDateTimeFormat.WEEK_OF_YEAR;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYMMDD;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMM;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDD;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDDHH24;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDDHH24MI;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDDHH24MISS;
import static org.apache.calcite.sql.SqlDateTimeFormat.YYYYMMDDHHMISS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ACOS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT2;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_DIFF;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FARM_FINGERPRINT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FORMAT_TIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.IFNULL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PARSE_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PARSE_DATETIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PARSE_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_SECONDS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_SECONDS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REGEXP_SUBSTR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN;
import static org.apache.calcite.util.Util.modifyRegexStringForMatchArgument;
import static org.apache.calcite.util.Util.removeLeadingAndTrailingSingleQuotes;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for Google BigQuery's "Standard SQL"
 * dialect.
 */
public class BigQuerySqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
      .withLiteralQuoteString("'")
      .withLiteralEscapedQuoteString("\\'")
      .withIdentifierQuoteString("`")
      .withNullCollation(NullCollation.LOW)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withCaseSensitive(false)
      .withConformance(SqlConformanceEnum.BIG_QUERY);

  public static final SqlDialect DEFAULT = new BigQuerySqlDialect(DEFAULT_CONTEXT);

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

  /**
   * An unquoted BigQuery identifier must start with a letter and be followed
   * by zero or more letters, digits or _.
   */
  private static final Pattern IDENTIFIER_REGEX =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  private static final String TEMP_REGEX = "\\s?°([CcFf])";

  private static final Pattern FLOAT_REGEX =
      Pattern.compile("[\"|'][+\\-]?([0-9]*[.])[0-9]+[\"|']");
  /**
   * Creates a BigQuerySqlDialect.
   */
  public BigQuerySqlDialect(SqlDialect.Context context) {
    super(context);
  }

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
        put(HOURMINSEC, "%I%M%S");
        put(MINUTE, "%M");
        put(SECOND, "%S");
        put(SECONDS_PRECISION, "%E");
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
        put(POST_MERIDIAN_INDICATOR_WITH_DOT, "%p");
        put(ANTE_MERIDIAN_INDICATOR, "%p");
        put(ANTE_MERIDIAN_INDICATOR_WITH_DOT, "%p");
        put(E3, "%a");
        put(E4, "%A");
        put(TWENTYFOURHOURMIN, "%H%M");
        put(TWENTYFOURHOURMINSEC, "%H%M%S");
        put(YYYYMMDDHH24MISS, "%Y%m%d%H%M%S");
        put(YYYYMMDDHH24MI, "%Y%m%d%H%M");
        put(YYYYMMDDHH24, "%Y%m%d%H");
        put(YYYYMMDDHHMISS, "%Y%m%d%I%M%S");
        put(MILLISECONDS_5, "*S");
        put(MILLISECONDS_4, "4S");
        put(U, "%u");
        put(NUMERIC_TIME_ZONE, "%Ez");
        put(SEC_FROM_MIDNIGHT, "SEC_FROM_MIDNIGHT");
        put(QUARTER, "%Q");
        put(TIMEOFDAY, "%c");
        put(WEEK_OF_YEAR, "%W");
      }};

  private static final String OR = "|";
  private static final String SHIFTRIGHT = ">>";
  private static final String XOR = "^";
  private static final String SHIFTLEFT = "<<";

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

  @Override public @Nullable SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public boolean supportsImplicitTypeCoercion(RexCall call) {
    return super.supportsImplicitTypeCoercion(call)
        && RexUtil.isLiteral(call.getOperands().get(0), false)
        && !SqlTypeUtil.isNumeric(call.type);
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsAggregateFunctionFilter() {
    return false;
  }

  @Override public SqlParser.Config configureParser(
      SqlParser.Config configBuilder) {
    return super.configureParser(configBuilder)
        .withCharLiteralStyles(Lex.BIG_QUERY.charLiteralStyles);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public boolean supportsIdenticalTableAndColumnName() {
    return false;
  }

  @Override public boolean supportsQualifyClause() {
    return true;
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

  @Override public boolean supportsColumnListForWithItem() {
    return false;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean requiresColumnsInMergeInsertClause() {
    return false;
  }

  @Override public JoinType emulateJoinTypeForCrossJoin() {
    return JoinType.INNER;
  }

  @Override public void unparseTitleInColumnDefinition(SqlWriter writer, String title,
      int leftPrec, int rightPrec) {
    writer.print("OPTIONS(description=" + title + ")");
  }

  @Override public boolean supportsUnpivot() {
    return true;
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
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_DAY_HOUR:
          if (call.op.kind == SqlKind.MINUS) {
            return MINUS;
          }
          return PLUS;
        case INTERVAL_DAY:
        case INTERVAL_MONTH:
        case INTERVAL_YEAR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_DAY_SECOND:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.DATE_SUB;
          }
          return SqlLibraryOperators.DATE_ADD;
        default:
          return super.getTargetFunc(call);
        }
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        switch (call.getOperands().get(1).getType().getSqlTypeName()) {
        case INTERVAL_DAY:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_HOUR:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.TIMESTAMP_SUB;
          }
          return PLUS;
        case INTERVAL_DAY_SECOND:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.TIMESTAMP_SUB;
          }
          return SqlLibraryOperators.TIMESTAMP_ADD;
        case INTERVAL_MONTH:
        case INTERVAL_YEAR:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.DATETIME_SUB;
          }
          return SqlLibraryOperators.DATETIME_ADD;
        }
      case TIME:
        switch (call.getOperands().get(1).getType().getSqlTypeName()) {
        case INTERVAL_MINUTE:
        case INTERVAL_SECOND:
        case INTERVAL_HOUR:
          if (call.op.kind == SqlKind.MINUS) {
            return SqlLibraryOperators.TIME_SUB;
          }
          return SqlLibraryOperators.TIME_ADD;
        }
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

  @Override public SqlNode getTimestampLiteral(
      TimestampString timestampString, int precision, SqlParserPos pos) {
    SqlNode timestampNode = getCastSpec(
        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
    return CAST.createCall(pos, SqlLiteral.createCharString(timestampString.toString(), pos),
        timestampNode);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    switch (call.getKind()) {
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("STRPOS");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      if (call.operand(0).toString().contains("\\")) {
        SqlCharStringLiteral regexNode = makeRegexNodeForPosition(call);
        call.setOperand(0, regexNode);
      }
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (3 == call.operandCount()) {
        throw new RuntimeException("3rd operand Not Supported for Function STRPOS in Big Query");
      }
      writer.endFunCall(frame);
      break;
    case UNION:
      if (((SqlSetOperator) call.getOperator()).isAll()) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
        SqlSyntax.BINARY.unparse(writer, UNION_DISTINCT, call, leftPrec,
            rightPrec);
      }
      break;
    case EXCEPT:
      if (((SqlSetOperator) call.getOperator()).isAll()) {
        throw new RuntimeException("BigQuery does not support EXCEPT ALL");
      }
      SqlSyntax.BINARY.unparse(writer, EXCEPT_DISTINCT, call, leftPrec,
          rightPrec);
      break;
    case INTERSECT:
      if (((SqlSetOperator) call.getOperator()).isAll()) {
        throw new RuntimeException("BigQuery does not support INTERSECT ALL");
      }
      SqlSyntax.BINARY.unparse(writer, INTERSECT_DISTINCT, call, leftPrec,
          rightPrec);
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
      ToNumberUtils.unparseToNumber(writer, call, leftPrec, rightPrec, this);
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
      //RAV-5569 is raised to handle intervals in plus and minus operations
      if (call.getOperator() == SqlLibraryOperators.TIMESTAMP_ADD
          && isIntervalHourAndSecond(call)) {
        unparseIntervalOperandsBasedFunctions(writer, call, leftPrec, rightPrec);
      } else {
        BigQueryDateTimestampInterval plusInterval = new BigQueryDateTimestampInterval();
        if (!plusInterval.handlePlusMinus(writer, call, leftPrec, rightPrec, "")) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }
      break;
    case MINUS:
      if (call.getOperator() == SqlLibraryOperators.TIMESTAMP_SUB
          && isIntervalHourAndSecond(call)) {
        unparseIntervalOperandsBasedFunctions(writer, call, leftPrec, rightPrec);
      } else {
        BigQueryDateTimestampInterval minusInterval = new BigQueryDateTimestampInterval();
        if (!minusInterval.handlePlusMinus(writer, call, leftPrec, rightPrec, "-")) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }
      break;
    case EXTRACT:
      unparseExtractFunction(writer, call, leftPrec, rightPrec);
      break;
    case GROUPING:
      unparseGroupingFunction(writer, call, leftPrec, rightPrec);
      break;
    case CAST:
      String firstOperand = call.operand(1).toString();
      if (firstOperand.equals("`TIMESTAMP`")) {
        SqlWriter.Frame castDateTimeFrame = writer.startFunCall("CAST");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("AS", true);
        writer.literal("DATETIME");
        writer.endFunCall(castDateTimeFrame);
      } else if (firstOperand.equals("INTEGER") || firstOperand.equals("INT64")) {
        unparseCastAsInteger(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case SELECT:
      SqlSelect select = (SqlSelect) call;
      SqlNodeList selectClause = select.getSelectList();
      if (selectClause != null) {
        processNodeList(selectClause);
      }
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      break;
    case NOT_IN:
    case EQUALS:
      processNode(call.operand(1), call);
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      break;
    case AS:
      SqlNode var = call.operand(0);
      if (call.operand(0) instanceof SqlCharStringLiteral
          && (var.toString().contains("\\")
          && !var.toString().substring(1, 3).startsWith("\\\\"))) {
        unparseAsOp(writer, call, leftPrec, rightPrec);
      } else {
        call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      }
      break;
    case IN:
      if (call.operand(0) instanceof SqlLiteral
          && call.operand(1) instanceof SqlNodeList
          && ((SqlNodeList) call.operand(1)).get(0).getKind() == SqlKind.UNNEST) {
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("IN");
        writer.setNeedWhitespace(true);
        writer.print(call.operand(1).toSqlString(writer.getDialect()).toString());
        break;
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        break;
      }
    case COLUMN_LIST:
      final SqlWriter.Frame columnListFrame = getColumnListFrame(writer, call);
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(columnListFrame);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseAsOp(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    assert call.operandCount() >= 2;
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.AS);
    SqlNode literalValue = replaceSingleBackslashes(call.operand(0));
    literalValue.unparse(writer, leftPrec, rightPrec);
    final boolean needsSpace = true;
    writer.setNeedWhitespace(needsSpace);
    writer.sep("AS");
    writer.setNeedWhitespace(needsSpace);
    call.operand(1).unparse(writer, SqlStdOperatorTable.AS.getRightPrec(), rightPrec);
    if (call.operandCount() > 2) {
      final SqlWriter.Frame frame1 =
          writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
      for (SqlNode operand : Util.skip(call.getOperandList(), 2)) {
        writer.sep(",", false);
        operand.unparse(writer, 0, 0);
      }
      writer.endList(frame1);
    }
    writer.endList(frame);
  }

  private void unparseCastAsInteger(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    boolean isFirstOperandFormatCall = (call.operand(0) instanceof SqlBasicCall)
        && ((SqlBasicCall) call.operand(0)).getOperator().getName().equals("FORMAT");
    boolean isFirstOperandString = (call.operand(0) instanceof SqlCharStringLiteral)
        && SqlTypeName.CHAR_TYPES.contains(((SqlCharStringLiteral) call.operand(0)).getTypeName());
    Matcher floatRegexMatcher = isFirstOperandString
        ? FLOAT_REGEX.matcher(call.operand(0).toString()) : null;
    boolean isFirstOperandFloatString = floatRegexMatcher != null && floatRegexMatcher.matches();

    if (isFirstOperandFormatCall || isFirstOperandFloatString) {
      SqlWriter.Frame castIntegerFrame = writer.startFunCall("CAST");
      SqlWriter.Frame castFloatFrame = writer.startFunCall("CAST");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep("AS", true);
      writer.literal("FLOAT64");
      writer.endFunCall(castFloatFrame);
      writer.sep("AS", true);
      writer.literal("INTEGER");
      writer.endFunCall(castIntegerFrame);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    return ((SqlBasicCall) aggCall).operand(0);
  }

  private SqlCharStringLiteral makeRegexNodeForPosition(SqlCall call) {
    String regexLiteral = call.operand(0).toString();
    regexLiteral = (regexLiteral.substring(1, regexLiteral.length() - 1)).replace("\\", "\\\\");
    return SqlLiteral.createCharString(regexLiteral,
        call.operand(0).getParserPosition());
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
    extractCall = makeRegexpSubstrSqlCall(call);
    REGEXP_SUBSTR.unparse(writer, extractCall, leftPrec, rightPrec);
  }

  private SqlCall makeRegexpSubstrSqlCall(SqlCall call) {
    if (call.operandCount() == 5 || call.operand(1).toString().contains("\\")) {
      SqlCharStringLiteral regexNode = makeRegexNode(call);
      call.setOperand(1, regexNode);
    }
    SqlNode[] extractNodeOperands;
    if (call.operandCount() == 5) {
      extractNodeOperands = new SqlNode[]{call.operand(0), call.operand(1),
          call.operand(2), call.operand(3)};
    } else {
      extractNodeOperands = call.getOperandList().toArray(new SqlNode[0]);
    }
    return new SqlBasicCall(REGEXP_SUBSTR, extractNodeOperands, SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral makeRegexNode(SqlCall call) {
    String regexLiteral = call.operand(1).toString();
    regexLiteral = (regexLiteral.substring(1, regexLiteral.length() - 1)).replace("\\", "\\\\");
    if (call.operandCount() == 5 && call.operand(4).toString().equals("'i'")) {
      regexLiteral = "(?i)".concat(regexLiteral);
    }
    return SqlLiteral.createCharString(regexLiteral,
        call.operand(1).getParserPosition());
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
    SqlWriter.Frame castTimeStampFrame = null;
    if (isDateTimeCall(call) && isIntervalYearAndMonth(call)) {
      castTimeStampFrame = writer.startFunCall("CAST");
    }
    final SqlWriter.Frame frame = writer.startFunCall(call.getOperator().toString());
    if (isDateTimeCall(call)) {
      SqlWriter.Frame castDateTimeFrame = writer.startFunCall("CAST");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep("AS", true);
      writer.literal("DATETIME");
      writer.endFunCall(castDateTimeFrame);
    } else {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
    }
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

    if (isDateTimeCall(call) && isIntervalYearAndMonth(call)) {
      writer.sep("AS", true);
      writer.literal("DATETIME");
      writer.endFunCall(castTimeStampFrame);
    }
  }

  private boolean isDateTimeCall(SqlCall call) {
    return (call.getOperator().getName().equals("DATETIME_ADD"))
        || (call.getOperator().getName().equals("DATETIME_SUB"));
  }

  private boolean isIntervalYearAndMonth(SqlCall call) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      return ((SqlIntervalLiteral) call.operand(1)).getTypeName().getFamily()
             == SqlTypeFamily.INTERVAL_YEAR_MONTH;
    }
    SqlLiteral literal = getIntervalLiteral(call.operand(1));
    return literal.getTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH;
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
    SqlLiteral intervalLiteral;
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      intervalLiteral = modifiedSqlIntervalLiteral(call.operand(1));
    } else {
      intervalLiteral = getIntervalLiteral(call);
    }
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
        final SqlWriter.Frame currentDatetimeFunc = writer.startFunCall("CURRENT_DATETIME");
        writer.endFunCall(currentDatetimeFunc);
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
      if (call.operand(0).toString().equals("'EEE'")
          || call.operand(0).toString().equals("'EEEE'")) {
        if (isOperandCastedToDateTime(call)) {
          String dateFormat = call.operand(0).toString();
          SqlCall secondOperand = call.operand(1);
          SqlWriter.Frame formatTimestampFrame = writer.startFunCall("FORMAT_TIMESTAMP");
          writer.sep(",");
          writer.literal(createDateTimeFormatSqlCharLiteral(dateFormat).toString());
          writer.sep(",");
          SqlWriter.Frame castTimestampFrame = writer.startFunCall("CAST");
          secondOperand.operand(0).unparse(writer, leftPrec, rightPrec);
          writer.sep("AS", true);
          writer.literal("TIMESTAMP");
          writer.endFunCall(castTimestampFrame);
          writer.endFunCall(formatTimestampFrame);
        } else {
          unparseFormatCall(writer, call, leftPrec, rightPrec);
        }
      } else {
        unparseFormatDatetime(writer, call, leftPrec, rightPrec);
      }
      break;
    case "FORMAT_DATE":
    case "FORMAT_DATETIME":
      unparseFormatDatetime(writer, call, leftPrec, rightPrec);
      break;
    case "PARSE_TIMESTAMP":
      String dateFormat = call.operand(0) instanceof SqlCharStringLiteral
          ? ((NlsString) requireNonNull(((SqlCharStringLiteral) call.operand(0)).getValue()))
          .getValue()
          : call.operand(0).toString();
      SqlCall formatCall = PARSE_DATETIME.createCall(SqlParserPos.ZERO,
          createDateTimeFormatSqlCharLiteral(dateFormat), call.operand(1));
      super.unparseCall(writer, formatCall, leftPrec, rightPrec);
      break;
    case "FORMAT_TIME":
      unparseFormatCall(writer, call, leftPrec, rightPrec);
      break;
    case "STR_TO_DATE":
      SqlCall parseDateCall = PARSE_DATE.createCall(SqlParserPos.ZERO,
          createDateTimeFormatSqlCharLiteral(call.operand(1).toString()), call.operand(0));
      unparseCall(writer, parseDateCall, leftPrec, rightPrec);
      break;
    case "SUBSTRING":
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case "TO_DATE":
      SqlCall parseToDateCall = PARSE_TIMESTAMP.createCall(SqlParserPos.ZERO,
          call.operand(1), call.operand(0));
      final SqlWriter.Frame timestampSecond = writer.startFunCall("DATE");
      unparseCall(writer, parseToDateCall, leftPrec, rightPrec);
      writer.endFunCall(timestampSecond);
      break;
    case "TO_TIMESTAMP":
      if (call.getOperandList().size() == 1) {
        SqlCall timestampSecondsCall = TIMESTAMP_SECONDS.createCall(SqlParserPos.ZERO,
            new SqlNode[] { call.operand(0) });
        unparseCall(writer, timestampSecondsCall, leftPrec, rightPrec);
        break;
      }
      SqlCall parseTimestampCall = PARSE_TIMESTAMP.createCall(SqlParserPos.ZERO,
          call.operand(1), call.operand(0));
      unparseCall(writer, parseTimestampCall, leftPrec, rightPrec);
      break;
    case "DATE_MOD":
      unparseDateMod(writer, call, leftPrec, rightPrec);
      break;
    case "TIMESTAMPINTMUL":
      unparseTimestampIntMul(writer, call, leftPrec, rightPrec);
      break;
    case "RAND_INTEGER":
      unparseRandomfunction(writer, call, leftPrec, rightPrec);
      break;
    case DateTimestampFormatUtil.WEEKNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.ISO_WEEKOFYEAR:
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
    case "STRTOK":
      unparseStrtok(writer, call, leftPrec, rightPrec);
      break;
    case "DAYOFMONTH":
      SqlNode daySymbolLiteral = SqlLiteral.createSymbol(TimeUnit.DAY, SqlParserPos.ZERO);
      SqlCall extractCall = EXTRACT.createCall(SqlParserPos.ZERO,
              daySymbolLiteral, call.operand(0));
      super.unparseCall(writer, extractCall, leftPrec, rightPrec);
      break;
    case "HOUR":
      SqlNode hourSymbolLiteral = SqlLiteral.createSymbol(TimeUnit.HOUR, SqlParserPos.ZERO);
      SqlCall extractHourCall = EXTRACT.createCall(SqlParserPos.ZERO,
          hourSymbolLiteral, call.operand(0));
      unparseExtractFunction(writer, extractHourCall, leftPrec, rightPrec);
      break;
    case "MINUTE":
      SqlNode minuteSymbolLiteral = SqlLiteral.createSymbol(TimeUnit.MINUTE, SqlParserPos.ZERO);
      SqlCall extractMinuteCall = EXTRACT.createCall(SqlParserPos.ZERO,
          minuteSymbolLiteral, call.operand(0));
      unparseExtractFunction(writer, extractMinuteCall, leftPrec, rightPrec);
      break;
    case "SECOND":
      SqlNode secondSymbolLiteral = SqlLiteral.createSymbol(TimeUnit.SECOND, SqlParserPos.ZERO);
      SqlCall extractSecondCall = EXTRACT.createCall(SqlParserPos.ZERO,
          secondSymbolLiteral, call.operand(0));
      unparseExtractFunction(writer, extractSecondCall, leftPrec, rightPrec);
      break;
    case "MONTHS_BETWEEN":
      unparseMonthsBetween(writer, call, leftPrec, rightPrec);
      break;
    case "REGEXP_MATCH_COUNT":
      unparseRegexMatchCount(writer, call, leftPrec, rightPrec);
      break;
    case "COT":
      unparseCot(writer, call, leftPrec, rightPrec);
      break;
    case "BITWISE_AND":
      SqlNode[] operands = new SqlNode[]{call.operand(0), call.operand(1)};
      unparseBitwiseAnd(writer, operands, leftPrec, rightPrec);
      break;
    case "BITWISE_OR":
      unparseBitwiseFunctions(writer, call, OR, leftPrec, rightPrec);
      break;
    case "BITWISE_XOR":
      unparseBitwiseFunctions(writer, call, XOR, leftPrec, rightPrec);
      break;
    case "INT2SHR":
      unparseInt2shFunctions(writer, call, SHIFTRIGHT, leftPrec, rightPrec);
      break;
    case "INT2SHL":
      unparseInt2shFunctions(writer, call, SHIFTLEFT, leftPrec, rightPrec);
      break;
    case "PI":
      unparsePI(writer, call, leftPrec, rightPrec);
      break;
    case "OCTET_LENGTH":
      unparseOctetLength(writer, call, leftPrec, rightPrec);
      break;
    case "REGEXP_LIKE":
      unParseRegexpLike(writer, call, leftPrec, rightPrec);
      break;
    case "DATE_DIFF":
      final SqlWriter.Frame date_diff = writer.startFunCall("DATE_DIFF");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      if (call.operandCount() == 3) {
        writer.print(",");
        writer.print(unquoteStringLiteral(call.operand(2).toString()));
      }
      writer.endFunCall(date_diff);
      break;
    case "HASHROW":
      unparseHashrowFunction(writer, call, leftPrec, rightPrec);
      break;
    case "TRUNC":
      final SqlWriter.Frame trunc = getTruncFrame(writer, call);
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(",");
      writer.sep(removeSingleQuotes(call.operand(1)));
      writer.endFunCall(trunc);
      break;
    case "HASHBUCKET":
      if (!call.getOperandList().isEmpty()) {
        unparseCall(writer, call.operand(0), leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case "ROWID":
      final SqlWriter.Frame generate_uuid = writer.startFunCall("GENERATE_UUID");
      writer.endFunCall(generate_uuid);
      break;
    case "TRANSLATE":
      unParseTranslate(writer, call, leftPrec, rightPrec);
      break;
    case "INSTR":
      unParseInStr(writer, call, leftPrec, rightPrec);
      break;
    case "TIMESTAMP_SECONDS":
      castAsDatetime(writer, call, leftPrec, rightPrec, TIMESTAMP_SECONDS);
      break;
    case "TIMESTAMP_MILLIS":
      castAsDatetime(writer, call, leftPrec, rightPrec, TIMESTAMP_MILLIS);
      break;
    case "TIMESTAMP_MICROS":
      castAsDatetime(writer, call, leftPrec, rightPrec, TIMESTAMP_MICROS);
      break;
    case "UNIX_SECONDS":
      castOperandToTimestamp(writer, call, leftPrec, rightPrec, UNIX_SECONDS);
      break;
    case "UNIX_MILLIS":
      castOperandToTimestamp(writer, call, leftPrec, rightPrec, UNIX_MILLIS);
      break;
    case "UNIX_MICROS":
      castOperandToTimestamp(writer, call, leftPrec, rightPrec, UNIX_MICROS);
      break;
    case "INTERVAL_SECONDS":
      unparseIntervalSeconds(writer, call, leftPrec, rightPrec);
      break;
    case "PARSE_DATE":
    case "PARSE_TIME":
      unparseDateTime(writer, call, leftPrec, rightPrec);
      break;
    case "FALSE":
    case "TRUE":
      unparseBoolean(writer, call);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unParseRegexpLike(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame ifFrame = writer.startFunCall("IF");
    unParseRegexpContains(writer, call, leftPrec, rightPrec);
    writer.sep(",");
    writer.literal("1");
    writer.sep(",");
    writer.literal("0");
    writer.endFunCall(ifFrame);
  }

  private void unParseRegexpContains(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame regexContainsFrame = writer.startFunCall("REGEXP_CONTAINS");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(", r");
    unParseRegexString(writer, call, leftPrec, rightPrec);
    writer.endFunCall(regexContainsFrame);
  }

  private void unParseRegexString(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getOperandList().size() == 3) {
      SqlCharStringLiteral modifiedRegexString = getModifiedRegexString(call);
      modifiedRegexString.unparse(writer, leftPrec, rightPrec);
    } else {
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
  }

  private SqlCharStringLiteral getModifiedRegexString(SqlCall call) {
    String matchArgument = call.operand(2).toString().replaceAll("'", "");
    switch (matchArgument) {
    case "i":
      return modifyRegexStringForMatchArgument(call, "(?i)");
    case "x":
      String updatedRegexForX = removeLeadingAndTrailingSingleQuotes
          (call.operand(1).toString().replaceAll("\\s+", ""));
      return SqlLiteral.createCharString(updatedRegexForX, SqlParserPos.ZERO);
    default:
      return call.operand(1);
    }
  }

  private void unParseInStr(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame instrFrame = writer.startFunCall("INSTR");
    for (SqlNode operand : call.getOperandList()) {
      if (operand instanceof SqlCharStringLiteral && operand.toString().contains("\\")) {
        operand = replaceSingleBackslashes(operand);
      }
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(instrFrame);
  }

  private void unParseTranslate(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame translateFuncFrame = writer.startFunCall("TRANSLATE");
    for (SqlNode operand : call.getOperandList()) {
      if (operand instanceof SqlCharStringLiteral) {
        operand = operand.toString().contains("\\") ? replaceSingleBackslashes(operand) : operand;
        operand = operand.toString().contains("\n") ? replaceNewLineChar(operand) : operand;
      }
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(translateFuncFrame);
  }

  private void unparseBoolean(SqlWriter writer, SqlCall call) {
    writer.print(call.getOperator().getName());
    writer.print(" ");
  }

  protected void unparseDateTime(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String dateFormat = call.operand(0) instanceof SqlCharStringLiteral
        ? ((NlsString) requireNonNull(((SqlCharStringLiteral) call.operand(0)).getValue()))
        .getValue() : call.operand(0).toString();
    SqlOperator function = call.getOperator();
    if (!dateFormat.contains("%")) {
      SqlCall formatCall = function.createCall(SqlParserPos.ZERO,
          createDateTimeFormatSqlCharLiteral(dateFormat), call.operand(1));
      function.unparse(writer, formatCall, leftPrec, rightPrec);
    } else {
      function.unparse(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseIntervalSeconds(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    writer.print("INTERVAL ");
    call.operand(0).unparse(writer, 0, 0);
    writer.print("SECOND");
  }

  private void unparseFormatDatetime(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.operand(0).toString()) {
    case "'W'":
      TimeUnit dayOfMonth = TimeUnit.DAY;
      unparseDayWithFormat(writer, call, dayOfMonth, leftPrec, rightPrec);
      break;
    case "'WW'":
      TimeUnit dayOfYear = TimeUnit.DOY;
      unparseDayWithFormat(writer, call, dayOfYear, leftPrec, rightPrec);
      break;
    case "'SEC_FROM_MIDNIGHT'":
      secFromMidnight(writer, call, leftPrec, rightPrec);
      break;
    default:
      unparseFormatCall(writer, call, leftPrec, rightPrec);
    }
  }
  private void castAsDatetime(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlFunction sqlFunction) {
    final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
    sqlFunction.unparse(writer, call, leftPrec, rightPrec);
    writer.sep("AS");
    writer.literal("DATETIME");
    writer.endFunCall(castFrame);
  }

  private void castNodeToTimestamp(SqlWriter writer, SqlNode sqlNode, int leftPrec, int rightPrec) {
    final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
    sqlNode.unparse(writer, leftPrec, rightPrec);
    writer.sep("AS");
    writer.literal("TIMESTAMP");
    writer.endFunCall(castFrame);
  }

  private boolean isOperandCastedToDateTime(SqlCall call) {
    return call.operand(1) instanceof SqlBasicCall
          && ((SqlBasicCall) (call.operand(1))).getOperator() instanceof SqlCastFunction
          && ((SqlBasicCall) (call.operand(1))).operand(1).toString().equals("DATETIME");
  }

  private void castOperandToTimestamp(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlFunction sqlFunction) {
    final SqlWriter.Frame sqlFunctionFrame = writer.startFunCall(sqlFunction.getName());
    final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
    call.getOperandList().get(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("AS");
    writer.literal("TIMESTAMP");
    writer.endFunCall(castFrame);
    writer.endFunCall(sqlFunctionFrame);
  }

  private void secFromMidnight(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode dateNode = getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE));
    SqlNode timestampNode = getCastSpec(
        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
    SqlNode stringNode = getCastSpec(
        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR));
    SqlNode secSymbol = SqlLiteral.createSymbol(TimeUnit.SECOND, SqlParserPos.ZERO);
    SqlNode secondOperand = CAST.createCall(SqlParserPos.ZERO,
        CAST.createCall(SqlParserPos.ZERO, call.operand(1), dateNode), timestampNode);
    SqlCall midnightSec = CAST.createCall(
        SqlParserPos.ZERO, DATE_DIFF.createCall(SqlParserPos.ZERO,
        call.operand(1), secondOperand, secSymbol), stringNode);
    unparseCall(writer, midnightSec, leftPrec, rightPrec);
  }

  private void unparseFormatCall(SqlWriter writer,
      SqlCall call, int leftPrec, int rightPrec) {
    String dateFormat = call.operand(0) instanceof SqlCharStringLiteral
        ? ((NlsString) requireNonNull(((SqlCharStringLiteral) call.operand(0)).getValue()))
        .getValue()
        : call.operand(0).toString();
    SqlCall formatCall = call.getOperator().createCall(SqlParserPos.ZERO,
        createDateTimeFormatSqlCharLiteral(dateFormat), call.operand(1));
    super.unparseCall(writer, formatCall, leftPrec, rightPrec);
  }

  /**
   * Format_date function does not use format types of 'W' and 'WW', So to handle that
   * we have to make a separate function that will use extract, divide, Ceil and Cast
   * functions to make the same logic.
   */
  private void unparseDayWithFormat(SqlWriter writer, SqlCall call,
                                    TimeUnit day, int leftPrec, int rightPrec) {
    SqlNode extractNode = EXTRACT.createCall(SqlParserPos.ZERO,
            SqlLiteral.createSymbol(day, SqlParserPos.ZERO), call.operand(1));

    SqlNode divideNode = DIVIDE.createCall(SqlParserPos.ZERO, extractNode,
            SqlLiteral.createExactNumeric("7", SqlParserPos.ZERO));

    SqlNode ceilNode = CEIL.createCall(SqlParserPos.ZERO, divideNode);

    SqlNode castCall = CAST.createCall(SqlParserPos.ZERO, ceilNode,
            getCastSpec(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)));
    castCall.unparse(writer, leftPrec, rightPrec);
  }

  private void unparseMonthsBetween(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode monthSymbol = SqlLiteral.createSymbol(TimeUnit.MONTH, SqlParserPos.ZERO);
    SqlNode dateDiffNode = DATE_DIFF.createCall(SqlParserPos.ZERO,
            call.operand(0), call.operand(1), monthSymbol);

    SqlNode daySymbolLiteral = SqlLiteral.createSymbol(TimeUnit.DAY, SqlParserPos.ZERO);

    SqlNode firstExtractedDay = EXTRACT.createCall(SqlParserPos.ZERO,
            daySymbolLiteral, call.operand(0));
    SqlNode secondExtractedDay = EXTRACT.createCall(SqlParserPos.ZERO,
            daySymbolLiteral, call.operand(1));

    SqlNode subtractNode = MINUS.createCall(SqlParserPos.ZERO,
            firstExtractedDay, secondExtractedDay);

    SqlNode divideNode = DIVIDE.createCall(SqlParserPos.ZERO, subtractNode,
            SqlLiteral.createExactNumeric("31", SqlParserPos.ZERO));

    SqlNode addNode = PLUS.createCall(SqlParserPos.ZERO, dateDiffNode, divideNode);

    SqlCall roundCall = ROUND.createCall(SqlParserPos.ZERO, addNode,
            SqlLiteral.createExactNumeric("9", SqlParserPos.ZERO));

    roundCall.unparse(writer, leftPrec, rightPrec);
  }

  private void unparseRegexMatchCount(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    SqlWriter.Frame arrayLengthFrame = writer.startFunCall("ARRAY_LENGTH");
    unparseRegexpExtractAll(writer, call, leftPrec, rightPrec);
    writer.endFunCall(arrayLengthFrame);
  }

  private void unparseRegexpExtractAll(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    SqlWriter.Frame regexpExtractAllFrame = writer.startFunCall("REGEXP_EXTRACT_ALL");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(", r");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(regexpExtractAllFrame);
  }

  private void unparseCot(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode tanNode = TAN.createCall(SqlParserPos.ZERO, call.getOperandList());
    SqlCall divideCall = DIVIDE.createCall(SqlParserPos.ZERO,
            SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO), tanNode);
    divideCall.unparse(writer, leftPrec, rightPrec);
  }

  private void unparsePI(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode numericNode = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
    SqlCall acosCall = ACOS.createCall(SqlParserPos.ZERO, numericNode);
    unparseCall(writer, acosCall, leftPrec, rightPrec);
  }

  private void unparseOctetLength(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode operandCall = call.operand(0);
    if (call.operand(0) instanceof SqlLiteral) {
      String newOperand = call.operand(0).toString().replace("\\",
              "\\\\");

      operandCall = SqlLiteral.createCharString(
              unquoteStringLiteral(newOperand), SqlParserPos.ZERO);
    }
    final SqlWriter.Frame octetFrame = writer.startFunCall("OCTET_LENGTH");
    operandCall.unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(octetFrame);
  }

  private void unparseInt2shFunctions(SqlWriter writer, SqlCall call,
                                      String s, int leftPrec, int rightPrec) {
    SqlNode[] operands = new SqlNode[] {call.operand(0), call.operand(2)};
    writer.print("(");
    unparseBitwiseAnd(writer, operands, leftPrec, rightPrec);
    writer.sep(") " + s);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
  }

  private void unparseBitwiseFunctions(SqlWriter writer, SqlCall call,
                                       String s, int leftPrec, int rightPrec) {
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(s);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
  }

  private void unparseBitwiseAnd(SqlWriter writer, SqlNode[] operands,
                                 int leftPrec, int rightPrec) {
    operands[0].unparse(writer, leftPrec, rightPrec);
    writer.print("& ");
    operands[1].unparse(writer, leftPrec, rightPrec);
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

  private SqlCharStringLiteral createDateTimeFormatSqlCharLiteral(String format) {
    String formatString = getDateTimeFormatString(unquoteStringLiteral(format),
        DATE_TIME_FORMAT_MAP);
    return SqlLiteral.createCharString(formatString, SqlParserPos.ZERO);
  }

  /**
   * unparse method for Random function.
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
        .replace("'", "")
        .replace("%S.", "%E");
  }

  /**
   * BigQuery interval syntax: INTERVAL int64 time_unit.
   */
  @Override public void unparseSqlIntervalLiteral(
    SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    literal = modifiedSqlIntervalLiteral(literal);
    SqlIntervalLiteral.IntervalValue interval =
        literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
    writer.keyword("INTERVAL");
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    try {
      Long.parseLong(interval.getIntervalLiteral());
    } catch (NumberFormatException e) {
      throw new RuntimeException("Only INT64 is supported as the interval value for BigQuery.");
    }
    writer.literal(interval.getIntervalLiteral());
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
            RelDataTypeSystem.DEFAULT);
  }

  private SqlIntervalLiteral modifiedSqlIntervalLiteral(SqlIntervalLiteral literal) {
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

  @Override public void unparseSqlIntervalQualifier(
          SqlWriter writer, SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.endUnit == null) {
      writer.keyword(start);
    } else {
      throw new RuntimeException("Range time unit is not supported for BigQuery.");
    }
  }

  /**
   * For usage of TRIM, LTRIM and RTRIM in BQ see
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#trim">
   *  BQ Trim Function</a>.
   */
  private static void unparseTrim(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    SqlNode valueToTrim = call.operand(1);
    requireNonNull(valueToTrim, "valueToTrim in unparseTrim() must not be null");
    String value = Util.removeLeadingAndTrailingSingleQuotes(valueToTrim.toString());
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

    // If the trimmed character is a non-space character, add it to the target SQL.
    // eg: TRIM(BOTH 'A' from 'ABCD'
    // Output Query: TRIM('ABC', 'A')
    if (!value.matches("\\s+")) {
      writer.literal(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(trimFrame);
  }

  private static TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
    case MICROSECOND:
    case MILLISECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case DAY:
    case WEEK:
    case MONTH:
    case QUARTER:
    case YEAR:
    case ISOYEAR:
      return timeUnit;
    default:
      throw new RuntimeException("Time unit " + timeUnit + " is not supported for BigQuery.");
    }
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

  private void handleSqlBasicCallForTimestampMulti(SqlWriter writer, SqlCall call) {
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

  private void unparseExtractFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.operand(0).toString()) {
    case "EPOCH" :
      SqlNode firstOperand = call.operand(1);
      if (firstOperand instanceof SqlBasicCall
          && ((SqlBasicCall) firstOperand).getOperator().kind == SqlKind.MINUS) {
        SqlNode leftOperand = ((SqlBasicCall) firstOperand).getOperands()[0];
        SqlNode rightOperand = ((SqlBasicCall) firstOperand).getOperands()[1];
        unparseExtractEpochOperands(writer, leftOperand, leftPrec, rightPrec);
        writer.print(" - ");
        unparseExtractEpochOperands(writer, rightOperand, leftPrec, rightPrec);
      } else {
        unparseExtractEpochOperands(writer, firstOperand, leftPrec, rightPrec);
      }
      break;
    default :
      ExtractFunctionFormatUtil extractFormatUtil = new ExtractFunctionFormatUtil();
      SqlCall extractCall = extractFormatUtil.unparseCall(call, this);
      super.unparseCall(writer, extractCall, leftPrec, rightPrec);
    }
  }

  private void unparseExtractEpochOperands(SqlWriter writer, SqlNode operand,
                                           int leftPrec, int rightPrec) {
    final SqlWriter.Frame epochFrame = writer.startFunCall("UNIX_SECONDS");
    unparseOperandAsTimestamp(writer, operand, leftPrec, rightPrec);
    writer.endFunCall(epochFrame);
  }

  private boolean isDateTimeCast(SqlNode operand) {
    boolean isCastCall = ((SqlBasicCall) operand).getOperator() == CAST;
    boolean isDateTimeCast = isCastCall
        && ((SqlDataTypeSpec) ((SqlBasicCall) operand).operands[1])
            .getTypeName().toString().equals("TIMESTAMP");
    return isDateTimeCast;
  }

  private void unparseCurrentTimestampCall(SqlWriter writer) {
    final SqlWriter.Frame currentTimestampFunc = writer.startFunCall("CURRENT_TIMESTAMP");
    writer.endFunCall(currentTimestampFunc);
  }

  private void unparseOperandAsTimestamp(SqlWriter writer, SqlNode operand,
                                         int leftPrec, int rightPrec) {
    if (operand instanceof SqlBasicCall) {
      if (((SqlBasicCall) operand).getOperator() == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
        unparseCurrentTimestampCall(writer);
      } else if (isDateTimeCast(operand)) {
        SqlNode node = ((SqlBasicCall) operand).operands[0];
        castNodeToTimestamp(writer, node, leftPrec, rightPrec);
      }
    } else {
      castNodeToTimestamp(writer, operand, leftPrec, rightPrec);
    }
  }

  private void unparseGroupingFunction(SqlWriter writer, SqlCall call,
                                       int leftPrec, int rightPrec) {
    SqlCall isNull = new SqlBasicCall(IS_NULL, new SqlNode[]{call.operand(0)}, SqlParserPos.ZERO);
    SqlNumericLiteral oneLiteral = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    SqlNumericLiteral zeroLiteral = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
    SqlNodeList whenList = new SqlNodeList(SqlParserPos.ZERO);
    whenList.add(isNull);
    SqlNodeList thenList = new SqlNodeList(SqlParserPos.ZERO);
    thenList.add(oneLiteral);
    SqlCall groupingSqlCall = new SqlCase(SqlParserPos.ZERO, null, whenList, thenList, zeroLiteral);
    unparseCall(writer, groupingSqlCall, leftPrec, rightPrec);
  }

  private boolean isIntervalHourAndSecond(SqlCall call) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      return ((SqlIntervalLiteral) call.operand(1)).getTypeName()
          == SqlTypeName.INTERVAL_HOUR_SECOND;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>BigQuery data type reference:
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">
   * BigQuery Standard SQL Data Types</a>.
   */
  @Override public @Nullable SqlNode getCastSpec(final RelDataType type) {
    if (type instanceof BasicSqlType) {
      final SqlTypeName typeName = type.getSqlTypeName();
      switch (typeName) {
      // BigQuery only supports INT64 for integer types.
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_HOUR:
        return createSqlDataTypeSpecByName("INT64", typeName);
      // BigQuery only supports FLOAT64(aka. Double) for floating point types.
      case FLOAT:
      case DOUBLE:
        return createSqlDataTypeSpecByName("FLOAT64", typeName);
      case DECIMAL:
        return createSqlDataTypeSpecByName("NUMERIC", typeName);
      case BOOLEAN:
        return createSqlDataTypeSpecByName("BOOL", typeName);
      case CHAR:
      case VARCHAR:
        return createSqlDataTypeSpecByName("STRING", typeName);
      case BINARY:
      case VARBINARY:
        return createSqlDataTypeSpecByName("BYTES", typeName);
      case DATE:
        return createSqlDataTypeSpecByName("DATE", typeName);
      case TIME:
        return createSqlDataTypeSpecByName("TIME", typeName);
      case TIMESTAMP:
        return createSqlDataTypeSpecByName("DATETIME", typeName);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return createSqlDataTypeSpecByName("TIMESTAMP_WITH_LOCAL_TIME_ZONE", typeName);
      default:
        break;
      }
    }
    return super.getCastSpec(type);
  }

  @Override public @Nullable SqlNode getCastSpecWithPrecisionAndScale(final RelDataType type) {
    if (type instanceof BasicSqlType) {
      final SqlTypeName typeName = type.getSqlTypeName();
      final int precision = type.getPrecision();
      final int scale = type.getScale();
      boolean isContainsPrecision = type.toString().matches("\\w+\\(\\d+(, (-)?\\d+)?\\)");
      boolean isContainsScale = type.toString().contains(",");
      boolean isContainsNegativePrecisionOrScale = type.toString().contains("-");
      String typeAlias;
      switch (typeName) {
      case DECIMAL:
        if (isContainsPrecision) {
          String dataType = getDataTypeBasedOnPrecision(precision, scale);
          if (!isContainsNegativePrecisionOrScale) {
            typeAlias = precision > 0 ? isContainsScale ? dataType + "(" + precision + ","
                + scale + ")" : dataType + "(" + precision + ")" : dataType;
          } else {
            typeAlias = dataType;
          }
        } else {
          typeAlias = "NUMERIC";
        }
        return createSqlDataTypeSpecByName(typeAlias, typeName);
      case CHAR:
      case VARCHAR:
        if (isContainsPrecision) {
          typeAlias =  precision > 0 ? "STRING(" + precision + ")" : "STRING";
        } else {
          typeAlias = "STRING";
        }
        return createSqlDataTypeSpecByName(typeAlias, typeName);
      case BINARY:
      case VARBINARY:
        if (isContainsPrecision) {
          typeAlias =  precision > 0 ? "BYTES(" + precision + ")" : "BYTES";
        } else {
          typeAlias = "BYTES";
        }
        return createSqlDataTypeSpecByName(typeAlias, typeName);
      default:
        break;
      }
    }
    return this.getCastSpec(type);
  }

  private String getDataTypeBasedOnPrecision(int precision, int scale)  {
    if (scale > 0) {
      return scale <= 9 ? scale + precision > 38 ? "BIGNUMERIC" : "NUMERIC" : "BIGNUMERIC";
    } else {
      return precision > 29 ? "BIGNUMERIC" : "NUMERIC";
    }
  }

  private static SqlDataTypeSpec createSqlDataTypeSpecByName(String typeAlias,
      SqlTypeName typeName) {
    SqlAlienSystemTypeNameSpec typeNameSpec = new SqlAlienSystemTypeNameSpec(
        typeAlias, typeName, SqlParserPos.ZERO);
    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
  }

  private static String removeSingleQuotes(SqlNode sqlNode) {
    return ((SqlCharStringLiteral) sqlNode).getValue().toString().replaceAll("'",
        "");
  }

  public static void processNode(SqlNode binaryNode, SqlCall call) {
    if (binaryNode instanceof SqlNodeList) {
      SqlNodeList operandList = (SqlNodeList) binaryNode;
      processNodeList(operandList);
    }
    if (binaryNode instanceof SqlCharStringLiteral && binaryNode.toString().contains("\n")) {
      binaryNode = replaceNewLineChar(binaryNode);
      call.setOperand(1, binaryNode);
    }
  }

  public static void processNodeList(SqlNodeList operandList) {
    SqlNode node;
    for (SqlNode operand : operandList) {
      if (operand instanceof SqlCharStringLiteral) {
        node = operand.toString().contains("\\") ? replaceSingleBackslashes(operand) : operand;
        node = node.toString().contains("\n") ? replaceNewLineChar(node) : node;
        operandList.set(operandList.indexOf(operand), node);
      }
    }
  }

  public static SqlNode replaceSingleBackslashes(SqlNode operand) {
    String replacedOperand = ((SqlCharStringLiteral) operand).toValue().replace("\\", "\\\\");
    return SqlLiteral.createCharString(replacedOperand, SqlParserPos.ZERO);
  }

  public static SqlNode replaceNewLineChar(SqlNode operand) {
    String replacedOperand = ((SqlCharStringLiteral) operand).toValue().replace("\n", "");
    return SqlLiteral.createCharString(replacedOperand, SqlParserPos.ZERO);
  }

  /**
   * In BigQuery, the equivalent for HASHROW is FARM_FINGERPRINT, and FARM_FINGERPRINT supports
   * only one argument.
   * So, to handle this scenario, we CONCAT all the arguments of HASHROW.
   * And
   * For single argument,we directly cast that element to VARCHAR.
   * Example:
   * BQ:  FARM_FINGERPRINT(CONCAT(first_name, employee_id, last_name, hire_date))
   */
  private void unparseHashrowFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode farmFingerprintOperandCall;
    if (call.operandCount() > 1) {
      farmFingerprintOperandCall = CONCAT2.createCall(SqlParserPos.ZERO, call.getOperandList());
    } else {
      SqlNode varcharNode = getCastSpec(
          new BasicSqlType(RelDataTypeSystem.DEFAULT,
          SqlTypeName.VARCHAR));
      farmFingerprintOperandCall = CAST.createCall(SqlParserPos.ZERO, call.operand(0),
          varcharNode);
    }
    SqlCall farmFingerprintCall = FARM_FINGERPRINT.createCall(SqlParserPos.ZERO,
        farmFingerprintOperandCall);
    super.unparseCall(writer, farmFingerprintCall, leftPrec, rightPrec);
  }

  private SqlWriter.Frame getTruncFrame(SqlWriter writer, SqlCall call) {
    SqlWriter.Frame frame = null;
    String dateFormatOperand = call.operand(1).toString();
    boolean isDateTimeOperand = call.operand(0).toString().contains("DATETIME");
    if (isDateTimeOperand) {
      frame = writer.startFunCall("DATETIME_TRUNC");
    } else {
      switch (dateFormatOperand) {
      case "'HOUR'":
      case "'MINUTE'":
      case "'SECOND'":
      case "'MILLISECOND'":
      case "'MICROSECOND'":
        frame = writer.startFunCall("TIME_TRUNC");
        break;
      default:
        frame = writer.startFunCall("DATE_TRUNC");

      }
    }
    return frame;
  }

  private SqlWriter.Frame getColumnListFrame(SqlWriter writer, SqlCall call) {
    SqlWriter.Frame frame = null;
    if (call.getOperandList().size() == 1) {
      frame = writer.startList("", "");
    } else {
      frame = writer.startList("(", ")");
    }
    return frame;
  }
}
