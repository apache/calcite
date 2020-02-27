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
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.CurrentTimestampHandler;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.ToNumberUtils;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.IFNULL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT_ALL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SUBSTR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;

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

  /**
   * Creates a BigQuerySqlDialect.
   */
  public BigQuerySqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public String quoteIdentifier(String val) {
    return quoteIdentifier(new StringBuilder(), val).toString();
  }

  @Override protected boolean identifierNeedsQuote(String val) {
    return !IDENTIFIER_REGEX.matcher(val).matches()
        || RESERVED_KEYWORDS.contains(val.toUpperCase(Locale.ROOT));
  }

  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public boolean supportsImplicitTypeCoercion(RexCall call) {
    return super.supportsImplicitTypeCoercion(call)
        && RexUtil.isLiteral(call.getOperands().get(0), false)
        && !SqlTypeUtil.isNumeric(call.type);
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
    switch (call.type.getSqlTypeName()) {
    case DATE:
      switch (call.getOperands().get(1).getType().getSqlTypeName()) {
      case INTERVAL_DAY:
      case INTERVAL_MONTH:
        if (call.op.kind == SqlKind.MINUS) {
          return SqlLibraryOperators.DATE_SUB;
        }
        return SqlLibraryOperators.DATE_ADD;
      }
    default:
      return super.getTargetFunc(call);
    }
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
    case CHAR_LENGTH:
    case CHARACTER_LENGTH:
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
      ToNumberUtils.handleToNumber(writer, call, leftPrec, rightPrec);
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
      unparseOtherFunction(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "CURRENT_TIMESTAMP":
      if (((SqlBasicCall) call).getOperands().length > 0) {
        unparseCurrentTimestamp(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case "CURRENT_USER":
    case "SESSION_USER":
      final SqlWriter.Frame sessionUserFunc = writer.startFunCall(SESSION_USER.getName());
      writer.endFunCall(sessionUserFunc);
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

  private void unparseCurrentTimestamp(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    CurrentTimestampHandler timestampHandler = new CurrentTimestampHandler(this);
    SqlCall formatTimestampCall = timestampHandler.makeFormatTimestampCall(call);
    SqlCall castCall = timestampHandler.makeCastCall(formatTimestampCall);
    unparseCall(writer, castCall, leftPrec, rightPrec);
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
   * @param writer    Target SqlWriter to write the call
   * @param call      SqlCall : date + Store_id * INTERVAL 2 DAY
   * @param leftPrec  Indicate left precision
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
      unparseLiteralIntervalCall(call.operand(1), writer);
      break;
    case TIMES:
      unparseExpressionIntervalCall(call.operand(1), writer, leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
    writer.endFunCall(frame);
  }

  /**
   * Unparse the literal call from input query and write the INTERVAL part. Below is an example:
   * Input: date + INTERVAL 1 DAY
   * It will write this as: INTERVAL 1 DAY
   *
   * @param call   SqlCall :INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   */
  private void unparseLiteralIntervalCall(
      SqlLiteral call, SqlWriter writer) {
    SqlIntervalLiteral intervalLiteralValue = (SqlIntervalLiteral) call;
    SqlIntervalLiteral.IntervalValue literalValue =
        (SqlIntervalLiteral.IntervalValue) intervalLiteralValue.getValue();
    writer.sep("INTERVAL");
    writer.sep(literalValue.getIntervalLiteral());
    writer.print(literalValue.getIntervalQualifier().toString());
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
   * @param call      SqlCall : store_id * INTERVAL 1 DAY
   * @param writer    Target SqlWriter to write the call
   * @param leftPrec  Indicate left precision
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

  /**
   * BigQuery interval syntax: INTERVAL int64 time_unit.
   */
  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    writer.keyword("INTERVAL");
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    Long intervalValueInLong;
    try {
      intervalValueInLong = Long.parseLong(literal.getValue().toString());
    } catch (NumberFormatException e) {
      throw new RuntimeException("Only INT64 is supported as the interval value for BigQuery.");
    }
    writer.literal(intervalValueInLong.toString());
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
        RelDataTypeSystem.DEFAULT);
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

  private TimeUnit validate(TimeUnit timeUnit) {
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

  /**
   * BigQuery data type reference:
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">
   * BigQuery Standard SQL Data Types</a>
   */
  @Override public SqlNode getCastSpec(final RelDataType type) {
    if (type instanceof BasicSqlType) {
      final SqlTypeName typeName = type.getSqlTypeName();
      switch (typeName) {
      // BigQuery only supports INT64 for integer types.
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
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
        return createSqlDataTypeSpecByName("TIMESTAMP", typeName);
      }
    }
    return super.getCastSpec(type);
  }

  private SqlDataTypeSpec createSqlDataTypeSpecByName(String typeAlias, SqlTypeName typeName) {
    SqlAlienSystemTypeNameSpec typeNameSpec = new SqlAlienSystemTypeNameSpec(
        typeAlias, typeName, SqlParserPos.ZERO);
    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
  }
}
