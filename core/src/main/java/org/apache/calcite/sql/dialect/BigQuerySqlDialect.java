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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.format.FormatElement;
import org.apache.calcite.util.format.FormatModels;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.calcite.util.format.FormatElementEnum.D;
import static org.apache.calcite.util.format.FormatElementEnum.DAY;
import static org.apache.calcite.util.format.FormatElementEnum.DD;
import static org.apache.calcite.util.format.FormatElementEnum.DDD;
import static org.apache.calcite.util.format.FormatElementEnum.DY;
import static org.apache.calcite.util.format.FormatElementEnum.HH24;
import static org.apache.calcite.util.format.FormatElementEnum.IW;
import static org.apache.calcite.util.format.FormatElementEnum.MI;
import static org.apache.calcite.util.format.FormatElementEnum.MM;
import static org.apache.calcite.util.format.FormatElementEnum.MON;
import static org.apache.calcite.util.format.FormatElementEnum.MONTH;
import static org.apache.calcite.util.format.FormatElementEnum.Q;
import static org.apache.calcite.util.format.FormatElementEnum.SS;
import static org.apache.calcite.util.format.FormatElementEnum.TZR;
import static org.apache.calcite.util.format.FormatElementEnum.WW;
import static org.apache.calcite.util.format.FormatElementEnum.YY;
import static org.apache.calcite.util.format.FormatElementEnum.YYYY;

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
      .withIdentifierEscapedQuoteString("\\`")
      .withNullCollation(NullCollation.LOW)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withCaseSensitive(false);

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

  private static final Map<String, FormatElement> BIG_QUERY_FORMAT_ELEMENT_MAP =
      new ImmutableMap.Builder<String, FormatElement>()
          .put("%A", DAY)
          .put("%a", DY)
          .put("%B", MONTH)
          .put("%b", MON)
          .put(
              "%c", FormatModels.compositeElement(
              Arrays.asList(DY, FormatModels.literalElement(" "), MON,
                  FormatModels.literalElement(" "), DD, FormatModels.literalElement(" "), HH24,
                  FormatModels.literalElement(":"), MI, FormatModels.literalElement(":"), SS,
                  FormatModels.literalElement(" "), YYYY),
              "The date and time representation (English)."))
          .put("%d", DD)
          .put("%H", HH24)
          .put("%j", DDD)
          .put("%M", MI)
          .put("%m", MM)
          .put("%Q", Q)
          .put(
              "%R", FormatModels.compositeElement(
              Arrays.asList(HH24, FormatModels.literalElement(":"), MI),
              "The time in the format %H:%M"))
          .put("%S", SS)
          .put("%u", D)
          .put("%V", IW)
          .put("%W", WW)
          .put(
              "%x", FormatModels.compositeElement(
              Arrays.asList(MM, FormatModels.literalElement("/"), DD,
                  FormatModels.literalElement("/"), YY),
              "The date representation in MM/DD/YY format."))
          .put("%Y", YYYY)
          .put("%y", YY)
          .put("%Z", TZR)
          .build();

  /** An unquoted BigQuery identifier must start with a letter and be followed
   * by zero or more letters, digits or _. */
  private static final Pattern IDENTIFIER_REGEX =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  /** Creates a BigQuerySqlDialect. */
  public BigQuerySqlDialect(SqlDialect.Context context) {
    super(context);
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

  @Override public boolean supportsApproxCountDistinct() {
    return true;
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

  @Override public boolean supportsAliasedValues() {
    return false;
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
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /** BigQuery interval syntax: INTERVAL int64 time_unit. */
  @Override public void unparseSqlIntervalLiteral(SqlWriter writer,
      SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
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

    // If the trimmed character is a non-space character, add it to the target SQL.
    // eg: TRIM(BOTH 'A' from 'ABCD'
    // Output Query: TRIM('ABC', 'A')
    String value = requireNonNull(valueToTrim.toValue(), "valueToTrim.toValue()");
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

  /**
   * {@inheritDoc}
   *
   * <p>BigQuery format element reference:
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements">
   * BigQuery Standard SQL Format Elements</a>.
   */
  @Override public Map<String, FormatElement> getFormatElementMap() {
    return BIG_QUERY_FORMAT_ELEMENT_MAP;
  }

  /** {@inheritDoc}
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
      default:
        break;
      }
    }
    return super.getCastSpec(type);
  }

  private static SqlDataTypeSpec createSqlDataTypeSpecByName(String typeAlias,
      SqlTypeName typeName) {
    SqlAlienSystemTypeNameSpec typeNameSpec = new SqlAlienSystemTypeNameSpec(
        typeAlias, typeName, SqlParserPos.ZERO);
    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
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

}
