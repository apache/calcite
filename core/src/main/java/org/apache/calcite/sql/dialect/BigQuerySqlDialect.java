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
import org.apache.calcite.rel.type.DelegatingTypeSystem;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.RelToSqlConverterUtil;
import org.apache.calcite.util.format.FormatModel;
import org.apache.calcite.util.format.FormatModels;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static java.lang.Long.parseLong;

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

  // The BigQuery type system differs from the DEFAULT type system in this respect,
  // as evidenced by tests in big-query.iq
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new DelegatingTypeSystem(RelDataTypeSystem.DEFAULT) {
    @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
      return true;
    }
  };

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

  @Override public boolean supportsImplicitTypeCoercion(RexCall call) {
    return super.supportsImplicitTypeCoercion(call)
            && RexUtil.isLiteral(call.getOperands().get(0), false)
            && !SqlTypeUtil.isNumeric(call.type);
  }

  @Override public RelDataTypeSystem getTypeSystem() {
    return TYPE_SYSTEM;
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
      final SqlWriter.Frame frame = writer.startFunCall("INSTR");
      switch (call.operandCount()) {
      case 2:
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        break;
      case 3:
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
        break;
      case 4:
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(3).unparse(writer, leftPrec, rightPrec);
        break;
      default:
        throw new RuntimeException("BigQuery does not support " + call.operandCount()
            + " operands in the position function");
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
      RelToSqlConverterUtil.unparseTrimLR(writer, call, leftPrec, rightPrec);
      break;
    case ITEM:
      if (call.getOperator().getName().equals("ITEM")) {
        throw new RuntimeException("BigQuery requires an array subscript operator"
            + " to index an array");
      }
      unparseItem(writer, call, leftPrec);
      break;
    case OVER:
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.keyword("OVER");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
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
      parseLong(interval.getIntervalLiteral());
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

  /** When indexing an array in BigQuery, an array subscript operator must
   * surround the desired index. For the standard ITEM operator used by other
   * dialects in Calcite, ITEM is not included in the unparsing. This helper
   * ensures that the operator is preserved when being unparsed. */
  private static void unparseItem(SqlWriter writer, SqlCall call, int leftPrec) {
    String operatorName = call.getOperator().getName();
    call.operand(0).unparse(writer, leftPrec, 0);
    final SqlWriter.Frame frame = writer.startList("[", "]");
    final SqlWriter.Frame subscriptFrame = writer.startFunCall(operatorName);
    call.operand(1).unparse(writer, 0, 0);
    writer.endFunCall(subscriptFrame);
    writer.endList(frame);
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
   * @see FormatModels#BIG_QUERY
   */
  @Override public FormatModel getFormatModel() {
    return FormatModels.BIG_QUERY;
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
      case REAL:
        return createSqlDataTypeSpecByName("FLOAT32", typeName);
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
    SqlAlienSystemTypeNameSpec typeNameSpec =
        new SqlAlienSystemTypeNameSpec(typeAlias, typeName, SqlParserPos.ZERO);
    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
  }

  /**
   * List of BigQuery Specific Operators needed to form Syntactically Correct SQL.
   */
  private static final SqlOperator UNION_DISTINCT =
      new SqlSetOperator("UNION DISTINCT", SqlKind.UNION, 14, false);

  private static final SqlSetOperator EXCEPT_DISTINCT =
      new SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false);

  private static final SqlSetOperator INTERSECT_DISTINCT =
      new SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false);

}
