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
package org.apache.calcite.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of SQL.
 *
 * <p>It is used by classes such as {@link SqlWriter} and
 * {@link org.apache.calcite.sql.util.SqlBuilder}.
 *
 * <p>To add a new {@link SqlDialect} sub-class, extends this class to hold 2 public final
 * static member:
 * <ul>
 *   <li>DEFAULT_CONTEXT: a default {@link Context} instance, which can be used to customize
 *   or extending the dialect if the DEFAULT instance does not meet the requests</li>
 *   <li>DEFAULT: the default {@link SqlDialect} instance with context properties defined with
 *   <code>DEFAULT_CONTEXT</code></li>
 * </ul>
 */
public class SqlDialect {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(SqlDialect.class);

  /** Empty context. */
  public static final Context EMPTY_CONTEXT = emptyContext();

  /** @deprecated Use {@link AnsiSqlDialect#DEFAULT} instead. */
  @Deprecated // to be removed before 2.0
  public static final SqlDialect DUMMY =
      AnsiSqlDialect.DEFAULT;

  /** @deprecated Use {@link CalciteSqlDialect#DEFAULT} instead. */
  @Deprecated // to be removed before 2.0
  public static final SqlDialect CALCITE =
      CalciteSqlDialect.DEFAULT;

  /** Built-in scalar functions and operators common for every dialect. */
  protected static final Set<SqlOperator> BUILT_IN_OPERATORS_LIST =
      ImmutableSet.<SqlOperator>builder()
          .add(SqlStdOperatorTable.ABS)
          .add(SqlStdOperatorTable.ACOS)
          .add(SqlStdOperatorTable.AND)
          .add(SqlStdOperatorTable.ASIN)
          .add(SqlStdOperatorTable.BETWEEN)
          .add(SqlStdOperatorTable.CASE)
          .add(SqlStdOperatorTable.CAST)
          .add(SqlStdOperatorTable.CEIL)
          .add(SqlStdOperatorTable.CHAR_LENGTH)
          .add(SqlStdOperatorTable.CHARACTER_LENGTH)
          .add(SqlStdOperatorTable.COALESCE)
          .add(SqlStdOperatorTable.CONCAT)
          .add(SqlStdOperatorTable.COS)
          .add(SqlStdOperatorTable.COT)
          .add(SqlStdOperatorTable.DIVIDE)
          .add(SqlStdOperatorTable.EQUALS)
          .add(SqlStdOperatorTable.FLOOR)
          .add(SqlStdOperatorTable.GREATER_THAN)
          .add(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
          .add(SqlStdOperatorTable.IN)
          .add(SqlStdOperatorTable.IS_NOT_NULL)
          .add(SqlStdOperatorTable.IS_NULL)
          .add(SqlStdOperatorTable.LESS_THAN)
          .add(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
          .add(SqlStdOperatorTable.LIKE)
          .add(SqlStdOperatorTable.LN)
          .add(SqlStdOperatorTable.LOG10)
          .add(SqlStdOperatorTable.MINUS)
          .add(SqlStdOperatorTable.MOD)
          .add(SqlStdOperatorTable.MULTIPLY)
          .add(SqlStdOperatorTable.NOT)
          .add(SqlStdOperatorTable.NOT_BETWEEN)
          .add(SqlStdOperatorTable.NOT_EQUALS)
          .add(SqlStdOperatorTable.NOT_IN)
          .add(SqlStdOperatorTable.NOT_LIKE)
          .add(SqlStdOperatorTable.OR)
          .add(SqlStdOperatorTable.PI)
          .add(SqlStdOperatorTable.PLUS)
          .add(SqlStdOperatorTable.POWER)
          .add(SqlStdOperatorTable.RAND)
          .add(SqlStdOperatorTable.ROUND)
          .add(SqlStdOperatorTable.ROW)
          .add(SqlStdOperatorTable.SIN)
          .add(SqlStdOperatorTable.SQRT)
          .add(SqlStdOperatorTable.SUBSTRING)
          .add(SqlStdOperatorTable.TAN)
          .build();


  //~ Instance fields --------------------------------------------------------

  protected final String identifierQuoteString;
  protected final String identifierEndQuoteString;
  protected final String identifierEscapedQuote;
  protected final String literalQuoteString;
  protected final String literalEndQuoteString;
  protected final String literalEscapedQuote;
  private final DatabaseProduct databaseProduct;
  protected final NullCollation nullCollation;
  private final RelDataTypeSystem dataTypeSystem;
  private final Casing unquotedCasing;
  private final Casing quotedCasing;
  private final boolean caseSensitive;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SqlDialect</code> from a DatabaseMetaData.
   *
   * <p>Does not maintain a reference to the DatabaseMetaData -- or, more
   * importantly, to its {@link java.sql.Connection} -- after this call has
   * returned.
   *
   * @param databaseMetaData used to determine which dialect of SQL to generate
   *
   * @deprecated Replaced by {@link SqlDialectFactory}
   */
  @Deprecated // to be removed before 2.0
  public static SqlDialect create(DatabaseMetaData databaseMetaData) {
    return new SqlDialectFactoryImpl().create(databaseMetaData);
  }

  @Deprecated // to be removed before 2.0
  public SqlDialect(DatabaseProduct databaseProduct, String databaseProductName,
      String identifierQuoteString) {
    this(EMPTY_CONTEXT
        .withDatabaseProduct(databaseProduct)
        .withDatabaseProductName(databaseProductName)
        .withIdentifierQuoteString(identifierQuoteString));
  }

  /**
   * Creates a SqlDialect.
   *
   * @param databaseProduct       Database product; may be UNKNOWN, never null
   * @param databaseProductName   Database product name from JDBC driver
   * @param identifierQuoteString String to quote identifiers. Null if quoting
   *                              is not supported. If "[", close quote is
   *                              deemed to be "]".
   * @param nullCollation         Whether NULL values appear first or last
   *
   * @deprecated Use {@link #SqlDialect(Context)}
   */
  @Deprecated // to be removed before 2.0
  public SqlDialect(DatabaseProduct databaseProduct, String databaseProductName,
      String identifierQuoteString, NullCollation nullCollation) {
    this(EMPTY_CONTEXT
        .withDatabaseProduct(databaseProduct)
        .withDatabaseProductName(databaseProductName)
        .withIdentifierQuoteString(identifierQuoteString)
        .withNullCollation(nullCollation));
  }

  /**
   * Creates a SqlDialect.
   *
   * @param context All the information necessary to create a dialect
   */
  public SqlDialect(Context context) {
    this.nullCollation = Objects.requireNonNull(context.nullCollation());
    this.dataTypeSystem = Objects.requireNonNull(context.dataTypeSystem());
    this.databaseProduct =
        Objects.requireNonNull(context.databaseProduct());
    this.literalQuoteString =
        Objects.requireNonNull(context.literalQuoteString());
    this.literalEndQuoteString =
        Objects.requireNonNull(context.literalQuoteString());
    this.literalEscapedQuote =
        Objects.requireNonNull(context.literalEscapedQuoteString());
    String identifierQuoteString = context.identifierQuoteString();
    if (identifierQuoteString != null) {
      identifierQuoteString = identifierQuoteString.trim();
      if (identifierQuoteString.equals("")) {
        identifierQuoteString = null;
      }
    }
    this.identifierQuoteString = identifierQuoteString;
    this.identifierEndQuoteString =
        identifierQuoteString == null ? null
            : identifierQuoteString.equals("[") ? "]"
            : identifierQuoteString;
    this.identifierEscapedQuote =
        identifierQuoteString == null ? null
            : this.identifierEndQuoteString + this.identifierEndQuoteString;
    this.unquotedCasing = Objects.requireNonNull(context.unquotedCasing());
    this.quotedCasing = Objects.requireNonNull(context.quotedCasing());
    this.caseSensitive = context.caseSensitive();
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates an empty context. Use {@link #EMPTY_CONTEXT} to reference the instance. */
  private static Context emptyContext() {
    return new ContextImpl(DatabaseProduct.UNKNOWN, null, null, -1, -1,
        "'", "''", null,
        Casing.UNCHANGED, Casing.TO_UPPER, true, SqlConformanceEnum.DEFAULT,
        NullCollation.HIGH, RelDataTypeSystemImpl.DEFAULT,
        JethroDataSqlDialect.JethroInfo.EMPTY);
  }

  /**
   * Converts a product name and version (per the JDBC driver) into a product
   * enumeration.
   *
   * @param productName    Product name
   * @param productVersion Product version
   * @return database product
   */
  @Deprecated // to be removed before 2.0
  public static DatabaseProduct getProduct(
      String productName,
      String productVersion) {
    final String upperProductName =
        productName.toUpperCase(Locale.ROOT).trim();
    switch (upperProductName) {
    case "ACCESS":
      return DatabaseProduct.ACCESS;
    case "APACHE DERBY":
    case "DBMS:CLOUDSCAPE":
      return DatabaseProduct.DERBY;
    case "HIVE":
      return DatabaseProduct.HIVE;
    case "INGRES":
      return DatabaseProduct.INGRES;
    case "INTERBASE":
      return DatabaseProduct.INTERBASE;
    case "LUCIDDB":
      return DatabaseProduct.LUCIDDB;
    case "ORACLE":
      return DatabaseProduct.ORACLE;
    case "PHOENIX":
      return DatabaseProduct.PHOENIX;
    case "MYSQL (INFOBRIGHT)":
      return DatabaseProduct.INFOBRIGHT;
    case "MYSQL":
      return DatabaseProduct.MYSQL;
    case "REDSHIFT":
      return DatabaseProduct.REDSHIFT;
    }
    // Now the fuzzy matches.
    if (productName.startsWith("DB2")) {
      return DatabaseProduct.DB2;
    } else if (upperProductName.contains("FIREBIRD")) {
      return DatabaseProduct.FIREBIRD;
    } else if (productName.startsWith("Informix")) {
      return DatabaseProduct.INFORMIX;
    } else if (upperProductName.contains("NETEZZA")) {
      return DatabaseProduct.NETEZZA;
    } else if (upperProductName.contains("PARACCEL")) {
      return DatabaseProduct.PARACCEL;
    } else if (productName.startsWith("HP Neoview")) {
      return DatabaseProduct.NEOVIEW;
    } else if (upperProductName.contains("POSTGRE")) {
      return DatabaseProduct.POSTGRESQL;
    } else if (upperProductName.contains("SQL SERVER")) {
      return DatabaseProduct.MSSQL;
    } else if (upperProductName.contains("SYBASE")) {
      return DatabaseProduct.SYBASE;
    } else if (upperProductName.contains("TERADATA")) {
      return DatabaseProduct.TERADATA;
    } else if (upperProductName.contains("HSQL")) {
      return DatabaseProduct.HSQLDB;
    } else if (upperProductName.contains("H2")) {
      return DatabaseProduct.H2;
    } else if (upperProductName.contains("VERTICA")) {
      return DatabaseProduct.VERTICA;
    } else if (upperProductName.contains("GOOGLE BIGQUERY")) {
      return DatabaseProduct.BIG_QUERY;
    } else {
      return DatabaseProduct.UNKNOWN;
    }
  }

  /** Returns the type system implementation for this dialect. */
  public RelDataTypeSystem getTypeSystem() {
    return dataTypeSystem;
  }

  /**
   * Encloses an identifier in quotation marks appropriate for the current SQL
   * dialect.
   *
   * <p>For example, <code>quoteIdentifier("emp")</code> yields a string
   * containing <code>"emp"</code> in Oracle, and a string containing <code>
   * [emp]</code> in Access.
   *
   * @param val Identifier to quote
   * @return Quoted identifier
   */
  public String quoteIdentifier(String val) {
    if (identifierQuoteString == null) {
      return val; // quoting is not supported
    }
    String val2 =
        val.replaceAll(
            identifierEndQuoteString,
            identifierEscapedQuote);
    return identifierQuoteString + val2 + identifierEndQuoteString;
  }

  /**
   * Encloses an identifier in quotation marks appropriate for the current SQL
   * dialect, writing the result to a {@link StringBuilder}.
   *
   * <p>For example, <code>quoteIdentifier("emp")</code> yields a string
   * containing <code>"emp"</code> in Oracle, and a string containing <code>
   * [emp]</code> in Access.
   *
   * @param buf Buffer
   * @param val Identifier to quote
   * @return The buffer
   */
  public StringBuilder quoteIdentifier(
      StringBuilder buf,
      String val) {
    if (identifierQuoteString == null // quoting is not supported
        || !identifierNeedsQuote(val)) {
      buf.append(val);
    } else {
      String val2 =
          val.replaceAll(
              identifierEndQuoteString,
              identifierEscapedQuote);
      buf.append(identifierQuoteString);
      buf.append(val2);
      buf.append(identifierEndQuoteString);
    }
    return buf;
  }

  /**
   * Quotes a multi-part identifier.
   *
   * @param buf         Buffer
   * @param identifiers List of parts of the identifier to quote
   * @return The buffer
   */
  public StringBuilder quoteIdentifier(
      StringBuilder buf,
      List<String> identifiers) {
    int i = 0;
    for (String identifier : identifiers) {
      if (i++ > 0) {
        buf.append('.');
      }
      quoteIdentifier(buf, identifier);
    }
    return buf;
  }

  /** Returns whether to quote an identifier.
   * By default, all identifiers are quoted. */
  protected boolean identifierNeedsQuote(String val) {
    return true;
  }

  /**
   * Converts a string into a string literal.
   *
   * <p>For example, {@code "can't run"} becomes {@code "'can''t run'"}.
   */
  public final String quoteStringLiteral(String val) {
    final StringBuilder buf = new StringBuilder();
    quoteStringLiteral(buf, null, val);
    return buf.toString();
  }

  /** Appends a string literal to a buffer.
   *
   * @param buf Buffer
   * @param charsetName Character set name, e.g. "utf16", or null
   * @param val String value
   */
  public void quoteStringLiteral(StringBuilder buf, String charsetName,
      String val) {
    if (containsNonAscii(val) && charsetName == null) {
      quoteStringLiteralUnicode(buf, val);
    } else {
      if (charsetName != null) {
        buf.append("_");
        buf.append(charsetName);
      }
      buf.append(literalQuoteString);
      buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
      buf.append(literalEndQuoteString);
    }
  }

  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    call.getOperator().unparse(writer, call, leftPrec, rightPrec);
  }

  public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    writer.literal(literal.toString());
  }

  public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep((SqlKind.PLUS == sqlKind) ? "+" : "-");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
    //Only two parameters are present normally
    //Checking parameter count to prevent errors
    if (call.getOperandList().size() > 2) {
      call.operand(2).unparse(writer, leftPrec, rightPrec);
    }
  }

  /** Converts an interval qualifier to a SQL string. The default implementation
   * returns strings such as
   * <code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code>. */
  public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    final String start = qualifier.timeUnitRange.startUnit.name();
    final int fractionalSecondPrecision =
        qualifier.getFractionalSecondPrecision(typeSystem);
    final int startPrecision = qualifier.getStartPrecision(typeSystem);
    if (qualifier.timeUnitRange.startUnit == TimeUnit.SECOND) {
      if (!qualifier.useDefaultFractionalSecondPrecision()) {
        final SqlWriter.Frame frame = writer.startFunCall(start);
        writer.print(startPrecision);
        writer.sep(",", true);
        writer.print(qualifier.getFractionalSecondPrecision(typeSystem));
        writer.endList(frame);
      } else if (!qualifier.useDefaultStartPrecision()) {
        final SqlWriter.Frame frame = writer.startFunCall(start);
        writer.print(startPrecision);
        writer.endList(frame);
      } else {
        writer.keyword(start);
      }
    } else {
      if (!qualifier.useDefaultStartPrecision()) {
        final SqlWriter.Frame frame = writer.startFunCall(start);
        writer.print(startPrecision);
        writer.endList(frame);
      } else {
        writer.keyword(start);
      }

      if (null != qualifier.timeUnitRange.endUnit) {
        writer.keyword("TO");
        final String end = qualifier.timeUnitRange.endUnit.name();
        if ((TimeUnit.SECOND == qualifier.timeUnitRange.endUnit)
                && (!qualifier.useDefaultFractionalSecondPrecision())) {
          final SqlWriter.Frame frame = writer.startFunCall(end);
          writer.print(fractionalSecondPrecision);
          writer.endList(frame);
        } else {
          writer.keyword(end);
        }
      }
    }
  }

  /** Converts an interval literal to a SQL string. The default implementation
   * returns strings such as
   * <code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code>. */
  public void unparseSqlIntervalLiteral(SqlWriter writer,
      SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    writer.keyword("INTERVAL");
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    writer.literal("'" + literal.getValue().toString() + "'");
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
        RelDataTypeSystem.DEFAULT);
  }

  /**
   * Returns whether the string contains any characters outside the
   * comfortable 7-bit ASCII range (32 through 127, plus linefeed (10) and
   * carriage return (13)).
   *
   * <p>Such characters can be used unquoted in SQL character literals.
   *
   * @param s String
   * @return Whether string contains any non-7-bit-ASCII characters
   */
  protected static boolean containsNonAscii(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < 32 && c != 10 && c != 13 || c >= 128) {
        return true;
      }
    }
    return false;
  }

  /**
   * Converts a string into a unicode string literal. For example,
   * <code>can't{tab}run\</code> becomes <code>u'can''t\0009run\\'</code>.
   */
  public void quoteStringLiteralUnicode(StringBuilder buf, String val) {
    buf.append("u&'");
    for (int i = 0; i < val.length(); i++) {
      char c = val.charAt(i);
      if (c < 32 || c >= 128) {
        buf.append('\\');
        buf.append(HEXITS[(c >> 12) & 0xf]);
        buf.append(HEXITS[(c >> 8) & 0xf]);
        buf.append(HEXITS[(c >> 4) & 0xf]);
        buf.append(HEXITS[c & 0xf]);
      } else if (c == '\'' || c == '\\') {
        buf.append(c);
        buf.append(c);
      } else {
        buf.append(c);
      }
    }
    buf.append("'");
  }

  private static final char[] HEXITS = {
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  };

  /**
   * Converts a string literal back into a string. For example, <code>'can''t
   * run'</code> becomes <code>can't run</code>.
   */
  public String unquoteStringLiteral(String val) {
    if (val != null
        && val.startsWith(literalQuoteString)
        && val.endsWith(literalEndQuoteString)) {
      final String stripped =
          val.substring(literalQuoteString.length(),
              val.length() - literalEndQuoteString.length());
      return stripped.replace(literalEscapedQuote, literalEndQuoteString);
    }
    return val;
  }

  protected boolean allowsAs() {
    return true;
  }

  // -- behaviors --

  /** Whether a sub-query in the FROM clause must have an alias.
   *
   * <p>For example, in PostgreSQL, this query is legal:
   *
   * <blockquote>{@code SELECT * FROM (SELECT * FROM Emp) As e}</blockquote>
   *
   * <p>but remove the alias {@code e} and it is not:
   *
   * <blockquote>{@code SELECT * FROM (SELECT * FROM Emp)}</blockquote>
   *
   * <p>In Oracle, both queries are legal.
   */
  public boolean requiresAliasForFromItems() {
    return false;
  }

  /** Returns whether a qualified table in the FROM clause has an implicit alias
   * which consists of just the table name.
   *
   * <p>For example, in {@link DatabaseProduct#ORACLE}
   *
   * <blockquote>SELECT * FROM sales.emp</blockquote>
   *
   * <p>is equivalent to
   *
   * <blockquote>SELECT * FROM sales.emp AS emp</blockquote>
   *
   * <p>and therefore
   *
   * <blockquote>SELECT emp.empno FROM sales.emp</blockquote>
   *
   * <p>is valid. But {@link DatabaseProduct#DB2} does not have an implicit
   * alias, so the previous query it not valid; you need to write
   *
   * <blockquote>SELECT sales.emp.empno FROM sales.emp</blockquote>
   *
   * <p>Returns true for all databases except DB2.
   */
  public boolean hasImplicitTableAlias() {
    return true;
  }

  /**
   * Converts a timestamp to a SQL timestamp literal, e.g.
   * {@code TIMESTAMP '2009-12-17 12:34:56'}.
   *
   * <p>Timestamp values do not have a time zone. We therefore interpret them
   * as the number of milliseconds after the UTC epoch, and the formatted
   * value is that time in UTC.
   *
   * <p>In particular,
   *
   * <blockquote><code>quoteTimestampLiteral(new Timestamp(0));</code>
   * </blockquote>
   *
   * <p>returns {@code TIMESTAMP '1970-01-01 00:00:00'}, regardless of the JVM's
   * time zone.
   *
   * @param timestamp Timestamp
   * @return SQL timestamp literal
   */
  public String quoteTimestampLiteral(Timestamp timestamp) {
    final SimpleDateFormat format =
        new SimpleDateFormat(
            "'TIMESTAMP' ''yyyy-MM-DD HH:mm:SS''",
            Locale.ROOT);
    format.setTimeZone(DateTimeUtils.UTC_ZONE);
    return format.format(timestamp);
  }

  /**
   * Returns the database this dialect belongs to,
   * {@link SqlDialect.DatabaseProduct#UNKNOWN} if not known, never null.
   *
   * <p>Please be judicious in how you use this method. If you wish to determine
   * whether a dialect has a particular capability or behavior, it is usually
   * better to add a method to SqlDialect and override that method in particular
   * sub-classes of SqlDialect.
   *
   * @return Database product
   * @deprecated To be removed without replacement
   */
  @Deprecated // to be removed before 2.0
  public DatabaseProduct getDatabaseProduct() {
    return databaseProduct;
  }

  /**
   * Returns whether the dialect supports character set names as part of a
   * data type, for instance {@code VARCHAR(30) CHARACTER SET `ISO-8859-1`}.
   */
  public boolean supportsCharSet() {
    return true;
  }

  public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case COUNT:
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
      return true;
    }
    return false;
  }

  /** Returns whether this dialect supports window functions (OVER clause). */
  public boolean supportsWindowFunctions() {
    return true;
  }

  /** Returns whether this dialect supports a given function or operator.
   * It only applies to built-in scalar functions and operators, since
   * user-defined functions and procedures should be read by JdbcSchema. */
  public boolean supportsFunction(SqlOperator operator, RelDataType type,
      List<RelDataType> paramTypes) {
    switch (operator.kind) {
    case AND:
    case BETWEEN:
    case CASE:
    case CAST:
    case CEIL:
    case COALESCE:
    case DIVIDE:
    case EQUALS:
    case FLOOR:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case IN:
    case IS_NULL:
    case IS_NOT_NULL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case MINUS:
    case MOD:
    case NOT:
    case NOT_IN:
    case NOT_EQUALS:
    case NVL:
    case OR:
    case PLUS:
    case ROW:
    case TIMES:
      return true;
    default:
      return BUILT_IN_OPERATORS_LIST.contains(operator);
    }
  }

  public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.NULL;
  }

  /** Returns whether this dialect supports a given type. */
  public boolean supportsDataType(RelDataType type) {
    return true;
  }

 /** Returns SqlNode for type in "cast(column as type)", which might be
  * different between databases by type name, precision etc. */
  public SqlNode getCastSpec(RelDataType type) {
    if (type instanceof BasicSqlType) {
      int maxPrecision = -1;
      switch (type.getSqlTypeName()) {
      case VARCHAR:
        // if needed, adjust varchar length to max length supported by the system
        maxPrecision = getTypeSystem().getMaxPrecision(type.getSqlTypeName());
      }
      String charSet = type.getCharset() != null && supportsCharSet()
          ? type.getCharset().name()
          : null;
      return SqlTypeUtil.convertTypeToSpec(type, charSet, maxPrecision);
    }
    return SqlTypeUtil.convertTypeToSpec(type);
  }

  /** Rewrite SINGLE_VALUE into expression based on database variants
   *  E.g. HSQLDB, MYSQL, ORACLE, etc
   */
  public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    LOGGER.debug("SINGLE_VALUE rewrite not supported for {}", databaseProduct);
    return aggCall;
  }

  /**
   * Returns the SqlNode for emulating the null direction for the given field
   * or <code>null</code> if no emulation needs to be done.
   *
   * @param node The SqlNode representing the expression
   * @param nullsFirst Whether nulls should come first
   * @param desc Whether the sort direction is
   * {@link org.apache.calcite.rel.RelFieldCollation.Direction#DESCENDING} or
   * {@link org.apache.calcite.rel.RelFieldCollation.Direction#STRICTLY_DESCENDING}
   * @return A SqlNode for null direction emulation or <code>null</code> if not required
   */
  public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst,
      boolean desc) {
    return null;
  }

  public JoinType emulateJoinTypeForCrossJoin() {
    return JoinType.COMMA;
  }

  protected SqlNode emulateNullDirectionWithIsNull(SqlNode node,
      boolean nullsFirst, boolean desc) {
    // No need for emulation if the nulls will anyways come out the way we want
    // them based on "nullsFirst" and "desc".
    if (nullCollation.isDefaultOrder(nullsFirst, desc)) {
      return null;
    }

    node = SqlStdOperatorTable.IS_NULL.createCall(SqlParserPos.ZERO, node);
    if (nullsFirst) {
      node = SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, node);
    }
    return node;
  }

  /**
   * Returns whether the dialect supports OFFSET/FETCH clauses
   * introduced by SQL:2008, for instance
   * {@code OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY}.
   * If false, we assume that the dialect supports the alternative syntax
   * {@code LIMIT 20 OFFSET 10}.
   *
   * @deprecated This method is no longer used. To change how the dialect
   * unparses offset/fetch, override the {@link #unparseOffsetFetch} method.
   */
  @Deprecated
  public boolean supportsOffsetFetch() {
    return true;
  }

  /**
   * Converts an offset and fetch into SQL.
   *
   * <p>At least one of {@code offset} and {@code fetch} must be provided.
   *
   * <p>Common options:
   * <ul>
   *   <li>{@code OFFSET offset ROWS FETCH NEXT fetch ROWS ONLY}
   *   (ANSI standard SQL, Oracle, PostgreSQL, and the default)
   *   <li>{@code LIMIT fetch OFFSET offset} (Apache Hive, MySQL, Redshift)
   * </ul>
   *
   * @param writer Writer
   * @param offset Number of rows to skip before emitting, or null
   * @param fetch Number of rows to fetch, or null
   *
   * @see #unparseFetchUsingAnsi(SqlWriter, SqlNode, SqlNode)
   * @see #unparseFetchUsingLimit(SqlWriter, SqlNode, SqlNode)
   */
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    unparseFetchUsingAnsi(writer, offset, fetch);
  }

  /**
   * Converts a fetch into a "SELECT TOP(fetch)".
   *
   * <p>A dialect that uses "TOP" syntax should override this method to print
   * "TOP(fetch)", and override {@link #unparseOffsetFetch} to no-op.
   *
   * <p>The default implementation of this method is no-op.
   *
   * @param writer Writer
   * @param offset Number of rows to skip before emitting, or null
   * @param fetch Number of rows to fetch, or null
   */
  public void unparseTopN(SqlWriter writer, SqlNode offset, SqlNode fetch) {
  }

  /** Unparses offset/fetch using ANSI standard "OFFSET offset ROWS FETCH NEXT
   * fetch ROWS ONLY" syntax. */
  protected final void unparseFetchUsingAnsi(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    Preconditions.checkArgument(fetch != null || offset != null);
    if (offset != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame offsetFrame =
          writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
      writer.keyword("OFFSET");
      offset.unparse(writer, -1, -1);
      writer.keyword("ROWS");
      writer.endList(offsetFrame);
    }
    if (fetch != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame fetchFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FETCH);
      writer.keyword("FETCH");
      writer.keyword("NEXT");
      fetch.unparse(writer, -1, -1);
      writer.keyword("ROWS");
      writer.keyword("ONLY");
      writer.endList(fetchFrame);
    }
  }

  /** Unparses offset/fetch using "LIMIT fetch OFFSET offset" syntax. */
  protected final void unparseFetchUsingLimit(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    Preconditions.checkArgument(fetch != null || offset != null);
    if (fetch != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame fetchFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FETCH);
      writer.keyword("LIMIT");
      fetch.unparse(writer, -1, -1);
      writer.endList(fetchFrame);
    }
    if (offset != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame offsetFrame =
          writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
      writer.keyword("OFFSET");
      offset.unparse(writer, -1, -1);
      writer.endList(offsetFrame);
    }
  }

  /**
   * Returns whether the dialect supports nested aggregations, for instance
   * {@code SELECT SUM(SUM(1)) }.
   */
  public boolean supportsNestedAggregations() {
    return true;
  }

  /**
   * Returns whether this dialect supports "WITH ROLLUP" in the "GROUP BY"
   * clause.
   *
   * <p>For instance, in MySQL version 5,
   *
   * <blockquote>
   *   <code>
   *     SELECT deptno, job, COUNT(*) AS c
   *     FROM emp
   *     GROUP BY deptno, job WITH ROLLUP
   *   </code>
   * </blockquote>
   *
   * <p>is equivalent to standard SQL
   *
   * <blockquote>
   *   <code>
   *     SELECT deptno, job, COUNT(*) AS c
   *     FROM emp
   *     GROUP BY ROLLUP(deptno, job)
   *     ORDER BY deptno, job
   *   </code>
   * </blockquote>
   *
   * <p>The "WITH ROLLUP" clause was introduced in MySQL and is not standard
   * SQL.
   *
   * <p>See also {@link #supportsAggregateFunction(SqlKind)} applied to
   * {@link SqlKind#ROLLUP}, which returns true in MySQL 8 and higher.
   */
  public boolean supportsGroupByWithRollup() {
    return false;
  }

  /**
   * Returns whether this dialect supports "WITH CUBE" in "GROUP BY" clause.
   */
  public boolean supportsGroupByWithCube() {
    return false;
  }

  /** Returns how NULL values are sorted if an ORDER BY item does not contain
   * NULLS ASCENDING or NULLS DESCENDING. */
  public NullCollation getNullCollation() {
    return nullCollation;
  }

  /** Returns whether NULL values are sorted first or last, in this dialect,
   * in an ORDER BY item of a given direction. */
  public @Nonnull RelFieldCollation.NullDirection defaultNullDirection(
      RelFieldCollation.Direction direction) {
    switch (direction) {
    case ASCENDING:
    case STRICTLY_ASCENDING:
      return getNullCollation().last(false)
          ? RelFieldCollation.NullDirection.LAST
          : RelFieldCollation.NullDirection.FIRST;
    case DESCENDING:
    case STRICTLY_DESCENDING:
      return getNullCollation().last(true)
          ? RelFieldCollation.NullDirection.LAST
          : RelFieldCollation.NullDirection.FIRST;
    default:
      return RelFieldCollation.NullDirection.UNSPECIFIED;
    }
  }

  /**
   * Returns whether the dialect supports VALUES in a sub-query with
   * and an "AS t(column, ...)" values to define column names.
   *
   * <p>Currently, only Oracle does not. For this, we generate "SELECT v0 AS c0,
   * v1 AS c1 ... UNION ALL ...". We may need to refactor this method when we
   * support VALUES for other dialects. */
  @Experimental
  public boolean supportsAliasedValues() {
    return true;
  }

  /**
   * Returns whether the dialect supports implicit type coercion.
   *
   * <p>Most of the sql dialects support implicit type coercion, so we make this method
   * default return true. For instance, "cast('10' as integer) &gt; 5"
   * can be simplified to "'10' &gt; 5" if the dialect supports implicit type coercion
   * for VARCHAR and INTEGER comparison.
   *
   * <p>For sql dialect that does not support implicit type coercion, such as the BigQuery,
   * we can not convert '10' into INT64 implicitly.
   *
   * <p>Now this method is used for some auxiliary decision when translating some {@link RexCall}s,
   * see SqlImplementor#stripCastFromString for details.
   *
   * @param call the call to make decision
   */
  public boolean supportsImplicitTypeCoercion(RexCall call) {
    final RexNode operand0 = call.getOperands().get(0);
    return SqlTypeUtil.isCharacter(operand0.getType());
  }

  /** Returns the name of the system table that has precisely one row.
   * If there is no such table, returns null, and we will generate SELECT with
   * no FROM clause.
   *
   * <p>For {@code VALUES 1},
   * Oracle returns ["DUAL"] and we generate "SELECT 1 FROM DUAL";
   * MySQL returns null and we generate "SELECT 1".
   */
  @Experimental
  public @Nullable List<String> getSingleRowTableName() {
    return null;
  }

  /**
   * Copies settings from this dialect into a parser configuration.
   *
   * <p>{@code SqlDialect}, {@link SqlParser.Config} and {@link SqlConformance}
   * cover different aspects of the same thing - the dialect of SQL spoken by a
   * database - and this method helps to bridge between them. (The aspects are,
   * respectively, generating SQL to send to a source database, parsing SQL
   * sent to Calcite, and validating queries sent to Calcite. It makes sense to
   * keep them as separate interfaces because they are used by different
   * modules.)
   *
   * <p>The settings copied may differ among dialects, and may change over time,
   * but currently include the following:
   *
   * <ul>
   *   <li>{@link #getQuoting()}
   *   <li>{@link #getQuotedCasing()}
   *   <li>{@link #getUnquotedCasing()}
   *   <li>{@link #isCaseSensitive()}
   *   <li>{@link #getConformance()}
   * </ul>
   *
   * @param configBuilder Parser configuration builder
   *
   * @return The configuration builder
   */
  public @Nonnull SqlParser.ConfigBuilder configureParser(
      SqlParser.ConfigBuilder configBuilder) {
    final Quoting quoting = getQuoting();
    if (quoting != null) {
      configBuilder.setQuoting(quoting);
    }
    configBuilder.setQuotedCasing(getQuotedCasing());
    configBuilder.setUnquotedCasing(getUnquotedCasing());
    configBuilder.setCaseSensitive(isCaseSensitive());
    configBuilder.setConformance(getConformance());
    return configBuilder;
  }

  /** Returns the {@link SqlConformance} that matches this dialect.
   *
   * <p>The base implementation returns its best guess, based upon
   * {@link #databaseProduct}; sub-classes may override. */
  @Nonnull public SqlConformance getConformance() {
    switch (databaseProduct) {
    case CALCITE:
      return SqlConformanceEnum.DEFAULT;
    case MYSQL:
      return SqlConformanceEnum.MYSQL_5;
    case ORACLE:
      return SqlConformanceEnum.ORACLE_10;
    case MSSQL:
      return SqlConformanceEnum.SQL_SERVER_2008;
    default:
      return SqlConformanceEnum.PRAGMATIC_2003;
    }
  }

  /** Returns the quoting scheme, or null if the combination of
   * {@link #identifierQuoteString} and {@link #identifierEndQuoteString}
   * does not correspond to any known quoting scheme. */
  protected Quoting getQuoting() {
    if ("\"".equals(identifierQuoteString)
        && "\"".equals(identifierEndQuoteString)) {
      return Quoting.DOUBLE_QUOTE;
    } else if ("`".equals(identifierQuoteString)
        && "`".equals(identifierEndQuoteString)) {
      return Quoting.BACK_TICK;
    } else if ("[".equals(identifierQuoteString)
        && "]".equals(identifierEndQuoteString)) {
      return Quoting.BRACKET;
    } else {
      return null;
    }
  }

  /** Returns how unquoted identifiers are stored. */
  public Casing getUnquotedCasing() {
    return unquotedCasing;
  }

  /** Returns how quoted identifiers are stored. */
  public Casing getQuotedCasing() {
    return quotedCasing;
  }

  /** Returns whether matching of identifiers is case-sensitive. */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * A few utility functions copied from org.apache.calcite.util.Util. We have
   * copied them because we wish to keep SqlDialect's dependencies to a
   * minimum.
   */
  @Deprecated // to be removed before 2.0
  public static class FakeUtil {
    public static Error newInternal(Throwable e, String s) {
      String message = "Internal error: \u0000" + s;
      AssertionError ae = new AssertionError(message);
      ae.initCause(e);
      return ae;
    }

    /**
     * Replaces every occurrence of <code>find</code> in <code>s</code> with
     * <code>replace</code>.
     */
    public static String replace(
        String s,
        String find,
        String replace) {
      // let's be optimistic
      int found = s.indexOf(find);
      if (found == -1) {
        return s;
      }
      StringBuilder sb = new StringBuilder(s.length());
      int start = 0;
      for (;;) {
        for (; start < found; start++) {
          sb.append(s.charAt(start));
        }
        if (found == s.length()) {
          break;
        }
        sb.append(replace);
        start += find.length();
        found = s.indexOf(find, start);
        if (found == -1) {
          found = s.length();
        }
      }
      return sb.toString();
    }
  }


  /** Whether this JDBC driver needs you to pass a Calendar object to methods
   * such as {@link ResultSet#getTimestamp(int, java.util.Calendar)}. */
  public enum CalendarPolicy {
    NONE,
    NULL,
    LOCAL,
    DIRECT,
    SHIFT;
  }

  /**
   * Rough list of flavors of database.
   *
   * <p>These values cannot help you distinguish between features that exist
   * in different versions or ports of a database, but they are sufficient
   * to drive a {@code switch} statement if behavior is broadly different
   * between say, MySQL and Oracle.
   *
   * <p>If possible, you should not refer to particular database at all; write
   * extend the dialect to describe the particular capability, for example,
   * whether the database allows expressions to appear in the GROUP BY clause.
   */
  public enum DatabaseProduct {
    ACCESS("Access", "\"", NullCollation.HIGH),
    BIG_QUERY("Google BigQuery", "`", NullCollation.LOW),
    CALCITE("Apache Calcite", "\"", NullCollation.HIGH),
    MSSQL("Microsoft SQL Server", "[", NullCollation.HIGH),
    MYSQL("MySQL", "`", NullCollation.LOW),
    ORACLE("Oracle", "\"", NullCollation.HIGH),
    DERBY("Apache Derby", null, NullCollation.HIGH),
    DB2("IBM DB2", null, NullCollation.HIGH),
    FIREBIRD("Firebird", null, NullCollation.HIGH),
    H2("H2", "\"", NullCollation.HIGH),
    HIVE("Apache Hive", null, NullCollation.LOW),
    INFORMIX("Informix", null, NullCollation.HIGH),
    INGRES("Ingres", null, NullCollation.HIGH),
    JETHRO("JethroData", "\"", NullCollation.LOW),
    LUCIDDB("LucidDB", "\"", NullCollation.HIGH),
    INTERBASE("Interbase", null, NullCollation.HIGH),
    PHOENIX("Phoenix", "\"", NullCollation.HIGH),
    POSTGRESQL("PostgreSQL", "\"", NullCollation.HIGH),
    NETEZZA("Netezza", "\"", NullCollation.HIGH),
    INFOBRIGHT("Infobright", "`", NullCollation.HIGH),
    NEOVIEW("Neoview", null, NullCollation.HIGH),
    SYBASE("Sybase", null, NullCollation.HIGH),
    TERADATA("Teradata", "\"", NullCollation.HIGH),
    HSQLDB("Hsqldb", null, NullCollation.HIGH),
    VERTICA("Vertica", "\"", NullCollation.HIGH),
    SQLSTREAM("SQLstream", "\"", NullCollation.HIGH),
    SPARK("Spark", null, NullCollation.LOW),

    /** Paraccel, now called Actian Matrix. Redshift is based on this, so
     * presumably the dialect capabilities are similar. */
    PARACCEL("Paraccel", "\"", NullCollation.HIGH),
    REDSHIFT("Redshift", "\"", NullCollation.HIGH),
    SNOWFLAKE("Snowflake", "\"", NullCollation.HIGH),

    /**
     * Placeholder for the unknown database.
     *
     * <p>Its dialect is useful for generating generic SQL. If you need to
     * do something database-specific like quoting identifiers, don't rely
     * on this dialect to do what you want.
     */
    UNKNOWN("Unknown", "`", NullCollation.HIGH);

    private final Supplier<SqlDialect> dialect;

    DatabaseProduct(String databaseProductName, String quoteString,
        NullCollation nullCollation) {
      Objects.requireNonNull(databaseProductName);
      Objects.requireNonNull(nullCollation);
      dialect = Suppliers.memoize(() -> {
        final SqlDialect dialect =
            SqlDialectFactoryImpl.simple(DatabaseProduct.this);
        if (dialect != null) {
          return dialect;
        }
        return new SqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.this)
            .withDatabaseProductName(databaseProductName)
            .withIdentifierQuoteString(quoteString)
            .withNullCollation(nullCollation));
      })::get;
    }

    /**
     * Returns a dummy dialect for this database.
     *
     * <p>Since databases have many versions and flavors, this dummy dialect
     * is at best an approximation. If you want exact information, better to
     * use a dialect created from an actual connection's metadata
     * (see {@link SqlDialectFactory#create(java.sql.DatabaseMetaData)}).
     *
     * @return Dialect representing lowest-common-denominator behavior for
     * all versions of this database
     */
    public SqlDialect getDialect() {
      return dialect.get();
    }
  }

  /** Information for creating a dialect.
   *
   * <p>It is immutable; to "set" a property, call one of the "with" methods,
   * which returns a new context with the desired property value. */
  public interface Context {
    @Nonnull DatabaseProduct databaseProduct();
    Context withDatabaseProduct(@Nonnull DatabaseProduct databaseProduct);
    String databaseProductName();
    Context withDatabaseProductName(String databaseProductName);
    String databaseVersion();
    Context withDatabaseVersion(String databaseVersion);
    int databaseMajorVersion();
    Context withDatabaseMajorVersion(int databaseMajorVersion);
    int databaseMinorVersion();
    Context withDatabaseMinorVersion(int databaseMinorVersion);
    @Nonnull String literalQuoteString();
    @Nonnull Context withLiteralQuoteString(String literalQuoteString);
    @Nonnull String literalEscapedQuoteString();
    @Nonnull Context withLiteralEscapedQuoteString(
        String literalEscapedQuoteString);
    String identifierQuoteString();
    @Nonnull Context withIdentifierQuoteString(String identifierQuoteString);
    @Nonnull Casing unquotedCasing();
    @Nonnull Context withUnquotedCasing(Casing unquotedCasing);
    @Nonnull Casing quotedCasing();
    @Nonnull Context withQuotedCasing(Casing unquotedCasing);
    boolean caseSensitive();
    @Nonnull Context withCaseSensitive(boolean caseSensitive);
    @Nonnull SqlConformance conformance();
    @Nonnull Context withConformance(SqlConformance conformance);
    @Nonnull NullCollation nullCollation();
    @Nonnull Context withNullCollation(@Nonnull NullCollation nullCollation);
    @Nonnull RelDataTypeSystem dataTypeSystem();
    Context withDataTypeSystem(@Nonnull RelDataTypeSystem dataTypeSystem);
    JethroDataSqlDialect.JethroInfo jethroInfo();
    Context withJethroInfo(JethroDataSqlDialect.JethroInfo jethroInfo);
  }

  /** Implementation of Context. */
  private static class ContextImpl implements Context {
    private final DatabaseProduct databaseProduct;
    private final String databaseProductName;
    private final String databaseVersion;
    private final int databaseMajorVersion;
    private final int databaseMinorVersion;
    private final String literalQuoteString;
    private final String literalEscapedQuoteString;
    private final String identifierQuoteString;
    private final Casing unquotedCasing;
    private final Casing quotedCasing;
    private final boolean caseSensitive;
    private final SqlConformance conformance;
    private final NullCollation nullCollation;
    private final RelDataTypeSystem dataTypeSystem;
    private final JethroDataSqlDialect.JethroInfo jethroInfo;

    private ContextImpl(DatabaseProduct databaseProduct,
        String databaseProductName, String databaseVersion,
        int databaseMajorVersion, int databaseMinorVersion,
        String literalQuoteString, String literalEscapedQuoteString,
        String identifierQuoteString, Casing quotedCasing,
        Casing unquotedCasing, boolean caseSensitive,
        SqlConformance conformance, NullCollation nullCollation,
        RelDataTypeSystem dataTypeSystem,
        JethroDataSqlDialect.JethroInfo jethroInfo) {
      this.databaseProduct = Objects.requireNonNull(databaseProduct);
      this.databaseProductName = databaseProductName;
      this.databaseVersion = databaseVersion;
      this.databaseMajorVersion = databaseMajorVersion;
      this.databaseMinorVersion = databaseMinorVersion;
      this.literalQuoteString = literalQuoteString;
      this.literalEscapedQuoteString = literalEscapedQuoteString;
      this.identifierQuoteString = identifierQuoteString;
      this.quotedCasing = Objects.requireNonNull(quotedCasing);
      this.unquotedCasing = Objects.requireNonNull(unquotedCasing);
      this.caseSensitive = caseSensitive;
      this.conformance = Objects.requireNonNull(conformance);
      this.nullCollation = Objects.requireNonNull(nullCollation);
      this.dataTypeSystem = Objects.requireNonNull(dataTypeSystem);
      this.jethroInfo = Objects.requireNonNull(jethroInfo);
    }

    @Nonnull public DatabaseProduct databaseProduct() {
      return databaseProduct;
    }

    public Context withDatabaseProduct(
        @Nonnull DatabaseProduct databaseProduct) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public String databaseProductName() {
      return databaseProductName;
    }

    public Context withDatabaseProductName(String databaseProductName) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public String databaseVersion() {
      return databaseVersion;
    }

    public Context withDatabaseVersion(String databaseVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public int databaseMajorVersion() {
      return databaseMajorVersion;
    }

    public Context withDatabaseMajorVersion(int databaseMajorVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public int databaseMinorVersion() {
      return databaseMinorVersion;
    }

    public Context withDatabaseMinorVersion(int databaseMinorVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public String literalQuoteString() {
      return literalQuoteString;
    }

    public Context withLiteralQuoteString(String literalQuoteString) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public String literalEscapedQuoteString() {
      return literalEscapedQuoteString;
    }

    public Context withLiteralEscapedQuoteString(
        String literalEscapedQuoteString) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public String identifierQuoteString() {
      return identifierQuoteString;
    }

    @Nonnull public Context withIdentifierQuoteString(
        String identifierQuoteString) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public Casing unquotedCasing() {
      return unquotedCasing;
    }

    @Nonnull public Context withUnquotedCasing(Casing unquotedCasing) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public Casing quotedCasing() {
      return quotedCasing;
    }

    @Nonnull public Context withQuotedCasing(Casing quotedCasing) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    public boolean caseSensitive() {
      return caseSensitive;
    }

    @Nonnull public Context withCaseSensitive(boolean caseSensitive) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public SqlConformance conformance() {
      return conformance;
    }

    @Nonnull public Context withConformance(SqlConformance conformance) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public NullCollation nullCollation() {
      return nullCollation;
    }

    @Nonnull public Context withNullCollation(
        @Nonnull NullCollation nullCollation) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public RelDataTypeSystem dataTypeSystem() {
      return dataTypeSystem;
    }

    public Context withDataTypeSystem(@Nonnull RelDataTypeSystem dataTypeSystem) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }

    @Nonnull public JethroDataSqlDialect.JethroInfo jethroInfo() {
      return jethroInfo;
    }

    public Context withJethroInfo(JethroDataSqlDialect.JethroInfo jethroInfo) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          literalQuoteString, literalEscapedQuoteString,
          identifierQuoteString, quotedCasing, unquotedCasing, caseSensitive,
          conformance, nullCollation, dataTypeSystem, jethroInfo);
    }
  }
}

// End SqlDialect.java
