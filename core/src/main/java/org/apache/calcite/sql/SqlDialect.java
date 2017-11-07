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

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of SQL.
 *
 * <p>It is used by classes such as {@link SqlWriter} and
 * {@link org.apache.calcite.sql.util.SqlBuilder}.
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

  //~ Instance fields --------------------------------------------------------

  private final String identifierQuoteString;
  private final String identifierEndQuoteString;
  private final String identifierEscapedQuote;
  private final DatabaseProduct databaseProduct;
  protected final NullCollation nullCollation;

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
    this.nullCollation = Preconditions.checkNotNull(context.nullCollation());
    this.databaseProduct =
        Preconditions.checkNotNull(context.databaseProduct());
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
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates an empty context. Use {@link #EMPTY_CONTEXT} if possible. */
  protected static Context emptyContext() {
    return new ContextImpl(DatabaseProduct.UNKNOWN, null, null, -1, -1, null,
        NullCollation.HIGH);
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
      return DatabaseProduct.DERBY;
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
    if (identifierQuoteString == null) {
      buf.append(val); // quoting is not supported
      return buf;
    }
    String val2 =
        val.replaceAll(
            identifierEndQuoteString,
            identifierEscapedQuote);
    buf.append(identifierQuoteString);
    buf.append(val2);
    buf.append(identifierEndQuoteString);
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

  /**
   * Returns whether a given identifier needs to be quoted.
   */
  public boolean identifierNeedsToBeQuoted(String val) {
    return !Pattern.compile("^[A-Z_$0-9]+").matcher(val).matches();
  }

  /**
   * Converts a string into a string literal. For example, <code>can't
   * run</code> becomes <code>'can''t run'</code>.
   */
  public String quoteStringLiteral(String val) {
    if (containsNonAscii(val)) {
      final StringBuilder buf = new StringBuilder();
      quoteStringLiteralUnicode(buf, val);
      return buf.toString();
    } else {
      val = FakeUtil.replace(val, "'", "''");
      return "'" + val + "'";
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

  /**
   * Returns whether the string contains any characters outside the
   * comfortable 7-bit ASCII range (32 through 127).
   *
   * @param s String
   * @return Whether string contains any non-7-bit-ASCII characters
   */
  private static boolean containsNonAscii(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < 32 || c >= 128) {
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
    if ((val != null)
        && (val.charAt(0) == '\'')
        && (val.charAt(val.length() - 1) == '\'')) {
      if (val.length() > 2) {
        val = FakeUtil.replace(val, "''", "'");
        return val.substring(1, val.length() - 1);
      } else {
        // zero length string
        return "";
      }
    }
    return val;
  }

  protected boolean allowsAs() {
    return true;
  }

  // -- behaviors --
  protected boolean requiresAliasForFromItems() {
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

  public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.NULL;
  }

  public SqlNode getCastSpec(RelDataType type) {
    if (type instanceof BasicSqlType) {
      return new SqlDataTypeSpec(
          new SqlIdentifier(type.getSqlTypeName().name(), SqlParserPos.ZERO),
              type.getPrecision(),
              type.getScale(),
              type.getCharset() != null
                  && supportsCharSet()
                  ? type.getCharset().name()
                  : null,
              null,
              SqlParserPos.ZERO);
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
   */
  public boolean supportsOffsetFetch() {
    return true;
  }

  /**
   * Returns whether the dialect supports nested aggregations, for instance
   * {@code SELECT SUM(SUM(1)) }.
   */
  public boolean supportsNestedAggregations() {
    return true;
  }

  /** Returns how NULL values are sorted if an ORDER BY item does not contain
   * NULLS ASCENDING or NULLS DESCENDING. */
  public NullCollation getNullCollation() {
    return nullCollation;
  }

  /** Returns whether NULL values are sorted first or last, in this dialect,
   * in an ORDER BY item of a given direction. */
  public RelFieldCollation.NullDirection defaultNullDirection(
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
   * A few utility functions copied from org.apache.calcite.util.Util. We have
   * copied them because we wish to keep SqlDialect's dependencies to a
   * minimum.
   */
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

    /** Paraccel, now called Actian Matrix. Redshift is based on this, so
     * presumably the dialect capabilities are similar. */
    PARACCEL("Paraccel", "\"", NullCollation.HIGH),
    REDSHIFT("Redshift", "\"", NullCollation.HIGH),

    /**
     * Placeholder for the unknown database.
     *
     * <p>Its dialect is useful for generating generic SQL. If you need to
     * do something database-specific like quoting identifiers, don't rely
     * on this dialect to do what you want.
     */
    UNKNOWN("Unknown", "`", NullCollation.HIGH);

    private final Supplier<SqlDialect> dialect =
        Suppliers.memoize(new Supplier<SqlDialect>() {
          public SqlDialect get() {
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
          }
        });

    private String databaseProductName;
    private String quoteString;
    private final NullCollation nullCollation;

    DatabaseProduct(String databaseProductName, String quoteString,
        NullCollation nullCollation) {
      this.databaseProductName =
          Preconditions.checkNotNull(databaseProductName);
      this.quoteString = quoteString;
      this.nullCollation = Preconditions.checkNotNull(nullCollation);
    }

    /**
     * Returns a dummy dialect for this database.
     *
     * <p>Since databases have many versions and flavors, this dummy dialect
     * is at best an approximation. If you want exact information, better to
     * use a dialect created from an actual connection's metadata
     * (see {@link SqlDialect#create(java.sql.DatabaseMetaData)}).
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
    String identifierQuoteString();
    Context withIdentifierQuoteString(String identifierQuoteString);
    @Nonnull NullCollation nullCollation();
    Context withNullCollation(@Nonnull NullCollation nullCollation);
  }

  /** Implementation of Context. */
  private static class ContextImpl implements Context {
    private final DatabaseProduct databaseProduct;
    private final String databaseProductName;
    private final String databaseVersion;
    private final int databaseMajorVersion;
    private final int databaseMinorVersion;
    private final String identifierQuoteString;
    private final NullCollation nullCollation;

    private ContextImpl(DatabaseProduct databaseProduct,
        String databaseProductName, String databaseVersion,
        int databaseMajorVersion, int databaseMinorVersion,
        String identifierQuoteString, NullCollation nullCollation) {
      this.databaseProduct = Preconditions.checkNotNull(databaseProduct);
      this.databaseProductName = databaseProductName;
      this.databaseVersion = databaseVersion;
      this.databaseMajorVersion = databaseMajorVersion;
      this.databaseMinorVersion = databaseMinorVersion;
      this.identifierQuoteString = identifierQuoteString;
      this.nullCollation = Preconditions.checkNotNull(nullCollation);
    }

    @Nonnull public DatabaseProduct databaseProduct() {
      return databaseProduct;
    }

    public Context withDatabaseProduct(
        @Nonnull DatabaseProduct databaseProduct) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    public String databaseProductName() {
      return databaseProductName;
    }

    public Context withDatabaseProductName(String databaseProductName) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    public String databaseVersion() {
      return databaseVersion;
    }

    public Context withDatabaseVersion(String databaseVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    public int databaseMajorVersion() {
      return databaseMajorVersion;
    }

    public Context withDatabaseMajorVersion(int databaseMajorVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    public int databaseMinorVersion() {
      return databaseMinorVersion;
    }

    public Context withDatabaseMinorVersion(int databaseMinorVersion) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    public String identifierQuoteString() {
      return identifierQuoteString;
    }

    public Context withIdentifierQuoteString(String identifierQuoteString) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }

    @Nonnull public NullCollation nullCollation() {
      return nullCollation;
    }

    public Context withNullCollation(@Nonnull NullCollation nullCollation) {
      return new ContextImpl(databaseProduct, databaseProductName,
          databaseVersion, databaseMajorVersion, databaseMinorVersion,
          identifierQuoteString, nullCollation);
    }
  }
}

// End SqlDialect.java
