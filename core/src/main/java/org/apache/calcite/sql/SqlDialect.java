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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.RelFieldCollation;

import com.google.common.base.Preconditions;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of SQL.
 *
 * <p>It is used by classes such as {@link SqlWriter} and
 * {@link org.apache.calcite.sql.util.SqlBuilder}.
 */
public class SqlDialect {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * A dialect useful for generating generic SQL. If you need to do something
   * database-specific like quoting identifiers, don't rely on this dialect to
   * do what you want.
   */
  public static final SqlDialect DUMMY =
      DatabaseProduct.UNKNOWN.getDialect();

  /**
   * A dialect useful for generating SQL which can be parsed by the
   * Calcite parser, in particular quoting literals and identifiers. If you
   * want a dialect that knows the full capabilities of the database, create
   * one from a connection.
   */
  public static final SqlDialect CALCITE =
      DatabaseProduct.CALCITE.getDialect();

  //~ Instance fields --------------------------------------------------------

  private final String identifierQuoteString;
  private final String identifierEndQuoteString;
  private final String identifierEscapedQuote;
  private final DatabaseProduct databaseProduct;
  private final NullCollation nullCollation;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SqlDialect</code> from a DatabaseMetaData.
   *
   * <p>Does not maintain a reference to the DatabaseMetaData -- or, more
   * importantly, to its {@link java.sql.Connection} -- after this call has
   * returned.
   *
   * @param databaseMetaData used to determine which dialect of SQL to
   *                         generate
   */
  public static SqlDialect create(DatabaseMetaData databaseMetaData) {
    String identifierQuoteString;
    try {
      identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
    } catch (SQLException e) {
      throw FakeUtil.newInternal(e, "while quoting identifier");
    }
    String databaseProductName;
    try {
      databaseProductName = databaseMetaData.getDatabaseProductName();
    } catch (SQLException e) {
      throw FakeUtil.newInternal(e, "while detecting database product");
    }
    String databaseProductVersion;
    try {
      databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
    } catch (SQLException e) {
      throw FakeUtil.newInternal(e, "while detecting database version");
    }
    final DatabaseProduct databaseProduct =
        getProduct(databaseProductName, databaseProductVersion);
    NullCollation nullCollation;
    try {
      if (databaseMetaData.nullsAreSortedAtEnd()) {
        nullCollation = NullCollation.LAST;
      } else if (databaseMetaData.nullsAreSortedAtStart()) {
        nullCollation = NullCollation.FIRST;
      } else if (databaseMetaData.nullsAreSortedLow()) {
        nullCollation = NullCollation.LOW;
      } else if (databaseMetaData.nullsAreSortedHigh()) {
        nullCollation = NullCollation.HIGH;
      } else {
        throw new IllegalArgumentException("cannot deduce null collation");
      }
    } catch (SQLException e) {
      throw new IllegalArgumentException("cannot deduce null collation", e);
    }
    return new SqlDialect(databaseProduct, databaseProductName,
        identifierQuoteString, nullCollation);
  }

  /**
   * Creates a SqlDialect.
   *
   * @param databaseProduct       Database product; may be UNKNOWN, never null
   * @param databaseProductName   Database product name from JDBC driver
   * @param identifierQuoteString String to quote identifiers. Null if quoting
   *                              is not supported. If "[", close quote is
   *                              deemed to be "]".
   */
  @Deprecated // to be removed before 2.0
  public SqlDialect(
      DatabaseProduct databaseProduct,
      String databaseProductName,
      String identifierQuoteString) {
    this(databaseProduct, databaseProductName, identifierQuoteString,
        NullCollation.HIGH);
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
   */
  public SqlDialect(
      DatabaseProduct databaseProduct,
      String databaseProductName,
      String identifierQuoteString, NullCollation nullCollation) {
    Preconditions.checkNotNull(this.nullCollation = nullCollation);
    Preconditions.checkNotNull(databaseProductName);
    this.databaseProduct = Preconditions.checkNotNull(databaseProduct);
    if (identifierQuoteString != null) {
      identifierQuoteString = identifierQuoteString.trim();
      if (identifierQuoteString.equals("")) {
        identifierQuoteString = null;
      }
    }
    this.identifierQuoteString = identifierQuoteString;
    this.identifierEndQuoteString =
        identifierQuoteString == null
            ? null
            : identifierQuoteString.equals("[")
                ? "]"
                : identifierQuoteString;
    this.identifierEscapedQuote =
        identifierQuoteString == null
            ? null
            : this.identifierEndQuoteString + this.identifierEndQuoteString;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a product name and version (per the JDBC driver) into a product
   * enumeration.
   *
   * @param productName    Product name
   * @param productVersion Product version
   * @return database product
   */
  public static DatabaseProduct getProduct(
      String productName,
      String productVersion) {
    final String upperProductName = productName.toUpperCase();
    if (productName.equals("ACCESS")) {
      return DatabaseProduct.ACCESS;
    } else if (upperProductName.trim().equals("APACHE DERBY")) {
      return DatabaseProduct.DERBY;
    } else if (upperProductName.trim().equals("DBMS:CLOUDSCAPE")) {
      return DatabaseProduct.DERBY;
    } else if (productName.startsWith("DB2")) {
      return DatabaseProduct.DB2;
    } else if (upperProductName.contains("FIREBIRD")) {
      return DatabaseProduct.FIREBIRD;
    } else if (productName.equals("Hive")) {
      return DatabaseProduct.HIVE;
    } else if (productName.startsWith("Informix")) {
      return DatabaseProduct.INFORMIX;
    } else if (upperProductName.equals("INGRES")) {
      return DatabaseProduct.INGRES;
    } else if (productName.equals("Interbase")) {
      return DatabaseProduct.INTERBASE;
    } else if (upperProductName.equals("LUCIDDB")) {
      return DatabaseProduct.LUCIDDB;
    } else if (upperProductName.contains("SQL SERVER")) {
      return DatabaseProduct.MSSQL;
    } else if (upperProductName.contains("PARACCEL")) {
      return DatabaseProduct.PARACCEL;
    } else if (productName.equals("Oracle")) {
      return DatabaseProduct.ORACLE;
    } else if (productName.equals("Phoenix")) {
      return DatabaseProduct.PHOENIX;
    } else if (upperProductName.contains("POSTGRE")) {
      return DatabaseProduct.POSTGRESQL;
    } else if (upperProductName.contains("NETEZZA")) {
      return DatabaseProduct.NETEZZA;
    } else if (upperProductName.equals("MYSQL (INFOBRIGHT)")) {
      return DatabaseProduct.INFOBRIGHT;
    } else if (upperProductName.equals("MYSQL")) {
      return DatabaseProduct.MYSQL;
    } else if (productName.startsWith("HP Neoview")) {
      return DatabaseProduct.NEOVIEW;
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
    } else {
      return DatabaseProduct.UNKNOWN;
    }
  }

  // -- detect various databases --

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
    switch (databaseProduct) {
    case ORACLE:
    case HIVE:
      return false;
    default:
      return true;
    }
  }

  // -- behaviors --
  protected boolean requiresAliasForFromItems() {
    return getDatabaseProduct() == DatabaseProduct.POSTGRESQL;
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
   * returns {@code TIMESTAMP '1970-01-01 00:00:00'}, regardless of the JVM's
   * time zone.
   *
   * @param timestamp Timestamp
   * @return SQL timestamp literal
   */
  public String quoteTimestampLiteral(Timestamp timestamp) {
    final SimpleDateFormat format =
        new SimpleDateFormat(
            "'TIMESTAMP' ''yyyy-MM-DD HH:mm:SS''");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));
    return format.format(timestamp);
  }

  /**
   * Returns the database this dialect belongs to,
   * {@link SqlDialect.DatabaseProduct#UNKNOWN} if not known, never null.
   *
   * @return Database product
   */
  public DatabaseProduct getDatabaseProduct() {
    return databaseProduct;
  }

  /**
   * Returns whether the dialect supports character set names as part of a
   * data type, for instance {@code VARCHAR(30) CHARACTER SET `ISO-8859-1`}.
   */
  public boolean supportsCharSet() {
    switch (databaseProduct) {
    case MYSQL:
    case H2:
    case HSQLDB:
    case PHOENIX:
    case POSTGRESQL:
      return false;
    default:
      return true;
    }
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
    CALCITE("Apache Calcite", "\"", NullCollation.HIGH),
    MSSQL("Microsoft SQL Server", "[", NullCollation.HIGH),
    MYSQL("MySQL", "`", NullCollation.HIGH),
    ORACLE("Oracle", "\"", NullCollation.HIGH),
    DERBY("Apache Derby", null, NullCollation.HIGH),
    DB2("IBM DB2", null, NullCollation.HIGH),
    FIREBIRD("Firebird", null, NullCollation.HIGH),
    H2("H2", "\"", NullCollation.HIGH),
    HIVE("Apache Hive", null, NullCollation.HIGH),
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
    PARACCEL("Paraccel", "\"", NullCollation.HIGH),
    /**
     * Placeholder for the unknown database.
     *
     * <p>Its dialect is useful for generating generic SQL. If you need to
     * do something database-specific like quoting identifiers, don't rely
     * on this dialect to do what you want.
     */
    UNKNOWN("Unknown", "`", NullCollation.HIGH);

    private SqlDialect dialect = null;
    private String databaseProductName;
    private String quoteString;
    private final NullCollation nullCollation;

    DatabaseProduct(String databaseProductName, String quoteString,
        NullCollation nullCollation) {
      this.databaseProductName = databaseProductName;
      this.quoteString = quoteString;
      this.nullCollation = nullCollation;
    }

    /**
     * Returns a dummy dialect for this database.
     *
     * <p>Since databases have many versions and flavors, this dummy dialect
     * is at best an approximation. If you want exact information, better to
     * use a dialect created from an actual connection's metadata
     * (see {@link SqlDialect#create(java.sql.DatabaseMetaData)}).
     *
     * @return Dialect representing lowest-common-demoninator behavior for
     * all versions of this database
     */
    public SqlDialect getDialect() {
      if (dialect == null) {
        dialect =
            new SqlDialect(this, databaseProductName, quoteString,
                nullCollation);
      }
      return dialect;
    }
  }
}

// End SqlDialect.java
