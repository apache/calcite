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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Glossary;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Abstract base for parsers generated from CommonParser.jj.
 */
public abstract class SqlAbstractParserImpl {
  //~ Static fields/initializers ---------------------------------------------

  private static final ImmutableSet<String> SQL_92_RESERVED_WORD_SET =
      ImmutableSet.of(
          "ABSOLUTE",
          "ACTION",
          "ADD",
          "ALL",
          "ALLOCATE",
          "ALTER",
          "AND",
          "ANY",
          "ARE",
          "AS",
          "ASC",
          "ASSERTION",
          "AT",
          "AUTHORIZATION",
          "AVG",
          "BEGIN",
          "BETWEEN",
          "BIT",
          "BIT_LENGTH",
          "BOTH",
          "BY",
          "CALL",
          "CASCADE",
          "CASCADED",
          "CASE",
          "CAST",
          "CATALOG",
          "CHAR",
          "CHARACTER",
          "CHARACTER_LENGTH",
          "CHAR_LENGTH",
          "CHECK",
          "CLOSE",
          "COALESCE",
          "COLLATE",
          "COLLATION",
          "COLUMN",
          "COMMIT",
          "CONDITION",
          "CONNECT",
          "CONNECTION",
          "CONSTRAINT",
          "CONSTRAINTS",
          "CONTAINS",
          "CONTINUE",
          "CONVERT",
          "CORRESPONDING",
          "COUNT",
          "CREATE",
          "CROSS",
          "CURRENT",
          "CURRENT_DATE",
          "CURRENT_PATH",
          "CURRENT_TIME",
          "CURRENT_TIMESTAMP",
          "CURRENT_USER",
          "CURSOR",
          "DATE",
          "DAY",
          "DEALLOCATE",
          "DEC",
          "DECIMAL",
          "DECLARE",
          "DEFAULT",
          "DEFERRABLE",
          "DEFERRED",
          "DELETE",
          "DESC",
          "DESCRIBE",
          "DESCRIPTOR",
          "DETERMINISTIC",
          "DIAGNOSTICS",
          "DISCONNECT",
          "DISTINCT",
          "DOMAIN",
          "DOUBLE",
          "DROP",
          "ELSE",
          "END",
          "ESCAPE",
          "EXCEPT",
          "EXCEPTION",
          "EXEC",
          "EXECUTE",
          "EXISTS",
          "EXTERNAL",
          "EXTRACT",
          "FALSE",
          "FETCH",
          "FIRST",
          "FLOAT",
          "FOR",
          "FOREIGN",
          "FOUND",
          "FROM",
          "FULL",
          "FUNCTION",
          "GET",
          "GLOBAL",
          "GO",
          "GOTO",
          "GRANT",
          "GROUP",
          "HAVING",
          "HOUR",
          "IDENTITY",
          "IMMEDIATE",
          "IN",
          "INADD",
          "INDICATOR",
          "INITIALLY",
          "INNER",
          "INOUT",
          "INPUT",
          "INSENSITIVE",
          "INSERT",
          "INT",
          "INTEGER",
          "INTERSECT",
          "INTERVAL",
          "INTO",
          "IS",
          "ISOLATION",
          "JOIN",
          "KEY",
          "LANGUAGE",
          "LAST",
          "LEADING",
          "LEFT",
          "LEVEL",
          "LIKE",
          "LOCAL",
          "LOWER",
          "MATCH",
          "MAX",
          "MIN",
          "MINUTE",
          "MODULE",
          "MONTH",
          "NAMES",
          "NATIONAL",
          "NATURAL",
          "NCHAR",
          "NEXT",
          "NO",
          "NOT",
          "NULL",
          "NULLIF",
          "NUMERIC",
          "OCTET_LENGTH",
          "OF",
          "ON",
          "ONLY",
          "OPEN",
          "OPTION",
          "OR",
          "ORDER",
          "OUT",
          "OUTADD",
          "OUTER",
          "OUTPUT",
          "OVERLAPS",
          "PAD",
          "PARAMETER",
          "PARTIAL",
          "PATH",
          "POSITION",
          "PRECISION",
          "PREPARE",
          "PRESERVE",
          "PRIMARY",
          "PRIOR",
          "PRIVILEGES",
          "PROCEDURE",
          "PUBLIC",
          "READ",
          "REAL",
          "REFERENCES",
          "RELATIVE",
          "RESTRICT",
          "RETURN",
          "RETURNS",
          "REVOKE",
          "RIGHT",
          "ROLLBACK",
          "ROUTINE",
          "ROWS",
          "SCHEMA",
          "SCROLL",
          "SECOND",
          "SECTION",
          "SELECT",
          "SESSION",
          "SESSION_USER",
          "SET",
          "SIZE",
          "SMALLINT",
          "SOME",
          "SPACE",
          "SPECIFIC",
          "SQL",
          "SQLCODE",
          "SQLERROR",
          "SQLEXCEPTION",
          "SQLSTATE",
          "SQLWARNING",
          "SUBSTRING",
          "SUM",
          "SYSTEM_USER",
          "TABLE",
          "TEMPORARY",
          "THEN",
          "TIME",
          "TIMESTAMP",
          "TIMEZONE_HOUR",
          "TIMEZONE_MINUTE",
          "TO",
          "TRAILING",
          "TRANSACTION",
          "TRANSLATE",
          "TRANSLATION",
          "TRIM",
          "TRUE",
          "UNION",
          "UNIQUE",
          "UNKNOWN",
          "UPDATE",
          "UPPER",
          "USAGE",
          "USER",
          "USING",
          "VALUE",
          "VALUES",
          "VARCHAR",
          "VARYING",
          "VIEW",
          "WHEN",
          "WHENEVER",
          "WHERE",
          "WITH",
          "WORK",
          "WRITE",
          "YEAR",
          "ZONE");

  //~ Enums ------------------------------------------------------------------

  /**
   * Type-safe enum for context of acceptable expressions.
   */
  protected enum ExprContext {
    /**
     * Accept any kind of expression in this context.
     */
    ACCEPT_ALL,

    /**
     * Accept any kind of expression in this context, with the exception of
     * CURSOR constructors.
     */
    ACCEPT_NONCURSOR,

    /**
     * Accept only query expressions in this context.
     *
     * <p>Valid: "SELECT x FROM a",
     * "SELECT x FROM a UNION SELECT y FROM b",
     * "TABLE a",
     * "VALUES (1, 2), (3, 4)",
     * "(SELECT x FROM a UNION SELECT y FROM b) INTERSECT SELECT z FROM c",
     * "(SELECT x FROM a UNION SELECT y FROM b) ORDER BY 1 LIMIT 10".
     * Invalid: "e CROSS JOIN d".
     * Debatable: "(SELECT x FROM a)".
     */
    ACCEPT_QUERY,

    /**
     * Accept only query expressions or joins in this context.
     *
     * <p>Valid: "(SELECT x FROM a)",
     * "e CROSS JOIN d",
     * "((SELECT x FROM a) CROSS JOIN d)",
     * "((e CROSS JOIN d) LEFT JOIN c)".
     * Invalid: "e, d",
     * "SELECT x FROM a",
     * "(e)".
     */
    ACCEPT_QUERY_OR_JOIN,

    /**
     * Accept only non-query expressions in this context.
     */
    ACCEPT_NON_QUERY,

    /**
     * Accept only parenthesized queries or non-query expressions in this
     * context.
     */
    ACCEPT_SUB_QUERY,

    /**
     * Accept only CURSOR constructors, parenthesized queries, or non-query
     * expressions in this context.
     */
    ACCEPT_CURSOR;

    @Deprecated // to be removed before 2.0
    public static final ExprContext ACCEPT_SUBQUERY = ACCEPT_SUB_QUERY;

    @Deprecated // to be removed before 2.0
    public static final ExprContext ACCEPT_NONQUERY = ACCEPT_NON_QUERY;

    public void throwIfNotCompatible(SqlNode e) {
      switch (this) {
      case ACCEPT_NON_QUERY:
      case ACCEPT_SUB_QUERY:
      case ACCEPT_CURSOR:
        if (e.isA(SqlKind.QUERY)) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.illegalQueryExpression());
        }
        break;
      case ACCEPT_QUERY:
        if (!e.isA(SqlKind.QUERY)) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.illegalNonQueryExpression());
        }
        break;
      case ACCEPT_QUERY_OR_JOIN:
        if (!e.isA(SqlKind.QUERY) && e.getKind() != SqlKind.JOIN) {
          throw SqlUtil.newContextException(e.getParserPosition(),
              RESOURCE.expectedQueryOrJoinExpression());
        }
        break;
      default:
        break;
      }
    }
  }

  //~ Instance fields --------------------------------------------------------

  protected int nDynamicParams;

  protected @Nullable String originalSql;

  protected final List<CalciteContextException> warnings = new ArrayList<>();

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns immutable set of all reserved words defined by SQL-92.
   *
   * @see Glossary#SQL92 SQL-92 Section 5.2
   */
  public static Set<String> getSql92ReservedWords() {
    return SQL_92_RESERVED_WORD_SET;
  }

  /**
   * Creates a call.
   *
   * @param funName           Name of function
   * @param pos               Position in source code
   * @param funcType          Type of function
   * @param functionQualifier Qualifier
   * @param operands          Operands to call
   * @return Call
   */
  @SuppressWarnings("argument.type.incompatible")
  protected SqlCall createCall(
      SqlIdentifier funName,
      SqlParserPos pos,
      SqlFunctionCategory funcType,
      SqlLiteral functionQualifier,
      Iterable<? extends SqlNode> operands) {
    return createCall(funName, pos, funcType, functionQualifier,
        Iterables.toArray(operands, SqlNode.class));
  }

  /**
   * Creates a call.
   *
   * @param funName           Name of function
   * @param pos               Position in source code
   * @param funcType          Type of function
   * @param functionQualifier Qualifier
   * @param operands          Operands to call
   * @return Call
   */
  protected SqlCall createCall(
      SqlIdentifier funName,
      SqlParserPos pos,
      SqlFunctionCategory funcType,
      SqlLiteral functionQualifier,
      SqlNode[] operands) {
    // Create a placeholder function.  Later, during
    // validation, it will be resolved into a real function reference.
    SqlOperator fun = new SqlUnresolvedFunction(funName, null, null, null, null,
        funcType);

    return fun.createCall(functionQualifier, pos, operands);
  }

  /**
   * Returns metadata about this parser: keywords, etc.
   */
  public abstract Metadata getMetadata();

  /**
   * Removes or transforms misleading information from a parse exception or
   * error, and converts to {@link SqlParseException}.
   *
   * @param ex dirty excn
   * @return clean excn
   */
  public abstract SqlParseException normalizeException(@Nullable Throwable ex);

  protected abstract SqlParserPos getPos() throws Exception;

  /**
   * Reinitializes parser with new input.
   *
   * @param reader provides new input
   */
  // CHECKSTYLE: IGNORE 1
  public abstract void ReInit(Reader reader);

  /**
   * Parses a SQL expression ending with EOF and constructs a
   * parse tree.
   *
   * @return constructed parse tree.
   */
  public abstract SqlNode parseSqlExpressionEof() throws Exception;

  /**
   * Parses a SQL statement ending with EOF and constructs a
   * parse tree.
   *
   * @return constructed parse tree.
   */
  public abstract SqlNode parseSqlStmtEof() throws Exception;

  /**
   * Parses a list of SQL statements separated by semicolon and constructs a
   * parse tree. The semicolon is required between statements, but is
   * optional at the end.
   *
   * @return constructed list of SQL statements.
   */
  public abstract SqlNodeList parseSqlStmtList() throws Exception;

  /**
   * Sets the tab stop size.
   *
   * @param tabSize Tab stop size
   */
  public abstract void setTabSize(int tabSize);

  /**
   * Sets the casing policy for quoted identifiers.
   *
   * @param quotedCasing Casing to set.
   */
  public abstract void setQuotedCasing(Casing quotedCasing);

  /**
   * Sets the casing policy for unquoted identifiers.
   *
   * @param unquotedCasing Casing to set.
   */
  public abstract void setUnquotedCasing(Casing unquotedCasing);

  /**
   * Sets the maximum length for sql identifier.
   */
  public abstract void setIdentifierMaxLength(int identifierMaxLength);

  /**
   * Sets the map from identifier to time unit.
   */
  public abstract void setTimeUnitCodes(Map<String, TimeUnit> timeUnitCodes);

  /**
   * Sets the SQL language conformance level.
   */
  public abstract void setConformance(SqlConformance conformance);

  /**
   * Sets the SQL text that is being parsed.
   */
  public void setOriginalSql(String originalSql) {
    this.originalSql = originalSql;
  }

  /**
   * Returns the SQL text.
   */
  public @Nullable String getOriginalSql() {
    return originalSql;
  }

  /**
   * Change parser state.
   *
   * @param state New state
   */
  public abstract void switchTo(LexicalState state);

  //~ Inner Interfaces -------------------------------------------------------

  /** Valid starting states of the parser.
   *
   * <p>(There are other states that the parser enters during parsing, such as
   * being inside a multi-line comment.)
   *
   * <p>The starting states generally control the syntax of quoted
   * identifiers. */
  public enum LexicalState {
    /** Starting state where quoted identifiers use brackets, like Microsoft SQL
     * Server. */
    DEFAULT,

    /** Starting state where quoted identifiers use double-quotes, like
     * Oracle and PostgreSQL. */
    DQID,

    /** Starting state where quoted identifiers use back-ticks, like MySQL. */
    BTID,

    /** Starting state where quoted identifiers use back-ticks,
     * unquoted identifiers that are part of table names may contain hyphens,
     * and character literals may be enclosed in single- or double-quotes,
     * like BigQuery. */
    BQID;

    /** Returns the corresponding parser state with the given configuration
     * (in particular, quoting style). */
    public static LexicalState forConfig(SqlParser.Config config) {
      switch (config.quoting()) {
      case BRACKET:
        return DEFAULT;
      case DOUBLE_QUOTE:
        return DQID;
      case BACK_TICK_BACKSLASH:
        return BQID;
      case BACK_TICK:
        if (config.conformance().allowHyphenInUnquotedTableName()
            && config.charLiteralStyles().equals(
                EnumSet.of(CharLiteralStyle.BQ_SINGLE,
                    CharLiteralStyle.BQ_DOUBLE))) {
          return BQID;
        }
        if (!config.conformance().allowHyphenInUnquotedTableName()
            && config.charLiteralStyles().equals(
                EnumSet.of(CharLiteralStyle.STANDARD))) {
          return BTID;
        }
        // fall through
      default:
        throw new AssertionError(config);
      }
    }
  }

  /**
   * Metadata about the parser. For example:
   *
   * <ul>
   * <li>"KEY" is a keyword: it is meaningful in certain contexts, such as
   * "CREATE FOREIGN KEY", but can be used as an identifier, as in <code>
   * "CREATE TABLE t (key INTEGER)"</code>.
   * <li>"SELECT" is a reserved word. It can not be used as an identifier.
   * <li>"CURRENT_USER" is the name of a context variable. It cannot be used
   * as an identifier.
   * <li>"ABS" is the name of a reserved function. It cannot be used as an
   * identifier.
   * <li>"DOMAIN" is a reserved word as specified by the SQL:92 standard.
   * </ul>
   */
  public interface Metadata {
    /**
     * Returns true if token is a keyword but not a reserved word. For
     * example, "KEY".
     */
    boolean isNonReservedKeyword(String token);

    /**
     * Returns whether token is the name of a context variable such as
     * "CURRENT_USER".
     */
    boolean isContextVariableName(String token);

    /**
     * Returns whether token is a reserved function name such as
     * "CURRENT_USER".
     */
    boolean isReservedFunctionName(String token);

    /**
     * Returns whether token is a keyword. (That is, a non-reserved keyword,
     * a context variable, or a reserved function name.)
     */
    boolean isKeyword(String token);

    /**
     * Returns whether token is a reserved word.
     */
    boolean isReservedWord(String token);

    /**
     * Returns whether token is a reserved word as specified by the SQL:92
     * standard.
     */
    boolean isSql92ReservedWord(String token);

    /**
     * Returns comma-separated list of JDBC keywords.
     */
    String getJdbcKeywords();

    /**
     * Returns a list of all tokens in alphabetical order.
     */
    List<String> getTokens();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Default implementation of the {@link Metadata} interface.
   */
  public static class MetadataImpl implements Metadata {
    private final Set<String> reservedFunctionNames = new HashSet<>();
    private final Set<String> contextVariableNames = new HashSet<>();
    private final Set<String> nonReservedKeyWordSet = new HashSet<>();

    /**
     * Set of all tokens.
     */
    private final NavigableSet<String> tokenSet = new TreeSet<>();

    /**
     * Immutable list of all tokens, in alphabetical order.
     */
    private final List<String> tokenList;
    private final Set<String> reservedWords = new HashSet<>();
    private final String sql92ReservedWords;

    /**
     * Creates a MetadataImpl.
     *
     * @param sqlParser Parser
     */
    public MetadataImpl(SqlAbstractParserImpl sqlParser) {
      initList(sqlParser, reservedFunctionNames, "ReservedFunctionName");
      initList(sqlParser, contextVariableNames, "ContextVariable");
      initList(sqlParser, nonReservedKeyWordSet, "NonReservedKeyWord");
      tokenList = ImmutableList.copyOf(tokenSet);
      sql92ReservedWords = constructSql92ReservedWordList();
      Set<String> reservedWordSet = new TreeSet<>();
      reservedWordSet.addAll(tokenSet);
      reservedWordSet.removeAll(nonReservedKeyWordSet);
      reservedWords.addAll(reservedWordSet);
    }

    /**
     * Initializes lists of keywords.
     */
    private void initList(
        @UnderInitialization MetadataImpl this,
        SqlAbstractParserImpl parserImpl,
        Set<String> keywords,
        String name) {
      parserImpl.ReInit(new StringReader("1"));
      try {
        Object o = virtualCall(parserImpl, name);
        throw new AssertionError("expected call to fail, got " + o);
      } catch (SqlParseException parseException) {
        // First time through, build the list of all tokens.
        final String[] tokenImages = parseException.getTokenImages();
        if (tokenSet.isEmpty()) {
          for (String token : tokenImages) {
            String tokenVal = SqlParserUtil.getTokenVal(token);
            if (tokenVal != null) {
              tokenSet.add(tokenVal);
            }
          }
        }

        // Add the tokens which would have been expected in this
        // syntactic context to the list we're building.
        final int[][] expectedTokenSequences =
            parseException.getExpectedTokenSequences();
        for (final int[] tokens : expectedTokenSequences) {
          assert tokens.length == 1;
          final int tokenId = tokens[0];
          String token = tokenImages[tokenId];
          String tokenVal = SqlParserUtil.getTokenVal(token);
          if (tokenVal != null) {
            keywords.add(tokenVal);
          }
        }
      } catch (Throwable e) {
        throw new RuntimeException("While building token lists", e);
      }
    }

    /**
     * Uses reflection to invoke a method on this parser. The method must be
     * public and have no parameters.
     *
     * @param parserImpl Parser
     * @param name       Name of method. For example "ReservedFunctionName".
     * @return Result of calling method
     */
    private @Nullable Object virtualCall(
        @UnderInitialization MetadataImpl this,
        SqlAbstractParserImpl parserImpl,
        String name) throws Throwable {
      Class<?> clazz = parserImpl.getClass();
      try {
        final Method method = clazz.getMethod(name);
        return method.invoke(parserImpl);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        throw parserImpl.normalizeException(cause);
      }
    }

    /**
     * Builds a comma-separated list of JDBC reserved words.
     */
    private String constructSql92ReservedWordList(
        @UnderInitialization MetadataImpl this) {
      StringBuilder sb = new StringBuilder();
      TreeSet<String> jdbcReservedSet = new TreeSet<>();
      jdbcReservedSet.addAll(tokenSet);
      jdbcReservedSet.removeAll(SQL_92_RESERVED_WORD_SET);
      jdbcReservedSet.removeAll(nonReservedKeyWordSet);
      int j = 0;
      for (String jdbcReserved : jdbcReservedSet) {
        if (j++ > 0) {
          sb.append(",");
        }
        sb.append(jdbcReserved);
      }
      return sb.toString();
    }

    @Override
    public List<String> getTokens() {
      return tokenList;
    }

    @Override
    public boolean isSql92ReservedWord(String token) {
      return SQL_92_RESERVED_WORD_SET.contains(token);
    }

    @Override
    public String getJdbcKeywords() {
      return sql92ReservedWords;
    }

    @Override
    public boolean isKeyword(String token) {
      return isNonReservedKeyword(token)
          || isReservedFunctionName(token)
          || isContextVariableName(token)
          || isReservedWord(token);
    }

    @Override
    public boolean isNonReservedKeyword(String token) {
      return nonReservedKeyWordSet.contains(token);
    }

    @Override
    public boolean isReservedFunctionName(String token) {
      return reservedFunctionNames.contains(token);
    }

    @Override
    public boolean isContextVariableName(String token) {
      return contextVariableNames.contains(token);
    }

    @Override
    public boolean isReservedWord(String token) {
      return reservedWords.contains(token);
    }
  }
}
