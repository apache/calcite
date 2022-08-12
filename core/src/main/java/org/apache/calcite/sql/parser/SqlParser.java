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
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.config.Lex;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.util.SourceStringReader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
@Value.Enclosing
@SuppressWarnings("deprecation")
public class SqlParser {
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

  /** Default value of {@link Config#timeUnitCodes()}.
   * The map is empty, which means that there are no abbreviations other than
   * the time unit names ("YEAR", "SECOND", etc.) */
  public static final ImmutableMap<String, TimeUnit> DEFAULT_IDENTIFIER_TIMEUNIT_MAP =
      ImmutableMap.of();

  @Deprecated // to be removed before 2.0
  public static final boolean DEFAULT_ALLOW_BANG_EQUAL =
      SqlConformanceEnum.DEFAULT.isBangEqualAllowed();

  //~ Instance fields --------------------------------------------------------
  private final SqlAbstractParserImpl parser;

  //~ Constructors -----------------------------------------------------------
  private SqlParser(SqlAbstractParserImpl parser,
      Config config) {
    this.parser = parser;
    parser.setTabSize(1);
    parser.setQuotedCasing(config.quotedCasing());
    parser.setUnquotedCasing(config.unquotedCasing());
    parser.setIdentifierMaxLength(config.identifierMaxLength());
    parser.setTimeUnitCodes(config.timeUnitCodes());
    parser.setConformance(config.conformance());
    parser.switchTo(SqlAbstractParserImpl.LexicalState.forConfig(config));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a <code>SqlParser</code> to parse the given string using
   * Calcite's parser implementation.
   *
   * <p>The default lexical policy is similar to Oracle.
   *
   * @see Lex#ORACLE
   *
   * @param s An SQL statement or expression to parse.
   * @return A parser
   */
  public static SqlParser create(String s) {
    return create(s, config());
  }

  /**
   * Creates a <code>SqlParser</code> to parse the given string using the
   * parser implementation created from given {@link SqlParserImplFactory}
   * with given quoting syntax and casing policies for identifiers.
   *
   * @param sql A SQL statement or expression to parse
   * @param config The parser configuration (identifier max length, etc.)
   * @return A parser
   */
  public static SqlParser create(String sql, Config config) {
    return create(new SourceStringReader(sql), config);
  }

  /**
   * Creates a <code>SqlParser</code> to parse the given string using the
   * parser implementation created from given {@link SqlParserImplFactory}
   * with given quoting syntax and casing policies for identifiers.
   *
   * <p>Unlike
   * {@link #create(java.lang.String, org.apache.calcite.sql.parser.SqlParser.Config)},
   * the parser is not able to return the original query string, but will
   * instead return "?".
   *
   * @param reader The source for the SQL statement or expression to parse
   * @param config The parser configuration (identifier max length, etc.)
   * @return A parser
   */
  public static SqlParser create(Reader reader, Config config) {
    SqlAbstractParserImpl parser =
        config.parserFactory().getParser(reader);

    return new SqlParser(parser, config);
  }

  /**
   * Parses a SQL expression.
   *
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseExpression() throws SqlParseException {
    try {
      return parser.parseSqlExpressionEof();
    } catch (Throwable ex) {
      if (ex instanceof CalciteContextException) {
        final String originalSql = parser.getOriginalSql();
        if (originalSql != null) {
          ((CalciteContextException) ex).setOriginalStatement(originalSql);
        }
      }
      throw parser.normalizeException(ex);
    }
  }

  /** Normalizes a SQL exception. */
  private SqlParseException handleException(Throwable ex) {
    if (ex instanceof CalciteContextException) {
      final String originalSql = parser.getOriginalSql();
      if (originalSql != null) {
        ((CalciteContextException) ex).setOriginalStatement(originalSql);
      }
    }
    return parser.normalizeException(ex);
  }

  /**
   * Parses a <code>SELECT</code> statement.
   *
   * @return A {@link org.apache.calcite.sql.SqlSelect} for a regular <code>
   * SELECT</code> statement; a {@link org.apache.calcite.sql.SqlBinaryOperator}
   * for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseQuery() throws SqlParseException {
    try {
      return parser.parseSqlStmtEof();
    } catch (Throwable ex) {
      throw handleException(ex);
    }
  }

  /**
   * Parses a <code>SELECT</code> statement and reuses parser.
   *
   * @param sql sql to parse
   * @return A {@link org.apache.calcite.sql.SqlSelect} for a regular <code>
   * SELECT</code> statement; a {@link org.apache.calcite.sql.SqlBinaryOperator}
   * for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseQuery(String sql) throws SqlParseException {
    parser.ReInit(new StringReader(sql));
    return parseQuery();
  }

  /**
   * Parses an SQL statement.
   *
   * @return top-level SqlNode representing stmt
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseStmt() throws SqlParseException {
    return parseQuery();
  }

  /**
   * Parses a list of SQL statements separated by semicolon.
   * The semicolon is required between statements, but is
   * optional at the end.
   *
   * @return list of SqlNodeList representing the list of SQL statements
   * @throws SqlParseException if there is a parse error
   */
  public SqlNodeList parseStmtList() throws SqlParseException {
    try {
      return parser.parseSqlStmtList();
    } catch (Throwable ex) {
      throw handleException(ex);
    }
  }

  /**
   * Get the parser metadata.
   *
   * @return {@link SqlAbstractParserImpl.Metadata} implementation of
   *     underlying parser.
   */
  public SqlAbstractParserImpl.Metadata getMetadata() {
    return parser.getMetadata();
  }

  /**
   * Returns the warnings that were generated by the previous invocation
   * of the parser.
   */
  public List<CalciteContextException> getWarnings() {
    return parser.warnings;
  }

  /** Returns a default {@link Config}. */
  public static Config config() {
    return Config.DEFAULT;
  }

  /**
   * Builder for a {@link Config}.
   *
   * @deprecated Use {@link #config()}
   */
  @Deprecated // to be removed before 2.0
  public static ConfigBuilder configBuilder() {
    return new ConfigBuilder();
  }

  /**
   * Builder for a {@link Config} that starts with an existing {@code Config}.
   *
   * @deprecated Use {@code config}, and modify it using its mutator methods
   */
  @Deprecated // to be removed before 2.0
  public static ConfigBuilder configBuilder(Config config) {
    return new ConfigBuilder().setConfig(config);
  }

  /**
   * Interface to define the configuration for a SQL parser.
   */
  @Value.Immutable
  @SuppressWarnings("deprecation")
  public interface Config {
    /** Default configuration. */
    Config DEFAULT = ImmutableSqlParser.Config.of();

    @Value.Default default int identifierMaxLength() {
      return DEFAULT_IDENTIFIER_MAX_LENGTH;
    }

    /** Sets {@link #identifierMaxLength()}. */
    Config withIdentifierMaxLength(int identifierMaxLength);

    @Value.Default default Casing quotedCasing() {
      return Casing.UNCHANGED;
    }

    /** Sets {@link #quotedCasing()}. */
    Config withQuotedCasing(Casing casing);

    @Value.Default default Casing unquotedCasing() {
      return Casing.TO_UPPER;
    }

    /** Sets {@link #unquotedCasing()}. */
    Config withUnquotedCasing(Casing casing);

    @Value.Default default Quoting quoting() {
      return Quoting.DOUBLE_QUOTE;
    }

    /** Sets {@link #quoting()}. */
    Config withQuoting(Quoting quoting);

    @Value.Default default boolean caseSensitive() {
      return true;
    }

    /** Sets {@link #caseSensitive()}. */
    Config withCaseSensitive(boolean caseSensitive);

    @Value.Default default SqlConformance conformance() {
      return SqlConformanceEnum.DEFAULT;
    }

    /** Sets {@link #conformance()}. */
    Config withConformance(SqlConformance conformance);

    @Deprecated // to be removed before 2.0
    @SuppressWarnings("deprecation")
    @Value.Default default boolean allowBangEqual() {
      return DEFAULT_ALLOW_BANG_EQUAL;
    }

    /** Returns which character literal styles are supported. */
    @Value.Default default Set<CharLiteralStyle> charLiteralStyles() {
      return ImmutableSet.of(CharLiteralStyle.STANDARD);
    }

    /** Sets {@link #charLiteralStyles()}. */
    Config withCharLiteralStyles(Iterable<CharLiteralStyle> charLiteralStyles);

    /** Returns a mapping from abbreviations to time units.
     *
     * <p>For example, if the map contains the entry
     * ("Y", {@link TimeUnit#YEAR}) then you can write
     * "{@code EXTRACT(S FROM orderDate)}". */
    @Value.Default default Map<String, TimeUnit> timeUnitCodes() {
      return DEFAULT_IDENTIFIER_TIMEUNIT_MAP;
    }

    /** Sets {@link #timeUnitCodes()}. */
    Config withTimeUnitCodes(Map<String, ? extends TimeUnit> timeUnitCodes);

    @Value.Default default SqlParserImplFactory parserFactory() {
      return SqlParserImpl.FACTORY;
    }

    /** Sets {@link #parserFactory()}. */
    Config withParserFactory(SqlParserImplFactory factory);

    default Config withLex(Lex lex) {
      return withCaseSensitive(lex.caseSensitive)
          .withUnquotedCasing(lex.unquotedCasing)
          .withQuotedCasing(lex.quotedCasing)
          .withQuoting(lex.quoting)
          .withCharLiteralStyles(lex.charLiteralStyles);
    }
  }

  /** Builder for a {@link Config}. */
  @Deprecated // to be removed before 2.0
  public static class ConfigBuilder {
    private Config config = Config.DEFAULT;

    private ConfigBuilder() {}

    /** Sets configuration to a given {@link Config}. */
    public ConfigBuilder setConfig(Config config) {
      this.config = config;
      return this;
    }

    public ConfigBuilder setQuotedCasing(Casing quotedCasing) {
      return setConfig(config.withQuotedCasing(quotedCasing));
    }

    public ConfigBuilder setUnquotedCasing(Casing unquotedCasing) {
      return setConfig(config.withUnquotedCasing(unquotedCasing));
    }

    public ConfigBuilder setQuoting(Quoting quoting) {
      return setConfig(config.withQuoting(quoting));
    }

    public ConfigBuilder setCaseSensitive(boolean caseSensitive) {
      return setConfig(config.withCaseSensitive(caseSensitive));
    }

    public ConfigBuilder setIdentifierMaxLength(int identifierMaxLength) {
      return setConfig(config.withIdentifierMaxLength(identifierMaxLength));
    }

    public ConfigBuilder setIdentifierTimeUnitMap(
        ImmutableMap<String, TimeUnit> identifierTimeUnitMap) {
      return setConfig(config.withTimeUnitCodes(identifierTimeUnitMap));
    }

    @SuppressWarnings("unused")
    @Deprecated // to be removed before 2.0
    public ConfigBuilder setAllowBangEqual(final boolean allowBangEqual) {
      if (allowBangEqual != config.conformance().isBangEqualAllowed()) {
        return setConformance(
            new SqlDelegatingConformance(config.conformance()) {
              @Override public boolean isBangEqualAllowed() {
                return allowBangEqual;
              }
            });
      }
      return this;
    }

    public ConfigBuilder setConformance(SqlConformance conformance) {
      return setConfig(config.withConformance(conformance));
    }

    public ConfigBuilder setCharLiteralStyles(
        Set<CharLiteralStyle> charLiteralStyles) {
      return setConfig(config.withCharLiteralStyles(charLiteralStyles));
    }

    public ConfigBuilder setParserFactory(SqlParserImplFactory factory) {
      return setConfig(config.withParserFactory(factory));
    }

    public ConfigBuilder setLex(Lex lex) {
      return setConfig(config.withLex(lex));
    }

    /** Builds a {@link Config}. */
    public Config build() {
      return config;
    }
  }
}
