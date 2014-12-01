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

import org.apache.calcite.avatica.Casing;
import org.apache.calcite.avatica.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import com.google.common.base.Preconditions;

import java.io.StringReader;

/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
public class SqlParser {
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

  //~ Instance fields --------------------------------------------------------
  private final SqlAbstractParserImpl parser;
  private final String originalInput;

  //~ Constructors -----------------------------------------------------------
  private SqlParser(String s, SqlAbstractParserImpl parser,
      Config config) {
    this.originalInput = s;
    this.parser = parser;
    parser.setTabSize(1);
    parser.setQuotedCasing(config.quotedCasing());
    parser.setUnquotedCasing(config.unquotedCasing());
    parser.setIdentifierMaxLength(config.identifierMaxLength());
    switch (config.quoting()) {
    case DOUBLE_QUOTE:
      parser.switchTo("DQID");
      break;
    case BACK_TICK:
      parser.switchTo("BTID");
      break;
    case BRACKET:
      parser.switchTo("DEFAULT");
      break;
    }
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
    return create(s, configBuilder().build());
  }

  /**
   * Creates a <code>SqlParser</code> to parse the given string using the
   * parser implementation created from given {@link SqlParserImplFactory}
   * with given quoting syntax and casing policies for identifiers.
   *
   * @param sql A SQL statement or expression to parse.
   * @param config The parser configuration (identifier max length, etc.)
   * @return A parser
   */
  public static SqlParser create(String sql, Config config) {
    SqlAbstractParserImpl parser =
        config.parserFactory().getParser(new StringReader(sql));

    return new SqlParser(sql, parser, config);
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
      if ((ex instanceof CalciteContextException)
          && (originalInput != null)) {
        ((CalciteContextException) ex).setOriginalStatement(
            originalInput);
      }
      throw parser.normalizeException(ex);
    }
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
      if ((ex instanceof CalciteContextException)
          && (originalInput != null)) {
        ((CalciteContextException) ex).setOriginalStatement(
            originalInput);
      }
      throw parser.normalizeException(ex);
    }
  }

  /**
   * Parses an SQL statement.
   *
   * @return top-level SqlNode representing stmt
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseStmt() throws SqlParseException {
    try {
      return parser.parseSqlStmtEof();
    } catch (Throwable ex) {
      if ((ex instanceof CalciteContextException)
          && (originalInput != null)) {
        ((CalciteContextException) ex).setOriginalStatement(
            originalInput);
      }
      throw parser.normalizeException(ex);
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
   * Builder for a {@link Config}.
   */
  public static ConfigBuilder configBuilder() {
    return new ConfigBuilder();
  }

  /**
   * Builder for a {@link Config} that starts with an existing {@code Config}.
   */
  public static ConfigBuilder configBuilder(Config config) {
    return new ConfigBuilder().setConfig(config);
  }

  /**
   * Interface to define the configuration for a SQL parser.
   *
   * @see ConfigBuilder
   */
  public interface Config {
    /** Default configuration. */
    Config DEFAULT = configBuilder().build();

    int identifierMaxLength();
    Casing quotedCasing();
    Casing unquotedCasing();
    Quoting quoting();
    boolean caseSensitive();
    SqlParserImplFactory parserFactory();
  }

  /** Builder for a {@link Config}. */
  public static class ConfigBuilder {
    private Casing quotedCasing = Lex.ORACLE.quotedCasing;
    private Casing unquotedCasing = Lex.ORACLE.unquotedCasing;
    private Quoting quoting = Lex.ORACLE.quoting;
    private int identifierMaxLength = DEFAULT_IDENTIFIER_MAX_LENGTH;
    private boolean caseSensitive = Lex.ORACLE.caseSensitive;
    private SqlParserImplFactory parserFactory = SqlParserImpl.FACTORY;

    private ConfigBuilder() {}

    /** Sets configuration identical to a given {@link Config}. */
    public ConfigBuilder setConfig(Config config) {
      this.quotedCasing = config.quotedCasing();
      this.unquotedCasing = config.unquotedCasing();
      this.quoting = config.quoting();
      this.identifierMaxLength = config.identifierMaxLength();
      this.parserFactory = config.parserFactory();
      return this;
    }

    public ConfigBuilder setQuotedCasing(Casing quotedCasing) {
      this.quotedCasing = Preconditions.checkNotNull(quotedCasing);
      return this;
    }

    public ConfigBuilder setUnquotedCasing(Casing unquotedCasing) {
      this.unquotedCasing = Preconditions.checkNotNull(unquotedCasing);
      return this;
    }

    public ConfigBuilder setQuoting(Quoting quoting) {
      this.quoting = Preconditions.checkNotNull(quoting);
      return this;
    }

    public ConfigBuilder setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      return this;
    }

    public ConfigBuilder setIdentifierMaxLength(int identifierMaxLength) {
      this.identifierMaxLength = identifierMaxLength;
      return this;
    }

    public ConfigBuilder setParserFactory(SqlParserImplFactory factory) {
      this.parserFactory = Preconditions.checkNotNull(factory);
      return this;
    }

    public ConfigBuilder setLex(Lex lex) {
      setCaseSensitive(lex.caseSensitive);
      setUnquotedCasing(lex.unquotedCasing);
      setQuotedCasing(lex.quotedCasing);
      setQuoting(lex.quoting);
      return this;
    }

    /** Builds a
     * {@link Config}. */
    public Config build() {
      return new ConfigImpl(identifierMaxLength, quotedCasing,
          unquotedCasing, quoting, caseSensitive, parserFactory);
    }
  }

  /** Implementation of
   * {@link Config}.
   * Called by builder; all values are in private final fields. */
  private static class ConfigImpl implements Config {
    private final int identifierMaxLength;
    private final boolean caseSensitive;
    private final Casing quotedCasing;
    private final Casing unquotedCasing;
    private final Quoting quoting;
    private final SqlParserImplFactory parserFactory;

    private ConfigImpl(int identifierMaxLength, Casing quotedCasing,
        Casing unquotedCasing, Quoting quoting, boolean caseSensitive,
        SqlParserImplFactory parserFactory) {
      this.identifierMaxLength = identifierMaxLength;
      this.caseSensitive = caseSensitive;
      this.quotedCasing = Preconditions.checkNotNull(quotedCasing);
      this.unquotedCasing = Preconditions.checkNotNull(unquotedCasing);
      this.quoting = Preconditions.checkNotNull(quoting);
      this.parserFactory = Preconditions.checkNotNull(parserFactory);
    }

    public int identifierMaxLength() {
      return identifierMaxLength;
    }

    public Casing quotedCasing() {
      return quotedCasing;
    }

    public Casing unquotedCasing() {
      return unquotedCasing;
    }

    public Quoting quoting() {
      return quoting;
    }

    public boolean caseSensitive() {
      return caseSensitive;
    }

    public SqlParserImplFactory parserFactory() {
      return parserFactory;
    }
  }
}

// End SqlParser.java
