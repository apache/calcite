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

import java.io.StringReader;

/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
public class SqlParser {
  //~ Instance fields --------------------------------------------------------
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;
  public static final ParserConfig ORACLE_PARSER_CONFIG =
      new ParserConfigImpl(Lex.ORACLE, DEFAULT_IDENTIFIER_MAX_LENGTH);
  public static final ParserConfig MYSQL_PARSER_CONFIG =
      new ParserConfigImpl(Lex.MYSQL, DEFAULT_IDENTIFIER_MAX_LENGTH);
  public static final ParserConfig SQLSERVER_PARSER_CONFIG =
      new ParserConfigImpl(Lex.SQL_SERVER, DEFAULT_IDENTIFIER_MAX_LENGTH);

  private final SqlAbstractParserImpl parser;
  private String originalInput;

  //~ Constructors -----------------------------------------------------------
  private SqlParser(String s, SqlAbstractParserImpl parser,
      ParserConfig config) {
    this(s, parser, config.getLex().quoting, config.getLex().unquotedCasing,
        config.getLex().quotedCasing, config.getIdentifierMaxLength());
  }

  //~ Constructors -----------------------------------------------------------
  private SqlParser(String s, SqlAbstractParserImpl parser,
                    Quoting quoting, Casing unquotedCasing,
                    Casing quotedCasing) {
    this(s, parser, quoting, unquotedCasing, quotedCasing,
        DEFAULT_IDENTIFIER_MAX_LENGTH);
  }
    //~ Constructors -----------------------------------------------------------
  private SqlParser(String s, SqlAbstractParserImpl parser,
                    Quoting quoting, Casing unquotedCasing,
                    Casing quotedCasing,
                    int identifierMaxLength) {
    this.originalInput = s;
    this.parser = parser;
    parser.setTabSize(1);
    parser.setQuotedCasing(quotedCasing);
    parser.setUnquotedCasing(unquotedCasing);
    parser.setIdentifierMaxLength(identifierMaxLength);
    switch (quoting) {
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
   * @param s An SQL statement or expression to parse.
   * @return A <code>SqlParser</code> object.
   */
  public static SqlParser create(String s) {
    return create(SqlParserImpl.FACTORY, s, ORACLE_PARSER_CONFIG);
  }

  /**
   * Creates a <code>SqlParser</code> to parse the given string using the
   * parser implementation created from given {@link SqlParserImplFactory}
   * with given quoting syntax and casing policies for identifiers.
   *
   * @param parserFactory {@link SqlParserImplFactory} to get the parser
   *     implementation.
   * @param s An SQL statement or expression to parse.
   * @param quoting Syntax for quoting identifiers in SQL statements.
   * @param unquotedCasing Policy for converting case of <i>unquoted</i>
   *     identifiers.
   * @param quotedCasing Policy for converting case of <i>quoted</i>
   *     identifiers.
   * @return
   */
  public static SqlParser create(SqlParserImplFactory parserFactory, String s,
                                 Quoting quoting, Casing unquotedCasing,
                                 Casing quotedCasing) {
    SqlAbstractParserImpl parser =
        parserFactory.getParser(new StringReader(s));

    return new SqlParser(s, parser, quoting, unquotedCasing, quotedCasing);
  }

  /**
   * Creates a <code>SqlParser</code> to parse the given string using the
   * parser implementation created from given {@link SqlParserImplFactory}
   * with given quoting syntax and casing policies for identifiers.
   *
   * @param parserFactory {@link SqlParserImplFactory} to get the parser
   *     implementation.
   * @param s An SQL statement or expression to parse.
   * @param config the parer configuration (lex,
   *                                   identifier max length etc).
   * @return A <code>SqlParser</code> object.
   */
  public static SqlParser create(SqlParserImplFactory parserFactory, String s,
                                 ParserConfig config) {
    SqlAbstractParserImpl parser = parserFactory.getParser(
        new StringReader(s));

    return new SqlParser(s, parser, config);
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
   * Interface to define the configuration for a SQL parser.
   */
  public interface ParserConfig {
    int getIdentifierMaxLength();
    Casing getQuotedCasing();
    Casing getUnquotedCasing();
    Quoting getQuoting();
    Lex getLex();
  }

  /**
   * Implementation of ParserConfig interface.
   */
  public static class ParserConfigImpl implements ParserConfig {
    int identifierMaxLength;
    Lex lex;

    public ParserConfigImpl(Lex lex, int identifierMaxLength) {
      this.identifierMaxLength = identifierMaxLength;
      this.lex = lex;
    }

    public int getIdentifierMaxLength() {
      return this.identifierMaxLength;
    }

    public Casing getQuotedCasing() {
      return this.lex.quotedCasing;
    }

    public Casing getUnquotedCasing() {
      return this.lex.unquotedCasing;
    }

    public Quoting getQuoting() {
      return this.lex.quoting;
    }

    public Lex getLex() {
      return this.lex;
    }
  }

}

// End SqlParser.java
