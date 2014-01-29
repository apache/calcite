/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.parser;

import java.io.*;
import java.util.Arrays;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.impl.*;
import org.eigenbase.util.*;

import net.hydromatic.avatica.Casing;
import net.hydromatic.avatica.Quoting;

/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
public class SqlParser {
  //~ Instance fields --------------------------------------------------------

  private final SqlParserImpl parser;
  private String originalInput;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SqlParser</code> that reads input from a string.
   */
  public SqlParser(String s) {
    this(s, Quoting.DOUBLE_QUOTE, Casing.TO_UPPER, Casing.UNCHANGED);
  }

  /**
   * Creates a <code>SqlParser</code> that reads input from a string.
   */
  public SqlParser(String s, Quoting quoting, Casing unquotedCasing,
      Casing quotedCasing) {
    parser = new SqlParserImpl(new StringReader(s));
    parser.setTabSize(1);
    parser.quotedCasing = quotedCasing;
    parser.unquotedCasing = unquotedCasing;
    this.originalInput = s;
    switch (quoting) {
    case DOUBLE_QUOTE:
      switchTo("DQID");
      break;
    case BACK_TICK:
      switchTo("BTID");
      break;
    case BRACKET:
      switchTo("DEFAULT");
      break;
    }
  }

  public void switchTo(String stateName) {
    int state = Arrays.asList(SqlParserImplTokenManager.lexStateNames)
        .indexOf(stateName);
    parser.token_source.SwitchTo(state);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Parses a SQL expression.
   *
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseExpression() throws SqlParseException {
    try {
      return parser.SqlExpressionEof();
    } catch (Throwable ex) {
      if ((ex instanceof EigenbaseContextException)
          && (originalInput != null)) {
        ((EigenbaseContextException) ex).setOriginalStatement(
            originalInput);
      }
      throw parser.normalizeException(ex);
    }
  }

  /**
   * Parses a <code>SELECT</code> statement.
   *
   * @return A {@link org.eigenbase.sql.SqlSelect} for a regular <code>
   * SELECT</code> statement; a {@link org.eigenbase.sql.SqlBinaryOperator}
   * for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
   * @throws SqlParseException if there is a parse error
   */
  public SqlNode parseQuery() throws SqlParseException {
    try {
      return parser.SqlStmtEof();
    } catch (Throwable ex) {
      if ((ex instanceof EigenbaseContextException)
          && (originalInput != null)) {
        ((EigenbaseContextException) ex).setOriginalStatement(
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
      return parser.SqlStmtEof();
    } catch (Throwable ex) {
      if ((ex instanceof EigenbaseContextException)
          && (originalInput != null)) {
        ((EigenbaseContextException) ex).setOriginalStatement(
            originalInput);
      }
      throw parser.normalizeException(ex);
    }
  }

  /**
   * Returns the underlying generated parser.
   */
  public SqlParserImpl getParserImpl() {
    return parser;
  }
}

// End SqlParser.java
