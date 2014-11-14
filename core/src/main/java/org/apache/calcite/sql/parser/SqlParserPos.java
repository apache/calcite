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
package org.eigenbase.sql.parser;

import java.io.*;
import java.util.*;

import org.eigenbase.sql.*;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * SqlParserPos represents the position of a parsed token within SQL statement
 * text.
 */
public class SqlParserPos implements Serializable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * SqlParserPos representing line one, character one. Use this if the node
   * doesn't correspond to a position in piece of SQL text.
   */
  public static final SqlParserPos ZERO = new SqlParserPos(0, 0);

  private static final long serialVersionUID = 1L;

  //~ Instance fields --------------------------------------------------------

  private final int lineNumber;
  private final int columnNumber;
  private final int endLineNumber;
  private final int endColumnNumber;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new parser position.
   */
  public SqlParserPos(
      int lineNumber,
      int columnNumber) {
    this.lineNumber = lineNumber;
    this.columnNumber = columnNumber;
    this.endLineNumber = lineNumber;
    this.endColumnNumber = columnNumber;
  }

  /**
   * Creates a new parser range.
   */
  public SqlParserPos(
      int startLineNumber,
      int startColumnNumber,
      int endLineNumber,
      int endColumnNumber) {
    this.lineNumber = startLineNumber;
    this.columnNumber = startColumnNumber;
    this.endLineNumber = endLineNumber;
    this.endColumnNumber = endColumnNumber;
  }

  //~ Methods ----------------------------------------------------------------

  public int hashCode() {
    return lineNumber
        ^ (columnNumber << 2)
        ^ (endLineNumber << 5)
        ^ (endColumnNumber << 7);
  }

  public boolean equals(Object obj) {
    if (obj instanceof SqlParserPos) {
      final SqlParserPos that = (SqlParserPos) obj;
      return (that.lineNumber == this.lineNumber)
          && (that.columnNumber == this.columnNumber)
          && (that.endLineNumber == this.endLineNumber)
          && (that.endColumnNumber == this.endColumnNumber);
    } else {
      return false;
    }
  }

  /**
   * @return 1-based starting line number
   */
  public int getLineNum() {
    return lineNumber;
  }

  /**
   * @return 1-based starting column number
   */
  public int getColumnNum() {
    return columnNumber;
  }

  /**
   * @return 1-based end line number (same as starting line number if the
   * ParserPos is a point, not a range)
   */
  public int getEndLineNum() {
    return endLineNumber;
  }

  /**
   * @return 1-based end column number (same as starting column number if the
   * ParserPos is a point, not a range)
   */
  public int getEndColumnNum() {
    return endColumnNumber;
  }

  // implements Object
  public String toString() {
    return RESOURCE.parserContext(lineNumber, columnNumber).str();
  }

  /**
   * Combines this parser position with another to create a position which
   * spans from the first point in the first to the last point in the other.
   */
  public SqlParserPos plus(SqlParserPos pos) {
    return new SqlParserPos(
        getLineNum(),
        getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }

  /**
   * Combines this parser position with an array of positions to create a
   * position which spans from the first point in the first to the last point
   * in the other.
   */
  public SqlParserPos plusAll(SqlNode[] nodes) {
    int line = getLineNum();
    int column = getColumnNum();
    int endLine = getEndLineNum();
    int endColumn = getEndColumnNum();
    return sum(nodes, line, column, endLine, endColumn);
  }

  /**
   * Combines this parser position with a list of positions.
   */
  public SqlParserPos plusAll(Collection<SqlNode> nodeList) {
    final SqlNode[] nodes = nodeList.toArray(new SqlNode[nodeList.size()]);
    return plusAll(nodes);
  }

  /**
   * Combines the parser positions of an array of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(
      SqlNode[] nodes) {
    return sum(nodes, Integer.MAX_VALUE, Integer.MAX_VALUE, -1, -1);
  }

  /**
   * Combines the parser positions of a list of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(List<? extends SqlNode> nodes) {
    return sum(nodes.toArray(new SqlNode[nodes.size()]));
  }

  /**
   * Computes the parser position which is the sum of the positions of an
   * array of parse tree nodes and of a parser position represented by (line,
   * column, endLine, endColumn).
   *
   * @param nodes     Array of parse tree nodes
   * @param line      Start line
   * @param column    Start column
   * @param endLine   End line
   * @param endColumn End column
   * @return Sum of parser positions
   */
  private static SqlParserPos sum(
      SqlNode[] nodes,
      int line,
      int column,
      int endLine,
      int endColumn) {
    int testLine;
    int testColumn;
    for (SqlNode node : nodes) {
      if (node == null) {
        continue;
      }
      SqlParserPos pos = node.getParserPosition();
      if (pos.equals(SqlParserPos.ZERO)) {
        continue;
      }
      testLine = pos.getLineNum();
      testColumn = pos.getColumnNum();
      if (testLine < line || testLine == line && testColumn < column) {
        line = testLine;
        column = testColumn;
      }

      testLine = pos.getEndLineNum();
      testColumn = pos.getEndColumnNum();
      if (testLine > endLine || testLine == endLine && testColumn > endColumn) {
        endLine = testLine;
        endColumn = testColumn;
      }
    }
    return new SqlParserPos(line, column, endLine, endColumn);
  }

  /**
   * Combines an array of parser positions to create a position which spans
   * from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(
      SqlParserPos[] poses) {
    return sum(poses, Integer.MAX_VALUE, Integer.MAX_VALUE, -1, -1);
  }

  /**
   * Computes the parser position which is the sum of an array of parser
   * positions and of a parser position represented by (line, column, endLine,
   * endColumn).
   *
   * @param poses     Array of parser positions
   * @param line      Start line
   * @param column    Start column
   * @param endLine   End line
   * @param endColumn End column
   * @return Sum of parser positions
   */
  private static SqlParserPos sum(
      SqlParserPos[] poses,
      int line,
      int column,
      int endLine,
      int endColumn) {
    int testLine;
    int testColumn;
    for (SqlParserPos pos : poses) {
      if (pos == null) {
        continue;
      }
      testLine = pos.getLineNum();
      testColumn = pos.getColumnNum();
      if (testLine < line || testLine == line && testColumn < column) {
        line = testLine;
        column = testColumn;
      }

      testLine = pos.getEndLineNum();
      testColumn = pos.getEndColumnNum();
      if (testLine > endLine || testLine == endLine && testColumn > endColumn) {
        endLine = testLine;
        endColumn = testColumn;
      }
    }
    return new SqlParserPos(line, column, endLine, endColumn);
  }
}

// End SqlParserPos.java
