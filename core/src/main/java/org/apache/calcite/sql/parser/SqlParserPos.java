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

import org.apache.calcite.sql.SqlNode;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

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
    return Objects.hash(lineNumber, columnNumber, endLineNumber, endColumnNumber);
  }

  public boolean equals(Object o) {
    return o == this
        || o instanceof SqlParserPos
        && this.lineNumber == ((SqlParserPos) o).lineNumber
        && this.columnNumber == ((SqlParserPos) o).columnNumber
        && this.endLineNumber == ((SqlParserPos) o).endLineNumber
        && this.endColumnNumber == ((SqlParserPos) o).endColumnNumber;
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

  @Override public String toString() {
    return RESOURCE.parserContext(lineNumber, columnNumber).str();
  }

  /**
   * Combines this parser position with another to create a
   * position that spans from the first point in the first to the last point
   * in the other.
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
   * position that spans from the first point in the first to the last point
   * in the other.
   */
  public SqlParserPos plusAll(SqlNode[] nodes) {
    return plusAll(Arrays.asList(nodes));
  }

  /**
   * Combines this parser position with a list of positions.
   */
  public SqlParserPos plusAll(Collection<SqlNode> nodeList) {
    int line = getLineNum();
    int column = getColumnNum();
    int endLine = getEndLineNum();
    int endColumn = getEndColumnNum();
    return sum(toPos(nodeList), line, column, endLine, endColumn);
  }

  /**
   * Combines the parser positions of an array of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(SqlNode[] nodes) {
    final Iterable<SqlParserPos> poses = toPos(Arrays.asList(nodes));
    return sum(poses, Integer.MAX_VALUE, Integer.MAX_VALUE, -1, -1);
  }

  private static Iterable<SqlParserPos> toPos(Iterable<SqlNode> nodes) {
    return Iterables.transform(nodes,
        new Function<SqlNode, SqlParserPos>() {
          public SqlParserPos apply(SqlNode input) {
            return input.getParserPosition();
          }
        });
  }

  /**
   * Combines the parser positions of a list of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(List<? extends SqlNode> nodes) {
    return sum(nodes.toArray(new SqlNode[nodes.size()]));
  }

  /**
   * Combines an array of parser positions to create a position which spans
   * from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(Iterable<SqlParserPos> poses) {
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
      Iterable<SqlParserPos> poses,
      int line,
      int column,
      int endLine,
      int endColumn) {
    int testLine;
    int testColumn;
    for (SqlParserPos pos : poses) {
      if (pos == null || pos.equals(SqlParserPos.ZERO)) {
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

  public boolean overlaps(SqlParserPos pos) {
    return startsBefore(pos) && endsAfter(pos)
        || pos.startsBefore(this) && pos.endsAfter(this);
  }

  private boolean startsBefore(SqlParserPos pos) {
    return lineNumber < pos.lineNumber
        || lineNumber == pos.lineNumber
        && columnNumber <= pos.columnNumber;
  }

  private boolean endsAfter(SqlParserPos pos) {
    return endLineNumber > pos.endLineNumber
        || endLineNumber == pos.endLineNumber
        && endColumnNumber >= pos.endColumnNumber;
  }

  public boolean startsAt(SqlParserPos pos) {
    return lineNumber == pos.lineNumber
        && columnNumber == pos.columnNumber;
  }
}

// End SqlParserPos.java
