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

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
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

  /** Same as {@link #ZERO} but always quoted. */
  public static final SqlParserPos QUOTED_ZERO = new QuotedParserPos(0, 0, 0, 0);

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
    this(lineNumber, columnNumber, lineNumber, columnNumber);
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
    assert startLineNumber < endLineNumber
        || startLineNumber == endLineNumber
        && startColumnNumber <= endColumnNumber;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public int hashCode() {
    return Objects.hash(lineNumber, columnNumber, endLineNumber, endColumnNumber);
  }

  @Override public boolean equals(@Nullable Object o) {
    return o == this
        || o instanceof SqlParserPos
        && this.lineNumber == ((SqlParserPos) o).lineNumber
        && this.columnNumber == ((SqlParserPos) o).columnNumber
        && this.endLineNumber == ((SqlParserPos) o).endLineNumber
        && this.endColumnNumber == ((SqlParserPos) o).endColumnNumber;
  }

  /** Returns 1-based starting line number. */
  public int getLineNum() {
    return lineNumber;
  }

  /** Returns 1-based starting column number. */
  public int getColumnNum() {
    return columnNumber;
  }

  /** Returns 1-based end line number (same as starting line number if the
   * ParserPos is a point, not a range). */
  public int getEndLineNum() {
    return endLineNumber;
  }

  /** Returns 1-based end column number (same as starting column number if the
   * ParserPos is a point, not a range). */
  public int getEndColumnNum() {
    return endColumnNumber;
  }

  /** Returns a {@code SqlParserPos} the same as this but quoted. */
  public SqlParserPos withQuoting(boolean quoted) {
    if (isQuoted() == quoted) {
      return this;
    } else if (quoted) {
      return new QuotedParserPos(lineNumber, columnNumber, endLineNumber,
          endColumnNumber);
    } else {
      return new SqlParserPos(lineNumber, columnNumber, endLineNumber,
          endColumnNumber);
    }
  }

  /** Returns whether this SqlParserPos is quoted. */
  public boolean isQuoted() {
    return false;
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
  public SqlParserPos plusAll(@Nullable SqlNode[] nodes) {
    final PosBuilder b = new PosBuilder(this);
    for (SqlNode node : nodes) {
      if (node != null) {
        b.add(node.getParserPosition());
      }
    }
    return b.build(this);
  }

  /**
   * Combines this parser position with a list of positions.
   */
  public SqlParserPos plusAll(Collection<? extends @Nullable SqlNode> nodes) {
    final PosBuilder b = new PosBuilder(this);
    for (SqlNode node : nodes) {
      if (node != null) {
        b.add(node.getParserPosition());
      }
    }
    return b.build(this);
  }

  /**
   * Combines the parser positions of an array of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(final SqlNode[] nodes) {
    if (nodes.length == 0) {
      throw new AssertionError();
    }
    final SqlParserPos pos0 = nodes[0].getParserPosition();
    if (nodes.length == 1) {
      return pos0;
    }
    final PosBuilder b = new PosBuilder(pos0);
    for (int i = 1; i < nodes.length; i++) {
      b.add(nodes[i].getParserPosition());
    }
    return b.build(pos0);
  }

  /**
   * Combines the parser positions of a list of nodes to create a position
   * which spans from the beginning of the first to the end of the last.
   */
  public static SqlParserPos sum(final List<? extends SqlNode> nodes) {
    if (nodes.isEmpty()) {
      throw new AssertionError();
    }
    SqlParserPos pos0 = nodes.get(0).getParserPosition();
    if (nodes.size() == 1) {
      return pos0;
    }
    final PosBuilder b = new PosBuilder(pos0);
    for (int i = 1; i < nodes.size(); i++) {
      b.add(nodes.get(i).getParserPosition());
    }
    return b.build(pos0);
  }

  /** Returns a position spanning the earliest position to the latest.
   * Does not assume that the positions are sorted.
   * Throws if the list is empty. */
  public static SqlParserPos sum(Iterable<SqlParserPos> poses) {
    final List<SqlParserPos> list =
        poses instanceof List
            ? (List<SqlParserPos>) poses
            : Lists.newArrayList(poses);
    if (list.isEmpty()) {
      throw new AssertionError();
    }
    final SqlParserPos pos0 = list.get(0);
    if (list.size() == 1) {
      return pos0;
    }
    final PosBuilder b = new PosBuilder(pos0);
    for (int i = 1; i < list.size(); i++) {
      b.add(list.get(i));
    }
    return b.build(pos0);
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

  /** Parser position for an identifier segment that is quoted. */
  private static class QuotedParserPos extends SqlParserPos {
    QuotedParserPos(int startLineNumber, int startColumnNumber,
        int endLineNumber, int endColumnNumber) {
      super(startLineNumber, startColumnNumber, endLineNumber,
          endColumnNumber);
    }

    @Override public boolean isQuoted() {
      return true;
    }
  }

  /** Builds a parser position. */
  private static class PosBuilder {
    private int line;
    private int column;
    private int endLine;
    private int endColumn;

    PosBuilder(SqlParserPos p) {
      this(p.lineNumber, p.columnNumber, p.endLineNumber, p.endColumnNumber);
    }

    PosBuilder(int line, int column, int endLine, int endColumn) {
      this.line = line;
      this.column = column;
      this.endLine = endLine;
      this.endColumn = endColumn;
    }

    void add(SqlParserPos pos) {
      if (pos.equals(SqlParserPos.ZERO)) {
        return;
      }
      int testLine = pos.getLineNum();
      int testColumn = pos.getColumnNum();
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

    SqlParserPos build(SqlParserPos p) {
      return p.lineNumber == line
          && p.columnNumber == column
          && p.endLineNumber == endLine
          && p.endColumnNumber == endColumn
          ? p
          : build();
    }

    SqlParserPos build() {
      return new SqlParserPos(line, column, endLine, endColumn);
    }
  }
}
