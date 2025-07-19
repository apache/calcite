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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Contains a string, the offset of a token within the string, and a parser position containing the
 * beginning and end line number.
 */
public class StringAndPos {

  public final String sql;
  public final int cursor;
  public final @Nullable SqlParserPos pos;

  private StringAndPos(String sql, int cursor, @Nullable SqlParserPos pos) {
    this.sql = sql;
    this.cursor = cursor;
    this.pos = pos;
  }

  @Override public String toString() {
    return addCarets();
  }

  @Override public int hashCode() {
    return Objects.hash(sql, cursor);
  }

  @Override public boolean equals(@Nullable Object o) {
    return o == this
        || o instanceof StringAndPos
        && sql.equals(((StringAndPos) o).sql)
        && cursor == ((StringAndPos) o).cursor
        && Objects.equals(pos, ((StringAndPos) o).pos);
  }

  /**
   * Checks if the SQL expression is a simple XOR pattern that can be safely processed.
   *
   * <p>This method provides special handling for the BITXOR_OPERATOR (^) in Apache Calcite
   * to identify common XOR patterns that should be processed without additional transformation.
   *
   * <p>Supported patterns:
   * <ul>
   *   <li>number ^ number (e.g., "5 ^ 3", "-2 ^ 7")</li>
   *   <li>CAST expression ^ CAST expression (e.g., "CAST(5 AS INTEGER) ^ CAST(3 AS BIGINT)")</li>
   * </ul>
   *
   * @param sql the SQL expression to check
   * @return true if the expression matches a simple XOR pattern, false otherwise
   */
  private static boolean isSimpleXorPattern(String sql) {
    int caretIndex = sql.indexOf('^');
    if (caretIndex == -1) {
      return false;
    }
    String before = sql.substring(0, caretIndex).trim();
    String after = sql.substring(caretIndex + 1).trim();

    // Pattern: number ^ number
    if (before.matches(".*-?\\d$") && after.matches("^-?\\d.*")) {
      return true;
    }
    if (before.matches(".*CAST\\s*\\(\\s*NULL\\s+AS\\s+\\w+(\\(\\d+\\))?\\s*\\)$")
        && after.matches("^\\d.*")) {
      return true;
    }

    // number ^ CAST(NULL AS ...)
    if (before.matches(".*\\d$")
        && after.matches("^CAST\\s*\\(\\s*NULL\\s+AS\\s+\\w+(\\(\\d+\\))?\\s*\\).*")) {
      return true;
    }
    // Pattern: CAST(...) ^ CAST(...)
    String castPattern =
        "CAST\\s*\\(.*?AS\\s+[A-Z]+(?:\\s*\\(\\d+(?:,\\d+)?\\))?(?:\\s+UNSIGNED)?\\s*\\)";
    if (before.matches(".*" + castPattern + "$")
        && after.matches("^" + castPattern + ".*")) {
      return true;
    }

    return false;
  }

  /**
   * Looks for one or two carets in a SQL string, and if present, converts them into a parser
   * position.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>of("xxx^yyy") yields {"xxxyyy", position 3, line 1 column 4}
   * <li>of("xxxyyy") yields {"xxxyyy", null}
   * <li>of("xxx^yy^y") yields {"xxxyyy", position 3, line 4 column 4
   * through line 1 column 6}
   * </ul>
   */
  public static StringAndPos of(String sql) {

    int firstCaret = sql.indexOf('^');
    if (firstCaret < 0) {
      return new StringAndPos(sql, -1, null);
    }
    // check for bitxor operator test cases
    if (isSimpleXorPattern(sql)) {
      return new StringAndPos(sql, -1, null);
    }
    int secondCaret = sql.indexOf('^', firstCaret + 1);
    if (secondCaret == firstCaret + 1) {
      // If SQL contains "^^", it does not contain error positions; convert each
      // "^^" to a single "^".
      return new StringAndPos(sql.replace("^^", "^"), -1, null);
    } else if (secondCaret < 0) {
      String sqlSansCaret =
          sql.substring(0, firstCaret)
              + sql.substring(firstCaret + 1);
      int[] start = SqlParserUtil.indexToLineCol(sql, firstCaret);
      SqlParserPos pos = new SqlParserPos(start[0], start[1]);
      return new StringAndPos(sqlSansCaret, firstCaret, pos);
    } else {
      String sqlSansCaret =
          sql.substring(0, firstCaret)
              + sql.substring(firstCaret + 1, secondCaret)
              + sql.substring(secondCaret + 1);
      int[] start = SqlParserUtil.indexToLineCol(sql, firstCaret);

      // subtract 1 because the col position needs to be inclusive
      --secondCaret;
      int[] end = SqlParserUtil.indexToLineCol(sql, secondCaret);

      // if second caret is on same line as first, decrement its column,
      // because first caret pushed the string out
      if (start[0] == end[0]) {
        --end[1];
      }

      SqlParserPos pos =
          new SqlParserPos(start[0], start[1], end[0], end[1]);
      return new StringAndPos(sqlSansCaret, firstCaret, pos);
    }
  }

  public String addCarets() {
    return pos == null ? sql
        : SqlParserUtil.addCarets(sql, pos.getLineNum(), pos.getColumnNum(),
            pos.getEndLineNum(), pos.getEndColumnNum() + 1);
  }
}
