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

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlString;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.function.Consumer;

/**
 * A <code>SqlWriter</code> is the target to construct a SQL statement from a
 * parse tree. It deals with dialect differences; for example, Oracle quotes
 * identifiers as <code>"scott"</code>, while SQL Server quotes them as <code>
 * [scott]</code>.
 */
public interface SqlWriter {
  //~ Enums ------------------------------------------------------------------

  /**
   * Style of formatting sub-queries.
   */
  enum SubQueryStyle {
    /**
     * Julian's style of sub-query nesting. Like this:
     *
     * <blockquote><pre>SELECT *
     * FROM (
     *     SELECT *
     *     FROM t
     * )
     * WHERE condition</pre></blockquote>
     */
    HYDE,

    /**
     * Damian's style of sub-query nesting. Like this:
     *
     * <blockquote><pre>SELECT *
     * FROM
     * (   SELECT *
     *     FROM t
     * )
     * WHERE condition</pre></blockquote>
     */
    BLACK
  }

  /**
   * Enumerates the types of frame.
   */
  enum FrameTypeEnum implements FrameType {
    /**
     * SELECT query (or UPDATE or DELETE). The items in the list are the
     * clauses: FROM, WHERE, etc.
     */
    SELECT,

    /**
     * Simple list.
     */
    SIMPLE,

    /**
     * Comma-separated list surrounded by parentheses.
     * The parentheses are present even if the list is empty.
     */
    PARENTHESES,

    /**
     * The SELECT clause of a SELECT statement.
     */
    SELECT_LIST,

    /**
     * The WINDOW clause of a SELECT statement.
     */
    WINDOW_DECL_LIST,

    /**
     * The SET clause of an UPDATE statement.
     */
    UPDATE_SET_LIST,

    /**
     * Function declaration.
     */
    FUN_DECL,

    /**
     * Function call or datatype declaration.
     *
     * <p>Examples:</p>
     * <ul>
     * <li><code>SUBSTRING('foobar' FROM 1 + 2 TO 4)</code></li>
     * <li><code>DECIMAL(10, 5)</code></li>
     * </ul>
     */
    FUN_CALL,

    /**
     * Window specification.
     *
     * <p>Examples:</p>
     * <ul>
     * <li><code>SUM(x) OVER (ORDER BY hireDate ROWS 3 PRECEDING)</code></li>
     * <li><code>WINDOW w1 AS (ORDER BY hireDate), w2 AS (w1 PARTITION BY gender
     * RANGE BETWEEN INTERVAL '1' YEAR PRECEDING AND '2' MONTH
     * PRECEDING)</code></li>
     * </ul>
     */
    WINDOW,

    /**
     * ORDER BY clause of a SELECT statement. The "list" has only two items:
     * the query and the order by clause, with ORDER BY as the separator.
     */
    ORDER_BY,

    /**
     * ORDER BY list.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>ORDER BY x, y DESC, z</code></li>
     * </ul>
     */
    ORDER_BY_LIST,

    /**
     * WITH clause of a SELECT statement. The "list" has only two items:
     * the WITH clause and the query, with AS as the separator.
     */
    WITH,

    /**
     * The body query of WITH.
     */
    WITH_BODY,

    /**
     * OFFSET clause.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>OFFSET 10 ROWS</code></li>
     * </ul>
     */
    OFFSET,

    /**
     * FETCH clause.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>FETCH FIRST 3 ROWS ONLY</code></li>
     * </ul>
     */
    FETCH,

    /**
     * GROUP BY list.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>GROUP BY x, FLOOR(y)</code></li>
     * </ul>
     */
    GROUP_BY_LIST,

    /**
     * Sub-query list. Encloses a SELECT, UNION, EXCEPT, INTERSECT query
     * with optional ORDER BY.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>GROUP BY x, FLOOR(y)</code></li>
     * </ul>
     */
    SUB_QUERY(true),

    /**
     * Set operation.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>SELECT * FROM a UNION SELECT * FROM b</code></li>
     * </ul>
     */
    SETOP,

    /**
     * VALUES clause.
     *
     * <p>Example:
     *
     * <blockquote><pre>VALUES (1, 'a'),
     *   (2, 'b')</pre></blockquote>
     */
    VALUES,

    /**
     * FROM clause (containing various kinds of JOIN).
     */
    FROM_LIST,

    /**
     * Pair-wise join.
     */
    JOIN(false),

    /**
     * WHERE clause.
     */
    WHERE_LIST,

    /**
     * Compound identifier.
     *
     * <p>Example:</p>
     * <ul>
     * <li><code>"A"."B"."C"</code></li>
     * </ul>
     */
    IDENTIFIER(false),

    /**
     * Alias ("AS"). No indent.
     */
    AS(false),

    /**
     * CASE expression.
     */
    CASE,

    /**
     * Same behavior as user-defined frame type.
     */
    OTHER;

    private final boolean needsIndent;

    /**
     * Creates a list type.
     */
    FrameTypeEnum() {
      this(true);
    }

    /**
     * Creates a list type.
     */
    FrameTypeEnum(boolean needsIndent) {
      this.needsIndent = needsIndent;
    }

    @Override public boolean needsIndent() {
      return needsIndent;
    }

    /**
     * Creates a frame type.
     *
     * @param name Name
     * @return frame type
     */
    public static FrameType create(final String name) {
      return new FrameType() {
        @Override public String getName() {
          return name;
        }

        @Override public boolean needsIndent() {
          return true;
        }
      };
    }

    @Override public String getName() {
      return name();
    }
  }

  /** Comma operator.
   *
   * <p>Defined in {@code SqlWriter} because it is only used while converting
   * {@link SqlNode} to SQL;
   * see {@link SqlWriter#list(FrameTypeEnum, SqlBinaryOperator, SqlNodeList)}.
   *
   * <p>The precedence of the comma operator is low but not zero. For
   * instance, this ensures parentheses in
   * {@code select x, (select * from foo order by z), y from t}. */
  SqlBinaryOperator COMMA =
      new SqlBinaryOperator(",", SqlKind.OTHER, 2, false, null, null, null);

  //~ Methods ----------------------------------------------------------------

  /**
   * Resets this writer so that it can format another expression. Does not
   * affect formatting preferences (see {@link #resetSettings()}
   */
  void reset();

  /**
   * Resets all properties to their default values.
   */
  void resetSettings();

  /**
   * Returns the dialect of SQL.
   *
   * @return SQL dialect
   */
  SqlDialect getDialect();

  /**
   * Returns the contents of this writer as a 'certified kocher' SQL string.
   *
   * @return SQL string
   */
  SqlString toSqlString();

  /**
   * Prints a literal, exactly as provided. Does not attempt to indent or
   * convert to upper or lower case. Does not add quotation marks. Adds
   * preceding whitespace if necessary.
   */
  @Pure
  void literal(String s);

  /**
   * Prints a sequence of keywords. Must not start or end with space, but may
   * contain a space. For example, <code>keyword("SELECT")</code>, <code>
   * keyword("CHARACTER SET")</code>.
   */
  @Pure
  void keyword(String s);

  /**
   * Prints a string, preceded by whitespace if necessary.
   */
  @Pure
  void print(String s);

  /**
   * Prints an integer.
   *
   * @param x Integer
   */
  @Pure
  void print(int x);

  /**
   * Prints an identifier, quoting as necessary.
   *
   * @param name   The identifier name
   * @param quoted Whether this identifier was quoted in the original sql statement,
   *               this may not be the only factor to decide whether this identifier
   *               should be quoted
   */
  void identifier(String name, boolean quoted);

  /**
   * Prints a dynamic parameter (e.g. {@code ?} for default JDBC)
   */
  void dynamicParam(int index);

  /**
   * Prints the OFFSET/FETCH clause.
   */
  void fetchOffset(@Nullable SqlNode fetch, @Nullable SqlNode offset);

  /**
   * Prints the TOP(n) clause.
   *
   * @see #fetchOffset
   */
  void topN(@Nullable SqlNode fetch, @Nullable SqlNode offset);

  /**
   * Prints a new line, and indents.
   */
  void newlineAndIndent();

  /**
   * Returns whether this writer should quote all identifiers, even those
   * that do not contain mixed-case identifiers or punctuation.
   *
   * @return whether to quote all identifiers
   */
  boolean isQuoteAllIdentifiers();

  /**
   * Returns whether this writer should start each clause (e.g. GROUP BY) on
   * a new line.
   *
   * @return whether to start each clause on a new line
   */
  boolean isClauseStartsLine();

  /**
   * Returns whether the items in the SELECT clause should each be on a
   * separate line.
   *
   * @return whether to put each SELECT clause item on a new line
   */
  boolean isSelectListItemsOnSeparateLines();

  /**
   * Returns whether to output all keywords (e.g. SELECT, GROUP BY) in lower
   * case.
   *
   * @return whether to output SQL keywords in lower case
   */
  boolean isKeywordsLowerCase();

  /**
   * Starts a list which is a call to a function.
   *
   * @see #endFunCall(Frame)
   */
  @Pure
  Frame startFunCall(String funName);

  /**
   * Ends a list which is a call to a function.
   *
   * @param frame Frame
   * @see #startFunCall(String)
   */
  @Pure
  void endFunCall(Frame frame);

  /**
   * Starts a list.
   */
  @Pure
  Frame startList(String open, String close);

  /**
   * Starts a list with no opening string.
   *
   * @param frameType Type of list. For example, a SELECT list will be
   * governed according to SELECT-list formatting preferences.
   */
  @Pure
  Frame startList(FrameTypeEnum frameType);

  /**
   * Starts a list.
   *
   * @param frameType Type of list. For example, a SELECT list will be
   *                  governed according to SELECT-list formatting preferences.
   * @param open      String to start the list; typically "(" or the empty
   *                  string.
   * @param close     String to close the list
   */
  @Pure
  Frame startList(FrameType frameType, String open, String close);

  /**
   * Ends a list.
   *
   * @param frame The frame which was created by {@link #startList}.
   */
  @Pure
  void endList(@Nullable Frame frame);

  /**
   * Writes a list.
   */
  @Pure
  SqlWriter list(FrameTypeEnum frameType, Consumer<SqlWriter> action);

  /**
   * Writes a list separated by a binary operator
   * ({@link SqlStdOperatorTable#AND AND},
   * {@link SqlStdOperatorTable#OR OR}, or
   * {@link #COMMA COMMA}).
   */
  @Pure
  SqlWriter list(FrameTypeEnum frameType, SqlBinaryOperator sepOp,
      SqlNodeList list);

  /**
   * Writes a list separator, unless the separator is "," and this is the
   * first occurrence in the list.
   *
   * @param sep List separator, typically ",".
   */
  @Pure
  void sep(String sep);

  /**
   * Writes a list separator.
   *
   * @param sep        List separator, typically ","
   * @param printFirst Whether to print the first occurrence of the separator
   */
  @Pure
  void sep(String sep, boolean printFirst);

  /**
   * Sets whether whitespace is needed before the next token.
   */
  @Pure
  void setNeedWhitespace(boolean needWhitespace);

  /**
   * Returns the offset for each level of indentation. Default 4.
   */
  int getIndentation();

  /**
   * Returns whether to enclose all expressions in parentheses, even if the
   * operator has high enough precedence that the parentheses are not
   * required.
   *
   * <p>For example, the parentheses are required in the expression <code>(a +
   * b) * c</code> because the '*' operator has higher precedence than the '+'
   * operator, and so without the parentheses, the expression would be
   * equivalent to <code>a + (b * c)</code>. The fully-parenthesized
   * expression, <code>((a + b) * c)</code> is unambiguous even if you don't
   * know the precedence of every operator.
   */
  boolean isAlwaysUseParentheses();

  /**
   * Returns whether we are currently in a query context (SELECT, INSERT,
   * UNION, INTERSECT, EXCEPT, and the ORDER BY operator).
   */
  boolean inQuery();

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * A Frame is a piece of generated text which shares a common indentation
   * level.
   *
   * <p>Every frame has a beginning, a series of clauses and separators, and
   * an end. A typical frame is a comma-separated list. It begins with a "(",
   * consists of expressions separated by ",", and ends with a ")".
   *
   * <p>A select statement is also a kind of frame. The beginning and end are
   * are empty strings, but it consists of a sequence of clauses. "SELECT",
   * "FROM", "WHERE" are separators.
   *
   * <p>A frame is current between a call to one of the
   * {@link SqlWriter#startList} methods and the call to
   * {@link SqlWriter#endList(Frame)}. If other code starts a frame in the mean
   * time, the sub-frame is put onto a stack.
   */
  interface Frame {
  }

  /** Frame type. */
  interface FrameType {
    /**
     * Returns the name of this frame type.
     *
     * @return name
     */
    String getName();

    /**
     * Returns whether this frame type should cause the code be further
     * indented.
     *
     * @return whether to further indent code within a frame of this type
     */
    boolean needsIndent();
  }
}
