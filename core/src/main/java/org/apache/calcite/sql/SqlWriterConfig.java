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

import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableBeans;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration for {@link SqlWriter} and {@link SqlPrettyWriter}. */
public interface SqlWriterConfig {
  /** Returns the dialect. */
  @ImmutableBeans.Property
  @Nullable SqlDialect dialect();

  /** Sets {@link #dialect()}. */
  SqlWriterConfig withDialect(@Nullable SqlDialect dialect);

  /** Returns whether to print keywords (SELECT, AS, etc.) in lower-case.
   * Default is false: keywords are printed in upper-case. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean keywordsLowerCase();

  /** Sets {@link #keywordsLowerCase}. */
  SqlWriterConfig withKeywordsLowerCase(boolean keywordsLowerCase);

  /** Returns whether to quote all identifiers, even those which would be
   * correct according to the rules of the {@link SqlDialect} if quotation
   * marks were omitted. Default is true. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean quoteAllIdentifiers();

  /** Sets {@link #quoteAllIdentifiers}. */
  SqlWriterConfig withQuoteAllIdentifiers(boolean quoteAllIdentifiers);

  /** Returns the number of spaces indentation. Default is 4. */
  @ImmutableBeans.Property
  @ImmutableBeans.IntDefault(4)
  int indentation();

  /** Sets {@link #indentation}. */
  SqlWriterConfig withIndentation(int indentation);

  /** Returns whether a clause (FROM, WHERE, GROUP BY, HAVING, WINDOW,
   * ORDER BY) starts a new line. Default is true. SELECT is always at the
   * start of a line. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean clauseStartsLine();

  /** Sets {@link #clauseStartsLine}. */
  SqlWriterConfig withClauseStartsLine(boolean clauseStartsLine);

  /** Returns whether a clause (FROM, WHERE, GROUP BY, HAVING, WINDOW,
   * ORDER BY) is followed by a new line. Default is false. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean clauseEndsLine();

  /** Sets {@link #clauseEndsLine()}. */
  SqlWriterConfig withClauseEndsLine(boolean clauseEndsLine);

  /** Returns whether each item in a SELECT list, GROUP BY list, or ORDER BY
   * list is on its own line.
   *
   * <p>Default is false;
   * this property is superseded by {@link #selectFolding()},
   * {@link #groupByFolding()}, {@link #orderByFolding()}. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean selectListItemsOnSeparateLines();

  /** Sets {@link #selectListItemsOnSeparateLines}. */
  SqlWriterConfig withSelectListItemsOnSeparateLines(
      boolean selectListItemsOnSeparateLines);

  /** Returns the line-folding policy for lists in the SELECT, GROUP BY and
   * ORDER clauses, for items in the SET clause of UPDATE, and for items in
   * VALUES.
   *
   * @see #foldLength()
   *
   * <p>If not set, the values of
   * {@link #selectListItemsOnSeparateLines()},
   * {@link #valuesListNewline()},
   * {@link #updateSetListNewline()},
   * {@link #windowDeclListNewline()} are used. */
  @ImmutableBeans.Property
  @Nullable LineFolding lineFolding();

  /** Sets {@link #lineFolding()}. */
  SqlWriterConfig withLineFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the SELECT clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding selectFolding();

  /** Sets {@link #selectFolding()}. */
  SqlWriterConfig withSelectFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the FROM clause (and JOIN).
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @ImmutableBeans.EnumDefault("TALL")
  LineFolding fromFolding();

  /** Sets {@link #fromFolding()}. */
  SqlWriterConfig withFromFolding(LineFolding lineFolding);

  /** Returns the line-folding policy for the WHERE clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding whereFolding();

  /** Sets {@link #whereFolding()}. */
  SqlWriterConfig withWhereFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the GROUP BY clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding groupByFolding();

  /** Sets {@link #groupByFolding()}. */
  SqlWriterConfig withGroupByFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the HAVING clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding havingFolding();

  /** Sets {@link #havingFolding()}. */
  SqlWriterConfig withHavingFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the WINDOW clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding windowFolding();

  /** Sets {@link #windowFolding()}. */
  SqlWriterConfig withWindowFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the MATCH_RECOGNIZE clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding matchFolding();

  /** Sets {@link #matchFolding()}. */
  SqlWriterConfig withMatchFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the ORDER BY clause.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding orderByFolding();

  /** Sets {@link #orderByFolding()}. */
  SqlWriterConfig withOrderByFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the OVER clause or a window
   * declaration. If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding overFolding();

  /** Sets {@link #overFolding()}. */
  SqlWriterConfig withOverFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the VALUES expression.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding valuesFolding();

  /** Sets {@link #valuesFolding()}. */
  SqlWriterConfig withValuesFolding(@Nullable LineFolding lineFolding);

  /** Returns the line-folding policy for the SET clause of an UPDATE statement.
   * If not set, the value of {@link #lineFolding()} is used. */
  @ImmutableBeans.Property
  @Nullable LineFolding updateSetFolding();

  /** Sets {@link #updateSetFolding()}. */
  SqlWriterConfig withUpdateSetFolding(@Nullable LineFolding lineFolding);

  /**
   * Returns whether to use a fix for SELECT list indentations.
   *
   * <ul>
   * <li>If set to "false":
   *
   * <blockquote><pre>
   * SELECT
   *     A as A,
   *         B as B,
   *         C as C,
   *     D
   * </pre></blockquote>
   *
   * <li>If set to "true" (the default):
   *
   * <blockquote><pre>
   * SELECT
   *     A as A,
   *     B as B,
   *     C as C,
   *     D
   * </pre></blockquote>
   * </ul>
   */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean selectListExtraIndentFlag();

  /** Sets {@link #selectListExtraIndentFlag}. */
  SqlWriterConfig withSelectListExtraIndentFlag(boolean selectListExtraIndentFlag);

  /** Returns whether each declaration in a WINDOW clause should be on its own
   * line.
   *
   * <p>Default is true;
   * this property is superseded by {@link #windowFolding()}. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean windowDeclListNewline();

  /** Sets {@link #windowDeclListNewline}. */
  SqlWriterConfig withWindowDeclListNewline(boolean windowDeclListNewline);

  /** Returns whether each row in a VALUES clause should be on its own
   * line.
   *
   * <p>Default is true;
   * this property is superseded by {@link #valuesFolding()}. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean valuesListNewline();

  /** Sets {@link #valuesListNewline}. */
  SqlWriterConfig withValuesListNewline(boolean valuesListNewline);

  /** Returns whether each assignment in the SET clause of an UPDATE or MERGE
   * statement should be on its own line.
   *
   * <p>Default is true;
   * this property is superseded by {@link #updateSetFolding()}. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(true)
  boolean updateSetListNewline();

  /** Sets {@link #updateSetListNewline}. */
  SqlWriterConfig withUpdateSetListNewline(boolean updateSetListNewline);

  /** Returns whether a WINDOW clause should start its own line. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean windowNewline();

  /** Sets {@link #windowNewline}. */
  SqlWriterConfig withWindowNewline(boolean windowNewline);

  /** Returns whether commas in SELECT, GROUP BY and ORDER clauses should
   * appear at the start of the line. Default is false. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean leadingComma();

  /** Sets {@link #leadingComma()}. */
  SqlWriterConfig withLeadingComma(boolean leadingComma);

  /** Returns the sub-query style.
   * Default is {@link SqlWriter.SubQueryStyle#HYDE}. */
  @ImmutableBeans.Property
  @ImmutableBeans.EnumDefault("HYDE")
  SqlWriter.SubQueryStyle subQueryStyle();

  /** Sets {@link #subQueryStyle}. */
  SqlWriterConfig withSubQueryStyle(SqlWriter.SubQueryStyle subQueryStyle);

  /** Returns whether to print a newline before each AND or OR (whichever is
   * higher level) in WHERE clauses.
   *
   * <p>NOTE: Ignored when alwaysUseParentheses is set to true. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean whereListItemsOnSeparateLines();

  /** Sets {@link #whereListItemsOnSeparateLines}. */
  SqlWriterConfig withWhereListItemsOnSeparateLines(
      boolean whereListItemsOnSeparateLines);

  /** Returns whether expressions should always be included in parentheses.
   * Default is false. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean alwaysUseParentheses();

  /** Sets {@link #alwaysUseParentheses}. */
  SqlWriterConfig withAlwaysUseParentheses(boolean alwaysUseParentheses);

  /** Returns the maximum line length. Default is zero, which means there is
   * no maximum. */
  @ImmutableBeans.Property
  @ImmutableBeans.IntDefault(0)
  int lineLength();

  /** Sets {@link #lineLength}. */
  SqlWriterConfig withLineLength(int lineLength);

  /** Returns the line length at which items are chopped or folded (for clauses
   * that have chosen {@link LineFolding#CHOP} or {@link LineFolding#FOLD}).
   * Default is 80. */
  @ImmutableBeans.Property
  @ImmutableBeans.IntDefault(80)
  int foldLength();

  /** Sets {@link #foldLength()}. */
  SqlWriterConfig withFoldLength(int lineLength);

  /** Returns whether the WHEN, THEN and ELSE clauses of a CASE expression
   * appear at the start of a new line. The default is false. */
  @ImmutableBeans.Property
  @ImmutableBeans.BooleanDefault(false)
  boolean caseClausesOnNewLines();

  /** Sets {@link #caseClausesOnNewLines}. */
  SqlWriterConfig withCaseClausesOnNewLines(boolean caseClausesOnNewLines);

  /** Policy for how to do deal with long lines.
   *
   * <p>The following examples all have
   * {@link #clauseEndsLine ClauseEndsLine=true},
   * {@link #indentation Indentation=4}, and
   * {@link #foldLength FoldLength=25} (so that the long {@code SELECT}
   * clause folds but the shorter {@code GROUP BY} clause does not).
   *
   * <p>Note that {@link #clauseEndsLine ClauseEndsLine} is observed in
   * STEP and TALL modes, and in CHOP mode when a line is long.
   *
   * <table border=1>
   * <caption>SQL formatted with each Folding value</caption>
   * <tr>
   *   <th>Folding</th>
   *   <th>Example</th>
   * </tr>
   *
   * <tr>
   *   <td>WIDE</td>
   *   <td><pre>
   * SELECT abc, def, ghi, jkl, mno, pqr
   * FROM t
   * GROUP BY abc, def</pre></td>
   * </tr>
   *
   * <tr>
   *   <td>STEP</td>
   *   <td><pre>
   * SELECT
   *     abc, def, ghi, jkl, mno, pqr
   * FROM t
   * GROUP BY
   *     abc, def</pre></td>
   * </tr>
   *
   * <tr>
   *   <td>FOLD</td>
   *   <td><pre>
   * SELECT abc, def, ghi,
   *     jkl, mno, pqr
   * FROM t
   * GROUP BY abc, def</pre></td>
   * </tr>
   *
   * <tr>
   *   <td>CHOP</td>
   *   <td><pre>
   * SELECT
   *     abc,
   *     def,
   *     ghi,
   *     jkl,
   *     mno,
   *     pqr
   * FROM t
   * GROUP BY abc, def</pre></td>
   * </tr>
   *
   * <tr>
   *   <td>TALL</td>
   *   <td><pre>
   * SELECT
   *     abc,
   *     def,
   *     ghi,
   *     jkl,
   *     mno,
   *     pqr
   * FROM t
   * GROUP BY
   *     abc,
   *     def</pre></td>
   * </tr>
   * </table>
   */
  enum LineFolding {
    /** Do not wrap. Items are on the same line, regardless of length. */
    WIDE,

    /** As {@link #WIDE} but start a new line if {@link #clauseEndsLine()}. */
    STEP,

    /** Wrap if long. Items are on the same line, but if the line's length
     * exceeds {@link #foldLength()}, move items to the next line. */
    FOLD,

    /** Chop down if long. Items are on the same line, but if the line grows
     * longer than {@link #foldLength()}, put all items on separate lines. */
    CHOP,

    /** Wrap always. Items are on separate lines. */
    TALL
  }
}
