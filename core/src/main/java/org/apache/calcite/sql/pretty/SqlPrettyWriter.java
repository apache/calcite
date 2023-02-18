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
package org.apache.calcite.sql.pretty;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteLogger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Pretty printer for SQL statements.
 *
 * <p>There are several options to control the format.
 *
 * <table>
 * <caption>Formatting options</caption>
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Default</th>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#clauseStartsLine()} ClauseStartsLine}</td>
 * <td>Whether a clause ({@code FROM}, {@code WHERE}, {@code GROUP BY},
 * {@code HAVING}, {@code WINDOW}, {@code ORDER BY}) starts a new line.
 * {@code SELECT} is always at the start of a line.</td>
 * <td>true</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#clauseEndsLine ClauseEndsLine}</td>
 * <td>Whether a clause ({@code SELECT}, {@code FROM}, {@code WHERE},
 * {@code GROUP BY}, {@code HAVING}, {@code WINDOW}, {@code ORDER BY}) is
 * followed by a new line.</td>
 * <td>false</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#caseClausesOnNewLines CaseClausesOnNewLines}</td>
 * <td>Whether the WHEN, THEN and ELSE clauses of a CASE expression appear at
 * the start of a new line.</td>
 * <td>false</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#indentation Indentation}</td>
 * <td>Number of spaces to indent</td>
 * <td>4</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#keywordsLowerCase KeywordsLowerCase}</td>
 * <td>Whether to print keywords (SELECT, AS, etc.) in lower-case.</td>
 * <td>false</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#alwaysUseParentheses AlwaysUseParentheses}</td>
 * <td><p>Whether to enclose all expressions in parentheses, even if the
 * operator has high enough precedence that the parentheses are not required.
 *
 * <p>For example, the parentheses are required in the expression
 * {@code (a + b) * c} because the '*' operator has higher precedence than the
 * '+' operator, and so without the parentheses, the expression would be
 * equivalent to {@code a + (b * c)}. The fully-parenthesized expression,
 * {@code ((a + b) * c)} is unambiguous even if you don't know the precedence
 * of every operator.</td>
 * <td>false</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#quoteAllIdentifiers QuoteAllIdentifiers}</td>
 * <td>Whether to quote all identifiers, even those which would be correct
 * according to the rules of the {@link SqlDialect} if quotation marks were
 * omitted.</td>
 * <td>true</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#subQueryStyle SubQueryStyle}</td>
 * <td>Style for formatting sub-queries. Values are:
 * {@link org.apache.calcite.sql.SqlWriter.SubQueryStyle#HYDE Hyde},
 * {@link org.apache.calcite.sql.SqlWriter.SubQueryStyle#BLACK Black}.</td>
 *
 * <td>{@link org.apache.calcite.sql.SqlWriter.SubQueryStyle#HYDE Hyde}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#lineLength LineLength}</td>
 * <td>The desired maximum length for lines (to look nice in editors,
 * printouts, etc.).</td>
 * <td>-1 (no maximum)</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#foldLength FoldLength}</td>
 * <td>The line length at which lines are folded or chopped down
 * (see {@code LineFolding}). Only has an effect if clauses are marked
 * {@link SqlWriterConfig.LineFolding#CHOP CHOP} or
 * {@link SqlWriterConfig.LineFolding#FOLD FOLD}.</td>
 * <td>80</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#lineFolding LineFolding}</td>
 * <td>How long lines are to be handled. Options are lines are
 * WIDE (do not wrap),
 * FOLD (wrap if long),
 * CHOP (chop down if long),
 * and TALL (wrap always).</td>
 * <td>WIDE</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#selectFolding() SelectFolding}</td>
 * <td>How the {@code SELECT} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#fromFolding FromFolding}</td>
 * <td>How the {@code FROM} clause and nested {@code JOIN} clauses are to be
 * folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#whereFolding WhereFolding}</td>
 * <td>How the {@code WHERE} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#groupByFolding GroupByFolding}</td>
 * <td>How the {@code GROUP BY} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#havingFolding HavingFolding}</td>
 * <td>How the {@code HAVING} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#orderByFolding OrderByFolding}</td>
 * <td>How the {@code ORDER BY} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#windowFolding WindowFolding}</td>
 * <td>How the {@code WINDOW} clause is to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#overFolding OverFolding}</td>
 * <td>How window declarations in the {@code WINDOW} clause
 * and in the {@code OVER} clause of aggregate functions are to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#valuesFolding ValuesFolding}</td>
 * <td>How lists of values in the {@code VALUES} clause are to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * <tr>
 * <td>{@link SqlWriterConfig#updateSetFolding UpdateSetFolding}</td>
 * <td>How assignments in the {@code SET} clause of an {@code UPDATE} statement
 * are to be folded.</td>
 * <td>{@code LineFolding}</td>
 * </tr>
 *
 * </table>
 *
 * <p>The following options exist for backwards compatibility. They are
 * used if {@link SqlWriterConfig#lineFolding LineFolding} and clause-specific
 * options such as {@link SqlWriterConfig#selectFolding SelectFolding} are not
 * specified:
 *
 * <ul>
 *
 * <li>{@link SqlWriterConfig#selectListItemsOnSeparateLines SelectListItemsOnSeparateLines}
 * replaced by {@link SqlWriterConfig#selectFolding SelectFolding},
 * {@link SqlWriterConfig#groupByFolding GroupByFolding}, and
 * {@link SqlWriterConfig#orderByFolding OrderByFolding};
 *
 * <li>{@link SqlWriterConfig#updateSetListNewline UpdateSetListNewline}
 * replaced by {@link SqlWriterConfig#updateSetFolding UpdateSetFolding};
 *
 * <li>{@link SqlWriterConfig#windowDeclListNewline WindowDeclListNewline}
 * replaced by {@link SqlWriterConfig#windowFolding WindowFolding};
 *
 * <li>{@link SqlWriterConfig#windowNewline WindowNewline}
 * replaced by {@link SqlWriterConfig#overFolding OverFolding};
 *
 * <li>{@link SqlWriterConfig#valuesListNewline ValuesListNewline}
 * replaced by {@link SqlWriterConfig#valuesFolding ValuesFolding}.
 *
 * </ul>
 */
public class SqlPrettyWriter implements SqlWriter {
  //~ Static fields/initializers ---------------------------------------------

  protected static final CalciteLogger LOGGER =
      new CalciteLogger(
          LoggerFactory.getLogger("org.apache.calcite.sql.pretty.SqlPrettyWriter"));

  /**
   * Default SqlWriterConfig, reduce the overhead of "ImmutableBeans.create"
   */
  private static final SqlWriterConfig CONFIG =
      SqlWriterConfig.of()
          .withDialect(CalciteSqlDialect.DEFAULT);

  /**
   * Bean holding the default property values.
   */
  private static final Bean DEFAULT_BEAN =
      new SqlPrettyWriter(SqlPrettyWriter.config()
          .withDialect(AnsiSqlDialect.DEFAULT)).getBean();
  protected static final String NL = System.getProperty("line.separator");

  //~ Instance fields --------------------------------------------------------

  private final SqlDialect dialect;
  private final StringBuilder buf;
  private final Deque<FrameImpl> listStack = new ArrayDeque<>();
  private ImmutableList.@Nullable Builder<Integer> dynamicParameters;
  protected @Nullable FrameImpl frame;
  private boolean needWhitespace;
  protected @Nullable String nextWhitespace;
  private SqlWriterConfig config;
  private @Nullable Bean bean;
  private int currentIndent;

  private int lineStart;

  //~ Constructors -----------------------------------------------------------

  @SuppressWarnings("method.invocation.invalid")
  private SqlPrettyWriter(SqlWriterConfig config,
      StringBuilder buf, @SuppressWarnings("unused") boolean ignore) {
    this.buf = requireNonNull(buf, "buf");
    this.dialect = requireNonNull(config.dialect());
    this.config = requireNonNull(config, "config");
    lineStart = 0;
    reset();
  }

  /** Creates a writer with the given configuration
   * and a given buffer to write to. */
  public SqlPrettyWriter(SqlWriterConfig config,
      StringBuilder buf) {
    this(config, requireNonNull(buf, "buf"), false);
  }

  /** Creates a writer with the given configuration and dialect,
   * and a given print writer (or a private print writer if it is null). */
  public SqlPrettyWriter(
      SqlDialect dialect,
      SqlWriterConfig config,
      StringBuilder buf) {
    this(config.withDialect(requireNonNull(dialect, "dialect")), buf);
  }

  /** Creates a writer with the given configuration
   * and a private print writer. */
  @Deprecated
  public SqlPrettyWriter(SqlDialect dialect, SqlWriterConfig config) {
    this(config.withDialect(requireNonNull(dialect, "dialect")));
  }

  @Deprecated
  public SqlPrettyWriter(
      SqlDialect dialect,
      boolean alwaysUseParentheses,
      PrintWriter pw) {
    // NOTE that 'pw' is ignored; there is no place for it in the new API
    this(config().withDialect(requireNonNull(dialect, "dialect"))
        .withAlwaysUseParentheses(alwaysUseParentheses));
  }

  @Deprecated
  public SqlPrettyWriter(
      SqlDialect dialect,
      boolean alwaysUseParentheses) {
    this(config().withDialect(requireNonNull(dialect, "dialect"))
        .withAlwaysUseParentheses(alwaysUseParentheses));
  }

  /** Creates a writer with a given dialect, the default configuration
   * and a private print writer. */
  @Deprecated
  public SqlPrettyWriter(SqlDialect dialect) {
    this(config().withDialect(requireNonNull(dialect, "dialect")));
  }

  /** Creates a writer with the given configuration,
   * and a private builder. */
  public SqlPrettyWriter(SqlWriterConfig config) {
    this(config, new StringBuilder(), true);
  }

  /** Creates a writer with the default configuration.
   *
   * @see #config() */
  public SqlPrettyWriter() {
    this(config());
  }

  /** Creates a {@link SqlWriterConfig} with Calcite's SQL dialect. */
  public static SqlWriterConfig config() {
    return CONFIG;
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated
  public void setCaseClausesOnNewLines(boolean caseClausesOnNewLines) {
    this.config = config.withCaseClausesOnNewLines(caseClausesOnNewLines);
  }

  @Deprecated
  public void setSubQueryStyle(SubQueryStyle subQueryStyle) {
    this.config = config.withSubQueryStyle(subQueryStyle);
  }

  @Deprecated
  public void setWindowNewline(boolean windowNewline) {
    this.config = config.withWindowNewline(windowNewline);
  }

  @Deprecated
  public void setWindowDeclListNewline(boolean windowDeclListNewline) {
    this.config = config.withWindowDeclListNewline(windowDeclListNewline);
  }

  @Deprecated
  @Override public int getIndentation() {
    return config.indentation();
  }

  @Deprecated
  @Override public boolean isAlwaysUseParentheses() {
    return config.alwaysUseParentheses();
  }

  @Override public boolean inQuery() {
    return (frame == null)
        || (frame.frameType == FrameTypeEnum.SELECT)
        || (frame.frameType == FrameTypeEnum.ORDER_BY)
        || (frame.frameType == FrameTypeEnum.WITH_BODY)
        || (frame.frameType == FrameTypeEnum.SETOP);
  }

  @Deprecated
  @Override public boolean isQuoteAllIdentifiers() {
    return config.quoteAllIdentifiers();
  }

  @Deprecated
  @Override public boolean isClauseStartsLine() {
    return config.clauseStartsLine();
  }

  @Deprecated
  @Override public boolean isSelectListItemsOnSeparateLines() {
    return config.selectListItemsOnSeparateLines();
  }

  @Deprecated
  public boolean isWhereListItemsOnSeparateLines() {
    return config.whereListItemsOnSeparateLines();
  }

  @Deprecated
  public boolean isSelectListExtraIndentFlag() {
    return config.selectListExtraIndentFlag();
  }

  @Deprecated
  @Override public boolean isKeywordsLowerCase() {
    return config.keywordsLowerCase();
  }

  @Deprecated
  public int getLineLength() {
    return config.lineLength();
  }

  @Override public void resetSettings() {
    reset();
    config = config();
  }

  @Override public void reset() {
    buf.setLength(0);
    lineStart = 0;
    dynamicParameters = null;
    setNeedWhitespace(false);
    nextWhitespace = " ";
  }

  /**
   * Returns an object which encapsulates each property as a get/set method.
   */
  private Bean getBean() {
    if (bean == null) {
      bean = new Bean(this);
    }
    return bean;
  }

  @Deprecated
  public void setIndentation(int indentation) {
    this.config = config.withIndentation(indentation);
  }

  /**
   * Prints the property settings of this pretty-writer to a writer.
   *
   * @param pw           Writer
   * @param omitDefaults Whether to omit properties whose value is the same as
   *                     the default
   */
  public void describe(PrintWriter pw, boolean omitDefaults) {
    final Bean properties = getBean();
    final String[] propertyNames = properties.getPropertyNames();
    int count = 0;
    for (String key : propertyNames) {
      final Object value = properties.get(key);
      final Object defaultValue = DEFAULT_BEAN.get(key);
      if (Objects.equals(value, defaultValue)) {
        continue;
      }
      if (count++ > 0) {
        pw.print(",");
      }
      pw.print(key + "=" + value);
    }
  }

  /**
   * Sets settings from a properties object.
   */
  public void setSettings(Properties properties) {
    resetSettings();
    final Bean bean = getBean();
    final String[] propertyNames = bean.getPropertyNames();
    for (String propertyName : propertyNames) {
      final String value = properties.getProperty(propertyName);
      if (value != null) {
        bean.set(propertyName, value);
      }
    }
  }

  @Deprecated
  public void setClauseStartsLine(boolean clauseStartsLine) {
    this.config = config.withClauseStartsLine(clauseStartsLine);
  }

  @Deprecated
  public void setSelectListItemsOnSeparateLines(boolean b) {
    this.config = config.withSelectListItemsOnSeparateLines(b);
  }

  @Deprecated
  public void setSelectListExtraIndentFlag(boolean selectListExtraIndentFlag) {
    this.config =
        config.withSelectListExtraIndentFlag(selectListExtraIndentFlag);
  }

  @Deprecated
  public void setKeywordsLowerCase(boolean keywordsLowerCase) {
    this.config = config.withKeywordsLowerCase(keywordsLowerCase);
  }

  @Deprecated
  public void setWhereListItemsOnSeparateLines(
      boolean whereListItemsOnSeparateLines) {
    this.config =
        config.withWhereListItemsOnSeparateLines(whereListItemsOnSeparateLines);
  }

  @Deprecated
  public void setAlwaysUseParentheses(boolean alwaysUseParentheses) {
    this.config = config.withAlwaysUseParentheses(alwaysUseParentheses);
  }

  @Override public void newlineAndIndent() {
    newlineAndIndent(currentIndent);
  }

  public void newlineAndIndent(int indent) {
    buf.append(NL);
    lineStart = buf.length();
    indent(indent);
    setNeedWhitespace(false); // no further whitespace necessary
  }

  void indent(int indent) {
    if (indent < 0) {
      throw new IllegalArgumentException("negative indent " + indent);
    }
    Spaces.append(buf, indent);
  }

  @Deprecated
  public void setQuoteAllIdentifiers(boolean quoteAllIdentifiers) {
    this.config = config.withQuoteAllIdentifiers(quoteAllIdentifiers);
  }

  /**
   * Creates a list frame.
   *
   * <p>Derived classes should override this method to specify the indentation
   * of the list.
   *
   * @param frameType What type of list
   * @param keyword   The keyword to be printed at the start of the list
   * @param open      The string to print at the start of the list
   * @param close     The string to print at the end of the list
   * @return A frame
   */
  protected FrameImpl createListFrame(
      FrameType frameType,
      @Nullable String keyword,
      String open,
      String close) {
    final FrameTypeEnum frameTypeEnum =
        frameType instanceof FrameTypeEnum ? (FrameTypeEnum) frameType
            : FrameTypeEnum.OTHER;
    final int indentation = config.indentation();
    boolean newlineAfterOpen = false;
    boolean newlineBeforeSep = false;
    boolean newlineAfterSep = false;
    boolean newlineBeforeClose = false;
    int left = column();
    int sepIndent = indentation;
    int extraIndent = 0;
    final SqlWriterConfig.LineFolding fold = fold(frameTypeEnum);
    final boolean newline = fold == SqlWriterConfig.LineFolding.TALL;

    switch (frameTypeEnum) {
    case SELECT:
      extraIndent = indentation;
      newlineAfterOpen = false;
      newlineBeforeSep = config.clauseStartsLine(); // newline before FROM, WHERE etc.
      newlineAfterSep = false;
      sepIndent = 0; // all clauses appear below SELECT
      break;

    case SETOP:
      extraIndent = 0;
      newlineAfterOpen = false;
      newlineBeforeSep = config.clauseStartsLine(); // newline before UNION, EXCEPT
      newlineAfterSep = config.clauseStartsLine(); // newline after UNION, EXCEPT
      sepIndent = 0; // all clauses appear below SELECT
      break;

    case SELECT_LIST:
    case FROM_LIST:
    case JOIN:
    case GROUP_BY_LIST:
    case ORDER_BY_LIST:
    case WINDOW_DECL_LIST:
    case VALUES:
      if (config.selectListExtraIndentFlag()) {
        extraIndent = indentation;
      }
      left = frame == null ? 0 : frame.left;
      newlineAfterOpen = config.clauseEndsLine()
          && (fold == SqlWriterConfig.LineFolding.TALL
              || fold == SqlWriterConfig.LineFolding.STEP);
      newlineBeforeSep = false;
      newlineAfterSep = newline;
      if (config.leadingComma() && newline) {
        newlineBeforeSep = true;
        newlineAfterSep = false;
        sepIndent = -", ".length();
      }
      break;

    case WHERE_LIST:
    case WINDOW:
      extraIndent = indentation;
      newlineAfterOpen = newline && config.clauseEndsLine();
      newlineBeforeSep = newline;
      sepIndent = 0;
      newlineAfterSep = false;
      break;

    case ORDER_BY:
    case OFFSET:
    case FETCH:
      newlineAfterOpen = false;
      newlineBeforeSep = true;
      sepIndent = 0;
      newlineAfterSep = false;
      break;

    case UPDATE_SET_LIST:
      extraIndent = indentation;
      newlineAfterOpen = newline;
      newlineBeforeSep = false;
      sepIndent = 0;
      newlineAfterSep = newline;
      break;

    case CASE:
      newlineAfterOpen = newline;
      newlineBeforeSep = newline;
      newlineBeforeClose = newline;
      sepIndent = 0;
      break;

    default:
      break;
    }

    final int chopColumn;
    SqlWriterConfig.LineFolding lineFolding = config.lineFolding();
    if (lineFolding == null) {
      lineFolding = SqlWriterConfig.LineFolding.WIDE;
      chopColumn = -1;
    } else {
      if (config.foldLength() > 0
          && (lineFolding == SqlWriterConfig.LineFolding.CHOP
              || lineFolding == SqlWriterConfig.LineFolding.FOLD
              || lineFolding == SqlWriterConfig.LineFolding.STEP)) {
        chopColumn = left + config.foldLength();
      } else {
        chopColumn = -1;
      }
    }

    switch (frameTypeEnum) {
    case UPDATE_SET_LIST:
    case WINDOW_DECL_LIST:
    case VALUES:
    case SELECT:
    case SETOP:
    case SELECT_LIST:
    case WHERE_LIST:
    case ORDER_BY_LIST:
    case GROUP_BY_LIST:
    case WINDOW:
    case ORDER_BY:
    case OFFSET:
    case FETCH:
      return new FrameImpl(frameType, keyword, open, close,
          left, extraIndent, chopColumn, lineFolding, newlineAfterOpen,
          newlineBeforeSep, sepIndent, newlineAfterSep, false, false);

    case SUB_QUERY:
      switch (config.subQueryStyle()) {
      case BLACK:
        // Generate, e.g.:
        //
        // WHERE foo = bar IN
        // (   SELECT ...
        open = Spaces.padRight("(", indentation - 1);
        return new FrameImpl(frameType, keyword, open, close,
            left, 0, chopColumn, lineFolding, false, true, indentation,
            false, false, false) {
          protected void _before() {
            newlineAndIndent();
          }
        };

      case HYDE:
        // Generate, e.g.:
        //
        // WHERE foo IN (
        //     SELECT ...
        return new FrameImpl(frameType, keyword, open, close, left, 0,
            chopColumn, lineFolding, false, true, 0, false, false, false) {
          protected void _before() {
            nextWhitespace = NL;
          }
        };

      default:
        throw Util.unexpected(config.subQueryStyle());
      }

    case FUN_CALL:
      setNeedWhitespace(false);
      return new FrameImpl(frameType, keyword, open, close,
          left, indentation, chopColumn, lineFolding, false, false,
          indentation, false, false, false);

    case PARENTHESES:
      open = "(";
      close = ")";
      // fall through
    case IDENTIFIER:
    case SIMPLE:
      return new FrameImpl(frameType, keyword, open, close,
          left, indentation, chopColumn, lineFolding, false, false,
          indentation, false, false, false);

    case FROM_LIST:
    case JOIN:
      newlineBeforeSep = newline;
      sepIndent = 0; // all clauses appear below SELECT
      newlineAfterSep = false;
      final boolean newlineBeforeComma = config.leadingComma() && newline;
      final boolean newlineAfterComma = !config.leadingComma() && newline;
      return new FrameImpl(frameType, keyword, open, close, left,
          indentation, chopColumn, lineFolding, newlineAfterOpen,
          newlineBeforeSep, sepIndent, newlineAfterSep, false, false) {
        @Override protected void sep(boolean printFirst, String sep) {
          final boolean newlineBeforeSep;
          final boolean newlineAfterSep;
          if (sep.equals(",")) {
            newlineBeforeSep = newlineBeforeComma;
            newlineAfterSep = newlineAfterComma;
          } else {
            newlineBeforeSep = this.newlineBeforeSep;
            newlineAfterSep = this.newlineAfterSep;
          }
          if (itemCount == 0) {
            if (newlineAfterOpen) {
              newlineAndIndent(currentIndent);
            }
          } else {
            if (newlineBeforeSep) {
              newlineAndIndent(currentIndent + sepIndent);
            }
          }
          if ((itemCount > 0) || printFirst) {
            keyword(sep);
            nextWhitespace = newlineAfterSep ? NL : " ";
          }
          ++itemCount;
        }
      };

    default:
    case OTHER:
      return new FrameImpl(frameType, keyword, open, close, left, indentation,
          chopColumn, lineFolding, newlineAfterOpen, newlineBeforeSep,
          sepIndent, newlineAfterSep, newlineBeforeClose, false);
    }
  }

  private SqlWriterConfig.LineFolding fold(FrameTypeEnum frameType) {
    switch (frameType) {
    case SELECT_LIST:
      return f3(config.selectFolding(), config.lineFolding(),
          config.selectListItemsOnSeparateLines());
    case GROUP_BY_LIST:
      return f3(config.groupByFolding(), config.lineFolding(),
          config.selectListItemsOnSeparateLines());
    case ORDER_BY_LIST:
      return f3(config.orderByFolding(), config.lineFolding(),
          config.selectListItemsOnSeparateLines());
    case UPDATE_SET_LIST:
      return f3(config.updateSetFolding(), config.lineFolding(),
          config.updateSetListNewline());
    case WHERE_LIST:
      return f3(config.whereFolding(), config.lineFolding(),
          config.whereListItemsOnSeparateLines());
    case WINDOW_DECL_LIST:
      return f3(config.windowFolding(), config.lineFolding(),
          config.clauseStartsLine() && config.windowDeclListNewline());
    case WINDOW:
      return f3(config.overFolding(), config.lineFolding(),
          config.windowNewline());
    case VALUES:
      return f3(config.valuesFolding(), config.lineFolding(),
          config.valuesListNewline());
    case FROM_LIST:
    case JOIN:
      return f3(config.fromFolding(), config.lineFolding(),
          config.caseClausesOnNewLines());
    case CASE:
      return f3(null, null, config.caseClausesOnNewLines());
    default:
      return SqlWriterConfig.LineFolding.WIDE;
    }
  }

  private static SqlWriterConfig.LineFolding f3(SqlWriterConfig.@Nullable LineFolding folding0,
      SqlWriterConfig.@Nullable LineFolding folding1, boolean opt) {
    return folding0 != null ? folding0
        : folding1 != null ? folding1
            : opt ? SqlWriterConfig.LineFolding.TALL
                : SqlWriterConfig.LineFolding.WIDE;
  }

  /**
   * Starts a list.
   *
   * @param frameType Type of list. For example, a SELECT list will be
   *                  governed according to SELECT-list formatting preferences.
   * @param open      String to print at the start of the list; typically "(" or
   *                  the empty string.
   * @param close     String to print at the end of the list.
   */
  protected Frame startList(
      FrameType frameType,
      @Nullable String keyword,
      String open,
      String close) {
    assert frameType != null;
    FrameImpl frame = this.frame;
    if (frame != null) {
      if (frame.itemCount++ == 0 && frame.newlineAfterOpen) {
        newlineAndIndent();
      } else if (frameType == FrameTypeEnum.SUB_QUERY
          && config.subQueryStyle() == SubQueryStyle.BLACK) {
        newlineAndIndent(currentIndent - frame.extraIndent);
      }

      // REVIEW jvs 9-June-2006:  This is part of the fix for FRG-149
      // (extra frame for identifier was leading to extra indentation,
      // causing select list to come out raggedy with identifiers
      // deeper than literals); are there other frame types
      // for which extra indent should be suppressed?
      currentIndent += frame.extraIndent(frameType);
      assert !listStack.contains(frame);
      listStack.push(frame);
    }
    frame = createListFrame(frameType, keyword, open, close);
    this.frame = frame;
    frame.before();
    return frame;
  }

  @Override public void endList(@Nullable Frame frame) {
    FrameImpl endedFrame = (FrameImpl) frame;
    Preconditions.checkArgument(frame == this.frame,
        "Frame does not match current frame");
    if (endedFrame == null) {
      throw new RuntimeException("No list started");
    }
    if (endedFrame.open.equals("(")) {
      if (!endedFrame.close.equals(")")) {
        throw new RuntimeException("Expected ')'");
      }
    }
    if (endedFrame.newlineBeforeClose) {
      newlineAndIndent();
    }
    keyword(endedFrame.close);
    if (endedFrame.newlineAfterClose) {
      newlineAndIndent();
    }

    // Pop the frame, and move to the previous indentation level.
    if (listStack.isEmpty()) {
      this.frame = null;
      assert currentIndent == 0 : currentIndent;
    } else {
      this.frame = listStack.pop();
      currentIndent -= this.frame.extraIndent(endedFrame.frameType);
    }
  }

  public String format(SqlNode node) {
    assert frame == null;
    node.unparse(this, 0, 0);
    assert frame == null;
    return toString();
  }

  @Override public String toString() {
    return buf.toString();
  }

  @Override public SqlString toSqlString() {
    ImmutableList<Integer> dynamicParameters =
        this.dynamicParameters == null ? null : this.dynamicParameters.build();
    return new SqlString(dialect, toString(), dynamicParameters);
  }

  @Override public SqlDialect getDialect() {
    return dialect;
  }

  @Override public void literal(String s) {
    print(s);
    setNeedWhitespace(true);
  }

  @Override public void keyword(String s) {
    maybeWhitespace(s);
    buf.append(
        isKeywordsLowerCase()
            ? s.toLowerCase(Locale.ROOT)
            : s.toUpperCase(Locale.ROOT));
    if (!s.equals("")) {
      setNeedWhitespace(needWhitespaceAfter(s));
    }
  }

  private void maybeWhitespace(String s) {
    if (tooLong(s) || (needWhitespace && needWhitespaceBefore(s))) {
      whiteSpace();
    }
  }

  private static boolean needWhitespaceBefore(String s) {
    return !(s.equals(",")
        || s.equals(".")
        || s.equals(")")
        || s.equals("[")
        || s.equals("]")
        || s.equals(""));
  }

  private static boolean needWhitespaceAfter(String s) {
    return !(s.equals("(")
        || s.equals("[")
        || s.equals("."));
  }

  protected void whiteSpace() {
    if (needWhitespace) {
      if (NL.equals(nextWhitespace)) {
        newlineAndIndent();
      } else {
        buf.append(nextWhitespace);
      }
      nextWhitespace = " ";
      setNeedWhitespace(false);
    }
  }

  /** Returns the number of characters appended since the last newline. */
  private int column() {
    return buf.length() - lineStart;
  }

  protected boolean tooLong(String s) {
    final int lineLength = config.lineLength();
    boolean result =
        lineLength > 0
            && (column() > currentIndent)
            && ((column() + s.length()) >= lineLength);
    if (result) {
      nextWhitespace = NL;
    }
    LOGGER.trace("Token is '{}'; result is {}", s, result);
    return result;
  }

  @Override public void print(String s) {
    maybeWhitespace(s);
    buf.append(s);
  }

  @Override public void print(int x) {
    maybeWhitespace("0");
    buf.append(x);
  }

  @Override public void identifier(String name, boolean quoted) {
    // If configured globally or the original identifier is quoted,
    // then quotes the identifier.
    maybeWhitespace(name);
    if (isQuoteAllIdentifiers() || quoted) {
      dialect.quoteIdentifier(buf, name);
    } else {
      buf.append(name);
    }
    setNeedWhitespace(true);
  }

  @Override public void dynamicParam(int index) {
    if (dynamicParameters == null) {
      dynamicParameters = ImmutableList.builder();
    }
    dynamicParameters.add(index);
    print("?");
    setNeedWhitespace(true);
  }

  @Override public void fetchOffset(@Nullable SqlNode fetch, @Nullable SqlNode offset) {
    if (fetch == null && offset == null) {
      return;
    }
    dialect.unparseOffsetFetch(this, offset, fetch);
  }

  @Override public void topN(@Nullable SqlNode fetch, @Nullable SqlNode offset) {
    if (fetch == null && offset == null) {
      return;
    }
    dialect.unparseTopN(this, offset, fetch);
  }

  @Override public Frame startFunCall(String funName) {
    keyword(funName);
    setNeedWhitespace(false);
    return startList(FrameTypeEnum.FUN_CALL, "(", ")");
  }

  @Override public void endFunCall(Frame frame) {
    endList(this.frame);
  }

  @Override public Frame startList(String open, String close) {
    return startList(FrameTypeEnum.SIMPLE, null, open, close);
  }

  @Override public Frame startList(FrameTypeEnum frameType) {
    assert frameType != null;
    return startList(frameType, null, "", "");
  }

  @Override public Frame startList(FrameType frameType, String open, String close) {
    assert frameType != null;
    return startList(frameType, null, open, close);
  }

  @Override public SqlWriter list(FrameTypeEnum frameType, Consumer<SqlWriter> action) {
    final SqlWriter.Frame selectListFrame =
        startList(SqlWriter.FrameTypeEnum.SELECT_LIST);
    final SqlWriter w = this;
    action.accept(w);
    endList(selectListFrame);
    return this;
  }

  @Override public SqlWriter list(FrameTypeEnum frameType, SqlBinaryOperator sepOp,
      SqlNodeList list) {
    final SqlWriter.Frame frame = startList(frameType);
    ((FrameImpl) frame).list(list, sepOp);
    endList(frame);
    return this;
  }

  @Override public void sep(String sep) {
    sep(sep, !(sep.equals(",") || sep.equals(".")));
  }

  @Override public void sep(String sep, boolean printFirst) {
    if (frame == null) {
      throw new RuntimeException("No list started");
    }
    if (sep.startsWith(" ") || sep.endsWith(" ")) {
      throw new RuntimeException("Separator must not contain whitespace");
    }
    frame.sep(printFirst, sep);
  }

  @Override public void setNeedWhitespace(boolean needWhitespace) {
    this.needWhitespace = needWhitespace;
  }

  @Deprecated
  public void setLineLength(int lineLength) {
    this.config = config.withLineLength(lineLength);
  }

  public void setFormatOptions(@Nullable SqlFormatOptions options) {
    if (options == null) {
      return;
    }
    setAlwaysUseParentheses(options.isAlwaysUseParentheses());
    setCaseClausesOnNewLines(options.isCaseClausesOnNewLines());
    setClauseStartsLine(options.isClauseStartsLine());
    setKeywordsLowerCase(options.isKeywordsLowercase());
    setQuoteAllIdentifiers(options.isQuoteAllIdentifiers());
    setSelectListItemsOnSeparateLines(
        options.isSelectListItemsOnSeparateLines());
    setWhereListItemsOnSeparateLines(
        options.isWhereListItemsOnSeparateLines());
    setWindowNewline(options.isWindowDeclarationStartsLine());
    setWindowDeclListNewline(options.isWindowListItemsOnSeparateLines());
    setIndentation(options.getIndentation());
    setLineLength(options.getLineLength());
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Implementation of {@link org.apache.calcite.sql.SqlWriter.Frame}.
   */
  protected class FrameImpl implements Frame {
    final FrameType frameType;
    final @Nullable String keyword;
    final String open;
    final String close;

    private final int left;
    /**
     * Indent of sub-frame with respect to this one.
     */
    final int extraIndent;

    /**
     * Indent of separators with respect to this frame's indent. Typically
     * zero.
     */
    final int sepIndent;

    /**
     * Number of items which have been printed in this list so far.
     */
    int itemCount;

    /**
     * Whether to print a newline before each separator.
     */
    public final boolean newlineBeforeSep;

    /**
     * Whether to print a newline after each separator.
     */
    public final boolean newlineAfterSep;
    protected final boolean newlineBeforeClose;
    protected final boolean newlineAfterClose;
    protected final boolean newlineAfterOpen;

    /** Character count after which we should move an item to the
     * next line. Or {@link Integer#MAX_VALUE} if we are not chopping. */
    private final int chopLimit;

    /** How lines are to be folded. */
    private final SqlWriterConfig.LineFolding lineFolding;

    FrameImpl(FrameType frameType, @Nullable String keyword, String open, String close,
        int left, int extraIndent, int chopLimit,
        SqlWriterConfig.LineFolding lineFolding, boolean newlineAfterOpen,
        boolean newlineBeforeSep, int sepIndent, boolean newlineAfterSep,
        boolean newlineBeforeClose, boolean newlineAfterClose) {
      this.frameType = frameType;
      this.keyword = keyword;
      this.open = open;
      this.close = close;
      this.left = left;
      this.extraIndent = extraIndent;
      this.chopLimit = chopLimit;
      this.lineFolding = lineFolding;
      this.newlineAfterOpen = newlineAfterOpen;
      this.newlineBeforeSep = newlineBeforeSep;
      this.newlineAfterSep = newlineAfterSep;
      this.newlineBeforeClose = newlineBeforeClose;
      this.newlineAfterClose = newlineAfterClose;
      this.sepIndent = sepIndent;
      assert chopLimit >= 0
          == (lineFolding == SqlWriterConfig.LineFolding.CHOP
          || lineFolding == SqlWriterConfig.LineFolding.FOLD
          || lineFolding == SqlWriterConfig.LineFolding.STEP);
    }

    protected void before() {
      if ((open != null) && !open.equals("")) {
        keyword(open);
      }
    }

    protected void after() {
    }

    protected void sep(boolean printFirst, String sep) {
      if (itemCount == 0) {
        if (newlineAfterOpen) {
          newlineAndIndent(currentIndent);
        }
      } else {
        if (newlineBeforeSep) {
          newlineAndIndent(currentIndent + sepIndent);
        }
      }
      if ((itemCount > 0) || printFirst) {
        keyword(sep);
        nextWhitespace = newlineAfterSep ? NL : " ";
      }
      ++itemCount;
    }

    /** Returns the extra indent required for a given type of sub-frame. */
    int extraIndent(FrameType subFrameType) {
      if (this.frameType == FrameTypeEnum.ORDER_BY
          && subFrameType == FrameTypeEnum.ORDER_BY_LIST) {
        return config.indentation();
      }
      if (subFrameType.needsIndent()) {
        return extraIndent;
      }
      return 0;
    }

    void list(SqlNodeList list, SqlBinaryOperator sepOp) {
      final Save save;
      switch (lineFolding) {
      case CHOP:
      case FOLD:
        save = new Save();
        if (!list2(list, sepOp)) {
          save.restore();
          final boolean newlineAfterOpen = config.clauseEndsLine();
          final SqlWriterConfig.LineFolding lineFolding;
          final int chopLimit;
          if (this.lineFolding == SqlWriterConfig.LineFolding.CHOP) {
            lineFolding = SqlWriterConfig.LineFolding.TALL;
            chopLimit = -1;
          } else {
            lineFolding = SqlWriterConfig.LineFolding.FOLD;
            chopLimit = this.chopLimit;
          }
          final boolean newline =
              lineFolding == SqlWriterConfig.LineFolding.TALL;
          final boolean newlineBeforeSep;
          final boolean newlineAfterSep;
          final int sepIndent;
          if (config.leadingComma() && newline) {
            newlineBeforeSep = true;
            newlineAfterSep = false;
            sepIndent = -", ".length();
          } else if (newline) {
            newlineBeforeSep = false;
            newlineAfterSep = true;
            sepIndent = this.sepIndent;
          } else {
            newlineBeforeSep = false;
            newlineAfterSep = false;
            sepIndent = this.sepIndent;
          }
          final FrameImpl frame2 =
              new FrameImpl(frameType, keyword, open, close, left, extraIndent,
                  chopLimit, lineFolding, newlineAfterOpen, newlineBeforeSep,
                  sepIndent, newlineAfterSep, newlineBeforeClose,
                  newlineAfterClose);
          frame2.list2(list, sepOp);
        }
        break;
      default:
        list2(list, sepOp);
      }
    }

    /** Tries to write a list. If the line runs too long, returns false,
     * indicating to retry. */
    private boolean list2(SqlNodeList list, SqlBinaryOperator sepOp) {
      // The precedence pulling on the LHS of a node is the
      // right-precedence of the separator operator. Similarly RHS.
      //
      // At the start and end of the list precedence should be 0, but non-zero
      // precedence is useful, because it forces parentheses around
      // sub-queries and empty lists, e.g. "SELECT a, (SELECT * FROM t), b",
      // "GROUP BY ()".
      final int lprec = sepOp.getRightPrec();
      final int rprec = sepOp.getLeftPrec();
      if (chopLimit < 0) {
        for (int i = 0; i < list.size(); i++) {
          SqlNode node = list.get(i);
          sep(false, sepOp.getName());
          node.unparse(SqlPrettyWriter.this, lprec, rprec);
        }
      } else if (newlineBeforeSep) {
        for (int i = 0; i < list.size(); i++) {
          SqlNode node = list.get(i);
          sep(false, sepOp.getName());
          final Save prevSize = new Save();
          node.unparse(SqlPrettyWriter.this, lprec, rprec);
          if (column() > chopLimit) {
            if (lineFolding == SqlWriterConfig.LineFolding.CHOP
                || lineFolding == SqlWriterConfig.LineFolding.TALL) {
              return false;
            }
            prevSize.restore();
            newlineAndIndent();
            node.unparse(SqlPrettyWriter.this, lprec, rprec);
          }
        }
      } else {
        for (int i = 0; i < list.size(); i++) {
          SqlNode node = list.get(i);
          if (i == 0) {
            sep(false, sepOp.getName());
          }
          final Save save = new Save();
          node.unparse(SqlPrettyWriter.this, lprec, rprec);
          if (i + 1 < list.size()) {
            sep(false, sepOp.getName());
          }
          if (column() > chopLimit) {
            switch (lineFolding) {
            case CHOP:
              return false;
            case FOLD:
              if (newlineAfterOpen != config.clauseEndsLine()) {
                return false;
              }
              break;
            default:
              break;
            }
            save.restore();
            newlineAndIndent();
            node.unparse(SqlPrettyWriter.this, lprec, rprec);
            if (i + 1 < list.size()) {
              sep(false, sepOp.getName());
            }
          }
        }
      }
      return true;
    }

    /** Remembers the state of the current frame and writer.
     *
     * <p>You can call {@link #restore} to restore to that state, or just
     * continue. It is useful if you wish to re-try with different options
     * (for example, with lines wrapped). */
    class Save {
      final int bufLength;

      Save() {
        this.bufLength = buf.length();
      }

      void restore() {
        buf.setLength(bufLength);
      }
    }
  }

  /**
   * Helper class which exposes the get/set methods of an object as
   * properties.
   */
  private static class Bean {
    private final SqlPrettyWriter o;
    private final Map<String, Method> getterMethods = new HashMap<>();
    private final Map<String, Method> setterMethods = new HashMap<>();

    Bean(SqlPrettyWriter o) {
      this.o = o;

      // Figure out the getter/setter methods for each attribute.
      for (Method method : o.getClass().getMethods()) {
        if (method.getName().startsWith("set")
            && (method.getReturnType() == Void.class)
            && (method.getParameterCount() == 1)) {
          String attributeName =
              stripPrefix(
                  method.getName(),
                  3);
          setterMethods.put(attributeName, method);
        }
        if (method.getName().startsWith("get")
            && (method.getReturnType() != Void.class)
            && (method.getParameterCount() == 0)) {
          String attributeName =
              stripPrefix(
                  method.getName(),
                  3);
          getterMethods.put(attributeName, method);
        }
        if (method.getName().startsWith("is")
            && (method.getReturnType() == Boolean.class)
            && (method.getParameterCount() == 0)) {
          String attributeName =
              stripPrefix(
                  method.getName(),
                  2);
          getterMethods.put(attributeName, method);
        }
      }
    }

    private static String stripPrefix(String name, int offset) {
      return name.substring(offset, offset + 1).toLowerCase(Locale.ROOT)
          + name.substring(offset + 1);
    }

    public void set(String name, String value) {
      final Method method = requireNonNull(
          setterMethods.get(name),
          () -> "setter method " + name + " not found"
      );
      try {
        method.invoke(o, value);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }

    public @Nullable Object get(String name) {
      final Method method = requireNonNull(
          getterMethods.get(name),
          () -> "getter method " + name + " not found"
      );
      try {
        return method.invoke(o);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }

    public String[] getPropertyNames() {
      final Set<String> names = new HashSet<>();
      names.addAll(getterMethods.keySet());
      names.addAll(setterMethods.keySet());
      return names.toArray(new String[0]);
    }
  }

}
