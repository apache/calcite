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
package org.apache.calcite.sql.advise;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * An assistant which offers hints and corrections to a partially-formed SQL
 * statement. It is used in the SQL editor user-interface.
 */
public class SqlAdvisor {
  //~ Static fields/initializers ---------------------------------------------

  public static final Logger LOGGER = CalciteTrace.PARSER_LOGGER;
  private static final String HINT_TOKEN = "_suggest_";
  private static final String UPPER_HINT_TOKEN =
      HINT_TOKEN.toUpperCase(Locale.ROOT);

  //~ Instance fields --------------------------------------------------------

  // Flags indicating precision/scale combinations
  private final SqlValidatorWithHints validator;
  private final SqlParser.Config parserConfig;

  // Cache for getPreferredCasing
  private String prevWord;
  private Casing prevPreferredCasing;

  // Reserved words cache
  private Set<String> reservedWordsSet;
  private List<String> reservedWordsList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlAdvisor with a validator instance
   *
   * @param validator Validator
   * @deprecated use {@link #SqlAdvisor(SqlValidatorWithHints, SqlParser.Config)}
   */
  @Deprecated
  public SqlAdvisor(
      SqlValidatorWithHints validator) {
    this(validator, SqlParser.Config.DEFAULT);
  }

  /**
   * Creates a SqlAdvisor with a validator instance and given parser configuration
   *
   * @param validator Validator
   * @param parserConfig parser config
   */
  public SqlAdvisor(
      SqlValidatorWithHints validator,
      SqlParser.Config parserConfig) {
    this.validator = validator;
    this.parserConfig = parserConfig;
  }

  //~ Methods ----------------------------------------------------------------

  private char quoteStart() {
    return parserConfig.quoting().string.charAt(0);
  }

  private char quoteEnd() {
    char quote = quoteStart();
    return quote == '[' ? ']' : quote;
  }

  /**
   * Gets completion hints for a partially completed or syntactically incorrect
   * sql statement with cursor pointing to the position where completion hints
   * are requested.
   *
   * <p>Writes into <code>replaced[0]</code> the string that is being
   * replaced. Includes the cursor and the preceding identifier. For example,
   * if <code>sql</code> is "select abc^de from t", sets <code>
   * replaced[0]</code> to "abc". If the cursor is in the middle of
   * whitespace, the replaced string is empty. The replaced string is never
   * null.
   *
   * @param sql      A partial or syntactically incorrect sql statement for
   *                 which to retrieve completion hints
   * @param cursor   to indicate the 0-based cursor position in the query at
   * @param replaced String which is being replaced (output)
   * @return completion hints
   */
  public List<SqlMoniker> getCompletionHints(
      String sql,
      int cursor,
      String[] replaced) {
    // search backward starting from current position to find a "word"
    int wordStart = cursor;
    boolean quoted = false;
    while (wordStart > 0
        && Character.isJavaIdentifierPart(sql.charAt(wordStart - 1))) {
      --wordStart;
    }
    if ((wordStart > 0)
        && (sql.charAt(wordStart - 1) == quoteStart())) {
      quoted = true;
      --wordStart;
    }

    if (wordStart < 0) {
      return Collections.emptyList();
    }

    // Search forwards to the end of the word we should remove. Eat up
    // trailing double-quote, if any
    int wordEnd = cursor;
    while (wordEnd < sql.length()
        && Character.isJavaIdentifierPart(sql.charAt(wordEnd))) {
      ++wordEnd;
    }
    if (quoted
        && (wordEnd < sql.length())
        && (sql.charAt(wordEnd) == quoteEnd())) {
      ++wordEnd;
    }

    // remove the partially composed identifier from the
    // sql statement - otherwise we get a parser exception
    String word = replaced[0] = sql.substring(wordStart, cursor);
    if (wordStart < wordEnd) {
      sql =
          sql.substring(0, wordStart)
              + sql.substring(wordEnd);
    }

    final List<SqlMoniker> completionHints =
        getCompletionHints0(sql, wordStart);

    if (quoted) {
      word = word.substring(1);
    }

    if (word.isEmpty()) {
      return completionHints;
    }

    // If cursor was part of the way through a word, only include hints
    // which start with that word in the result.
    final List<SqlMoniker> result = new ArrayList<>();
    Casing preferredCasing = getPreferredCasing(word);

    boolean ignoreCase = preferredCasing != Casing.UNCHANGED;
    for (SqlMoniker hint : completionHints) {
      List<String> names = hint.getFullyQualifiedNames();
      // For now we treat only simple cases where the added name is the last
      // See [CALCITE-2439] Smart complete for SqlAdvisor
      String cname = Util.last(names);
      if (cname.regionMatches(ignoreCase, 0, word, 0, word.length())) {
        result.add(hint);
      }
    }

    return result;
  }

  public List<SqlMoniker> getCompletionHints0(String sql, int cursor) {
    String simpleSql = simplifySql(sql, cursor);
    int idx = simpleSql.indexOf(HINT_TOKEN);
    if (idx < 0) {
      return Collections.emptyList();
    }
    SqlParserPos pos = new SqlParserPos(1, idx + 1);
    return getCompletionHints(simpleSql, pos);
  }

  /**
   * Returns casing which is preferred for replacement.
   * For instance, {@code en => ename, EN => ENAME}.
   * When input has mixed case, {@code Casing.UNCHANGED} is returned.
   * @param word input word
   * @return preferred casing when replacing input word
   */
  private Casing getPreferredCasing(String word) {
    if (word == prevWord) {
      return prevPreferredCasing;
    }
    boolean hasLower = false;
    boolean hasUpper = false;
    int i = 0;
    while (i < word.length() && !(hasLower && hasUpper)) {
      int codePoint = word.codePointAt(i);
      hasLower |= Character.isLowerCase(codePoint);
      hasUpper |= Character.isUpperCase(codePoint);
      i += Character.charCount(codePoint);
    }
    Casing preferredCasing;
    if (hasUpper && !hasLower) {
      preferredCasing = Casing.TO_UPPER;
    } else if (!hasUpper && hasLower) {
      preferredCasing = Casing.TO_LOWER;
    } else {
      preferredCasing = Casing.UNCHANGED;
    }
    prevWord = word;
    prevPreferredCasing = preferredCasing;
    return preferredCasing;
  }

  public String getReplacement(SqlMoniker hint, String word) {
    Casing preferredCasing = getPreferredCasing(word);
    boolean quoted = !word.isEmpty() && word.charAt(0) == quoteStart();
    return getReplacement(hint, quoted, preferredCasing);
  }

  public String getReplacement(SqlMoniker hint, boolean quoted, Casing preferredCasing) {
    String name = Util.last(hint.getFullyQualifiedNames());
    boolean isKeyword = hint.getType() == SqlMonikerType.KEYWORD;
    // If replacement has mixed case, we need to quote it (or not depending
    // on quotedCasing/unquotedCasing
    quoted &= !isKeyword;

    if (!quoted && !isKeyword && getReservedAndKeyWordsSet().contains(name)) {
      quoted = true;
    }

    StringBuilder sb =
        new StringBuilder(name.length() + (quoted ? 2 : 0));

    if (!isKeyword && !Util.isValidJavaIdentifier(name)) {
      // needs quotes ==> quoted
      quoted = true;
    }
    String idToAppend = name;

    if (!quoted) {
      // id ==preferredCasing==> preferredId ==unquotedCasing==> recasedId
      // if recasedId matches id, then use preferredId
      String preferredId = applyCasing(name, preferredCasing);
      if (isKeyword || matchesUnquoted(name, preferredId)) {
        idToAppend = preferredId;
      } else {
        // Check if we can use unquoted identifier as is: for instance, unquotedCasing==UNCHANGED
        quoted = !matchesUnquoted(name, idToAppend);
      }
    }
    if (quoted) {
      sb.append(quoteStart());
    }
    sb.append(idToAppend);
    if (quoted) {
      sb.append(quoteEnd());
    }

    return sb.toString();
  }

  private boolean matchesUnquoted(String name, String idToAppend) {
    String recasedId = applyCasing(idToAppend, parserConfig.unquotedCasing());
    return recasedId.regionMatches(!parserConfig.caseSensitive(), 0, name, 0, name.length());
  }

  private String applyCasing(String value, Casing casing) {
    return SqlParserUtil.strip(value, null, null, null, casing);
  }

  /**
   * Gets completion hints for a syntactically correct sql statement with dummy
   * SqlIdentifier
   *
   * @param sql A syntactically correct sql statement for which to retrieve
   *            completion hints
   * @param pos to indicate the line and column position in the query at which
   *            completion hints need to be retrieved. For example, "select
   *            a.ename, b.deptno from sales.emp a join sales.dept b "on
   *            a.deptno=b.deptno where empno=1"; setting pos to 'Line 1, Column
   *            17' returns all the possible column names that can be selected
   *            from sales.dept table setting pos to 'Line 1, Column 31' returns
   *            all the possible table names in 'sales' schema
   * @return an array of hints ({@link SqlMoniker}) that can fill in at the
   * indicated position
   */
  public List<SqlMoniker> getCompletionHints(String sql, SqlParserPos pos) {
    // First try the statement they gave us. If this fails, just return
    // the tokens which were expected at the failure point.
    List<SqlMoniker> hintList = new ArrayList<>();
    SqlNode sqlNode = tryParse(sql, hintList);
    if (sqlNode == null) {
      return hintList;
    }

    // Now construct a statement which is bound to fail. (Character 7 BEL
    // is not legal in any SQL statement.)
    final int x = pos.getColumnNum() - 1;
    sql = sql.substring(0, x)
        + " \07"
        + sql.substring(x);
    tryParse(sql, hintList);

    final SqlMoniker star =
        new SqlMonikerImpl(ImmutableList.of("*"), SqlMonikerType.KEYWORD);
    String hintToken =
        parserConfig.unquotedCasing() == Casing.TO_UPPER ? UPPER_HINT_TOKEN : HINT_TOKEN;
    if (hintList.contains(star) && !isSelectListItem(sqlNode, pos, hintToken)) {
      hintList.remove(star);
    }

    // Add the identifiers which are expected at the point of interest.
    try {
      validator.validate(sqlNode);
    } catch (Exception e) {
      // mask any exception that is thrown during the validation, i.e.
      // try to continue even if the sql is invalid. we are doing a best
      // effort here to try to come up with the requested completion
      // hints
      Util.swallow(e, LOGGER);
    }
    final List<SqlMoniker> validatorHints =
        validator.lookupHints(sqlNode, pos);
    hintList.addAll(validatorHints);
    return hintList;
  }

  private static boolean isSelectListItem(SqlNode root,
      final SqlParserPos pos, String hintToken) {
    List<SqlNode> nodes = SqlUtil.getAncestry(root,
        input -> input instanceof SqlIdentifier
            && ((SqlIdentifier) input).names.contains(hintToken),
        input -> Objects.requireNonNull(input).getParserPosition()
            .startsAt(pos));
    assert nodes.get(0) == root;
    nodes = Lists.reverse(nodes);
    return nodes.size() > 2
        && nodes.get(2) instanceof SqlSelect
        && nodes.get(1) == ((SqlSelect) nodes.get(2)).getSelectList();
  }

  /**
   * Tries to parse a SQL statement.
   *
   * <p>If succeeds, returns the parse tree node; if fails, populates the list
   * of hints and returns null.
   *
   * @param sql      SQL statement
   * @param hintList List of hints suggesting allowable tokens at the point of
   *                 failure
   * @return Parse tree if succeeded, null if parse failed
   */
  private SqlNode tryParse(String sql, List<SqlMoniker> hintList) {
    try {
      return parseQuery(sql);
    } catch (SqlParseException e) {
      for (String tokenName : e.getExpectedTokenNames()) {
        // Only add tokens which are keywords, like '"BY"'; ignore
        // symbols such as '<Identifier>'.
        if (tokenName.startsWith("\"")
            && tokenName.endsWith("\"")) {
          hintList.add(
              new SqlMonikerImpl(
                  tokenName.substring(1, tokenName.length() - 1),
                  SqlMonikerType.KEYWORD));
        }
      }
      return null;
    } catch (CalciteException e) {
      Util.swallow(e, null);
      return null;
    }
  }

  /**
   * Gets the fully qualified name for a {@link SqlIdentifier} at a given
   * position of a sql statement.
   *
   * @param sql    A syntactically correct sql statement for which to retrieve a
   *               fully qualified SQL identifier name
   * @param cursor to indicate the 0-based cursor position in the query that
   *               represents a SQL identifier for which its fully qualified
   *               name is to be returned.
   * @return a {@link SqlMoniker} that contains the fully qualified name of
   * the specified SQL identifier, returns null if none is found or the SQL
   * statement is invalid.
   */
  public SqlMoniker getQualifiedName(String sql, int cursor) {
    SqlNode sqlNode;
    try {
      sqlNode = parseQuery(sql);
      validator.validate(sqlNode);
    } catch (Exception e) {
      return null;
    }
    SqlParserPos pos = new SqlParserPos(1, cursor + 1);
    try {
      return validator.lookupQualifiedName(sqlNode, pos);
    } catch (CalciteContextException | AssertionError e) {
      return null;
    }
  }

  /**
   * Attempts to complete and validate a given partially completed sql
   * statement, and returns whether it is valid.
   *
   * @param sql A partial or syntactically incorrect sql statement to validate
   * @return whether SQL statement is valid
   */
  public boolean isValid(String sql) {
    SqlSimpleParser simpleParser = new SqlSimpleParser(HINT_TOKEN, parserConfig);
    String simpleSql = simpleParser.simplifySql(sql);
    SqlNode sqlNode;
    try {
      sqlNode = parseQuery(simpleSql);
    } catch (Exception e) {
      // if the sql can't be parsed we wont' be able to validate it
      return false;
    }
    try {
      validator.validate(sqlNode);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Attempts to parse and validate a SQL statement. Throws the first
   * exception encountered. The error message of this exception is to be
   * displayed on the UI
   *
   * @param sql A user-input sql statement to be validated
   * @return a List of ValidateErrorInfo (null if sql is valid)
   */
  public List<ValidateErrorInfo> validate(String sql) {
    SqlNode sqlNode;
    List<ValidateErrorInfo> errorList = new ArrayList<>();

    sqlNode = collectParserError(sql, errorList);
    if (!errorList.isEmpty()) {
      return errorList;
    }
    try {
      validator.validate(sqlNode);
    } catch (CalciteContextException e) {
      ValidateErrorInfo errInfo = new ValidateErrorInfo(e);

      // validator only returns 1 exception now
      errorList.add(errInfo);
      return errorList;
    } catch (Exception e) {
      ValidateErrorInfo errInfo =
          new ValidateErrorInfo(
              1,
              1,
              1,
              sql.length(),
              e.getMessage());

      // parser only returns 1 exception now
      errorList.add(errInfo);
      return errorList;
    }
    return null;
  }

  /**
   * Turns a partially completed or syntactically incorrect sql statement into
   * a simplified, valid one that can be passed into getCompletionHints()
   *
   * @param sql    A partial or syntactically incorrect sql statement
   * @param cursor to indicate column position in the query at which
   *               completion hints need to be retrieved.
   * @return a completed, valid (and possibly simplified SQL statement
   */
  public String simplifySql(String sql, int cursor) {
    SqlSimpleParser parser = new SqlSimpleParser(HINT_TOKEN, parserConfig);
    return parser.simplifySql(sql, cursor);
  }

  /**
   * Return an array of SQL reserved and keywords
   *
   * @return an of SQL reserved and keywords
   */
  public List<String> getReservedAndKeyWords() {
    ensureReservedAndKeyWords();
    return reservedWordsList;
  }

  private Set<String> getReservedAndKeyWordsSet() {
    ensureReservedAndKeyWords();
    return reservedWordsSet;
  }

  private void ensureReservedAndKeyWords() {
    if (reservedWordsSet != null) {
      return;
    }
    Collection<String> c = SqlAbstractParserImpl.getSql92ReservedWords();
    List<String> l =
        Arrays.asList(
            getParserMetadata().getJdbcKeywords().split(","));
    List<String> al = new ArrayList<>();
    al.addAll(c);
    al.addAll(l);
    reservedWordsList = al;
    reservedWordsSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    reservedWordsSet.addAll(reservedWordsList);
  }

  /**
   * Returns the underlying Parser metadata.
   *
   * <p>To use a different parser (recognizing a different dialect of SQL),
   * derived class should override.
   *
   * @return metadata
   */
  protected SqlAbstractParserImpl.Metadata getParserMetadata() {
    SqlParser parser = SqlParser.create("", parserConfig);
    return parser.getMetadata();
  }

  /**
   * Wrapper function to parse a SQL query (SELECT or VALUES, but not INSERT,
   * UPDATE, DELETE, CREATE, DROP etc.), throwing a {@link SqlParseException}
   * if the statement is not syntactically valid.
   *
   * @param sql SQL statement
   * @return parse tree
   * @throws SqlParseException if not syntactically valid
   */
  protected SqlNode parseQuery(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    return parser.parseStmt();
  }

  /**
   * Attempts to parse a SQL statement and adds to the errorList if any syntax
   * error is found. This implementation uses {@link SqlParser}. Subclass can
   * re-implement this with a different parser implementation
   *
   * @param sql       A user-input sql statement to be parsed
   * @param errorList A {@link List} of error to be added to
   * @return {@link SqlNode } that is root of the parse tree, null if the sql
   * is not valid
   */
  protected SqlNode collectParserError(
      String sql,
      List<ValidateErrorInfo> errorList) {
    try {
      return parseQuery(sql);
    } catch (SqlParseException e) {
      ValidateErrorInfo errInfo =
          new ValidateErrorInfo(
              e.getPos(),
              e.getMessage());

      // parser only returns 1 exception now
      errorList.add(errInfo);
      return null;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * An inner class that represents error message text and position info of a
   * validator or parser exception
   */
  public class ValidateErrorInfo {
    private int startLineNum;
    private int startColumnNum;
    private int endLineNum;
    private int endColumnNum;
    private String errorMsg;

    /**
     * Creates a new ValidateErrorInfo with the position coordinates and an
     * error string.
     *
     * @param startLineNum   Start line number
     * @param startColumnNum Start column number
     * @param endLineNum     End line number
     * @param endColumnNum   End column number
     * @param errorMsg       Error message
     */
    public ValidateErrorInfo(
        int startLineNum,
        int startColumnNum,
        int endLineNum,
        int endColumnNum,
        String errorMsg) {
      this.startLineNum = startLineNum;
      this.startColumnNum = startColumnNum;
      this.endLineNum = endLineNum;
      this.endColumnNum = endColumnNum;
      this.errorMsg = errorMsg;
    }

    /**
     * Creates a new ValidateErrorInfo with an CalciteContextException.
     *
     * @param e Exception
     */
    public ValidateErrorInfo(
        CalciteContextException e) {
      this.startLineNum = e.getPosLine();
      this.startColumnNum = e.getPosColumn();
      this.endLineNum = e.getEndPosLine();
      this.endColumnNum = e.getEndPosColumn();
      this.errorMsg = e.getCause().getMessage();
    }

    /**
     * Creates a new ValidateErrorInfo with a SqlParserPos and an error
     * string.
     *
     * @param pos      Error position
     * @param errorMsg Error message
     */
    public ValidateErrorInfo(
        SqlParserPos pos,
        String errorMsg) {
      this.startLineNum = pos.getLineNum();
      this.startColumnNum = pos.getColumnNum();
      this.endLineNum = pos.getEndLineNum();
      this.endColumnNum = pos.getEndColumnNum();
      this.errorMsg = errorMsg;
    }

    /**
     * @return 1-based starting line number
     */
    public int getStartLineNum() {
      return startLineNum;
    }

    /**
     * @return 1-based starting column number
     */
    public int getStartColumnNum() {
      return startColumnNum;
    }

    /**
     * @return 1-based end line number
     */
    public int getEndLineNum() {
      return endLineNum;
    }

    /**
     * @return 1-based end column number
     */
    public int getEndColumnNum() {
      return endColumnNum;
    }

    /**
     * @return error message
     */
    public String getMessage() {
      return errorMsg;
    }
  }
}

// End SqlAdvisor.java
