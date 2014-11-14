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

import java.math.*;
import java.nio.charset.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;

import org.eigenbase.reltype.RelDataTypeSystem;
import org.eigenbase.sql.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;

import net.hydromatic.avatica.Casing;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Utility methods relating to parsing SQL.
 */
public final class SqlParserUtil {
  //~ Static fields/initializers ---------------------------------------------

  static final Logger LOGGER = EigenbaseTrace.getParserTracer();

  //~ Constructors -----------------------------------------------------------

  private SqlParserUtil() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return the character-set prefix of an sql string literal; returns null
   * if there is none
   */
  public static String getCharacterSet(String s) {
    if (s.charAt(0) == '\'') {
      return null;
    }
    if (Character.toUpperCase(s.charAt(0)) == 'N') {
      return SaffronProperties.instance().defaultNationalCharset.get();
    }
    int i = s.indexOf("'");
    return s.substring(1, i); // skip prefixed '_'
  }

  /**
   * Converts the contents of an sql quoted string literal into the
   * corresponding Java string representation (removing leading and trailing
   * quotes and unescaping internal doubled quotes).
   */
  public static String parseString(String s) {
    int i = s.indexOf("'"); // start of body
    if (i > 0) {
      s = s.substring(i);
    }
    return strip(s, "'", "'", "''", Casing.UNCHANGED);
  }

  public static BigDecimal parseDecimal(String s) {
    return new BigDecimal(s);
  }

  public static BigDecimal parseInteger(String s) {
    return new BigDecimal(s);
  }

  /**
   * @deprecated this method is not localized for Farrago standards
   */
  public static java.sql.Date parseDate(String s) {
    return java.sql.Date.valueOf(s);
  }

  /**
   * @deprecated Does not parse SQL:99 milliseconds
   */
  public static java.sql.Time parseTime(String s) {
    return java.sql.Time.valueOf(s);
  }

  /**
   * @deprecated this method is not localized for Farrago standards
   */
  public static java.sql.Timestamp parseTimestamp(String s) {
    return java.sql.Timestamp.valueOf(s);
  }

  /**
   * Checks if the date/time format is valid
   *
   * @param pattern {@link SimpleDateFormat}  pattern
   */
  public static void checkDateFormat(String pattern) {
    SimpleDateFormat df = new SimpleDateFormat(pattern);
    Util.discard(df);
  }

  /**
   * Converts the interval value into a millisecond representation.
   *
   * @param interval Interval
   * @return a long value that represents millisecond equivalent of the
   * interval value.
   */
  public static long intervalToMillis(
      SqlIntervalLiteral.IntervalValue interval) {
    return intervalToMillis(
        interval.getIntervalLiteral(),
        interval.getIntervalQualifier());
  }

  public static long intervalToMillis(
      String literal,
      SqlIntervalQualifier intervalQualifier) {
    Util.permAssert(
        !intervalQualifier.isYearMonth(),
        "interval must be day time");
    int[] ret;
    try {
      ret = intervalQualifier.evaluateIntervalLiteral(literal,
          intervalQualifier.getParserPosition(), RelDataTypeSystem.DEFAULT);
      assert ret != null;
    } catch (EigenbaseContextException e) {
      throw Util.newInternal(
          e, "while parsing day-to-second interval " + literal);
    }
    long l = 0;
    long[] conv = new long[5];
    conv[4] = 1; // millisecond
    conv[3] = conv[4] * 1000; // second
    conv[2] = conv[3] * 60; // minute
    conv[1] = conv[2] * 60; // hour
    conv[0] = conv[1] * 24; // day
    for (int i = 1; i < ret.length; i++) {
      l += conv[i - 1] * ret[i];
    }
    return ret[0] * l;
  }

  /**
   * Converts the interval value into a months representation.
   *
   * @param interval Interval
   * @return a long value that represents months equivalent of the interval
   * value.
   */
  public static long intervalToMonths(
      SqlIntervalLiteral.IntervalValue interval) {
    return intervalToMonths(
        interval.getIntervalLiteral(),
        interval.getIntervalQualifier());
  }

  public static long intervalToMonths(
      String literal,
      SqlIntervalQualifier intervalQualifier) {
    Util.permAssert(
        intervalQualifier.isYearMonth(),
        "interval must be year month");
    int[] ret;
    try {
      ret = intervalQualifier.evaluateIntervalLiteral(literal,
          intervalQualifier.getParserPosition(), RelDataTypeSystem.DEFAULT);
      assert ret != null;
    } catch (EigenbaseContextException e) {
      throw Util.newInternal(
          e, "error parsing year-to-month interval " + literal);
    }

    long l = 0;
    long[] conv = new long[2];
    conv[1] = 1; // months
    conv[0] = conv[1] * 12; // years
    for (int i = 1; i < ret.length; i++) {
      l += conv[i - 1] * ret[i];
    }
    return ret[0] * l;
  }

  /**
   * Parses a positive int. All characters have to be digits.
   *
   * @see Integer#parseInt(String)
   * @throws java.lang.NumberFormatException if invalid number or leading '-'
   */
  public static int parsePositiveInt(String value) {
    value = value.trim();
    if (value.charAt(0) == '-') {
      throw new NumberFormatException(value);
    }
    return Integer.parseInt(value);
  }

  /**
   * Parses a Binary string. SQL:99 defines a binary string as a hexstring
   * with EVEN nbr of hex digits.
   */
  public static byte[] parseBinaryString(String s) {
    s = s.replaceAll(" ", "");
    s = s.replaceAll("\n", "");
    s = s.replaceAll("\t", "");
    s = s.replaceAll("\r", "");
    s = s.replaceAll("\f", "");
    s = s.replaceAll("'", "");

    if (s.length() == 0) {
      return new byte[0];
    }
    assert (s.length() & 1) == 0; // must be even nbr of hex digits

    final int lengthToBe = s.length() / 2;
    s = "ff" + s;
    BigInteger bigInt = new BigInteger(s, 16);
    byte[] ret = new byte[lengthToBe];
    System.arraycopy(
        bigInt.toByteArray(),
        2,
        ret,
        0,
        ret.length);
    return ret;
  }

  /**
   * Unquotes a quoted string, using different quotes for beginning and end.
   */
  public static String strip(String s, String startQuote, String endQuote,
      String escape, Casing casing) {
    if (startQuote != null) {
      assert endQuote != null;
      assert startQuote.length() == 1;
      assert endQuote.length() == 1;
      assert escape != null;
      assert s.startsWith(startQuote) && s.endsWith(endQuote) : s;
      s = s.substring(1, s.length() - 1).replace(escape, endQuote);
    }
    switch (casing) {
    case TO_UPPER:
      return s.toUpperCase();
    case TO_LOWER:
      return s.toLowerCase();
    default:
      return s;
    }
  }

  /**
   * Trims a string for given characters from left and right. E.g.
   * {@code trim("aBaac123AabC","abBcC")} returns {@code "123A"}.
   */
  public static String trim(
      String s,
      String chars) {
    if (s.length() == 0) {
      return "";
    }

    int start;
    for (start = 0; start < s.length(); start++) {
      char c = s.charAt(start);
      if (chars.indexOf(c) < 0) {
        break;
      }
    }

    int stop;
    for (stop = s.length(); stop > start; stop--) {
      char c = s.charAt(stop - 1);
      if (chars.indexOf(c) < 0) {
        break;
      }
    }

    if (start >= stop) {
      return "";
    }

    return s.substring(start, stop);
  }

  /**
   * Looks for one or two carets in a SQL string, and if present, converts
   * them into a parser position.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>findPos("xxx^yyy") yields {"xxxyyy", position 3, line 1 column 4}
   * <li>findPos("xxxyyy") yields {"xxxyyy", null}
   * <li>findPos("xxx^yy^y") yields {"xxxyyy", position 3, line 4 column 4
   * through line 1 column 6}
   * </ul>
   */
  public static StringAndPos findPos(String sql) {
    int firstCaret = sql.indexOf('^');
    if (firstCaret < 0) {
      return new StringAndPos(sql, -1, null);
    }
    int secondCaret = sql.indexOf('^', firstCaret + 1);
    if (secondCaret < 0) {
      String sqlSansCaret =
          sql.substring(0, firstCaret)
              + sql.substring(firstCaret + 1);
      int[] start = indexToLineCol(sql, firstCaret);
      SqlParserPos pos = new SqlParserPos(start[0], start[1]);
      return new StringAndPos(sqlSansCaret, firstCaret, pos);
    } else {
      String sqlSansCaret =
          sql.substring(0, firstCaret)
              + sql.substring(firstCaret + 1, secondCaret)
              + sql.substring(secondCaret + 1);
      int[] start = indexToLineCol(sql, firstCaret);

      // subtract 1 because the col position needs to be inclusive
      --secondCaret;
      int[] end = indexToLineCol(sql, secondCaret);

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

  /**
   * Returns the (1-based) line and column corresponding to a particular
   * (0-based) offset in a string.
   *
   * <p>Converse of {@link #lineColToIndex(String, int, int)}.
   */
  public static int[] indexToLineCol(String sql, int i) {
    int line = 0;
    int j = 0;
    while (true) {
      int prevj = j;
      j = nextLine(sql, j);
      if ((j < 0) || (j > i)) {
        return new int[]{line + 1, i - prevj + 1};
      }
      ++line;
    }
  }

  public static int nextLine(String sql, int j) {
    int rn = sql.indexOf("\r\n", j);
    int r = sql.indexOf("\r", j);
    int n = sql.indexOf("\n", j);
    if ((r < 0) && (n < 0)) {
      assert rn < 0;
      return -1;
    } else if ((rn >= 0) && (rn < n) && (rn <= r)) {
      return rn + 2; // looking at "\r\n"
    } else if ((r >= 0) && (r < n)) {
      return r + 1; // looking at "\r"
    } else {
      return n + 1; // looking at "\n"
    }
  }

  /**
   * Finds the position (0-based) in a string which corresponds to a given
   * line and column (1-based).
   *
   * <p>Converse of {@link #indexToLineCol(String, int)}.
   */
  public static int lineColToIndex(String sql, int line, int column) {
    --line;
    --column;
    int i = 0;
    while (line-- > 0) {
      i = nextLine(sql, i);
    }
    return i + column;
  }

  /**
   * Converts a string to a string with one or two carets in it. For example,
   * <code>addCarets("values (foo)", 1, 9, 1, 12)</code> yields "values
   * (^foo^)".
   */
  public static String addCarets(
      String sql,
      int line,
      int col,
      int endLine,
      int endCol) {
    String sqlWithCarets;
    int cut = lineColToIndex(sql, line, col);
    sqlWithCarets = sql.substring(0, cut) + "^"
        + sql.substring(cut);
    if ((col != endCol) || (line != endLine)) {
      cut = lineColToIndex(sqlWithCarets, endLine, endCol);
      ++cut; // for caret
      if (cut < sqlWithCarets.length()) {
        sqlWithCarets =
            sqlWithCarets.substring(0, cut)
                + "^" + sqlWithCarets.substring(cut);
      } else {
        sqlWithCarets += "^";
      }
    }
    return sqlWithCarets;
  }

  public static String getTokenVal(String token) {
    // We don't care about the token which are not string
    if (!token.startsWith("\"")) {
      return null;
    }

    // Remove the quote from the token
    int startIndex = token.indexOf("\"");
    int endIndex = token.lastIndexOf("\"");
    String tokenVal = token.substring(startIndex + 1, endIndex);
    char c = tokenVal.charAt(0);
    if (Character.isLetter(c)) {
      return tokenVal;
    }
    return null;
  }

  /**
   * Extracts the values from a collation name.
   *
   * <p>Collation names are on the form <i>charset$locale$strength</i>.
   *
   * @param in The collation name
   * @return A {@link ParsedCollation}
   */
  public static ParsedCollation parseCollation(String in) {
    StringTokenizer st = new StringTokenizer(in, "$");
    String charsetStr = st.nextToken();
    String localeStr = st.nextToken();
    String strength;
    if (st.countTokens() > 0) {
      strength = st.nextToken();
    } else {
      strength =
          SaffronProperties.instance().defaultCollationStrength.get();
    }

    Charset charset = Charset.forName(charsetStr);
    String[] localeParts = localeStr.split("_");
    Locale locale;
    if (1 == localeParts.length) {
      locale = new Locale(localeParts[0]);
    } else if (2 == localeParts.length) {
      locale = new Locale(localeParts[0], localeParts[1]);
    } else if (3 == localeParts.length) {
      locale = new Locale(localeParts[0], localeParts[1], localeParts[2]);
    } else {
      throw RESOURCE.illegalLocaleFormat(localeStr).ex();
    }
    return new ParsedCollation(charset, locale, strength);
  }

  public static String[] toStringArray(List<String> list) {
    return list.toArray(new String[list.size()]);
  }

  public static SqlNode[] toNodeArray(List<SqlNode> list) {
    return list.toArray(new SqlNode[list.size()]);
  }

  public static SqlNode[] toNodeArray(SqlNodeList list) {
    return list.toArray();
  }

  public static String rightTrim(
      String s,
      char c) {
    int stop;
    for (stop = s.length(); stop > 0; stop--) {
      if (s.charAt(stop - 1) != c) {
        break;
      }
    }
    if (stop > 0) {
      return s.substring(0, stop);
    }
    return "";
  }

  /**
   * Replaces a range of elements in a list with a single element. For
   * example, if list contains <code>{A, B, C, D, E}</code> then <code>
   * replaceSublist(list, X, 1, 4)</code> returns <code>{A, X, E}</code>.
   */
  public static <T> void replaceSublist(
      List<T> list,
      int start,
      int end,
      T o) {
    Util.pre(list != null, "list != null");
    Util.pre(start < end, "start < end");
    for (int i = end - 1; i > start; --i) {
      list.remove(i);
    }
    list.set(start, o);
  }

  /**
   * Converts a list of {expression, operator, expression, ...} into a tree,
   * taking operator precedence and associativity into account.
   */
  public static SqlNode toTree(List<Object> list) {
    if (LOGGER.isLoggable(Level.FINER)) {
      LOGGER.finer("Attempting to reduce " + list);
    }
    final SqlNode node = toTreeEx(list, 0, 0, SqlKind.OTHER);
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("Reduced " + node);
    }
    return node;
  }

  /**
   * Converts a list of {expression, operator, expression, ...} into a tree,
   * taking operator precedence and associativity into account.
   *
   * @param list        List of operands and operators. This list is modified as
   *                    expressions are reduced.
   * @param start       Position of first operand in the list. Anything to the
   *                    left of this (besides the immediately preceding operand)
   *                    is ignored. Generally use value 1.
   * @param minPrec     Minimum precedence to consider. If the method encounters
   *                    an operator of lower precedence, it doesn't reduce any
   *                    further.
   * @param stopperKind If not {@link SqlKind#OTHER}, stop reading the list if
   *                    we encounter a token of this kind.
   * @return the root node of the tree which the list condenses into
   */
  public static SqlNode toTreeEx(
      List<Object> list,
      int start,
      int minPrec,
      SqlKind stopperKind) {
// Make several passes over the list, and each pass, coalesce the
// expressions with the highest precedence.
  outer:
    while (true) {
      final int count = list.size();
      if (count <= (start + 1)) {
        break;
      }
      int i = start + 1;
      while (i < count) {
        SqlOperator previous;
        SqlOperator current = ((ToTreeListItem) list.get(i)).op;
        SqlParserPos currentPos = ((ToTreeListItem) list.get(i)).pos;
        if ((stopperKind != SqlKind.OTHER)
            && (current.getKind() == stopperKind)) {
          break outer;
        }
        SqlOperator next;
        int previousRight;
        int left = current.getLeftPrec();
        int right = current.getRightPrec();
        if (left < minPrec) {
          break outer;
        }
        int nextLeft;
        if (current instanceof SqlBinaryOperator) {
          if (i == (start + 1)) {
            previous = null;
            previousRight = 0;
          } else {
            previous = ((ToTreeListItem) list.get(i - 2)).op;
            previousRight = previous.getRightPrec();
          }
          if (i == (count - 2)) {
            next = null;
            nextLeft = 0;
          } else {
            next = ((ToTreeListItem) list.get(i + 2)).op;
            nextLeft = next.getLeftPrec();
            if ((next.getKind() == stopperKind)
                && (stopperKind != SqlKind.OTHER)) {
              // Suppose we're looking at 'AND' in
              //    a BETWEEN b OR c AND d
              //
              // Because 'AND' is our stopper token, we still
              // want to reduce 'b OR c', even though 'AND' has
              // higher precedence than 'OR'.
              nextLeft = 0;
            }
          }
          if ((previousRight < left) && (right >= nextLeft)) {
            // For example,
            //    i:  0 1 2 3 4 5 6 7 8
            // list:  a + b * c * d + e
            // prec: 0 1 2 3 4 3 4 1 2 0
            //
            // At i == 3, we have the first '*' operator, and its
            // surrounding precedences obey the relation 2 < 3 and
            // 4 >= 3, so we can reduce (b * c) to a single node.
            SqlNode leftExp = (SqlNode) list.get(i - 1);

            // For example,
            //    i:  0 1 2 3 4 5 6 7 8
            // list:  a + b * c * d + e
            // prec: 0 1 2 3 4 3 4 1 2 0
            //
            // At i == 3, we have the first '*' operator, and its
            // surrounding precedences obey the relation 2 < 3 and
            // 4 >= 3, so we can reduce (b * c) to a single node.
            SqlNode rightExp = (SqlNode) list.get(i + 1);
            SqlParserPos callPos =
                currentPos.plusAll(
                    new SqlNode[]{leftExp, rightExp});
            final SqlCall newExp =
                current.createCall(callPos, leftExp, rightExp);
            if (LOGGER.isLoggable(Level.FINE)) {
              LOGGER.fine("Reduced infix: " + newExp);
            }

            // Replace elements {i - 1, i, i + 1} with the new
            // expression.
            replaceSublist(list, i - 1, i + 2, newExp);
            break;
          }
          i += 2;
        } else if (current instanceof SqlPostfixOperator) {
          if (i == (start + 1)) {
            previous = null;
            previousRight = 0;
          } else {
            previous = ((ToTreeListItem) list.get(i - 2)).op;
            previousRight = previous.getRightPrec();
          }
          if (previousRight < left) {
            // For example,
            //    i:  0 1 2 3 4 5 6 7 8
            // list:  a + b * c ! + d
            // prec: 0 1 2 3 4 3 0 2
            //
            // At i == 3, we have the postfix '!' operator. Its
            // high precedence determines that it binds with 'b *
            // c'. The precedence of the following '+' operator is
            // irrelevant.
            SqlNode leftExp = (SqlNode) list.get(i - 1);

            SqlParserPos callPos =
                currentPos.plusAll(new SqlNode[]{leftExp});
            final SqlCall newExp =
                current.createCall(callPos, leftExp);
            if (LOGGER.isLoggable(Level.FINE)) {
              LOGGER.fine("Reduced postfix: " + newExp);
            }

            // Replace elements {i - 1, i} with the new expression.
            list.remove(i);
            list.set(i - 1, newExp);
            break;
          }
          ++i;

          //
        } else if (current instanceof SqlSpecialOperator) {
          SqlSpecialOperator specOp = (SqlSpecialOperator) current;

          // We decide to reduce a special operator only on the basis
          // of what's to the left of it. The operator then decides
          // how far to the right to chew off.
          if (i == (start + 1)) {
            previous = null;
            previousRight = 0;
          } else {
            previous = ((ToTreeListItem) list.get(i - 2)).op;
            previousRight = previous.getRightPrec();
          }
          int nextOrdinal = i + 2;
          if (i == (count - 2)) {
            next = null;
            nextLeft = 0;
          } else {
            // find next op
            next = null;
            nextLeft = 0;
            for (; nextOrdinal < count; nextOrdinal++) {
              Object listItem = list.get(nextOrdinal);
              if (listItem instanceof ToTreeListItem) {
                next = ((ToTreeListItem) listItem).op;
                nextLeft = next.getLeftPrec();
                if ((stopperKind != SqlKind.OTHER)
                    && (next.getKind() == stopperKind)) {
                  break outer;
                } else {
                  break;
                }
              }
            }
          }
          if (nextLeft < minPrec) {
            break outer;
          }
          if ((previousRight < left) && (right >= nextLeft)) {
            i = specOp.reduceExpr(i, list);
            if (LOGGER.isLoggable(Level.FINE)) {
              LOGGER.fine("Reduced special op: " + list.get(i));
            }
            break;
          }
          i = nextOrdinal;
        } else {
          throw Util.newInternal("Unexpected operator type: " + current);
        }
      }

      // Require the list shrinks each time around -- otherwise we will
      // never terminate.
      assert list.size() < count;
    }
    return (SqlNode) list.get(start);
  }

  /**
   * Checks a UESCAPE string for validity, and returns the escape character if
   * no exception is thrown.
   *
   * @param s UESCAPE string to check
   * @return validated escape character
   */
  public static char checkUnicodeEscapeChar(String s) {
    if (s.length() != 1) {
      throw RESOURCE.unicodeEscapeCharLength(s).ex();
    }
    char c = s.charAt(0);
    if (Character.isDigit(c)
        || Character.isWhitespace(c)
        || (c == '+')
        || (c == '"')
        || ((c >= 'a') && (c <= 'f'))
        || ((c >= 'A') && (c <= 'F'))) {
      throw RESOURCE.unicodeEscapeCharIllegal(s).ex();
    }
    return c;
  }

  //~ Inner Classes ----------------------------------------------------------

  public static class ParsedCollation {
    private final Charset charset;
    private final Locale locale;
    private final String strength;

    public ParsedCollation(
        Charset charset,
        Locale locale,
        String strength) {
      this.charset = charset;
      this.locale = locale;
      this.strength = strength;
    }

    public Charset getCharset() {
      return charset;
    }

    public Locale getLocale() {
      return locale;
    }

    public String getStrength() {
      return strength;
    }
  }

  /**
   * Class that holds a {@link SqlOperator} and a {@link SqlParserPos}. Used
   * by {@link SqlSpecialOperator#reduceExpr} and the parser to associate a
   * parsed operator with a parser position.
   */
  public static class ToTreeListItem {
    private final SqlOperator op;
    private final SqlParserPos pos;

    public ToTreeListItem(
        SqlOperator op,
        SqlParserPos pos) {
      this.op = op;
      this.pos = pos;
    }

    public SqlOperator getOperator() {
      return op;
    }

    public SqlParserPos getPos() {
      return pos;
    }
  }

  /**
   * Contains a string, the offset of a token within the string, and a parser
   * position containing the beginning and end line number.
   */
  public static class StringAndPos {
    public final String sql;
    public final int cursor;
    public final SqlParserPos pos;

    StringAndPos(String sql, int cursor, SqlParserPos pos) {
      this.sql = sql;
      this.cursor = cursor;
      this.pos = pos;
    }
  }
}

// End SqlParserUtil.java
