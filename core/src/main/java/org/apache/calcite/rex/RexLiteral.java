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
package org.apache.calcite.rex;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.SaffronProperties;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.ZonelessDate;
import org.apache.calcite.util.ZonelessDatetime;
import org.apache.calcite.util.ZonelessTime;
import org.apache.calcite.util.ZonelessTimestamp;

import com.google.common.base.Preconditions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Constant value in a row-expression.
 *
 * <p>There are several methods for creating literals in {@link RexBuilder}:
 * {@link RexBuilder#makeLiteral(boolean)} and so forth.</p>
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a black
 * box. There is a {@link #getValue} method which returns the value as an
 * object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use
 * task-oriented methods such as {@link #getValue2} and
 * {@link #toJavaString}.</p>
 *
 * <p>The allowable types and combinations are:</p>
 *
 * <table>
 * <caption>Allowable types for RexLiteral instances</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaing</th>
 * <th>Value type</th>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#NULL}</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BOOLEAN}</td>
 * <td>Boolean, namely <code>TRUE</code>, <code>FALSE</code> or <code>
 * UNKNOWN</code>.</td>
 * <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DECIMAL}</td>
 * <td>Exact number, for example <code>0</code>, <code>-.5</code>, <code>
 * 12345</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DOUBLE}</td>
 * <td>Approximate number, for example <code>6.023E-23</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DATE}</td>
 * <td>Date, for example <code>DATE '1969-04'29'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIME}</td>
 * <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIMESTAMP}</td>
 * <td>Timestamp, for example <code>TIMESTAMP '1969-04-29
 * 18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#CHAR}</td>
 * <td>Character constant, for example <code>'Hello, world!'</code>, <code>
 * ''</code>, <code>_N'Bonjour'</code>, <code>_ISO-8859-1'It''s superman!'
 * COLLATE SHIFT_JIS$ja_JP$2</code>. These are always CHAR, never VARCHAR.</td>
 * <td>{@link NlsString}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BINARY}</td>
 * <td>Binary constant, for example <code>X'7F34'</code>. (The number of hexits
 * must be even; see above.) These constants are always BINARY, never
 * VARBINARY.</td>
 * <td>{@link ByteBuffer}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a flag,
 * such as the LEADING flag in a call to the function <code>
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>An enum class</td>
 * </tr>
 * </table>
 */
public class RexLiteral extends RexNode {
  //~ Instance fields --------------------------------------------------------

  /**
   * The value of this literal. Must be consistent with its type, as per
   * {@link #valueMatchesType}. For example, you can't store an
   * {@link Integer} value here just because you feel like it -- all numbers are
   * represented by a {@link BigDecimal}. But since this field is private, it
   * doesn't really matter how the values are stored.
   */
  private final Comparable value;

  /**
   * The real type of this literal, as reported by {@link #getType}.
   */
  private final RelDataType type;

  // TODO jvs 26-May-2006:  Use SqlTypeFamily instead; it exists
  // for exactly this purpose (to avoid the confusion which results
  // from overloading SqlTypeName).
  /**
   * An indication of the broad type of this literal -- even if its type isn't
   * a SQL type. Sometimes this will be different than the SQL type; for
   * example, all exact numbers, including integers have typeName
   * {@link SqlTypeName#DECIMAL}. See {@link #valueMatchesType} for the
   * definitive story.
   */
  private final SqlTypeName typeName;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>RexLiteral</code>.
   */
  RexLiteral(
      Comparable value,
      RelDataType type,
      SqlTypeName typeName) {
    this.value = value;
    this.type = Preconditions.checkNotNull(type);
    this.typeName = Preconditions.checkNotNull(typeName);
    Preconditions.checkArgument(valueMatchesType(value, typeName, true));
    Preconditions.checkArgument((value == null) == type.isNullable());
    Preconditions.checkArgument(typeName != SqlTypeName.ANY);
    this.digest = toJavaString(value, typeName);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return whether value is appropriate for its type (we have rules about
   * these things)
   */
  public static boolean valueMatchesType(
      Comparable value,
      SqlTypeName typeName,
      boolean strict) {
    if (value == null) {
      return true;
    }
    switch (typeName) {
    case BOOLEAN:
      // Unlike SqlLiteral, we do not allow boolean null.
      return value instanceof Boolean;
    case NULL:
      return false; // value should have been null
    case INTEGER: // not allowed -- use Decimal
    case TINYINT:
    case SMALLINT:
      if (strict) {
        throw Util.unexpected(typeName);
      }
      // fall through
    case DECIMAL:
    case DOUBLE:
    case FLOAT:
    case REAL:
    case BIGINT:
      return value instanceof BigDecimal;
    case DATE:
    case TIME:
    case TIMESTAMP:
      return value instanceof Calendar
          && ((Calendar) value).getTimeZone().equals(DateTimeUtils.GMT_ZONE);
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      // The value of a DAY-TIME interval (whatever the start and end units,
      // even say HOUR TO MINUTE) is in milliseconds (perhaps fractional
      // milliseconds). The value of a YEAR-MONTH interval is in months.
      return value instanceof BigDecimal;
    case VARBINARY: // not allowed -- use Binary
      if (strict) {
        throw Util.unexpected(typeName);
      }
      // fall through
    case BINARY:
      return value instanceof ByteString;
    case VARCHAR: // not allowed -- use Char
      if (strict) {
        throw Util.unexpected(typeName);
      }
      // fall through
    case CHAR:
      // A SqlLiteral's charset and collation are optional; not so a
      // RexLiteral.
      return (value instanceof NlsString)
          && (((NlsString) value).getCharset() != null)
          && (((NlsString) value).getCollation() != null);
    case SYMBOL:
      return value instanceof Enum;
    case ROW:
    case MULTISET:
      return value instanceof List;
    case ANY:
      // Literal of type ANY is not legal. "CAST(2 AS ANY)" remains
      // an integer literal surrounded by a cast function.
      return false;
    default:
      throw Util.unexpected(typeName);
    }
  }

  private static String toJavaString(
      Comparable value,
      SqlTypeName typeName) {
    if (value == null) {
      return "null";
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printAsJava(value, pw, typeName, false);
    pw.flush();
    return sw.toString();
  }

  /** Returns whether a value is valid as a constant value, using the same
   * criteria as {@link #valueMatchesType}. */
  public static boolean validConstant(Object o, Litmus litmus) {
    if (o == null
        || o instanceof BigDecimal
        || o instanceof NlsString
        || o instanceof ByteString) {
      return litmus.succeed();
    } else if (o instanceof List) {
      List list = (List) o;
      for (Object o1 : list) {
        if (!validConstant(o1, litmus)) {
          return litmus.fail("not a constant: {}", o1);
        }
      }
      return litmus.succeed();
    } else if (o instanceof Map) {
      @SuppressWarnings("unchecked") final Map<Object, Object> map = (Map) o;
      for (Map.Entry entry : map.entrySet()) {
        if (!validConstant(entry.getKey(), litmus)) {
          return litmus.fail("not a constant: {}", entry.getKey());
        }
        if (!validConstant(entry.getValue(), litmus)) {
          return litmus.fail("not a constant: {}", entry.getValue());
        }
      }
      return litmus.succeed();
    } else {
      return litmus.fail("not a constant: {}", o);
    }
  }

  /**
   * Prints the value this literal as a Java string constant.
   */
  public void printAsJava(PrintWriter pw) {
    printAsJava(value, pw, typeName, true);
  }

  /**
   * Prints a value as a Java string. The value must be consistent with the
   * type, as per {@link #valueMatchesType}.
   *
   * <p>Typical return values:
   *
   * <ul>
   * <li>true</li>
   * <li>null</li>
   * <li>"Hello, world!"</li>
   * <li>1.25</li>
   * <li>1234ABCD</li>
   * </ul>
   *
   * @param value    Value
   * @param pw       Writer to write to
   * @param typeName Type family
   */
  private static void printAsJava(
      Comparable value,
      PrintWriter pw,
      SqlTypeName typeName,
      boolean java) {
    switch (typeName) {
    case CHAR:
      NlsString nlsString = (NlsString) value;
      if (java) {
        Util.printJavaString(
            pw,
            nlsString.getValue(),
            true);
      } else {
        boolean includeCharset =
            (nlsString.getCharsetName() != null)
                && !nlsString.getCharsetName().equals(
                    SaffronProperties.INSTANCE.defaultCharset().get());
        pw.print(nlsString.asSql(includeCharset, false));
      }
      break;
    case BOOLEAN:
      assert value instanceof Boolean;
      pw.print(((Boolean) value).booleanValue());
      break;
    case DECIMAL:
      assert value instanceof BigDecimal;
      pw.print(value.toString());
      break;
    case DOUBLE:
      assert value instanceof BigDecimal;
      pw.print(Util.toScientificNotation((BigDecimal) value));
      break;
    case BIGINT:
      assert value instanceof BigDecimal;
      pw.print(((BigDecimal) value).longValue());
      pw.print('L');
      break;
    case BINARY:
      assert value instanceof ByteString;
      pw.print("X'");
      pw.print(((ByteString) value).toString(16));
      pw.print("'");
      break;
    case NULL:
      assert value == null;
      pw.print("null");
      break;
    case SYMBOL:
      assert value instanceof Enum;
      pw.print("FLAG(");
      pw.print(value);
      pw.print(")");
      break;
    case DATE:
      printDatetime(pw, new ZonelessDate(), value);
      break;
    case TIME:
      printDatetime(pw, new ZonelessTime(), value);
      break;
    case TIMESTAMP:
      printDatetime(pw, new ZonelessTimestamp(), value);
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      if (value instanceof BigDecimal) {
        pw.print(value.toString());
      } else {
        assert value == null;
        pw.print("null");
      }
      break;
    case MULTISET:
    case ROW:
      @SuppressWarnings("unchecked") final List<RexLiteral> list = (List) value;
      pw.print(
          new AbstractList<String>() {
            public String get(int index) {
              return list.get(index).digest;
            }

            public int size() {
              return list.size();
            }
          });
      break;
    default:
      assert valueMatchesType(value, typeName, true);
      throw Util.needToImplement(typeName);
    }
  }

  private static void printDatetime(
      PrintWriter pw,
      ZonelessDatetime datetime,
      Comparable value) {
    assert value instanceof Calendar;
    datetime.setZonelessTime(
        ((Calendar) value).getTimeInMillis());
    pw.print(datetime);
  }

  /**
   * Converts a Jdbc string into a RexLiteral. This method accepts a string,
   * as returned by the Jdbc method ResultSet.getString(), and restores the
   * string into an equivalent RexLiteral. It allows one to use Jdbc strings
   * as a common format for data.
   *
   * <p>If a null literal is provided, then a null pointer will be returned.
   *
   * @param type     data type of literal to be read
   * @param typeName type family of literal
   * @param literal  the (non-SQL encoded) string representation, as returned
   *                 by the Jdbc call to return a column as a string
   * @return a typed RexLiteral, or null
   */
  public static RexLiteral fromJdbcString(
      RelDataType type,
      SqlTypeName typeName,
      String literal) {
    if (literal == null) {
      return null;
    }

    switch (typeName) {
    case CHAR:
      Charset charset = type.getCharset();
      SqlCollation collation = type.getCollation();
      NlsString str =
          new NlsString(
              literal,
              charset.name(),
              collation);
      return new RexLiteral(str, type, typeName);
    case BOOLEAN:
      boolean b = ConversionUtil.toBoolean(literal);
      return new RexLiteral(b, type, typeName);
    case DECIMAL:
    case DOUBLE:
      BigDecimal d = new BigDecimal(literal);
      return new RexLiteral(d, type, typeName);
    case BINARY:
      byte[] bytes = ConversionUtil.toByteArrayFromString(literal, 16);
      return new RexLiteral(new ByteString(bytes), type, typeName);
    case NULL:
      return new RexLiteral(null, type, typeName);
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      long millis =
          SqlParserUtil.intervalToMillis(
              literal,
              type.getIntervalQualifier());
      return new RexLiteral(BigDecimal.valueOf(millis), type, typeName);
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      long months =
          SqlParserUtil.intervalToMonths(
              literal,
              type.getIntervalQualifier());
      return new RexLiteral(BigDecimal.valueOf(months), type, typeName);
    case DATE:
    case TIME:
    case TIMESTAMP:
      String format = getCalendarFormat(typeName);
      TimeZone tz = DateTimeUtils.GMT_ZONE;
      Calendar cal = null;
      if (typeName == SqlTypeName.DATE) {
        cal =
            DateTimeUtils.parseDateFormat(literal, format, tz);
      } else {
        // Allow fractional seconds for times and timestamps
        DateTimeUtils.PrecisionTime ts =
            DateTimeUtils.parsePrecisionDateTimeLiteral(literal, format, tz);
        if (ts != null) {
          cal = ts.getCalendar();
        }
      }
      if (cal == null) {
        throw new AssertionError("fromJdbcString: invalid date/time value '"
            + literal + "'");
      }
      return new RexLiteral(cal, type, typeName);
    case SYMBOL:

      // Symbols are for internal use
    default:
      throw new AssertionError("fromJdbcString: unsupported type");
    }
  }

  private static String getCalendarFormat(SqlTypeName typeName) {
    switch (typeName) {
    case DATE:
      return DateTimeUtils.DATE_FORMAT_STRING;
    case TIME:
      return DateTimeUtils.TIME_FORMAT_STRING;
    case TIMESTAMP:
      return DateTimeUtils.TIMESTAMP_FORMAT_STRING;
    default:
      throw new AssertionError("getCalendarFormat: unknown type");
    }
  }

  public SqlTypeName getTypeName() {
    return typeName;
  }

  public RelDataType getType() {
    return type;
  }

  @Override public SqlKind getKind() {
    return SqlKind.LITERAL;
  }

  /**
   * Returns the value of this literal.
   */
  public Comparable getValue() {
    assert valueMatchesType(value, typeName, true) : value;
    return value;
  }

  /**
   * Returns the value of this literal, in the form that the calculator
   * program builder wants it.
   */
  public Object getValue2() {
    if (value == null) {
      return null;
    }
    switch (typeName) {
    case CHAR:
      return ((NlsString) value).getValue();
    case DECIMAL:
      return ((BigDecimal) value).unscaledValue().longValue();
    case DATE:
      return (int) (((Calendar) value).getTimeInMillis()
          / DateTimeUtils.MILLIS_PER_DAY);
    case TIME:
      return (int) (((Calendar) value).getTimeInMillis()
          % DateTimeUtils.MILLIS_PER_DAY);
    case TIMESTAMP:
      return ((Calendar) value).getTimeInMillis();
    default:
      return value;
    }
  }

  /**
   * Returns the value of this literal, in the form that the rex-to-lix
   * translator wants it.
   */
  public Object getValue3() {
    switch (typeName) {
    case DECIMAL:
      assert value instanceof BigDecimal;
      return value;
    default:
      return getValue2();
    }
  }

  public static boolean booleanValue(RexNode node) {
    return (Boolean) ((RexLiteral) node).value;
  }

  public boolean isAlwaysTrue() {
    if (typeName != SqlTypeName.BOOLEAN) {
      return false;
    }
    return booleanValue(this);
  }

  public boolean isAlwaysFalse() {
    if (typeName != SqlTypeName.BOOLEAN) {
      return false;
    }
    return !booleanValue(this);
  }

  public boolean equals(Object obj) {
    return (obj instanceof RexLiteral)
        && equals(((RexLiteral) obj).value, value)
        && equals(((RexLiteral) obj).type, type);
  }

  public int hashCode() {
    return Objects.hash(value, type);
  }

  public static Comparable value(RexNode node) {
    return findValue(node);
  }

  public static int intValue(RexNode node) {
    final Comparable value = findValue(node);
    return ((Number) value).intValue();
  }

  public static String stringValue(RexNode node) {
    final Comparable value = findValue(node);
    return (value == null) ? null : ((NlsString) value).getValue();
  }

  private static Comparable findValue(RexNode node) {
    if (node instanceof RexLiteral) {
      return ((RexLiteral) node).value;
    }
    if (node instanceof RexCall) {
      final RexCall call = (RexCall) node;
      final SqlOperator operator = call.getOperator();
      if (operator == SqlStdOperatorTable.CAST) {
        return findValue(call.getOperands().get(0));
      }
      if (operator == SqlStdOperatorTable.UNARY_MINUS) {
        final BigDecimal value =
            (BigDecimal) findValue(call.getOperands().get(0));
        return value.negate();
      }
    }
    throw new AssertionError("not a literal: " + node);
  }

  public static boolean isNullLiteral(RexNode node) {
    return (node instanceof RexLiteral)
        && (((RexLiteral) node).value == null);
  }

  private static boolean equals(Object o1, Object o2) {
    return (o1 == null) ? (o2 == null) : o1.equals(o2);
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitLiteral(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitLiteral(this, arg);
  }
}

// End RexLiteral.java
