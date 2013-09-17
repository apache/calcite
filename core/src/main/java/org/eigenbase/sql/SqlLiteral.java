/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

import java.math.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * A <code>SqlLiteral</code> is a constant. It is, appropriately, immutable.
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a black
 * box. There is a {@link #getValue} method which returns the value as an
 * object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use
 * task-oriented methods such as {@link #toSqlString(SqlDialect)} and {@link
 * #toValue}.</p>
 *
 * <p>If you really need to access the value directly, you should switch on the
 * value of the {@link #typeName} field, rather than making assumptions about
 * the runtime type of the {@link #value}.</p>
 *
 * <p>The allowable types and combinations are:
 *
 * <table>
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
 * <td>Binary constant, for example <code>X'ABC'</code>, <code>X'7F'</code>.
 * Note that strings with an odd number of hexits will later become values of
 * the BIT datatype, because they have an incomplete number of bytes. But here,
 * they are all binary constants, because that's how they were written. These
 * constants are always BINARY, never VARBINARY.</td>
 * <td>{@link BitString}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a
 * symbol, such as the LEADING flag in a call to the function <code>
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>A class which implements the {@link SqlSymbol} interface</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#INTERVAL_DAY_TIME}</td>
 * <td>Interval, for example <code>INTERVAL '1:34' HOUR</code>.</td>
 * <td><{@link SqlIntervalLiteral.IntervalValue}.</td>
 * </tr>
 * </table>
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlLiteral
    extends SqlNode
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The type with which this literal was declared. This type is very
     * approximate: the literal may have a different type once validated. For
     * example, all numeric literals have a type name of {@link
     * SqlTypeName#DECIMAL}, but on validation may become {@link
     * SqlTypeName#INTEGER}.
     */
    private final SqlTypeName typeName;

    /**
     * The value of this literal. The type of the value must be appropriate for
     * the typeName, as defined by the {@link #valueMatchesType} method.
     */
    protected final Object value;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>SqlLiteral</code>.
     *
     * @pre typeName != null
     * @pre valueMatchesType(value,typeName)
     */
    protected SqlLiteral(
        Object value,
        SqlTypeName typeName,
        SqlParserPos pos)
    {
        super(pos);
        this.value = value;
        this.typeName = typeName;
        Util.pre(typeName != null, "typeName != null");
        Util.pre(
            valueMatchesType(value, typeName),
            "valueMatchesType(value,typeName)");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return value of {@link #typeName}
     */
    public SqlTypeName getTypeName()
    {
        return typeName;
    }

    /**
     * @return whether value is appropriate for its type (we have rules about
     * these things)
     */
    public static boolean valueMatchesType(
        Object value,
        SqlTypeName typeName)
    {
        switch (typeName) {
        case BOOLEAN:
            return (value == null) || (value instanceof Boolean);
        case NULL:
            return value == null;
        case DECIMAL:
        case DOUBLE:
            return value instanceof BigDecimal;
        case DATE:
        case TIME:
        case TIMESTAMP:
            return value instanceof Calendar;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return value instanceof SqlIntervalLiteral.IntervalValue;
        case BINARY:
            return value instanceof BitString;
        case CHAR:
            return value instanceof NlsString;

        case SYMBOL:

            return (value instanceof SqlSymbol)
                || (value instanceof SqlSampleSpec);
        case MULTISET:
            return true;
        case INTEGER: // not allowed -- use Decimal
        case VARCHAR: // not allowed -- use Char
        case VARBINARY: // not allowed -- use Binary
        default:
            throw Util.unexpected(typeName);
        }
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlLiteral(value, typeName, pos);
    }

    public SqlKind getKind()
    {
        return SqlKind.LITERAL;
    }

    /**
     * Returns the value of this literal.
     *
     * <p>Try not to use this method! There are so many different kinds of
     * values, it's better to to let SqlLiteral do whatever it is you want to
     * do.
     *
     * @see #booleanValue(SqlNode)
     * @see #symbolValue(SqlNode)
     */
    public Object getValue()
    {
        return value;
    }

    /**
     * Converts extracts the value from a boolean literal.
     *
     * @throws ClassCastException if the value is not a boolean literal
     */
    public static boolean booleanValue(SqlNode node)
    {
        return ((Boolean) ((SqlLiteral) node).value).booleanValue();
    }

    /**
     * Extracts the enumerated value from a symbol literal.
     *
     * @throws ClassCastException if the value is not a symbol literal
     *
     * @see #createSymbol(SqlSymbol, SqlParserPos)
     */
    public static SqlSymbol symbolValue(SqlNode node)
    {
        return (SqlSymbol) ((SqlLiteral) node).value;
    }

    /**
     * Extracts the {@link SqlSampleSpec} value from a symbol literal.
     *
     * @throws ClassCastException if the value is not a symbol literal
     *
     * @see #createSymbol(SqlSymbol, SqlParserPos)
     */
    public static SqlSampleSpec sampleValue(SqlNode node)
    {
        return (SqlSampleSpec) ((SqlLiteral) node).value;
    }

    /**
     * Extracts the string value from a string literal, a chain of string
     * literals, or a CAST of a string literal.
     */
    public static String stringValue(SqlNode node)
    {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            assert SqlTypeUtil.inCharFamily(literal.getTypeName());
            return literal.toValue();
        } else if (SqlUtil.isLiteralChain(node)) {
            final SqlLiteral literal =
                SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
            assert SqlTypeUtil.inCharFamily(literal.getTypeName());
            return literal.toValue();
        } else if (
            (node instanceof SqlCall)
            && (((SqlCall) node).getOperator() == SqlStdOperatorTable.castFunc))
        {
            return stringValue(((SqlCall) node).getOperands()[0]);
        } else {
            throw Util.newInternal("invalid string literal: " + node);
        }
    }

    /**
     * Converts a chained string literals into regular literals; returns regular
     * literals unchanged.
     */
    public static SqlLiteral unchain(SqlNode node)
    {
        if (node instanceof SqlLiteral) {
            return (SqlLiteral) node;
        } else if (SqlUtil.isLiteralChain(node)) {
            return SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
        } else {
            throw Util.newInternal("invalid literal: " + node);
        }
    }

    /**
     * For calc program builder - value may be different than {@link #unparse}
     * Typical values:
     *
     * <ul>
     * <li>Hello, world!</li>
     * <li>12.34</li>
     * <li>{null}</li>
     * <li>1969-04-29</li>
     * </ul>
     *
     * @return string representation of the value
     */
    public String toValue()
    {
        if (value == null) {
            return null;
        }
        switch (typeName) {
        case CHAR:

            // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
            return ((NlsString) value).getValue();
        default:
            return value.toString();
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateLiteral(this);
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        if (!(node instanceof SqlLiteral)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        SqlLiteral that = (SqlLiteral) node;
        if (!this.equals(that)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        return true;
    }

    public SqlMonotonicity getMonotonicity(SqlValidatorScope scope)
    {
        return SqlMonotonicity.Constant;
    }

    /**
     * Creates a NULL literal.
     *
     * <p>There's no singleton constant for a NULL literal. Instead, nulls must
     * be instantiated via createNull(), because different instances have
     * different context-dependent types.
     */
    public static SqlLiteral createNull(SqlParserPos pos)
    {
        return new SqlLiteral(null, SqlTypeName.NULL, pos);
    }

    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral createBoolean(
        boolean b,
        SqlParserPos pos)
    {
        return b ? new SqlLiteral(Boolean.TRUE, SqlTypeName.BOOLEAN, pos)
            : new SqlLiteral(Boolean.FALSE, SqlTypeName.BOOLEAN, pos);
    }

    public static SqlLiteral createUnknown(SqlParserPos pos)
    {
        return new SqlLiteral(null, SqlTypeName.BOOLEAN, pos);
    }

    /**
     * Creates a literal which represents a parser symbol, for example the
     * <code>TRAILING</code> keyword in the call <code>Trim(TRAILING 'x' FROM
     * 'Hello world!')</code>.
     *
     * @see #symbolValue(SqlNode)
     */
    public static SqlLiteral createSymbol(
        SqlLiteral.SqlSymbol o,
        SqlParserPos pos)
    {
        return new SqlLiteral(o, SqlTypeName.SYMBOL, pos);
    }

    /**
     * Creates a literal which represents a sample specification.
     */
    public static SqlLiteral createSample(
        SqlSampleSpec sampleSpec,
        SqlParserPos pos)
    {
        return new SqlLiteral(sampleSpec, SqlTypeName.SYMBOL, pos);
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof SqlLiteral)) {
            return false;
        }
        SqlLiteral that = (SqlLiteral) obj;
        return Util.equal(value, that.value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    /**
     * Returns the integer value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal
     * is a fraction (e.g. 3.14), throws. If false, discards the fractional part
     * of the value.
     *
     * @return Integer value of this literal
     */
    public int intValue(boolean exact)
    {
        switch (typeName) {
        case DECIMAL:
        case DOUBLE:
            BigDecimal bd = (BigDecimal) value;
            if (exact) {
                try {
                    return bd.intValueExact();
                } catch (ArithmeticException e) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                            bd.toString()));
                }
            } else {
                return bd.intValue();
            }
        default:
            throw Util.unexpected(typeName);
        }
    }

    /**
     * Returns the long value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal
     * is a fraction (e.g. 3.14), throws. If false, discards the fractional part
     * of the value.
     *
     * @return Long value of this literal
     */
    public long longValue(boolean exact)
    {
        switch (typeName) {
        case DECIMAL:
        case DOUBLE:
            BigDecimal bd = (BigDecimal) value;
            if (exact) {
                try {
                    return bd.longValueExact();
                } catch (ArithmeticException e) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                            bd.toString()));
                }
            } else {
                return bd.longValue();
            }
        default:
            throw Util.unexpected(typeName);
        }
    }

    /**
     * Returns sign of value.
     *
     * @return -1, 0 or 1
     */
    public int signum() {
        return bigDecimalValue().compareTo(
            BigDecimal.ZERO);
    }

    /**
     * Returns a numeric literal's value as a {@link BigDecimal}.
     */
    public BigDecimal bigDecimalValue()
    {
        switch (typeName) {
        case DECIMAL:
        case DOUBLE:
            return (BigDecimal) value;
        default:
            throw Util.unexpected(typeName);
        }
    }

    public String getStringValue()
    {
        return ((NlsString) value).getValue();
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        switch (typeName) {
        case BOOLEAN:
            writer.keyword(
                (value == null) ? "UNKNOWN"
                : (((Boolean) value).booleanValue() ? "TRUE" : "FALSE"));
            break;
        case NULL:
            writer.keyword("NULL");
            break;
        case CHAR:
        case DECIMAL:
        case DOUBLE:
        case BINARY:

            // should be handled in subtype
            throw Util.unexpected(typeName);

        case SYMBOL:
            if (value instanceof EnumeratedValues.Value) {
                EnumeratedValues.Value enumVal = (EnumeratedValues.Value) value;
                writer.keyword(enumVal.getName().toUpperCase());
            } else if (value instanceof Enum) {
                Enum enumVal = (Enum) value;
                writer.keyword(enumVal.toString());
            } else {
                writer.keyword(String.valueOf(value));
            }
            break;
        default:
            writer.literal(value.toString());
        }
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        BitString bitString;
        switch (typeName) {
        case NULL:
        case BOOLEAN:
            RelDataType ret = typeFactory.createSqlType(typeName);
            ret = typeFactory.createTypeWithNullability(ret, null == value);
            return ret;
        case BINARY:
            bitString = (BitString) value;
            int bitCount = bitString.getBitCount();
            return typeFactory.createSqlType(SqlTypeName.BINARY, bitCount / 8);
        case CHAR:
            NlsString string = (NlsString) value;
            Charset charset = string.getCharset();
            if (null == charset) {
                charset = typeFactory.getDefaultCharset();
            }
            SqlCollation collation = string.getCollation();
            if (null == collation) {
                collation = SqlCollation.COERCIBLE;
            }
            RelDataType type =
                typeFactory.createSqlType(
                    SqlTypeName.CHAR,
                    string.getValue().length());
            type =
                typeFactory.createTypeWithCharsetAndCollation(
                    type,
                    charset,
                    collation);
            return type;

        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
            SqlIntervalLiteral.IntervalValue intervalValue =
                (SqlIntervalLiteral.IntervalValue) value;
            return typeFactory.createSqlIntervalType(
                intervalValue.getIntervalQualifier());

        case SYMBOL:
            return typeFactory.createSqlType(SqlTypeName.SYMBOL);

        case INTEGER: // handled in derived class
        case TIME: // handled in derived class
        case VARCHAR: // should never happen
        case VARBINARY: // should never happen

        default:
            throw Util.needToImplement(toString() + ", operand=" + value);
        }
    }

    public static SqlDateLiteral createDate(
        Calendar calendar,
        SqlParserPos pos)
    {
        return new SqlDateLiteral(calendar, pos);
    }

    public static SqlTimestampLiteral createTimestamp(
        Calendar calendar,
        int precision,
        SqlParserPos pos)
    {
        return new SqlTimestampLiteral(calendar, precision, false, pos);
    }

    public static SqlTimeLiteral createTime(
        Calendar calendar,
        int precision,
        SqlParserPos pos)
    {
        return new SqlTimeLiteral(calendar, precision, false, pos);
    }

    /**
     * Creates an interval literal.
     *
     * @param intervalStr input string of '1:23:04'
     * @param intervalQualifier describes the interval type and precision
     * @param pos Parser position
     */
    public static SqlIntervalLiteral createInterval(
        int sign,
        String intervalStr,
        SqlIntervalQualifier intervalQualifier,
        SqlParserPos pos)
    {
        SqlTypeName typeName =
            intervalQualifier.isYearMonth() ? SqlTypeName.INTERVAL_YEAR_MONTH
            : SqlTypeName.INTERVAL_DAY_TIME;
        return new SqlIntervalLiteral(
            sign,
            intervalStr,
            intervalQualifier,
            typeName,
            pos);
    }

    public static SqlNumericLiteral createNegative(
        SqlNumericLiteral num,
        SqlParserPos pos)
    {
        return new SqlNumericLiteral(
            ((BigDecimal) num.getValue()).negate(),
            num.getPrec(),
            num.getScale(),
            num.isExact(),
            pos);
    }

    public static SqlNumericLiteral createExactNumeric(
        String s,
        SqlParserPos pos)
    {
        BigDecimal value;
        int prec;
        int scale;

        int i = s.indexOf('.');
        if ((i >= 0) && ((s.length() - 1) != i)) {
            value = SqlParserUtil.parseDecimal(s);
            scale = s.length() - i - 1;
            assert scale == value.scale() : s;
            prec = s.length() - 1;
        } else if ((i >= 0) && ((s.length() - 1) == i)) {
            value = SqlParserUtil.parseInteger(s.substring(0, i));
            scale = 0;
            prec = s.length() - 1;
        } else {
            value = SqlParserUtil.parseInteger(s);
            scale = 0;
            prec = s.length();
        }
        return new SqlNumericLiteral(
            value,
            prec,
            scale,
            true,
            pos);
    }

    public static SqlNumericLiteral createApproxNumeric(
        String s,
        SqlParserPos pos)
    {
        BigDecimal value = SqlParserUtil.parseDecimal(s);
        return new SqlNumericLiteral(value, null, null, false, pos);
    }

    /**
     * Creates a literal like X'ABAB'. Although it matters when we derive a type
     * for this beastie, we don't care at this point whether the number of
     * hexits is odd or even.
     */
    public static SqlBinaryStringLiteral createBinaryString(
        String s,
        SqlParserPos pos)
    {
        BitString bits;
        try {
            bits = BitString.createFromHexString(s);
        } catch (NumberFormatException e) {
            throw SqlUtil.newContextException(
                pos,
                EigenbaseResource.instance().BinaryLiteralInvalid.ex());
        }
        return new SqlBinaryStringLiteral(bits, pos);
    }

    /**
     * Creates a literal like X'ABAB' from an array of bytes.
     *
     * @param bytes Contents of binary literal
     * @param pos Parser position
     *
     * @return Binary string literal
     */
    public static SqlBinaryStringLiteral createBinaryString(
        byte [] bytes,
        SqlParserPos pos)
    {
        BitString bits;
        try {
            bits = BitString.createFromBytes(bytes);
        } catch (NumberFormatException e) {
            throw SqlUtil.newContextException(
                pos,
                EigenbaseResource.instance().BinaryLiteralInvalid.ex());
        }
        return new SqlBinaryStringLiteral(bits, pos);
    }

    /**
     * Creates a string literal in the system character set.
     *
     * @param s a string (without the sql single quotes)
     * @param pos Parser position
     */
    public static SqlCharStringLiteral createCharString(
        String s,
        SqlParserPos pos)
    {
        // UnsupportedCharsetException not possible
        return createCharString(s, null, pos);
    }

    /**
     * Creates a string literal, with optional character-set.
     *
     * @param s a string (without the sql single quotes)
     * @param charSet character set name, null means take system default
     * @param pos Parser position
     *
     * @return A string literal
     *
     * @throws UnsupportedCharsetException if charSet is not null but there is
     * no character set with that name in this environment
     */
    public static SqlCharStringLiteral createCharString(
        String s,
        String charSet,
        SqlParserPos pos)
    {
        NlsString slit = new NlsString(s, charSet, null);
        return new SqlCharStringLiteral(slit, pos);
    }

    /**
     * Transforms this literal (which must be of type character) into a new one
     * in which 4-digit Unicode escape sequences have been replaced with the
     * corresponding Unicode characters.
     *
     * @param unicodeEscapeChar escape character (e.g. backslash) for Unicode
     * numeric sequences; 0 implies no transformation
     *
     * @return transformed literal
     */
    public SqlLiteral unescapeUnicode(char unicodeEscapeChar)
    {
        if (unicodeEscapeChar == 0) {
            return this;
        }
        assert (SqlTypeUtil.inCharFamily(getTypeName()));
        NlsString ns = (NlsString) value;
        String s = ns.getValue();
        StringBuilder sb = new StringBuilder();
        int n = s.length();
        for (int i = 0; i < n; ++i) {
            char c = s.charAt(i);
            if (c == unicodeEscapeChar) {
                if (n > (i + 1)) {
                    if (s.charAt(i + 1) == unicodeEscapeChar) {
                        sb.append(unicodeEscapeChar);
                        ++i;
                        continue;
                    }
                }
                if ((i + 5) > n) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        EigenbaseResource.instance().UnicodeEscapeMalformed.ex(
                            i));
                }
                String u = s.substring(i + 1, i + 5);
                short v;
                try {
                    v = Short.parseShort(u, 16);
                } catch (NumberFormatException ex) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        EigenbaseResource.instance().UnicodeEscapeMalformed.ex(
                            i));
                }
                sb.append((char) v);

                // skip hexits
                i += 4;
            } else {
                sb.append(c);
            }
        }
        ns = new NlsString(
            sb.toString(),
            ns.getCharsetName(),
            ns.getCollation());
        return new SqlCharStringLiteral(ns, getParserPosition());
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * A value must implement this interface if it is to be embedded as a
     * SqlLiteral of type SYMBOL. If the class is an {@link Enum} it trivially
     * implements this interface.
     *
     * <p>The {@link #toString()} method should return how the symbol should be
     * unparsed, which is sometimes not the same as the enumerated value's name
     * (e.g. "UNBOUNDED PRECEDING" versus "UnboundedPreceeding").
     */
    public interface SqlSymbol
    {
        String name();

        int ordinal();
    }
}

// End SqlLiteral.java
