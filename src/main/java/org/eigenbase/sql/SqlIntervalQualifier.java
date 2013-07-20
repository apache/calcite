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

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.*;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

/**
 * Represents an INTERVAL qualifier.
 *
 * <p>INTERVAL qualifier is defined as follows:
 *
 * <blockquote><code>
 *
 * &lt;interval qualifier&gt; ::=<br/>
 * &nbsp;&nbsp; &lt;start field&gt; TO &lt;end field&gt;<br/>
 * &nbsp;&nbsp;| &lt;single datetime field&gt;<br/>
 * &lt;start field&gt; ::=<br/>
 * &nbsp;&nbsp; &lt;non-second primary datetime field&gt;<br/>
 * &nbsp;&nbsp; [ &lt;left paren&gt; &lt;interval leading field precision&gt;
 * &lt;right paren&gt; ]<br/>
 * &lt;end field&gt; ::=<br/>
 * &nbsp;&nbsp; &lt;non-second primary datetime field&gt;<br/>
 * &nbsp;&nbsp;| SECOND [ &lt;left paren&gt;
 * &lt;interval fractional seconds precision&gt; &lt;right paren&gt; ]<br/>
 * &lt;single datetime field&gt; ::=<br/>
 * &nbsp;&nbsp;&lt;non-second primary datetime field&gt;<br/>
 * &nbsp;&nbsp;[ &lt;left paren&gt; &lt;interval leading field precision&gt;
 * &lt;right paren&gt; ]<br/>
 * &nbsp;&nbsp;| SECOND [ &lt;left paren&gt;
 * &lt;interval leading field precision&gt;<br/>
 * &nbsp;&nbsp;[ &lt;comma&gt; &lt;interval fractional seconds precision&gt; ]
 * &lt;right paren&gt; ]<br/>
 * &lt;primary datetime field&gt; ::=<br/>
 * &nbsp;&nbsp;&lt;non-second primary datetime field&gt;<br/>
 * &nbsp;&nbsp;| SECOND<br/>
 * &lt;non-second primary datetime field&gt; ::= YEAR | MONTH | DAY | HOUR
 * | MINUTE<br/>
 * &lt;interval fractional seconds precision&gt; ::=
 * &lt;unsigned integer&gt;<br/>
 * &lt;interval leading field precision&gt; ::= &lt;unsigned integer&gt;
 *
 * </code></blockquote>
 *
 * <p>Examples include:
 *
 * <ul>
 * <li><code>INTERVAL '1:23:45.678' HOUR TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code></li>
 * </ul>
 *
 * An instance of this class is immutable.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Oct 31, 2004
 */
public class SqlIntervalQualifier
    extends SqlNode
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int USE_DEFAULT_PRECISION = -1;
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    private static final BigDecimal INT_MAX_VALUE_PLUS_ONE =
        BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE);

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of time units used to construct an interval.
     */
    public enum TimeUnit
        implements SqlLiteral.SqlSymbol
    {
        YEAR(true, ' ', 12 /* months */, null),
        MONTH(true, '-', 1 /* months */, BigDecimal.valueOf(12)),
        DAY(false, '-', 86400000 /* millis = 24 * 3600000 */, null),
        HOUR(false, ' ', 3600000 /* millis */, BigDecimal.valueOf(24)),
        MINUTE(false, ':', 60000 /* millis */, BigDecimal.valueOf(60)),
        SECOND(false, ':', 1000 /* millis */, BigDecimal.valueOf(60));

        public final boolean yearMonth;
        public final char separator;
        public final long multiplier;
        private final BigDecimal limit;

        private static final TimeUnit [] CachedValues = values();

        private TimeUnit(
            boolean yearMonth,
            char separator,
            long multiplier,
            BigDecimal limit)
        {
            this.yearMonth = yearMonth;
            this.separator = separator;
            this.multiplier = multiplier;
            this.limit = limit;
        }

        /**
         * Returns the TimeUnit associated with an ordinal. The value returned
         * is null if the ordinal is not a member of the TimeUnit enumeration.
         */
        public static TimeUnit getValue(int ordinal)
        {
            return ((ordinal < 0) || (ordinal >= CachedValues.length)) ? null
                : CachedValues[ordinal];
        }

        public static final String GET_VALUE_METHOD_NAME = "getValue";

        /**
         * Returns whether a given value is valid for a field of this time unit.
         *
         * @param field Field value
         * @return Whether value
         */
        public boolean isValidValue(BigDecimal field)
        {
            return field.compareTo(ZERO) >= 0
                && (limit == null
                    || field.compareTo(limit) < 0);
        }
    }

    private enum TimeUnitRange {
        YEAR(TimeUnit.YEAR, null),
        YEAR_TO_MONTH(TimeUnit.YEAR, TimeUnit.MONTH),
        MONTH(TimeUnit.MONTH, null),
        DAY(TimeUnit.DAY, null),
        DAY_TO_HOUR(TimeUnit.DAY, TimeUnit.HOUR),
        DAY_TO_MINUTE(TimeUnit.DAY, TimeUnit.MINUTE),
        DAY_TO_SECOND(TimeUnit.DAY, TimeUnit.SECOND),
        HOUR(TimeUnit.HOUR, null),
        HOUR_TO_MINUTE(TimeUnit.HOUR, TimeUnit.MINUTE),
        HOUR_TO_SECOND(TimeUnit.HOUR, TimeUnit.SECOND),
        MINUTE(TimeUnit.MINUTE, null),
        MINUTE_TO_SECOND(TimeUnit.MINUTE, TimeUnit.SECOND),
        SECOND(TimeUnit.SECOND, null);

        private final TimeUnit startUnit;
        private final TimeUnit endUnit;
        private static final Map<Pair<TimeUnit, TimeUnit>, TimeUnitRange> map;

        static {
            map = new HashMap<Pair<TimeUnit, TimeUnit>, TimeUnitRange>();
            for (TimeUnitRange value : values()) {
                map.put(Pair.of(value.startUnit, value.endUnit), value);
            }
        }

        /**
         * Creates a TimeUnitRange.
         * @param startUnit Start time unit
         * @param endUnit End time unit
         */
        TimeUnitRange(TimeUnit startUnit, TimeUnit endUnit) {
            assert startUnit != null;
            this.startUnit = startUnit;
            this.endUnit = endUnit;
        }

        /**
         * Returns a TimeUnitRange with a given start and end unit.
         *
         * @param startUnit Start unit
         * @param endUnit End unit
         * @return Time unit range, or null if not valid
         */
        public static TimeUnitRange of(
            TimeUnit startUnit, TimeUnit endUnit)
        {
            return map.get(new Pair<TimeUnit, TimeUnit>(startUnit, endUnit));
        }
    }

    //~ Instance fields --------------------------------------------------------

    private final int startPrecision;
    private final TimeUnitRange timeUnitRange;
    private final int fractionalSecondPrecision;

    private final boolean useDefaultStartPrecision;
    private final boolean useDefaultFractionalSecondPrecision;

    //~ Constructors -----------------------------------------------------------

    public SqlIntervalQualifier(
        TimeUnit startUnit,
        int startPrecision,
        TimeUnit endUnit,
        int fractionalSecondPrecision,
        SqlParserPos pos)
    {
        super(pos);
        assert null != startUnit;

        this.timeUnitRange = TimeUnitRange.of(startUnit, endUnit);

        // if unspecified, start precision = 2
        if (startPrecision == USE_DEFAULT_PRECISION) {
            useDefaultStartPrecision = true;
            if (this.isYearMonth()) {
                this.startPrecision =
                    SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultPrecision();
            } else {
                this.startPrecision =
                    SqlTypeName.INTERVAL_DAY_TIME.getDefaultPrecision();
            }
        } else {
            useDefaultStartPrecision = false;
            this.startPrecision = startPrecision;
        }

        // unspecified fractional second precision = 6
        if (fractionalSecondPrecision == USE_DEFAULT_PRECISION) {
            useDefaultFractionalSecondPrecision = true;
            if (this.isYearMonth()) {
                this.fractionalSecondPrecision =
                    SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultScale();
            } else {
                this.fractionalSecondPrecision =
                    SqlTypeName.INTERVAL_DAY_TIME.getDefaultScale();
            }
        } else {
            useDefaultFractionalSecondPrecision = false;
            this.fractionalSecondPrecision = fractionalSecondPrecision;
        }
    }

    public SqlIntervalQualifier(
        TimeUnit startUnit,
        TimeUnit endUnit,
        SqlParserPos pos)
    {
        this(
            startUnit,
            USE_DEFAULT_PRECISION,
            endUnit,
            USE_DEFAULT_PRECISION,
            pos);
    }

    //~ Methods ----------------------------------------------------------------

    public void validate(
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        validator.validateIntervalQualifier(this);
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        final String thisString = this.toString();
        final String thatString = node.toString();
        if (!thisString.equals(thatString)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        return true;
    }

    public static int getDefaultPrecisionId()
    {
        return USE_DEFAULT_PRECISION;
    }

    public int getStartPrecision()
    {
        return startPrecision;
    }

    public int getStartPrecisionPreservingDefault()
    {
        if (useDefaultStartPrecision) {
            return USE_DEFAULT_PRECISION;
        } else {
            return startPrecision;
        }
    }

    public static int combineStartPrecisionPreservingDefault(
        SqlIntervalQualifier qual1,
        SqlIntervalQualifier qual2)
    {
        if (qual1.getStartPrecision()
            > qual2.getStartPrecision())
        {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual1.getStartPrecisionPreservingDefault());
        } else if (qual1.getStartPrecision()
            < qual2.getStartPrecision())
        {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual2.getStartPrecisionPreservingDefault());
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if (qual1.useDefaultStartPrecision
                && qual2.useDefaultStartPrecision)
            {
                return qual1.getStartPrecisionPreservingDefault();
            } else {
                return qual1.getStartPrecision();
            }
        }
    }

    public int getFractionalSecondPrecision()
    {
        return fractionalSecondPrecision;
    }

    public int getFractionalSecondPrecisionPreservingDefault()
    {
        if (useDefaultFractionalSecondPrecision) {
            return USE_DEFAULT_PRECISION;
        } else {
            return startPrecision;
        }
    }

    public static int combineFractionalSecondPrecisionPreservingDefault(
        SqlIntervalQualifier qual1,
        SqlIntervalQualifier qual2)
    {
        if (qual1.getFractionalSecondPrecision()
            > qual2.getFractionalSecondPrecision())
        {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual1.getFractionalSecondPrecisionPreservingDefault());
        } else if (
            qual1.getFractionalSecondPrecision()
            < qual2.getFractionalSecondPrecision())
        {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual2.getFractionalSecondPrecisionPreservingDefault());
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if (qual1.useDefaultFractionalSecondPrecision
                && qual2.useDefaultFractionalSecondPrecision)
            {
                return qual1.getFractionalSecondPrecisionPreservingDefault();
            } else {
                return qual1.getFractionalSecondPrecision();
            }
        }
    }

    public TimeUnit getStartUnit()
    {
        return timeUnitRange.startUnit;
    }

    public TimeUnit getEndUnit()
    {
        return timeUnitRange.endUnit;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlIntervalQualifier(
            timeUnitRange.startUnit,
            useDefaultStartPrecision ? USE_DEFAULT_PRECISION
                : startPrecision,
            timeUnitRange.endUnit,
            useDefaultFractionalSecondPrecision ? USE_DEFAULT_PRECISION
                : fractionalSecondPrecision,
            pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        final String start = timeUnitRange.startUnit.name();
        if (timeUnitRange.startUnit == TimeUnit.SECOND) {
            if (!useDefaultFractionalSecondPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.sep(",", true);
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            } else if (!useDefaultStartPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }
        } else {
            if (!useDefaultStartPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }

            if (null != timeUnitRange.endUnit) {
                writer.keyword("TO");
                final String end = timeUnitRange.endUnit.name();
                if ((TimeUnit.SECOND == timeUnitRange.endUnit)
                    && (!useDefaultFractionalSecondPrecision))
                {
                    final SqlWriter.Frame frame = writer.startFunCall(end);
                    writer.print(fractionalSecondPrecision);
                    writer.endList(frame);
                } else {
                    writer.keyword(end);
                }
            } else if (
                (TimeUnit.SECOND == timeUnitRange.startUnit)
                && (!useDefaultFractionalSecondPrecision))
            {
                final SqlWriter.Frame frame = writer.startList("(", ")");
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            }
        }
    }

    /**
     * Does this interval have a single datetime field
     *
     * Return true not of form unit TO unit.
     */
    public boolean isSingleDatetimeField()
    {
        return timeUnitRange.endUnit == null;
    }

    public final boolean isYearMonth()
    {
        return timeUnitRange.startUnit.yearMonth;
    }

    /**
     * @return 1 or -1
     */
    private int getIntervalSign(String value)
    {
        int sign = 1; // positive until proven otherwise

        if (!Util.isNullOrEmpty(value)) {
            if ('-' == value.charAt(0)) {
                sign = -1; // Negative
            }
        }

        return (sign);
    }

    private String stripLeadingSign(String value)
    {
        String unsignedValue = value;

        if (!Util.isNullOrEmpty(value)) {
            if (('-' == value.charAt(0)) || ('+' == value.charAt(0))) {
                unsignedValue = value.substring(1);
            }
        }

        return (unsignedValue);
    }

    private boolean isLeadFieldInRange(BigDecimal value, TimeUnit unit)
    {
        // we should never get handed a negative field value
        assert value.compareTo(ZERO) >= 0;

        // Leading fields are only restricted by startPrecision.
        return startPrecision < POWERS10.length
           ? value.compareTo(POWERS10[startPrecision]) < 0
           : value.compareTo(INT_MAX_VALUE_PLUS_ONE) < 0;
    }

    private void checkLeadFieldInRange(
        int sign, BigDecimal value, TimeUnit unit) throws SqlValidatorException
    {
        if (!isLeadFieldInRange(value, unit)) {
            throw fieldExceedsPrecisionException(
                sign, value, unit, startPrecision);
        }
    }

    private static final BigDecimal[] POWERS10 = {
        ZERO,
        BigDecimal.valueOf(10),
        BigDecimal.valueOf(100),
        BigDecimal.valueOf(1000),
        BigDecimal.valueOf(10000),
        BigDecimal.valueOf(100000),
        BigDecimal.valueOf(1000000),
        BigDecimal.valueOf(10000000),
        BigDecimal.valueOf(100000000),
        BigDecimal.valueOf(1000000000),
    };

    private boolean isFractionalSecondFieldInRange(BigDecimal field)
    {
        // we should never get handed a negative field value
        assert field.compareTo(ZERO) >= 0;

        // Fractional second fields are only restricted by precision, which
        // has already been checked for using pattern matching.
        // Therefore, always return true
        return true;
    }

    private boolean isSecondaryFieldInRange(BigDecimal field, TimeUnit unit)
    {
        // we should never get handed a negative field value
        assert field.compareTo(ZERO) >= 0;

        // YEAR and DAY can never be secondary units,
        // nor can unit be null.
        assert (unit != null);
        switch (unit) {
        case YEAR:
        case DAY:
        default:
            throw Util.unexpected(unit);

        // Secondary field limits, as per section 4.6.3 of SQL2003 spec
        case MONTH:
        case HOUR:
        case MINUTE:
        case SECOND:
            return unit.isValidValue(field);
        }
    }

    private BigDecimal normalizeSecondFraction(String secondFracStr)
    {
        // Decimal value can be more than 3 digits. So just get
        // the millisecond part.
        return new BigDecimal("0." + secondFracStr).multiply(THOUSAND);
    }

    private int [] fillIntervalValueArray(
        int sign,
        BigDecimal year,
        BigDecimal month)
    {
        int [] ret = new int[3];

        ret[0] = sign;
        ret[1] = year.intValue();
        ret[2] = month.intValue();

        return (ret);
    }

    private int [] fillIntervalValueArray(
        int sign,
        BigDecimal day,
        BigDecimal hour,
        BigDecimal minute,
        BigDecimal second,
        BigDecimal secondFrac)
    {
        int [] ret = new int[6];

        ret[0] = sign;
        ret[1] = day.intValue();
        ret[2] = hour.intValue();
        ret[3] = minute.intValue();
        ret[4] = second.intValue();
        ret[5] = secondFrac.intValue();

        return (ret);
    }

    /**
     * Validates an INTERVAL literal against a YEAR interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsYear(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal year;

        // validate as YEAR(startPrecision), e.g. 'YY'
        String intervalPattern = "(\\d+)";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                year = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, year, TimeUnit.YEAR);

            // package values up for return
            return fillIntervalValueArray(sign, year, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a YEAR TO MONTH interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsYearToMonth(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal year, month;

        // validate as YEAR(startPrecision) TO MONTH, e.g. 'YY-DD'
        String intervalPattern = "(\\d+)-(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                year = parseField(m, 1);
                month = parseField(m, 2);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, year, TimeUnit.YEAR);
            if (!(isSecondaryFieldInRange(month, TimeUnit.MONTH))) {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(sign, year, month);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a MONTH interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMonth(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal month;

        // validate as MONTH(startPrecision), e.g. 'MM'
        String intervalPattern = "(\\d+)";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                month = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, month, TimeUnit.MONTH);

            // package values up for return
            return fillIntervalValueArray(sign, ZERO, month);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDay(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal day;

        // validate as DAY(startPrecision), e.g. 'DD'
        String intervalPattern = "(\\d+)";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, day, TimeUnit.DAY);

            // package values up for return
            return fillIntervalValueArray(sign, day, ZERO, ZERO, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO HOUR interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToHour(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal day, hour;

        // validate as DAY(startPrecision) TO HOUR, e.g. 'DD HH'
        String intervalPattern = "(\\d+) (\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1);
                hour = parseField(m, 2);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, day, TimeUnit.DAY);
            if (!(isSecondaryFieldInRange(hour, TimeUnit.HOUR))) {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(sign, day, hour, ZERO, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO MINUTE interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToMinute(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal day, hour, minute;

        // validate as DAY(startPrecision) TO MINUTE, e.g. 'DD HH:MM'
        String intervalPattern = "(\\d+) (\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1);
                hour = parseField(m, 2);
                minute = parseField(m, 3);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, day, TimeUnit.DAY);
            if (!(isSecondaryFieldInRange(hour, TimeUnit.HOUR))
                || !(isSecondaryFieldInRange(minute, TimeUnit.MINUTE)))
            {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(sign, day, hour, minute, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO SECOND interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToSecond(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal day, hour, minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as DAY(startPrecision) TO MINUTE,
        // e.g. 'DD HH:MM:SS' or 'DD HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1);
                hour = parseField(m, 2);
                minute = parseField(m, 3);
                second = parseField(m, 4);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(5));
            } else {
                secondFrac = ZERO;
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, day, TimeUnit.DAY);
            if (!(isSecondaryFieldInRange(hour, TimeUnit.HOUR))
                 || !(isSecondaryFieldInRange(minute, TimeUnit.MINUTE))
                 || !(isSecondaryFieldInRange(second, TimeUnit.SECOND))
                 || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(
                sign,
                day,
                hour,
                minute,
                second,
                secondFrac);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHour(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal hour;

        // validate as HOUR(startPrecision), e.g. 'HH'
        String intervalPattern = "(\\d+)";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                hour = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, hour, TimeUnit.HOUR);

            // package values up for return
            return fillIntervalValueArray(sign, ZERO, hour, ZERO, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO MINUTE interval
     * qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHourToMinute(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal hour, minute;

        // validate as HOUR(startPrecision) TO MINUTE, e.g. 'HH:MM'
        String intervalPattern = "(\\d+):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                hour = parseField(m, 1);
                minute = parseField(m, 2);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, hour, TimeUnit.HOUR);
            if (!(isSecondaryFieldInRange(minute, TimeUnit.MINUTE))) {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(sign, ZERO, hour, minute, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO SECOND interval
     * qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHourToSecond(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal hour, minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as HOUR(startPrecision) TO SECOND,
        // e.g. 'HH:MM:SS' or 'HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d+):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d+):(\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                hour = parseField(m, 1);
                minute = parseField(m, 2);
                second = parseField(m, 3);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(4));
            } else {
                secondFrac = ZERO;
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, hour, TimeUnit.HOUR);
            if (!(isSecondaryFieldInRange(minute, TimeUnit.MINUTE))
                || !(isSecondaryFieldInRange(second, TimeUnit.SECOND))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(
                sign,
                ZERO,
                hour,
                minute,
                second,
                secondFrac);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMinute(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal minute;

        // validate as MINUTE(startPrecision), e.g. 'MM'
        String intervalPattern = "(\\d+)";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                minute = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, minute, TimeUnit.MINUTE);

            // package values up for return
            return fillIntervalValueArray(sign, ZERO, ZERO, minute, ZERO, ZERO);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE TO SECOND interval
     * qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMinuteToSecond(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as MINUTE(startPrecision) TO SECOND,
        // e.g. 'MM:SS' or 'MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d+):(\\d{1,2})\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d+):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                minute = parseField(m, 1);
                second = parseField(m, 2);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(3));
            } else {
                secondFrac = ZERO;
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, minute, TimeUnit.MINUTE);
            if (!(isSecondaryFieldInRange(second, TimeUnit.SECOND))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(
                sign,
                ZERO,
                ZERO,
                minute,
                second,
                secondFrac);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal against an SECOND interval qualifier.
     *
     * @throws SqlValidatorException if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsSecond(
        int sign,
        String value,
        String originalValue)
        throws SqlValidatorException
    {
        BigDecimal second, secondFrac;
        boolean hasFractionalSecond;

        // validate as SECOND(startPrecision, fractionalSecondPrecision)
        // e.g. 'SS' or 'SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d+)\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d+)";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                second = parseField(m, 1);
            } catch (NumberFormatException e) {
                throw intervalidValueException(originalValue);
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(2));
            } else {
                secondFrac = ZERO;
            }

            // Validate individual fields
            checkLeadFieldInRange(sign, second, TimeUnit.SECOND);
            if (!(isFractionalSecondFieldInRange(secondFrac))) {
                throw intervalidValueException(originalValue);
            }

            // package values up for return
            return fillIntervalValueArray(
                sign, ZERO, ZERO, ZERO, second, secondFrac);
        } else {
            throw intervalidValueException(originalValue);
        }
    }

    /**
     * Validates an INTERVAL literal according to the rules specified by the
     * interval qualifier. The assumption is made that the interval qualfier has
     * been validated prior to calling this method. Evaluating against an
     * invalid qualifier could lead to strange results.
     *
     * @return field values, never null
     * @throws SqlValidatorException if the interval value is illegal
     */
    public int [] evaluateIntervalLiteral(
        String value)
        throws SqlValidatorException
    {
        // save original value for if we have to throw
        final String value0 = value;

        // First strip off any leading whitespace
        value = value.trim();

        // check if the sign was explicitly specified.  Record
        // the explicit or implicit sign, and strip it off to
        // simplify pattern matching later.
        final int sign = getIntervalSign(value);
        value = stripLeadingSign(value);

        // If we have an empty or null literal at this point,
        // it's illegal.  Complain and bail out.
        if (Util.isNullOrEmpty(value)) {
            throw intervalidValueException(value0);
        }

        // Validate remaining string according to the pattern
        // that corresponds to the start and end units as
        // well as explicit or implicit precision and range.
        switch (timeUnitRange) {
        case YEAR:
            return evaluateIntervalLiteralAsYear(sign, value, value0);
        case YEAR_TO_MONTH:
            return evaluateIntervalLiteralAsYearToMonth(sign, value, value0);
        case MONTH:
            return evaluateIntervalLiteralAsMonth(sign, value, value0);
        case DAY:
            return evaluateIntervalLiteralAsDay(sign, value, value0);
        case DAY_TO_HOUR:
            return evaluateIntervalLiteralAsDayToHour(sign, value, value0);
        case DAY_TO_MINUTE:
            return evaluateIntervalLiteralAsDayToMinute(sign, value, value0);
        case DAY_TO_SECOND:
            return evaluateIntervalLiteralAsDayToSecond(sign, value, value0);
        case HOUR:
            return evaluateIntervalLiteralAsHour(sign, value, value0);
        case HOUR_TO_MINUTE:
            return evaluateIntervalLiteralAsHourToMinute(sign, value, value0);
        case HOUR_TO_SECOND:
            return evaluateIntervalLiteralAsHourToSecond(sign, value, value0);
        case MINUTE:
            return evaluateIntervalLiteralAsMinute(sign, value, value0);
        case MINUTE_TO_SECOND:
            return evaluateIntervalLiteralAsMinuteToSecond(sign, value, value0);
        case SECOND:
            return evaluateIntervalLiteralAsSecond(sign, value, value0);
        default:
            throw intervalidValueException(value0);
        }
    }

    private BigDecimal parseField(Matcher m, int i)
    {
        return new BigDecimal(m.group(i));
    }

    private SqlValidatorException intervalidValueException(String value)
    {
        return EigenbaseResource.instance().UnsupportedIntervalLiteral.ex(
            "'" + value + "'",
            "INTERVAL " + toString());
    }

    private SqlValidatorException fieldExceedsPrecisionException(
        int sign, BigDecimal value, TimeUnit type, int precision)
    {
        if (sign == -1) {
            value = value.negate();
        }
        return EigenbaseResource.instance().IntervalFieldExceedsPrecision.ex(
            value,
            type.name() + "(" + precision + ")");
    }
}

// End SqlIntervalQualifier.java
