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
package org.eigenbase.sql.type;

import java.math.*;

import java.sql.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * Enumeration of the type names which can be used to construct a SQL type.
 * Rationale for this class's existence (instead of just using the standard
 * java.sql.Type ordinals):
 *
 * <ul>
 * <li>{@link java.sql.Types} does not include all SQL2003 data-types;
 * <li>SqlTypeName provides a type-safe enumeration;
 * <li>SqlTypeName provides a place to hang extra information such as whether
 * the type carries precision and scale.
 * </ul>
 */
public enum SqlTypeName
{
    BOOLEAN(PrecScale.NoNo, false, Types.BOOLEAN),
    TINYINT(PrecScale.NoNo, false, Types.TINYINT),
    SMALLINT(PrecScale.NoNo, false, Types.SMALLINT),
    INTEGER(PrecScale.NoNo, false, Types.INTEGER),
    BIGINT(PrecScale.NoNo, false, Types.BIGINT),
    DECIMAL(
        PrecScale.NoNo
        | PrecScale.YesNo
        | PrecScale.YesYes,
        false,
        Types.DECIMAL), FLOAT(PrecScale.NoNo, false, Types.FLOAT),
    REAL(PrecScale.NoNo, false, Types.REAL),
    DOUBLE(PrecScale.NoNo, false, Types.DOUBLE),
    DATE(PrecScale.NoNo, false, Types.DATE),
    TIME(PrecScale.NoNo | PrecScale.YesNo, false, Types.TIME),
    TIMESTAMP(PrecScale.NoNo | PrecScale.YesNo, false, Types.TIMESTAMP),
    INTERVAL_YEAR_MONTH(PrecScale.NoNo, false, Types.OTHER),
    INTERVAL_DAY_TIME(
        PrecScale.NoNo
        | PrecScale.YesNo
        | PrecScale.YesYes,
        false,
        Types.OTHER),
    CHAR(PrecScale.NoNo | PrecScale.YesNo, false, Types.CHAR),
    VARCHAR(PrecScale.NoNo | PrecScale.YesNo, false, Types.VARCHAR),
    BINARY(PrecScale.NoNo | PrecScale.YesNo, false, Types.BINARY),
    VARBINARY(PrecScale.NoNo | PrecScale.YesNo, false, Types.VARBINARY),
    NULL(PrecScale.NoNo, true, Types.NULL),
    ANY(PrecScale.NoNo, true, Types.JAVA_OBJECT),
    SYMBOL(PrecScale.NoNo, true, Types.OTHER),
    MULTISET(PrecScale.NoNo, false, Types.ARRAY),
    ARRAY(PrecScale.NoNo, false, Types.ARRAY),
    MAP(PrecScale.NoNo, false, Types.OTHER),
    DISTINCT(PrecScale.NoNo, false, Types.DISTINCT),
    STRUCTURED(PrecScale.NoNo, false, Types.STRUCT),
    ROW(PrecScale.NoNo, false, Types.STRUCT),
    OTHER(PrecScale.NoNo, false, Types.OTHER),
    CURSOR(PrecScale.NoNo, false, Types.OTHER + 1),
    COLUMN_LIST(PrecScale.NoNo, false, Types.OTHER + 2);

    private static SqlTypeName [] jdbcTypeToName;

    // Basing type name mapping on these constants is fragile, since newer
    // JDK versions may introduce new types with values outside of these
    // boundaries.
    // TODO: Find a less fragile way to map type constants to names
    public static final int MIN_JDBC_TYPE = ExtraSqlTypes.LONGNVARCHAR;
    public static final int MAX_JDBC_TYPE = ExtraSqlTypes.NCLOB;

    public static final int JAVA6_NCHAR = -15;

    public static final int MAX_DATETIME_PRECISION = 3;
    public static final int MAX_NUMERIC_PRECISION = 19;
    public static final int MAX_NUMERIC_SCALE = 19;
    public static final int MAX_CHAR_LENGTH = 65536;
    public static final int MAX_BINARY_LENGTH = 65536;

    // Minimum and default interval precisions are  defined by SQL2003
    // Maximum interval precisions are implementation dependent,
    //  but must be at least the default value
    public static final int DEFAULT_INTERVAL_START_PRECISION = 2;
    public static final int DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION = 6;
    public static final int MIN_INTERVAL_START_PRECISION = 1;
    public static final int MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION = 1;
    public static final int MAX_INTERVAL_START_PRECISION = 10;
    public static final int MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION = 9;

    // Cached map of enum values
    private static final Map<String, SqlTypeName> VALUES_MAP =
        Util.enumConstants(SqlTypeName.class);

    // categorizations used by SqlTypeFamily definitions

    // you probably want to use JDK 1.5 support for treating enumeration
    // as collection instead; this is only here to support
    // SqlTypeFamily.ANY
    public static final List<SqlTypeName> allTypes =
        ImmutableList.of(
            BOOLEAN, INTEGER, VARCHAR, DATE, TIME, TIMESTAMP, NULL, DECIMAL,
            ANY, CHAR, BINARY, VARBINARY, TINYINT, SMALLINT, BIGINT, REAL,
            DOUBLE, SYMBOL, INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME,
            FLOAT, MULTISET, DISTINCT, STRUCTURED, ROW, CURSOR, COLUMN_LIST);

    public static final List<SqlTypeName> booleanTypes =
        ImmutableList.of(BOOLEAN);

    public static final List<SqlTypeName> binaryTypes =
        ImmutableList.of(BINARY, VARBINARY);

    public static final List<SqlTypeName> intTypes =
        ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT);

    public static final List<SqlTypeName> exactTypes =
        combine(
            intTypes,
            ImmutableList.of(DECIMAL));

    public static final List<SqlTypeName> approxTypes =
        ImmutableList.of(FLOAT, REAL, DOUBLE);

    public static final List<SqlTypeName> numericTypes =
        combine(exactTypes, approxTypes);

    public static final List<SqlTypeName> fractionalTypes =
        combine(approxTypes, ImmutableList.of(DECIMAL));

    public static final List<SqlTypeName> charTypes =
        ImmutableList.of(CHAR, VARCHAR);

    public static final List<SqlTypeName> stringTypes =
        combine(charTypes, binaryTypes);

    public static final List<SqlTypeName> datetimeTypes =
        ImmutableList.of(DATE, TIME, TIMESTAMP);

    public static final List<SqlTypeName> intervalTypes =
        ImmutableList.of(INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH);

    static {
        // This squanders some memory since MAX_JDBC_TYPE == 2006!
        jdbcTypeToName = new SqlTypeName[(1 + MAX_JDBC_TYPE) - MIN_JDBC_TYPE];

        setNameForJdbcType(Types.TINYINT, TINYINT);
        setNameForJdbcType(Types.SMALLINT, SMALLINT);
        setNameForJdbcType(Types.BIGINT, BIGINT);
        setNameForJdbcType(Types.INTEGER, INTEGER);
        setNameForJdbcType(Types.NUMERIC, DECIMAL); // REVIEW
        setNameForJdbcType(Types.DECIMAL, DECIMAL);

        setNameForJdbcType(Types.FLOAT, FLOAT);
        setNameForJdbcType(Types.REAL, REAL);
        setNameForJdbcType(Types.DOUBLE, DOUBLE);

        setNameForJdbcType(Types.CHAR, CHAR);
        setNameForJdbcType(Types.VARCHAR, VARCHAR);

        // TODO: provide real support for these eventually
        setNameForJdbcType(ExtraSqlTypes.NCHAR, CHAR);
        setNameForJdbcType(ExtraSqlTypes.NVARCHAR, VARCHAR);

        // TODO: additional types not yet supported. See ExtraSqlTypes.java
        // setNameForJdbcType(Types.LONGVARCHAR, Longvarchar);
        // setNameForJdbcType(Types.CLOB, Clob);
        // setNameForJdbcType(Types.LONGVARBINARY, Longvarbinary);
        // setNameForJdbcType(Types.BLOB, Blob);
        // setNameForJdbcType(Types.LONGNVARCHAR, Longnvarchar);
        // setNameForJdbcType(Types.NCLOB, Nclob);
        // setNameForJdbcType(Types.ROWID, Rowid);
        // setNameForJdbcType(Types.SQLXML, Sqlxml);

        setNameForJdbcType(Types.BINARY, BINARY);
        setNameForJdbcType(Types.VARBINARY, VARBINARY);

        setNameForJdbcType(Types.DATE, DATE);
        setNameForJdbcType(Types.TIME, TIME);
        setNameForJdbcType(Types.TIMESTAMP, TIMESTAMP);
        setNameForJdbcType(Types.BIT, BOOLEAN);
        setNameForJdbcType(Types.BOOLEAN, BOOLEAN);
        setNameForJdbcType(Types.DISTINCT, DISTINCT);
        setNameForJdbcType(Types.STRUCT, STRUCTURED);
    }

    /**
     * Bitwise-or of flags indicating allowable precision/scale combinations.
     */
    private final int signatures;

    /**
     * Returns true if not of a "pure" standard sql type. "Inpure" types are
     * {@link #ANY}, {@link #NULL} and {@link #SYMBOL}
     */
    private final boolean special;
    private final int jdbcOrdinal;

    private SqlTypeName(int signatures, boolean special, int jdbcType)
    {
        this.signatures = signatures;
        this.special = special;
        this.jdbcOrdinal = jdbcType;
    }

    /**
     * Looks up a type name from its name.
     *
     * @return Type name, or null if not found
     */
    public static SqlTypeName get(String name)
    {
        if (false) {
            // The following code works OK, but the spurious exceptions are
            // annoying.
            try {
                return SqlTypeName.valueOf(name);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return VALUES_MAP.get(name);
    }

    public boolean allowsNoPrecNoScale()
    {
        return (signatures & PrecScale.NoNo) != 0;
    }

    public boolean allowsPrecNoScale()
    {
        return (signatures & PrecScale.YesNo) != 0;
    }

    public boolean allowsPrec()
    {
        return allowsPrecScale(true, true)
            || allowsPrecScale(true, false);
    }

    public boolean allowsScale()
    {
        return allowsPrecScale(true, true);
    }

    /**
     * Returns whether this type can be specified with a given combination of
     * precision and scale. For example,
     *
     * <ul>
     * <li><code>Varchar.allowsPrecScale(true, false)</code> returns <code>
     * true</code>, because the VARCHAR type allows a precision parameter, as in
     * <code>VARCHAR(10)</code>.</li>
     * <li><code>Varchar.allowsPrecScale(true, true)</code> returns <code>
     * true</code>, because the VARCHAR type does not allow a precision and a
     * scale parameter, as in <code>VARCHAR(10, 4)</code>.</li>
     * <li><code>allowsPrecScale(false, true)</code> returns <code>false</code>
     * for every type.</li>
     * </ul>
     *
     * @param precision Whether the precision/length field is part of the type
     * specification
     * @param scale Whether the scale field is part of the type specification
     *
     * @return Whether this combination of precision/scale is valid
     */
    public boolean allowsPrecScale(
        boolean precision,
        boolean scale)
    {
        int mask =
            precision ? (scale ? PrecScale.YesYes : PrecScale.YesNo)
            : (scale ? 0 : PrecScale.NoNo);
        return (signatures & mask) != 0;
    }

    public boolean isSpecial()
    {
        return special;
    }

    /**
     * @return the ordinal from {@link java.sql.Types} corresponding to this
     * SqlTypeName
     */
    public int getJdbcOrdinal()
    {
        return jdbcOrdinal;
    }

    private static List<SqlTypeName> combine(
        List<SqlTypeName> array0,
        List<SqlTypeName> array1)
    {
        return ImmutableList.<SqlTypeName>builder()
            .addAll(array0)
            .addAll(array1)
            .build();
    }

    /**
     * @return default precision for this type if supported, otherwise -1 if
     * precision is either unsupported or must be specified explicitly
     */
    public int getDefaultPrecision()
    {
        switch (this) {
        case CHAR:
        case BINARY:
        case VARCHAR:
        case VARBINARY:
            return 1;
        case TIME:
            return 0;
        case TIMESTAMP:

            // TODO jvs 26-July-2004:  should be 6 for microseconds,
            // but we can't support that yet
            return 0;
        case DECIMAL:
            return MAX_NUMERIC_PRECISION;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return DEFAULT_INTERVAL_START_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * @return default scale for this type if supported, otherwise -1 if scale
     * is either unsupported or must be specified explicitly
     */
    public int getDefaultScale()
    {
        switch (this) {
        case DECIMAL:
            return 0;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * Gets the SqlTypeFamily containing this SqlTypeName.
     *
     * @return containing family, or null for none
     */
    public SqlTypeFamily getFamily()
    {
        return SqlTypeFamily.getFamilyForSqlType(this);
    }

    /**
     * Gets the SqlTypeName corresponding to a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     *
     * @return corresponding SqlTypeName
     */
    public static SqlTypeName getNameForJdbcType(int jdbcType)
    {
        return jdbcTypeToName[jdbcType - MIN_JDBC_TYPE];
    }

    private static void setNameForJdbcType(
        int jdbcType,
        SqlTypeName name)
    {
        jdbcTypeToName[jdbcType - MIN_JDBC_TYPE] = name;
    }

    /**
     * Returns the limit of this datatype. For example,
     *
     * <table border="1">
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
     * </tr>
     * <tr>
     * <td>Boolean</th>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
     * </tr>
     * <tr>
     * <td>Varchar</th>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
     * </tr>
     * </table>
     *
     * @param sign If true, returns upper limit, otherwise lower limit
     * @param limit If true, returns value at or near to overflow; otherwise
     * value at or near to underflow
     * @param beyond If true, returns the value just beyond the limit, otherwise
     * the value at the limit
     * @param precision Precision, or -1 if not applicable
     * @param scale Scale, or -1 if not applicable
     *
     * @return Limit value
     */
    public Object getLimit(
        boolean sign,
        Limit limit,
        boolean beyond,
        int precision,
        int scale)
    {
        assert allowsPrecScale(precision != -1, scale != -1) : this;
        if (limit == Limit.ZERO) {
            if (beyond) {
                return null;
            }
            sign = true;
        }
        Calendar calendar;

        switch (this) {
        case BOOLEAN:
            switch (limit) {
            case ZERO:
                return false;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond || !sign) {
                    return null;
                } else {
                    return true;
                }
            default:
                throw Util.unexpected(limit);
            }

        case TINYINT:
            return getNumericLimit(2, 8, sign, limit, beyond);

        case SMALLINT:
            return getNumericLimit(2, 16, sign, limit, beyond);

        case INTEGER:
            return getNumericLimit(2, 32, sign, limit, beyond);

        case BIGINT:
            return getNumericLimit(2, 64, sign, limit, beyond);

        case DECIMAL:
            BigDecimal decimal =
                getNumericLimit(10, precision, sign, limit, beyond);
            if (decimal == null) {
                return null;
            }

            // Decimal values must fit into 64 bits. So, the maximum value of
            // a DECIMAL(19, 0) is 2^63 - 1, not 10^19 - 1.
            switch (limit) {
            case OVERFLOW:
                final BigDecimal other =
                    (BigDecimal) BIGINT.getLimit(sign, limit, beyond, -1, -1);
                if (decimal.compareTo(other) == (sign ? 1 : -1)) {
                    decimal = other;
                }
            }

            // Apply scale.
            if (scale == 0) {
                ;
            } else if (scale > 0) {
                decimal = decimal.divide(BigDecimal.TEN.pow(scale));
            } else {
                decimal = decimal.multiply(BigDecimal.TEN.pow(-scale));
            }
            return decimal;

        case CHAR:
        case VARCHAR:
            if (!sign) {
                return null; // this type does not have negative values
            }
            StringBuilder buf = new StringBuilder();
            switch (limit) {
            case ZERO:
                break;
            case UNDERFLOW:
                if (beyond) {
                    // There is no value between the empty string and the
                    // smallest non-empty string.
                    return null;
                }
                buf.append("a");
                break;
            case OVERFLOW:
                for (int i = 0; i < precision; ++i) {
                    buf.append("Z");
                }
                if (beyond) {
                    buf.append("Z");
                }
                break;
            }
            return buf.toString();

        case BINARY:
        case VARBINARY:
            if (!sign) {
                return null; // this type does not have negative values
            }
            byte [] bytes;
            switch (limit) {
            case ZERO:
                bytes = new byte[0];
                break;
            case UNDERFLOW:
                if (beyond) {
                    // There is no value between the empty string and the
                    // smallest value.
                    return null;
                }
                bytes = new byte[] { 0x00 };
                break;
            case OVERFLOW:
                bytes = new byte[precision + (beyond ? 1 : 0)];
                Arrays.fill(bytes, (byte) 0xff);
                break;
            default:
                throw Util.unexpected(limit);
            }
            return bytes;

        case DATE:
            calendar = Calendar.getInstance();
            switch (limit) {
            case ZERO:

                // The epoch.
                calendar.set(Calendar.YEAR, 1970);
                calendar.set(Calendar.MONTH, 0);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond) {
                    // It is impossible to represent an invalid year as a date
                    // literal. SQL dates are represented as 'yyyy-mm-dd', and
                    // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                    // before 1AD is 1BC, so SimpleDateFormat renders the day
                    // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                    // like a valid date.
                    return null;
                }

                // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                // between 1 and 9999, and days/months are the valid Gregorian
                // calendar values for these years.
                if (sign) {
                    calendar.set(Calendar.YEAR, 9999);
                    calendar.set(Calendar.MONTH, 11);
                    calendar.set(Calendar.DAY_OF_MONTH, 31);
                } else {
                    calendar.set(Calendar.YEAR, 1);
                    calendar.set(Calendar.MONTH, 0);
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                }
                break;
            }
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            return calendar;

        case TIME:
            if (!sign) {
                return null; // this type does not have negative values
            }
            if (beyond) {
                return null; // invalid values are impossible to represent
            }
            calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            switch (limit) {
            case ZERO:

                // The epoch.
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                calendar.set(Calendar.HOUR_OF_DAY, 23);
                calendar.set(Calendar.MINUTE, 59);
                calendar.set(Calendar.SECOND, 59);
                int millis =
                    (precision >= 3) ? 999
                    : ((precision == 2) ? 990 : ((precision == 1) ? 900 : 0));
                calendar.set(Calendar.MILLISECOND, millis);
                break;
            }
            return calendar;

        case TIMESTAMP:
            calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            switch (limit) {
            case ZERO:

                // The epoch.
                calendar.set(Calendar.YEAR, 1970);
                calendar.set(Calendar.MONTH, 0);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond) {
                    // It is impossible to represent an invalid year as a date
                    // literal. SQL dates are represented as 'yyyy-mm-dd', and
                    // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                    // before 1AD is 1BC, so SimpleDateFormat renders the day
                    // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                    // like a valid date.
                    return null;
                }

                // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                // between 1 and 9999, and days/months are the valid Gregorian
                // calendar values for these years.
                if (sign) {
                    calendar.set(Calendar.YEAR, 9999);
                    calendar.set(Calendar.MONTH, 11);
                    calendar.set(Calendar.DAY_OF_MONTH, 31);
                    calendar.set(Calendar.HOUR_OF_DAY, 23);
                    calendar.set(Calendar.MINUTE, 59);
                    calendar.set(Calendar.SECOND, 59);
                    int millis =
                        (precision >= 3) ? 999
                        : ((precision == 2) ? 990
                            : ((precision == 1) ? 900 : 0));
                    calendar.set(Calendar.MILLISECOND, millis);
                } else {
                    calendar.set(Calendar.YEAR, 1);
                    calendar.set(Calendar.MONTH, 0);
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                }
                break;
            }
            return calendar;

        default:
            throw Util.unexpected(this);
        }
    }

    /**
     * Returns the maximum precision (or length) allowed for this type, or -1 if
     * precision/length are not applicable for this type.
     *
     * @return Maximum allowed precision
     */
    public int getMaxPrecision()
    {
        switch (this) {
        case DECIMAL:
            return MAX_NUMERIC_PRECISION;
        case VARCHAR:
        case CHAR:
            return MAX_CHAR_LENGTH;
        case VARBINARY:
        case BINARY:
            return MAX_BINARY_LENGTH;
        case TIME:
        case TIMESTAMP:
            return MAX_DATETIME_PRECISION;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return MAX_INTERVAL_START_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * Returns the maximum scale (or fractional second precision in the case of
     * intervals) allowed for this type, or -1 if precision/length are not
     * applicable for this type.
     *
     * @return Maximum allowed scale
     */
    public int getMaxScale()
    {
        switch (this) {
        case DECIMAL:
            return MAX_NUMERIC_SCALE;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * Returns the minimum precision (or length) allowed for this type, or -1 if
     * precision/length are not applicable for this type.
     *
     * @return Minimum allowed precision
     */
    public int getMinPrecision()
    {
        switch (this) {
        case DECIMAL:
        case VARCHAR:
        case CHAR:
        case VARBINARY:
        case BINARY:
        case TIME:
        case TIMESTAMP:
            return 1;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return MIN_INTERVAL_START_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * Returns the minimum scale (or fractional second precision in the case of
     * intervals) allowed for this type, or -1 if precision/length are not
     * applicable for this type.
     *
     * @return Minimum allowed scale
     */
    public int getMinScale()
    {
        switch (this) {
        // TODO: Minimum numeric scale for decimal
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION;
        default:
            return -1;
        }
    }

    public enum Limit
    {
        ZERO, UNDERFLOW, OVERFLOW
    }

    private BigDecimal getNumericLimit(
        int radix,
        int exponent,
        boolean sign,
        Limit limit,
        boolean beyond)
    {
        switch (limit) {
        case OVERFLOW:

            // 2-based schemes run from -2^(N-1) to 2^(N-1)-1 e.g. -128 to +127
            // 10-based schemas run from -(10^N-1) to 10^N-1 e.g. -99 to +99
            final BigDecimal bigRadix = BigDecimal.valueOf(radix);
            if (radix == 2) {
                --exponent;
            }
            BigDecimal decimal = bigRadix.pow(exponent);
            if (sign || (radix != 2)) {
                decimal = decimal.subtract(BigDecimal.ONE);
            }
            if (beyond) {
                decimal = decimal.add(BigDecimal.ONE);
            }
            if (!sign) {
                decimal = decimal.negate();
            }
            return decimal;
        case UNDERFLOW:
            return beyond ? null
                : (sign ? BigDecimal.ONE : BigDecimal.ONE.negate());
        case ZERO:
            return BigDecimal.ZERO;
        default:
            throw Util.unexpected(limit);
        }
    }

    public SqlLiteral createLiteral(Object o, SqlParserPos pos)
    {
        switch (this) {
        case BOOLEAN:
            return SqlLiteral.createBoolean((Boolean) o, pos);
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
            return SqlLiteral.createExactNumeric(o.toString(), pos);
        case VARCHAR:
        case CHAR:
            return SqlLiteral.createCharString((String) o, pos);
        case VARBINARY:
        case BINARY:
            return SqlLiteral.createBinaryString((byte []) o, pos);
        case DATE:
            return SqlLiteral.createDate((Calendar) o, pos);
        case TIME:
            return SqlLiteral.createTime((Calendar) o, 0 /* todo */, pos);
        case TIMESTAMP:
            return SqlLiteral.createTimestamp((Calendar) o, 0 /* todo */, pos);
        default:
            throw Util.unexpected(this);
        }
    }

    /**
     * @return name of this type
     */
    public String getName()
    {
        return toString();
    }

    /**
     * Flags indicating precision/scale combinations.
     *
     * <p>Note: for intervals:
     *
     * <ul>
     * <li>precision = start (leading field) precision</li>
     * <li>scale = fractional second precision</li>
     * </ul>
     */
    private interface PrecScale
    {
        int NoNo = 1;
        int YesNo = 2;
        int YesYes = 4;
    }
}

// End SqlTypeName.java
