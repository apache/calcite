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
package org.eigenbase.sql.test;

import java.math.*;

import java.sql.*;
import java.text.*;

import java.util.*;
import java.util.regex.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.SqlTester.VmName;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.test.*;
import org.eigenbase.util.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>The class is abstract. It contains a test for every operator, but does not
 * provide a mechanism to execute the tests: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which the derived class must provide.
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:
 *
 * <ul>
 * <li>Execute against a real farrago database
 * <li>Execute in pure java (parsing and validation can be done, but expression
 * evaluation is not possible)
 * <li>Generate a SQL script.
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing (say
 * <code>testSubstringFunc</code>). It first calls {@link
 * SqlTester#setFor(org.eigenbase.sql.SqlOperator,
 * org.eigenbase.sql.test.SqlTester.VmName...)} to declare which operator it is
 * testing. <blockqoute>
 *
 * <pre><code>
 * @Test public void testSubstringFunc() {
 *     getTester().setFor(SqlStdOperatorTable.substringFunc);
 *     getTester().checkScalar("sin(0)", "0");
 *     getTester().checkScalar("sin(1.5707)", "1");
 * }</code></pre>
 *
 * </blockqoute> The rest of the method contains calls to the various <code>
 * checkXxx</code> methods in the {@link SqlTester} interface. For an operator
 * to be adequately tested, there need to be tests for:
 *
 * <ul>
 * <li>Parsing all of its the syntactic variants.
 * <li>Deriving the type of in all combinations of arguments.
 *
 * <ul>
 * <li>Pay particular attention to nullability. For example, the result of the
 * "+" operator is NOT NULL if and only if both of its arguments are NOT
 * NULL.</li>
 * <li>Also pay attention to precsion/scale/length. For example, the maximum
 * length of the "||" operator is the sum of the maximum lengths of its
 * arguments.</li>
 * </ul>
 * </li>
 * <li>Executing the function. Pay particular attention to corner cases such as
 * null arguments or null results.</li>
 * </ul>
 */
public abstract class SqlOperatorBaseTest {
    //~ Static fields/initializers ---------------------------------------------

    public static final String NL = TestUtil.NL;

    // TODO: Change message when Fnl3Fixed to something like
    // "Invalid character for cast: PC=0 Code=22018"
    public static final String invalidCharMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Overflow during calculation or cast: PC=0 Code=22003"
    public static final String outOfRangeMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Division by zero: PC=0 Code=22012"
    public static final String divisionByZeroMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "String right truncation: PC=0 Code=22001"
    public static final String stringTruncMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Invalid datetime format: PC=0 Code=22007"
    public static final String badDatetimeMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    public static final String literalOutOfRangeMessage =
        "(?s).*Numeric literal.*out of range.*";

    public static final boolean todo = false;

    /**
     * Regular expression for a SQL TIME(0) value.
     */
    public static final Pattern timePattern =
        Pattern.compile(
            "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

    /**
     * Regular expression for a SQL TIMESTAMP(0) value.
     */
    public static final Pattern timestampPattern =
        Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] "
            + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

    /**
     * Regular expression for a SQL DATE value.
     */
    public static final Pattern datePattern =
        Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

    public static final String [] numericTypeNames =
        new String[] {
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "DECIMAL(5, 2)", "REAL", "FLOAT", "DOUBLE"
        };

    // REVIEW jvs 27-Apr-2006:  for Float and Double, MIN_VALUE
    // is the smallest positive value, not the smallest negative value
    public static final String [] minNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE), Long.toString(
                Short.MIN_VALUE), Long.toString(Integer.MIN_VALUE),
            Long.toString(Long.MIN_VALUE), "-999.99",

            // NOTE jvs 26-Apr-2006:  Win32 takes smaller values from
            // win32_values.h
            "1E-37", /*Float.toString(Float.MIN_VALUE)*/
            "2E-307", /*Double.toString(Double.MIN_VALUE)*/
            "2E-307" /*Double.toString(Double.MIN_VALUE)*/,
        };

    public static final String [] minOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE - 1),
            Long.toString(Short.MIN_VALUE - 1),
            Long.toString((long) Integer.MIN_VALUE - 1),
            new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString(),
            "-1000.00",
            "1e-46",
            "1e-324",
            "1e-324"
        };

    public static final String [] maxNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE), Long.toString(
                Short.MAX_VALUE), Long.toString(Integer.MAX_VALUE),
            Long.toString(Long.MAX_VALUE), "999.99",

            // NOTE jvs 26-Apr-2006:  use something slightly less than MAX_VALUE
            // because roundtripping string to approx to string doesn't preserve
            // MAX_VALUE on win32
            "3.4028234E38", /*Float.toString(Float.MAX_VALUE)*/
            "1.79769313486231E308", /*Double.toString(Double.MAX_VALUE)*/
            "1.79769313486231E308" /*Double.toString(Double.MAX_VALUE)*/
        };

    public static final String [] maxOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE + 1),
            Long.toString(Short.MAX_VALUE + 1),
            Long.toString((long) Integer.MAX_VALUE + 1),
            (new BigDecimal(Long.MAX_VALUE)).add(BigDecimal.ONE).toString(),
            "1000.00",
            "1e39",
            "-1e309",
            "1e309"
        };
    private static final boolean [] FalseTrue = new boolean[] { false, true };
    private static final SqlTester.VmName VM_FENNEL = SqlTester.VmName.FENNEL;
    private static final SqlTester.VmName VM_JAVA = SqlTester.VmName.JAVA;
    private static final SqlTester.VmName VM_EXPAND = SqlTester.VmName.EXPAND;
    protected static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");
    protected static final TimeZone defaultTimeZone = TimeZone.getDefault();

    private static final Pattern invalidArgForPower = Pattern.compile(
        "(?s).*Invalid argument\\(s\\) for 'POWER' function.*");

    private static final Pattern code2201f = Pattern.compile(
        "(?s).*could not calculate results for the following row.*PC=5 Code=2201F.*");

    /** Whether DECIMAL type is implemented. */
    public static final boolean DECIMAL = false;

    /** Whether INTERVAL type is implemented. */
    public static final boolean INTERVAL = false;

    /** Whether to run "failing" tests. */
    private final boolean enable;

    //~ Constructors -----------------------------------------------------------

    public SqlOperatorBaseTest() {
        this(true);
    }

    protected SqlOperatorBaseTest(boolean enable) {
        this.enable = enable;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Derived class must implement this method to provide a means to validate,
     * execute various statements.
     */
    protected abstract SqlTester getTester();

    @Before public void setUp() throws Exception {
        getTester().setFor(null);
    }

    //--- Tests -----------------------------------------------------------

    /** For development. Put any old code in here. */
    @Test public void testDummy() {
        checkCastToScalarOkay("'1'", "INTEGER", "1");
    }

    @Test public void testBetween() {
        getTester().setFor(
            SqlStdOperatorTable.betweenOperator,
            SqlTester.VmName.EXPAND);
        getTester().checkBoolean("2 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("2 between 3 and 2", Boolean.FALSE);
        getTester().checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
        getTester().checkBoolean("3 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("4 between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("1 between 4 and -3", Boolean.FALSE);
        getTester().checkBoolean("1 between -1 and -3", Boolean.FALSE);
        getTester().checkBoolean("1 between -1 and 3", Boolean.TRUE);
        getTester().checkBoolean("1 between 1 and 1", Boolean.TRUE);
        getTester().checkBoolean("1.5 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("1.2 between 1.1 and 1.3", Boolean.TRUE);
        getTester().checkBoolean("1.5 between 2 and 3", Boolean.FALSE);
        getTester().checkBoolean("1.5 between 1.6 and 1.7", Boolean.FALSE);
        getTester().checkBoolean("1.2e1 between 1.1 and 1.3", Boolean.FALSE);
        getTester().checkBoolean("1.2e0 between 1.1 and 1.3", Boolean.TRUE);
        getTester().checkBoolean("1.5e0 between 2 and 3", Boolean.FALSE);
        getTester().checkBoolean("1.5e0 between 2e0 and 3e0", Boolean.FALSE);
        getTester().checkBoolean(
            "1.5e1 between 1.6e1 and 1.7e1",
            Boolean.FALSE);
        if (!enable) {
            return;
        }
        getTester().checkBoolean("x'' between x'' and x''", Boolean.TRUE);
        getTester().checkNull("cast(null as integer) between -1 and 2");
        getTester().checkNull("1 between -1 and cast(null as integer)");
        getTester().checkNull(
            "1 between cast(null as integer) and cast(null as integer)");
        getTester().checkNull("1 between cast(null as integer) and 1");
    }

    @Test public void testNotBetween() {
        getTester().setFor(SqlStdOperatorTable.notBetweenOperator, VM_EXPAND);
        getTester().checkBoolean("2 not between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("3 not between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("4 not between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean(
            "1.2e0 not between 1.1 and 1.3",
            Boolean.FALSE);
        getTester().checkBoolean("1.2e1 not between 1.1 and 1.3", Boolean.TRUE);
        getTester().checkBoolean("1.5e0 not between 2 and 3", Boolean.TRUE);
        getTester().checkBoolean("1.5e0 not between 2e0 and 3e0", Boolean.TRUE);
    }

    private String getCastString(
        String value,
        String targetType,
        boolean errorLoc)
    {
        if (errorLoc) {
            value = "^" + value + "^";
        }
        return "cast(" + value + " as " + targetType + ")";
    }

    private void checkCastToApproxOkay(
        String value,
        String targetType,
        double expected,
        double delta)
    {
        getTester().checkScalarApprox(
            getCastString(value, targetType, false),
            targetType + " NOT NULL",
            expected,
            delta);
    }

    private void checkCastToStringOkay(
        String value,
        String targetType,
        String expected)
    {
        getTester().checkString(
            getCastString(value, targetType, false),
            expected,
            targetType + " NOT NULL");
    }

    private void checkCastToScalarOkay(
        String value,
        String targetType,
        String expected)
    {
        getTester().checkScalarExact(
            getCastString(value, targetType, false),
            targetType + " NOT NULL",
            expected);
    }

    private void checkCastToScalarOkay(String value, String targetType)
    {
        checkCastToScalarOkay(value, targetType, value);
    }

    private void checkCastFails(
        String value,
        String targetType,
        String expectedError,
        boolean runtime)
    {
        getTester().checkFails(
            getCastString(value, targetType, !runtime),
            expectedError,
            runtime);
    }

    private void checkCastToString(String value, String type, String expected)
    {
        String spaces = "     ";
        if (expected == null) {
            expected = value.trim();
        }
        int len = expected.length();
        if (type != null) {
            value = getCastString(value, type, false);
        }

        // currently no exception thrown for truncation
        if (Bug.Dt239Fixed) {
            checkCastFails(
                value,
                "VARCHAR(" + (len - 1) + ")",
                stringTruncMessage,
                true);
        }

        checkCastToStringOkay(value, "VARCHAR(" + len + ")", expected);
        checkCastToStringOkay(value, "VARCHAR(" + (len + 5) + ")", expected);

        // currently no exception thrown for truncation
        if (Bug.Dt239Fixed) {
            checkCastFails(
                value,
                "CHAR(" + (len - 1) + ")",
                stringTruncMessage,
                true);
        }

        checkCastToStringOkay(
            value,
            "CHAR(" + len + ")",
            expected);
        checkCastToStringOkay(
            value,
            "CHAR(" + (len + 5) + ")",
            expected + spaces);
    }

    @Test public void testCastToString() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // integer
        checkCastToString("123", "CHAR(3)", "123");
        checkCastToString("0", "CHAR", "0");
        checkCastToString("-123", "CHAR(4)", "-123");

        // decimal
        checkCastToString("123.4", "CHAR(5)", "123.4");
        checkCastToString("-0.0", "CHAR(2)", ".0");
        checkCastToString("-123.4", "CHAR(6)", "-123.4");

        getTester().checkString(
            "cast(1.29 as varchar(10))",
            "1.29",
            "VARCHAR(10) NOT NULL");
        getTester().checkString(
            "cast(.48 as varchar(10))",
            ".48",
            "VARCHAR(10) NOT NULL");
        getTester().checkFails(
            "cast(2.523 as char(2))",
            stringTruncMessage,
            true);

        getTester().checkString(
            "cast(-0.29 as varchar(10))",
            "-.29",
            "VARCHAR(10) NOT NULL");
        getTester().checkString(
            "cast(-1.29 as varchar(10))",
            "-1.29",
            "VARCHAR(10) NOT NULL");

        // approximate
        checkCastToString("1.23E45", "CHAR(7)", "1.23E45");
        checkCastToString("CAST(0 AS DOUBLE)", "CHAR(3)", "0E0");
        checkCastToString("-1.20e-07", "CHAR(7)", "-1.2E-7");
        checkCastToString("cast(0e0 as varchar(5))", "CHAR(3)", "0E0");
        if (todo) {
            checkCastToString(
                "cast(-45e-2 as varchar(17))", "CHAR(7)",
                "-4.5E-1");
        }
        if (todo) {
            checkCastToString(
                "cast(4683442.3432498375e0 as varchar(20))",
                "CHAR(19)",
                "4.683442343249838E6");
        }
        if (todo) {
            checkCastToString("cast(-0.1 as real)", "CHAR(5)", "-1E-1");
        }

        getTester().checkFails(
            "cast(1.3243232e0 as varchar(4))",
            stringTruncMessage,
            true);
        getTester().checkFails(
            "cast(1.9e5 as char(4))",
            stringTruncMessage,
            true);

        // string
        checkCastToString("'abc'", "CHAR(1)", "a");
        checkCastToString("'abc'", "CHAR(3)", "abc");
        checkCastToString("cast('abc' as varchar(6))", "CHAR(3)", "abc");

        // date & time
        checkCastToString("date '2008-01-01'", "CHAR(10)", "2008-01-01");
        checkCastToString("time '1:2:3'", "CHAR(8)", "01:02:03");
        checkCastToString(
            "timestamp '2008-1-1 1:2:3'",
            "CHAR(19)",
            "2008-01-01 01:02:03");
        checkCastToString(
            "timestamp '2008-1-1 1:2:3'",
            "VARCHAR(30)",
            "2008-01-01 01:02:03");

        // todo: cast of intervals to strings not supported in fennel
        if (!(getTester().isVm(VmName.FENNEL)) && INTERVAL) {
            checkCastToString(
                "interval '3-2' year to month",
                "CHAR(5)",
                "+3-02");
            checkCastToString(
                "interval '32' month",
                "CHAR(3)",
                "+32");
            checkCastToString(
                "interval '1 2:3:4' day to second",
                "CHAR(11)",
                "+1 02:03:04");
            checkCastToString(
                "interval '1234.56' second(4,2)",
                "CHAR(8)",
                "+1234.56");
        }

        // boolean
        checkCastToString("True", "CHAR(4)", "TRUE");
        checkCastToString("False", "CHAR(5)", "FALSE");
        getTester().checkFails(
            "cast(true as char(3))",
            invalidCharMessage,
            true);
        getTester().checkFails(
            "cast(false as char(4))",
            invalidCharMessage,
            true);
        getTester().checkFails(
            "cast(true as varchar(3))",
            invalidCharMessage,
            true);
        getTester().checkFails(
            "cast(false as varchar(4))",
            invalidCharMessage,
            true);
    }

    @Test public void testCastExactNumericLimits() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // Test casting for min,max, out of range for exact numeric types
        for (int i = 0; i < numericTypeNames.length; i++) {
            String type = numericTypeNames[i];

            if (type.equalsIgnoreCase("DOUBLE")
                || type.equalsIgnoreCase("FLOAT")
                || type.equalsIgnoreCase("REAL"))
            {
                // Skip approx types
                continue;
            }

            // Convert from literal to type
            checkCastToScalarOkay(maxNumericStrings[i], type);
            checkCastToScalarOkay(minNumericStrings[i], type);

            // Overflow test
            if (type.equalsIgnoreCase("BIGINT")) {
                // Literal of range
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
                checkCastFails(
                    minOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
            } else {
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
                checkCastFails(
                    minOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
            }

            if (!enable) {
                return;
            }
            // Convert from string to type
            checkCastToScalarOkay(
                "'" + maxNumericStrings[i] + "'",
                type,
                maxNumericStrings[i]);
            checkCastToScalarOkay(
                "'" + minNumericStrings[i] + "'",
                type,
                minNumericStrings[i]);

            checkCastFails(
                "'" + maxOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);
            checkCastFails(
                "'" + minOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);

            // Convert from type to string
            checkCastToString(maxNumericStrings[i], null, null);
            checkCastToString(maxNumericStrings[i], type, null);

            checkCastToString(minNumericStrings[i], null, null);
            checkCastToString(minNumericStrings[i], type, null);

            checkCastFails("'notnumeric'", type, invalidCharMessage, true);
        }
    }

    @Test public void testCastToExactNumeric() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        checkCastToScalarOkay("1", "BIGINT");
        checkCastToScalarOkay("1", "INTEGER");
        checkCastToScalarOkay("1", "SMALLINT");
        checkCastToScalarOkay("1", "TINYINT");
        checkCastToScalarOkay("1", "DECIMAL(4, 0)");
        checkCastToScalarOkay("-1", "BIGINT");
        checkCastToScalarOkay("-1", "INTEGER");
        checkCastToScalarOkay("-1", "SMALLINT");
        checkCastToScalarOkay("-1", "TINYINT");
        checkCastToScalarOkay("-1", "DECIMAL(4, 0)");

        checkCastToScalarOkay("1.234E3", "INTEGER", "1234");
        checkCastToScalarOkay("-9.99E2", "INTEGER", "-999");
        checkCastToScalarOkay("'1'", "INTEGER", "1");
        checkCastToScalarOkay("' 01 '", "INTEGER", "1");
        checkCastToScalarOkay("'-1'", "INTEGER", "-1");
        checkCastToScalarOkay("' -00 '", "INTEGER", "0");

        // string to integer
        getTester().checkScalarExact("cast('6543' as integer)", "6543");
        getTester().checkScalarExact("cast(' -123 ' as int)", "-123");
        getTester().checkScalarExact(
            "cast('654342432412312' as bigint)",
            "BIGINT NOT NULL",
            "654342432412312");
    }

    @Test public void testCastStringToDecimal() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        if (!DECIMAL) {
            return;
        }
        // string to decimal
        getTester().checkScalarExact(
            "cast('1.29' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        getTester().checkScalarExact(
            "cast(' 1.25 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        getTester().checkScalarExact(
            "cast('1.21' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.2");
        getTester().checkScalarExact(
            "cast(' -1.29 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        getTester().checkScalarExact(
            "cast('-1.25' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        getTester().checkScalarExact(
            "cast(' -1.21 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");
        getTester().checkFails(
            "cast(' -1.21e' as decimal(2,1))",
            invalidCharMessage,
            true);
    }

    @Test public void testCastIntervalToNumeric() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        if (!INTERVAL) {
            return;
        }

        // interval to decimal
        getTester().checkScalarExact(
            "cast(INTERVAL '1.29' second(1,2) as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        getTester().checkScalarExact(
            "cast(INTERVAL '1.25' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        getTester().checkScalarExact(
            "cast(INTERVAL '-1.29' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        getTester().checkScalarExact(
            "cast(INTERVAL '-1.25' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        getTester().checkScalarExact(
            "cast(INTERVAL '-1.21' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' minute as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' hour as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' day as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' month as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' year as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "cast(INTERVAL '-5' day as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-5.0");

        // Interval to bigint
        getTester().checkScalarExact(
            "cast(INTERVAL '1.25' second as bigint)",
            "BIGINT NOT NULL",
            "1");
        getTester().checkScalarExact(
            "cast(INTERVAL '-1.29' second(1,2) as bigint)",
            "BIGINT NOT NULL",
            "-1");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' day as bigint)",
            "BIGINT NOT NULL",
            "5");

        // Interval to integer
        getTester().checkScalarExact(
            "cast(INTERVAL '1.25' second as integer)",
            "INTEGER NOT NULL",
            "1");
        getTester().checkScalarExact(
            "cast(INTERVAL '-1.29' second(1,2) as integer)",
            "INTEGER NOT NULL",
            "-1");
        getTester().checkScalarExact(
            "cast(INTERVAL '5' day as integer)",
            "INTEGER NOT NULL",
            "5");
    }

    @Test public void testCastToInterval() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "cast(5 as interval second)",
            "+5",
            "INTERVAL SECOND NOT NULL");
        getTester().checkScalar(
            "cast(5 as interval minute)",
            "+5",
            "INTERVAL MINUTE NOT NULL");
        getTester().checkScalar(
            "cast(5 as interval hour)",
            "+5",
            "INTERVAL HOUR NOT NULL");
        getTester().checkScalar(
            "cast(5 as interval day)",
            "+5",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "cast(5 as interval month)",
            "+5",
            "INTERVAL MONTH NOT NULL");
        getTester().checkScalar(
            "cast(5 as interval year)",
            "+5",
            "INTERVAL YEAR NOT NULL");
        getTester().checkScalar(
            "cast(5.7 as interval day)",
            "+6",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "cast(-5.7 as interval day)",
            "-6",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "cast(3456 as interval month(4))",
            "+3456",
            "INTERVAL MONTH(4) NOT NULL");
        getTester().checkScalar(
            "cast(-5723 as interval minute(4))",
            "-5723",
            "INTERVAL MINUTE(4) NOT NULL");
    }

    @Test public void testCastWithRoundingToScalar() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        checkCastToScalarOkay("1.25", "INTEGER", "1");
        checkCastToScalarOkay("1.25E0", "INTEGER", "1");
        if (!enable) {
            return;
        }
        checkCastToScalarOkay("1.5", "INTEGER", "2");
        checkCastToScalarOkay("5E-1", "INTEGER", "1");
        checkCastToScalarOkay("1.75", "INTEGER", "2");
        checkCastToScalarOkay("1.75E0", "INTEGER", "2");

        checkCastToScalarOkay("-1.25", "INTEGER", "-1");
        checkCastToScalarOkay("-1.25E0", "INTEGER", "-1");
        checkCastToScalarOkay("-1.5", "INTEGER", "-2");
        checkCastToScalarOkay("-5E-1", "INTEGER", "-1");
        checkCastToScalarOkay("-1.75", "INTEGER", "-2");
        checkCastToScalarOkay("-1.75E0", "INTEGER", "-2");

        checkCastToScalarOkay("1.23454", "DECIMAL(8, 4)", "1.2345");
        checkCastToScalarOkay("1.23454E0", "DECIMAL(8, 4)", "1.2345");
        checkCastToScalarOkay("1.23455", "DECIMAL(8, 4)", "1.2346");
        checkCastToScalarOkay("5E-5", "DECIMAL(8, 4)", "0.0001");
        checkCastToScalarOkay("1.99995", "DECIMAL(8, 4)", "2.0000");
        checkCastToScalarOkay("1.99995E0", "DECIMAL(8, 4)", "2.0000");

        checkCastToScalarOkay("-1.23454", "DECIMAL(8, 4)", "-1.2345");
        checkCastToScalarOkay("-1.23454E0", "DECIMAL(8, 4)", "-1.2345");
        checkCastToScalarOkay("-1.23455", "DECIMAL(8, 4)", "-1.2346");
        checkCastToScalarOkay("-5E-5", "DECIMAL(8, 4)", "-0.0001");
        checkCastToScalarOkay("-1.99995", "DECIMAL(8, 4)", "-2.0000");
        checkCastToScalarOkay("-1.99995E0", "DECIMAL(8, 4)", "-2.0000");

        // 9.99 round to 10.0, should give out of range error
        getTester().checkFails(
            "cast(9.99 as decimal(2,1))",
            outOfRangeMessage,
            true);
    }

    @Test public void testCastDecimalToDoubleToInteger() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        getTester().checkScalarExact(
            "cast( cast(1.25 as double) as integer)",
            "1");
        getTester().checkScalarExact(
            "cast( cast(-1.25 as double) as integer)",
            "-1");
        if (!enable) {
            return;
        }
        getTester().checkScalarExact(
            "cast( cast(1.75 as double) as integer)",
            "2");
        getTester().checkScalarExact(
            "cast( cast(-1.75 as double) as integer)",
            "-2");
        getTester().checkScalarExact(
            "cast( cast(1.5 as double) as integer)",
            "2");
        getTester().checkScalarExact(
            "cast( cast(-1.5 as double) as integer)",
            "-2");
    }

    @Test public void testCastApproxNumericLimits() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // Test casting for min,max, out of range for approx numeric types
        for (int i = 0; i < numericTypeNames.length; i++) {
            String type = numericTypeNames[i];
            boolean isFloat;

            if (type.equalsIgnoreCase("DOUBLE")
                || type.equalsIgnoreCase("FLOAT"))
            {
                isFloat = false;
            } else if (type.equalsIgnoreCase("REAL")) {
                isFloat = true;
            } else {
                // Skip non-approx types
                continue;
            }

            if (!enable) {
                return;
            }

            // Convert from literal to type
            checkCastToApproxOkay(
                maxNumericStrings[i],
                type,
                Double.parseDouble(maxNumericStrings[i]),
                isFloat ? 1E32 : 0);
            checkCastToApproxOkay(
                minNumericStrings[i],
                type,
                Double.parseDouble(minNumericStrings[i]),
                0);

            if (isFloat) {
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
            } else {
                // Double: Literal out of range
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
            }

            // Underflow: goes to 0
            checkCastToApproxOkay(minOverflowNumericStrings[i], type, 0, 0);

            // Convert from string to type
            checkCastToApproxOkay(
                "'" + maxNumericStrings[i] + "'",
                type,
                Double.parseDouble(maxNumericStrings[i]),
                isFloat ? 1E32 : 0);
            checkCastToApproxOkay(
                "'" + minNumericStrings[i] + "'",
                type,
                Double.parseDouble(minNumericStrings[i]),
                0);

            checkCastFails(
                "'" + maxOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);

            // Underflow: goes to 0
            checkCastToApproxOkay(
                "'" + minOverflowNumericStrings[i] + "'",
                type,
                0,
                0);

            // Convert from type to string

            // Treated as DOUBLE
            checkCastToString(
                maxNumericStrings[i],
                null,
                isFloat ? null : "1.79769313486231E308");

            // TODO: The following tests are slightly different depending on
            // whether the java or fennel calc are used.
            // Try to make them the same
            if (false /* fennel calc*/) { // Treated as FLOAT or DOUBLE
                checkCastToString(
                    maxNumericStrings[i],
                    type,
                    // Treated as DOUBLE
                    isFloat ? "3.402824E38" : "1.797693134862316E308");
                checkCastToString(
                    minNumericStrings[i],
                    null,
                    // Treated as FLOAT or DOUBLE
                    isFloat ? null : "4.940656458412465E-324");
                checkCastToString(
                    minNumericStrings[i],
                    type,
                    isFloat ? "1.401299E-45" : "4.940656458412465E-324");
            } else if (false /* JavaCalc */) {
                // Treated as FLOAT or DOUBLE
                checkCastToString(
                    maxNumericStrings[i],
                    type,
                    // Treated as DOUBLE
                    isFloat ? "3.402823E38" : "1.797693134862316E308");
                checkCastToString(
                    minNumericStrings[i],
                    null,
                    isFloat ? null : null); // Treated as FLOAT or DOUBLE
                checkCastToString(
                    minNumericStrings[i],
                    type,
                    isFloat ? "1.401298E-45" : null);
            }

            checkCastFails("'notnumeric'", type, invalidCharMessage, true);
        }
    }

    @Test public void testCastToApproxNumeric() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        checkCastToApproxOkay("1", "DOUBLE", 1, 0);
        checkCastToApproxOkay("1.0", "DOUBLE", 1, 0);
        checkCastToApproxOkay("-2.3", "FLOAT", -2.3, 0.000001);
        checkCastToApproxOkay("'1'", "DOUBLE", 1, 0);
        checkCastToApproxOkay("'  -1e-37  '", "DOUBLE", -1e-37, 0);
        checkCastToApproxOkay("1e0", "DOUBLE", 1, 0);
        checkCastToApproxOkay("0e0", "REAL", 0, 0);
    }

    @Test public void testCastNull() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // null
        getTester().checkNull("cast(null as integer)");
        if (DECIMAL) {
            getTester().checkNull("cast(null as decimal(4,3))");
        }
        getTester().checkNull("cast(null as double)");
        getTester().checkNull("cast(null as varchar(10))");
        getTester().checkNull("cast(null as char(10))");
        if (!enable) {
            return;
        }
        getTester().checkNull("cast(null as date)");
        getTester().checkNull("cast(null as time)");
        getTester().checkNull("cast(null as timestamp)");
        if (INTERVAL) {
            getTester().checkNull("cast(null as interval year to month)");
        getTester().checkNull("cast(null as interval day to second(3))");
        }
        getTester().checkNull("cast(null as boolean)");
    }

    @Test public void testCastDateTime() {
        // Test cast for date/time/timestamp
        getTester().setFor(SqlStdOperatorTable.castFunc);

        getTester().checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");

        getTester().checkScalar(
            "cast(TIME '12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        // test rounding
        getTester().checkScalar(
            "cast(TIME '12:42:25.9' as TIME)",
            "12:42:26",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            // test precision
            getTester().checkScalar(
                "cast(TIME '12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        getTester().checkScalar(
            "cast(DATE '1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // timestamp <-> time
        getTester().checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        // time <-> string
        checkCastToString("TIME '12:42:25'", null, "12:42:25");
        if (todo) {
            checkCastToString("TIME '12:42:25.34'", null, "12:42:25.34");
        }

        // Generate the current date as a string, e.g. "2007-04-18". The value
        // is guaranteed to be good for at least 2 minutes, which should give
        // us time to run the rest of the tests.
        final String today =
            new SimpleDateFormat("yyyy-MM-dd").format(
                getCalendarNotTooNear(Calendar.DAY_OF_MONTH).getTime());

        getTester().checkScalar(
            "cast(DATE '1945-02-24' as TIMESTAMP)",
            "1945-02-24 00:00:00.0",
            "TIMESTAMP(0) NOT NULL");

        if (!enable) {
            return;
        }

        // Note: Casting to time(0) should lose date info and fractional
        // seconds, then casting back to timestamp should initialize to
        // current_date.
        getTester().checkScalar(
            "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME) as TIMESTAMP)",
            today + " 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        getTester().checkScalar(
            "cast(TIME '12:42:25.34' as TIMESTAMP)",
            today + " 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        // timestamp <-> date
        getTester().checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // Note: casting to Date discards Time fields
        getTester().checkScalar(
            "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE) as TIMESTAMP)",
            "1945-02-24 00:00:00",
            "TIMESTAMP(0) NOT NULL");

        getTester().checkScalar(
            "cast('12:42:25' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "cast('1:42:25' as TIME)",
            "01:42:25",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "cast('1:2:25' as TIME)",
            "01:02:25",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "cast('  12:42:25  ' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "cast('12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            getTester().checkScalar(
                "cast('12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        getTester().checkFails(
            "cast('nottime' as TIME)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('1241241' as TIME)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('12:54:78' as TIME)",
            badDatetimeMessage,
            true);

        // timestamp <-> string
        checkCastToString(
            "TIMESTAMP '1945-02-24 12:42:25'",
            null,
            "1945-02-24 12:42:25");

        if (todo) {
            // TODO: casting allows one to discard precision without error
            checkCastToString(
                "TIMESTAMP '1945-02-24 12:42:25.34'",
                null,
                "1945-02-24 12:42:25.34");
        }

        getTester().checkScalar(
            "cast('1945-02-24 12:42:25' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");
        getTester().checkScalar(
            "cast('1945-2-2 12:2:5' as TIMESTAMP)",
            "1945-02-02 12:02:05",
            "TIMESTAMP(0) NOT NULL");
        getTester().checkScalar(
            "cast('  1945-02-24 12:42:25  ' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");
        getTester().checkScalar(
            "cast('1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            getTester().checkScalar(
                "cast('1945-02-24 12:42:25.34' as TIMESTAMP(2))",
                "1945-02-24 12:42:25.34",
                "TIMESTAMP(2) NOT NULL");
        }
        getTester().checkFails(
            "cast('nottime' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('1241241' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('1945-20-24 12:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('1945-01-24 25:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);

        // date <-> string
        checkCastToString("DATE '1945-02-24'", null, "1945-02-24");
        checkCastToString("DATE '1945-2-24'", null, "1945-02-24");

        getTester().checkScalar(
            "cast('1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        getTester().checkScalar(
            "cast('  1945-02-24  ' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        getTester().checkFails(
            "cast('notdate' as DATE)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('52534253' as DATE)",
            badDatetimeMessage,
            true);
        getTester().checkFails(
            "cast('1945-30-24' as DATE)",
            badDatetimeMessage,
            true);

        // cast null
        getTester().checkNull("cast(null as date)");
        getTester().checkNull("cast(null as timestamp)");
        getTester().checkNull("cast(null as time)");
        getTester().checkNull("cast(cast(null as varchar(10)) as time)");
        getTester().checkNull("cast(cast(null as varchar(10)) as date)");
        getTester().checkNull("cast(cast(null as varchar(10)) as timestamp)");
        getTester().checkNull("cast(cast(null as date) as timestamp)");
        getTester().checkNull("cast(cast(null as time) as timestamp)");
        getTester().checkNull("cast(cast(null as timestamp) as date)");
        getTester().checkNull("cast(cast(null as timestamp) as time)");
    }

    /**
     * Returns a Calendar that is the current time, pausing if we are within 2
     * minutes of midnight or the top of the hour.
     *
     * @param timeUnit Time unit
     *
     * @return calendar
     */
    protected static Calendar getCalendarNotTooNear(int timeUnit)
    {
        while (true) {
            Calendar cal = Calendar.getInstance();
            final Calendar fcal;
            try {
                switch (timeUnit) {
                case Calendar.DAY_OF_MONTH:

                    // Within two minutes of the end of the day.
                    // Wait 2 minutes to force calendar into next
                    // day, then get a new instance to return
                    if ((cal.get(Calendar.HOUR_OF_DAY) == 23)
                        && (cal.get(Calendar.MINUTE) >= 58))
                    {
                        Thread.sleep(2 * 60 * 1000);
                        cal = Calendar.getInstance();
                        continue;
                    }
                    fcal = cal;
                    return fcal;
                case Calendar.HOUR_OF_DAY:

                    // Within two minutes of the top of the hour.
                    // Wait 2 minutes to force calendar into next
                    // hour, then get a new instance to return
                    if ((cal.get(Calendar.MINUTE) >= 58)) {
                        Thread.sleep(2 * 60 * 1000);
                        cal = Calendar.getInstance();
                        continue;
                    }
                    fcal = cal;
                    return fcal;
                default:
                    throw Util.newInternal("unexpected time unit " + timeUnit);
                }
            } catch (InterruptedException e) {
                throw Util.newInternal(e);
            }
        }
    }

    @Test public void testCastToBoolean() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // string to boolean
        getTester().checkBoolean("cast('true' as boolean)", Boolean.TRUE);
        getTester().checkBoolean("cast('false' as boolean)", Boolean.FALSE);
        getTester().checkBoolean("cast('  trUe' as boolean)", Boolean.TRUE);
        getTester().checkBoolean("cast('  fALse' as boolean)", Boolean.FALSE);
        getTester().checkFails(
            "cast('unknown' as boolean)",
            invalidCharMessage,
            true);

        getTester().checkBoolean(
            "cast(cast('true' as varchar(10))  as boolean)",
            Boolean.TRUE);
        getTester().checkBoolean(
            "cast(cast('false' as varchar(10)) as boolean)",
            Boolean.FALSE);
        getTester().checkFails(
            "cast(cast('blah' as varchar(10)) as boolean)",
            invalidCharMessage,
            true);
    }

    @Test public void testCase() {
        getTester().setFor(SqlStdOperatorTable.caseOperator);
        getTester().checkScalarExact("case when 'a'='a' then 1 end", "1");

        getTester().checkString(
            "case 2 when 1 then 'a' when 2 then 'bcd' end",
            "bcd",
            "CHAR(3)");
        getTester().checkString(
            "case 1 when 1 then 'a' when 2 then 'bcd' end",
            "a  ",
            "CHAR(3)");
        getTester().checkString(
            "case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
            "a",
            "VARCHAR(3)");

        if (DECIMAL) {
        getTester().checkScalarExact(
            "case 2 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "4.543");
        getTester().checkScalarExact(
            "case 1 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "11.200");
        }
        getTester().checkScalarExact("case 'a' when 'a' then 1 end", "1");
        getTester().checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then cast(4 as bigint) else 3 end",
            "DOUBLE NOT NULL",
            11.2,
            0);
        getTester().checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            11.2,
            0);
        getTester().checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            4,
            0);
        getTester().checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            11.2,
            0);
        getTester().checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            4.543,
            0);
        getTester().checkNull("case 'a' when 'b' then 1 end");
        if (!enable) {
            return;
        }
        getTester().checkScalarExact(
            "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
            "2");

        if (todo) {
            getTester().checkScalar(
                "case 1 when 1 then row(1,2) when 2 then row(2,3) end",
                "ROW(INTEGER NOT NULL, INTEGER NOT NULL)",
                "row(1,2)");
            getTester().checkScalar(
                "case 1 when 1 then row('a','b') when 2 then row('ab','cd') end",
                "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)",
                "row('a ','b ')");
        }
        // TODO: Check case with multisets
    }

    @Test public void testCaseType() {
        getTester().setFor(SqlStdOperatorTable.caseOperator);
        getTester().checkType(
            "case 1 when 1 then current_timestamp else null end",
            "TIMESTAMP(0)");
        getTester().checkType(
            "case 1 when 1 then current_timestamp else current_timestamp end",
            "TIMESTAMP(0) NOT NULL");
        getTester().checkType(
            "case when true then current_timestamp else null end",
            "TIMESTAMP(0)");
        getTester().checkType(
            "case when true then current_timestamp end",
            "TIMESTAMP(0)");
        getTester().checkType(
            "case 'x' when 'a' then 3 when 'b' then null else 4.5 end",
            "DECIMAL(11, 1)");
    }

    /**
     * Tests support for JDBC functions.
     *
     * <p>See FRG-97 "Support for JDBC escape syntax is incomplete".
     */
    @Test public void testJdbcFn() {
        getTester().setFor(new SqlJdbcFunctionCall("dummy"));

        // There follows one test for each function in appendix C of the JDBC
        // 3.0 specification. The test is 'if-false'd out if the function is
        // not implemented or is broken.

        // Numeric Functions
        if (!enable) {
            return;
        }
        getTester().checkScalar("{fn ABS(-3)}", 3, "INTEGER NOT NULL");
        if (false) {
            getTester().checkScalar("{fn ACOS(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn ASIN(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn ATAN(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn ATAN2(float1, float2)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn CEILING(-2.6)}", 2, "");
        }
        if (false) {
            getTester().checkScalar("{fn COS(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn COT(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn DEGREES(number)}", null, "");
        }
        getTester().checkScalarApprox(
            "{fn EXP(2)}",
            "DOUBLE NOT NULL",
            7.389,
            0.001);
        if (false) {
            getTester().checkScalar("{fn FLOOR(2.6)}", 2, "DOUBLE NOT NULL");
        }
        getTester().checkScalarApprox(
            "{fn LOG(10)}",
            "DOUBLE NOT NULL",
            2.30258,
            0.001);
        getTester().checkScalarApprox(
            "{fn LOG10(100)}",
            "DOUBLE NOT NULL",
            2,
            0);
        getTester().checkScalar("{fn MOD(19, 4)}", 3, "INTEGER NOT NULL");
        if (false) {
            getTester().checkScalar("{fn PI()}", null, "");
        }
        getTester().checkScalar("{fn POWER(2, 3)}", 8.0, "DOUBLE NOT NULL");
        if (false) {
            getTester().checkScalar("{fn RADIANS(number)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn RAND(integer)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn ROUND(number, places)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn SIGN(number)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn SIN(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn SQRT(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn TAN(float)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn TRUNCATE(number, places)}", null, "");
        }

        // String Functions
        if (false) {
            getTester().checkScalar("{fn ASCII(string)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn CHAR(code)}", null, "");
        }
        getTester().checkScalar(
            "{fn CONCAT('foo', 'bar')}",
            "foobar",
            "CHAR(6) NOT NULL");
        if (false) {
            getTester().checkScalar(
                "{fn DIFFERENCE(string1, string2)}",
                null,
                "");
        }

        // REVIEW: is this result correct? I think it should be "abcCdef"
        getTester().checkScalar(
            "{fn INSERT('abc', 1, 2, 'ABCdef')}",
            "ABCdefc",
            "VARCHAR(9) NOT NULL");
        getTester().checkScalar(
            "{fn LCASE('foo' || 'bar')}",
            "foobar",
            "CHAR(6) NOT NULL");
        if (false) {
            getTester().checkScalar("{fn LEFT(string, count)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn LENGTH(string)}", null, "");
        }
        getTester().checkScalar(
            "{fn LOCATE('ha', 'alphabet')}",
            4,
            "INTEGER NOT NULL");

        // only the 2 arg version of locate is implemented
        if (false) {
            getTester().checkScalar(
                "{fn LOCATE(string1, string2[, start])}",
                null,
                "");
        }

        // ltrim is implemented but has a bug in arg checking
        if (false) {
            getTester().checkScalar(
                "{fn LTRIM(' xxx  ')}",
                "xxx",
                "VARCHAR(6)");
        }
        if (false) {
            getTester().checkScalar("{fn REPEAT(string, count)}", null, "");
        }
        if (false) {
            getTester().checkScalar(
                "{fn REPLACE(string1, string2, string3)}",
                null,
                "");
        }
        if (false) {
            getTester().checkScalar("{fn RIGHT(string, count)}", null, "");
        }

        // rtrim is implemented but has a bug in arg checking
        if (false) {
            getTester().checkScalar(
                "{fn RTRIM(' xxx  ')}",
                "xxx",
                "VARCHAR(6)");
        }
        if (false) {
            getTester().checkScalar("{fn SOUNDEX(string)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn SPACE(count)}", null, "");
        }
        getTester().checkScalar(
            "{fn SUBSTRING('abcdef', 2, 3)}",
            "bcd",
            "VARCHAR(6) NOT NULL");
        getTester().checkScalar("{fn UCASE('xxx')}", "XXX", "CHAR(3) NOT NULL");

        // Time and Date Functions
        getTester().checkType("{fn CURDATE()}", "DATE NOT NULL");
        getTester().checkType("{fn CURTIME()}", "TIME(0) NOT NULL");
        if (false) {
            getTester().checkScalar("{fn DAYNAME(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn DAYOFMONTH(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn DAYOFWEEK(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn DAYOFYEAR(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn HOUR(time)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn MINUTE(time)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn MONTH(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn MONTHNAME(date)}", null, "");
        }
        getTester().checkType("{fn NOW()}", "TIMESTAMP(0) NOT NULL");
        if (false) {
            getTester().checkScalar("{fn QUARTER(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn SECOND(time)}", null, "");
        }
        if (false) {
            getTester().checkScalar(
                "{fn TIMESTAMPADD(interval, count, timestamp)}",
                null,
                "");
        }
        if (false) {
            getTester().checkScalar(
                "{fn TIMESTAMPDIFF(interval, timestamp1, timestamp2)}",
                null,
                "");
        }
        if (false) {
            getTester().checkScalar("{fn WEEK(date)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn YEAR(date)}", null, "");
        }

        // System Functions
        if (false) {
            getTester().checkScalar("{fn DATABASE()}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn IFNULL(expression, value)}", null, "");
        }
        if (false) {
            getTester().checkScalar("{fn USER()}", null, "");
        }

        // Conversion Functions
        if (false) {
            getTester().checkScalar("{fn CONVERT(value, SQLtype)}", null, "");
        }
    }

    @Test public void testSelect() {
        getTester().setFor(SqlStdOperatorTable.selectOperator, VM_EXPAND);
        getTester().check(
            "select * from (values(1))",
            AbstractSqlTester.IntegerTypeChecker,
            "1",
            0);

        // Check return type on scalar subquery in select list.  Note return
        // type is always nullable even if subquery select value is NOT NULL.
        // Bug FRG-189 causes this test to fail only in SqlOperatorTest; not
        // in subtypes.
        if (Bug.Frg189Fixed
            || (getClass() != SqlOperatorTest.class) && Bug.TodoFixed)
        {
            getTester().checkType(
                "SELECT *,(SELECT * FROM (VALUES(1))) FROM (VALUES(2))",
                "RecordType(INTEGER NOT NULL EXPR$0, INTEGER EXPR$1) NOT NULL");
            getTester().checkType(
                "SELECT *,(SELECT * FROM (VALUES(CAST(10 as BIGINT)))) "
                + "FROM (VALUES(CAST(10 as bigint)))",
                "RecordType(BIGINT NOT NULL EXPR$0, BIGINT EXPR$1) NOT NULL");
            getTester().checkType(
                " SELECT *,(SELECT * FROM (VALUES(10.5))) FROM (VALUES(10.5))",
                "RecordType(DECIMAL(3, 1) NOT NULL EXPR$0, DECIMAL(3, 1) EXPR$1) NOT NULL");
            getTester().checkType(
                "SELECT *,(SELECT * FROM (VALUES('this is a char'))) "
                + "FROM (VALUES('this is a char too'))",
                "RecordType(CHAR(18) NOT NULL EXPR$0, CHAR(14) EXPR$1) NOT NULL");
            getTester().checkType(
                "SELECT *,(SELECT * FROM (VALUES(true))) FROM (values(false))",
                "RecordType(BOOLEAN NOT NULL EXPR$0, BOOLEAN EXPR$1) NOT NULL");
            getTester().checkType(
                " SELECT *,(SELECT * FROM (VALUES(cast('abcd' as varchar(10))))) "
                + "FROM (VALUES(CAST('abcd' as varchar(10))))",
                "RecordType(VARCHAR(10) NOT NULL EXPR$0, VARCHAR(10) EXPR$1) NOT NULL");
            getTester().checkType(
                "SELECT *,"
                + "  (SELECT * FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))) "
                + "FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))",
                "RecordType(TIMESTAMP(0) NOT NULL EXPR$0, TIMESTAMP(0) EXPR$1) NOT NULL");
        }
    }

    @Test public void testLiteralChain() {
        getTester().setFor(SqlStdOperatorTable.literalChainOperator, VM_EXPAND);
        getTester().checkString(
            "'buttered'\n' toast'",
            "buttered toast",
            "CHAR(14) NOT NULL");
        getTester().checkString(
            "'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye",
            "CHAR(18) NOT NULL");
        getTester().checkString(
            "_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana",
            "CHAR(25) NOT NULL");
        if (!enable) {
            return;
        }
        getTester().checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
        getTester().checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
        getTester().checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
    }

    @Test public void testRow() {
        getTester().setFor(SqlStdOperatorTable.rowConstructor, VM_FENNEL);
    }

    @Test public void testAndOperator() {
        getTester().setFor(SqlStdOperatorTable.andOperator);
        getTester().checkBoolean("true and false", Boolean.FALSE);
        getTester().checkBoolean("true and true", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as boolean) and false",
            Boolean.FALSE);
        getTester().checkBoolean(
            "false and cast(null as boolean)",
            Boolean.FALSE);
        getTester().checkNull("cast(null as boolean) and true");
        getTester().checkBoolean("true and (not false)", Boolean.TRUE);
    }

    @Test public void testAndOperator2() {
        getTester().checkBoolean(
            "case when false then unknown else true end and true",
            Boolean.TRUE);
        getTester().checkBoolean(
            "case when false then cast(null as boolean) else true end and true",
            Boolean.TRUE);
        getTester().checkBoolean(
            "case when false then null else true end and true",
            Boolean.TRUE);
    }

    @Test public void testAndOperatorLazy() {
        getTester().setFor(SqlStdOperatorTable.andOperator);

        // lazy eval returns FALSE;
        // eager eval executes RHS of AND and throws;
        // both are valid
        getTester().check(
            "values 1 > 2 and sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                Boolean.FALSE, invalidArgForPower, code2201f));
    }

    @Test public void testConcatOperator() {
        getTester().setFor(SqlStdOperatorTable.concatOperator);
        getTester().checkString(" 'a'||'b' ", "ab", "CHAR(2) NOT NULL");
        getTester().checkNull(" 'a' || cast(null as char(2)) ");
        getTester().checkNull(" cast(null as char(2)) || 'b' ");
        getTester().checkNull(
            " cast(null as char(1)) || cast(null as char(2)) ");

        if (todo) {
            // not yet implemented
            getTester().checkString(
                " x'f'||x'f' ",
                "X'FF",
                "BINARY(1) NOT NULL");
            getTester().checkNull("x'ff' || cast(null as varbinary)");
        }
    }

    @Test public void testDivideOperator() {
        getTester().setFor(SqlStdOperatorTable.divideOperator);
        getTester().checkScalarExact("10 / 5", "2");
        getTester().checkScalarExact("-10 / 5", "-2");
        getTester().checkScalarExact("1 / 3", "0");
        getTester().checkScalarApprox(
            " cast(10.0 as double) / 5",
            "DOUBLE NOT NULL",
            2.0,
            0);
        getTester().checkScalarApprox(
            " cast(10.0 as real) / 5",
            "REAL NOT NULL",
            2.0,
            0);
        getTester().checkScalarApprox(
            " 6.0 / cast(10.0 as real) ",
            "DOUBLE NOT NULL",
            0.6,
            0);
        getTester().checkScalarExact(
            "10.0 / 5.0",
            "DECIMAL(9, 6) NOT NULL",
            "2");
        if (DECIMAL) {
            getTester().checkScalarExact(
                "1.0 / 3.0",
                "DECIMAL(8, 6) NOT NULL",
                "0.333333");
            getTester().checkScalarExact(
                "100.1 / 0.0001",
                "DECIMAL(14, 7) NOT NULL",
                "1001000.0000000");
            getTester().checkScalarExact(
                "100.1 / 0.00000001",
                "DECIMAL(19, 8) NOT NULL",
                "10010000000.00000000");
        }
        getTester().checkNull("1e1 / cast(null as float)");

        getTester().checkFails(
            "100.1 / 0.00000000000000001",
            outOfRangeMessage,
            true);
    }

    @Test public void testDivideOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "interval '-2:2' hour to minute / 3",
            "-0:40",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        getTester().checkScalar(
            "interval '2:5:12' hour to second / 2 / -3",
            "-0:20:52",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkNull(
            "interval '2' day / cast(null as bigint)");
        getTester().checkNull(
            "cast(null as interval month) / 2");
        if (todo) {
            getTester().checkScalar(
                "interval '3-3' year to month / 15e-1",
                "+02-02",
                "INTERVAL YEAR TO MONTH NOT NULL");
            getTester().checkScalar(
                "interval '3-4' year to month / 4.5",
                "+00-08",
                "INTERVAL YEAR TO MONTH NOT NULL");
        }
    }

    @Test public void testEqualsOperator() {
        getTester().setFor(SqlStdOperatorTable.equalsOperator);
        getTester().checkBoolean("1=1", Boolean.TRUE);
        getTester().checkBoolean("1=1.0", Boolean.TRUE);
        getTester().checkBoolean("1.34=1.34", Boolean.TRUE);
        getTester().checkBoolean("1=1.34", Boolean.FALSE);
        getTester().checkBoolean("1e2=100e0", Boolean.TRUE);
        getTester().checkBoolean("1e2=101", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(1e2 as real)=cast(101 as bigint)",
            Boolean.FALSE);
        getTester().checkBoolean("'a'='b'", Boolean.FALSE);
        getTester().checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        getTester().checkBoolean(
            "cast('a ' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        getTester().checkBoolean(
            "cast('a' as varchar(30))=cast('b' as varchar(30))",
            Boolean.FALSE);
        getTester().checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(15))",
            Boolean.TRUE);
        getTester().checkNull("cast(null as boolean)=cast(null as boolean)");
        getTester().checkNull("cast(null as integer)=1");
        getTester().checkNull("cast(null as varchar(10))='a'");
    }

    @Test public void testEqualsOperatorInterval() {
        // Intervals
        if (!INTERVAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day = interval '1' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day = interval '2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2:2:2' hour to second = interval '2' hour",
            Boolean.FALSE);
        getTester().checkNull(
            "cast(null as interval hour) = interval '2' minute");
    }

    @Test public void testGreaterThanOperator() {
        getTester().setFor(SqlStdOperatorTable.greaterThanOperator);
        getTester().checkBoolean("1>2", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(-1 as TINYINT)>cast(1 as TINYINT)",
            Boolean.FALSE);
        getTester().checkBoolean(
            "cast(1 as SMALLINT)>cast(1 as SMALLINT)",
            Boolean.FALSE);
        getTester().checkBoolean("2>1", Boolean.TRUE);
        getTester().checkBoolean("1.1>1.2", Boolean.FALSE);
        getTester().checkBoolean("-1.1>-1.2", Boolean.TRUE);
        getTester().checkBoolean("1.1>1.1", Boolean.FALSE);
        getTester().checkBoolean("1.2>1", Boolean.TRUE);
        getTester().checkBoolean("1.1e1>1.2e1", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(-1.1 as real) > cast(-1.2 as real)",
            Boolean.TRUE);
        getTester().checkBoolean("1.1e2>1.1e2", Boolean.FALSE);
        getTester().checkBoolean("1.2e0>1", Boolean.TRUE);
        getTester().checkBoolean("cast(1.2e0 as real)>1", Boolean.TRUE);
        getTester().checkBoolean("true>false", Boolean.TRUE);
        getTester().checkBoolean("true>true", Boolean.FALSE);
        getTester().checkBoolean("false>false", Boolean.FALSE);
        getTester().checkBoolean("false>true", Boolean.FALSE);
        getTester().checkNull("3.0>cast(null as double)");

        getTester().checkBoolean(
            "DATE '2013-02-23' > DATE '1945-02-24'", Boolean.TRUE);
        getTester().checkBoolean(
            "DATE '2013-02-23' > CAST(NULL AS DATE)", null);
    }

    @Test public void testGreaterThanOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day > interval '1' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day > interval '5' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2 2:2:2' day to second > interval '2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day > interval '2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day > interval '-2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day > interval '2' hour",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' minute > interval '2' hour",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' second > interval '2' minute",
            Boolean.FALSE);
        getTester().checkNull(
            "cast(null as interval hour) > interval '2' minute");
        getTester().checkNull(
            "interval '2:2' hour to minute > cast(null as interval second)");
    }

    @Test public void testIsDistinctFromOperator() {
        getTester().setFor(
            SqlStdOperatorTable.isDistinctFromOperator,
            VM_EXPAND);
        getTester().checkBoolean("1 is distinct from 1", Boolean.FALSE);
        if (!enable) {
            return;
        }
        getTester().checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
        getTester().checkBoolean("1 is distinct from 2", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as integer) is distinct from 2",
            Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as integer) is distinct from cast(null as integer)",
            Boolean.FALSE);
        getTester().checkBoolean("1.23 is distinct from 1.23", Boolean.FALSE);
        getTester().checkBoolean("1.23 is distinct from 5.23", Boolean.TRUE);
        getTester().checkBoolean(
            "-23e0 is distinct from -2.3e1",
            Boolean.FALSE);

        // IS DISTINCT FROM not implemented for ROW yet
        if (false) {
            getTester().checkBoolean(
                "row(1,1) is distinct from row(1,1)",
                true);
            getTester().checkBoolean(
                "row(1,1) is distinct from row(1,2)",
                false);
        }

        // Intervals
        getTester().checkBoolean(
            "interval '2' day is distinct from interval '1' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '10' hour is distinct from interval '10' hour",
            Boolean.FALSE);
    }

    @Test public void testIsNotDistinctFromOperator() {
        getTester().setFor(
            SqlStdOperatorTable.isNotDistinctFromOperator,
            VM_EXPAND);
        getTester().checkBoolean("1 is not distinct from 1", Boolean.TRUE);
        getTester().checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
        getTester().checkBoolean("1 is not distinct from 2", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as integer) is not distinct from 2",
            Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as integer) is not distinct from cast(null as integer)",
            Boolean.TRUE);
        getTester().checkBoolean(
            "1.23 is not distinct from 1.23",
            Boolean.TRUE);
        getTester().checkBoolean(
            "1.23 is not distinct from 5.23",
            Boolean.FALSE);
        getTester().checkBoolean(
            "-23e0 is not distinct from -2.3e1",
            Boolean.TRUE);

        // IS NOT DISTINCT FROM not implemented for ROW yet
        if (false) {
            getTester().checkBoolean(
                "row(1,1) is not distinct from row(1,1)",
                false);
            getTester().checkBoolean(
                "row(1,1) is not distinct from row(1,2)",
                true);
        }

        if (!enable) {
            return;
        }
        // Intervals
        getTester().checkBoolean(
            "interval '2' day is not distinct from interval '1' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '10' hour is not distinct from interval '10' hour",
            Boolean.TRUE);
    }

    @Test public void testGreaterThanOrEqualOperator() {
        getTester().setFor(SqlStdOperatorTable.greaterThanOrEqualOperator);
        getTester().checkBoolean("1>=2", Boolean.FALSE);
        getTester().checkBoolean("-1>=1", Boolean.FALSE);
        getTester().checkBoolean("1>=1", Boolean.TRUE);
        getTester().checkBoolean("2>=1", Boolean.TRUE);
        getTester().checkBoolean("1.1>=1.2", Boolean.FALSE);
        getTester().checkBoolean("-1.1>=-1.2", Boolean.TRUE);
        getTester().checkBoolean("1.1>=1.1", Boolean.TRUE);
        getTester().checkBoolean("1.2>=1", Boolean.TRUE);
        getTester().checkBoolean("1.2e4>=1e5", Boolean.FALSE);
        getTester().checkBoolean("1.2e4>=cast(1e5 as real)", Boolean.FALSE);
        getTester().checkBoolean("1.2>=cast(1e5 as double)", Boolean.FALSE);
        getTester().checkBoolean("120000>=cast(1e5 as real)", Boolean.TRUE);
        getTester().checkBoolean("true>=false", Boolean.TRUE);
        getTester().checkBoolean("true>=true", Boolean.TRUE);
        getTester().checkBoolean("false>=false", Boolean.TRUE);
        getTester().checkBoolean("false>=true", Boolean.FALSE);
        getTester().checkNull("cast(null as real)>=999");
    }

    @Test public void testGreaterThanOrEqualOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day >= interval '1' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day >= interval '5' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2 2:2:2' day to second >= interval '2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day >= interval '2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day >= interval '-2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day >= interval '2' hour",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' minute >= interval '2' hour",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' second >= interval '2' minute",
            Boolean.FALSE);
        getTester().checkNull(
            "cast(null as interval hour) >= interval '2' minute");
        getTester().checkNull(
            "interval '2:2' hour to minute >= cast(null as interval second)");
    }

    @Test public void testInOperator() {
        getTester().setFor(SqlStdOperatorTable.inOperator, VM_EXPAND);
        getTester().checkBoolean("1 in (0, 1, 2)", true);
        getTester().checkBoolean("3 in (0, 1, 2)", false);
        getTester().checkBoolean("cast(null as integer) in (0, 1, 2)", null);
        getTester().checkBoolean(
            "cast(null as integer) in (0, cast(null as integer), 2)",
            null);
        if (Bug.Frg327Fixed) {
            getTester().checkBoolean(
                "cast(null as integer) in (0, null, 2)",
                null);
            getTester().checkBoolean("1 in (0, null, 2)", null);
        }

        if (!enable) {
            return;
        }
        // AND has lower precedence than IN
        getTester().checkBoolean("false and true in (false, false)", false);

        if (!Bug.TodoFixed) {
            return;
        }
        getTester().checkFails(
            "'foo' in (^)^",
            "(?s).*Encountered \"\\)\" at .*",
            false);
    }

    @Test public void testNotInOperator() {
        getTester().setFor(SqlStdOperatorTable.notInOperator, VM_EXPAND);
        getTester().checkBoolean("1 not in (0, 1, 2)", false);
        getTester().checkBoolean("3 not in (0, 1, 2)", true);
        if (!enable) {
            return;
        }
        getTester().checkBoolean(
            "cast(null as integer) not in (0, 1, 2)",
            null);
        getTester().checkBoolean(
            "cast(null as integer) not in (0, cast(null as integer), 2)",
            null);
        if (Bug.Frg327Fixed) {
            getTester().checkBoolean(
                "cast(null as integer) not in (0, null, 2)",
                null);
            getTester().checkBoolean("1 not in (0, null, 2)", null);
        }

        // AND has lower precedence than NOT IN
        getTester().checkBoolean("true and false not in (true, true)", true);

        if (!Bug.TodoFixed) {
            return;
        }
        getTester().checkFails(
            "'foo' not in (^)^",
            "(?s).*Encountered \"\\)\" at .*",
            false);
    }

    @Test public void testOverlapsOperator() {
        getTester().setFor(SqlStdOperatorTable.overlapsOperator, VM_EXPAND);
        if (Bug.Frg187Fixed) {
            getTester().checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)",
                Boolean.TRUE);
            getTester().checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '4-5-6', interval '1' year)",
                Boolean.FALSE);
            getTester().checkBoolean(
                "(date '1-2-3', date '4-5-6') overlaps (date '2-2-3', date '3-4-5')",
                Boolean.TRUE);
            getTester().checkNull(
                "(cast(null as date), date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
            getTester().checkNull(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', cast(null as date))");

            getTester().checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')",
                Boolean.TRUE);
            getTester().checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:2')",
                Boolean.FALSE);
            getTester().checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', interval '2' hour)",
                Boolean.TRUE);
            getTester().checkNull(
                "(time '1:2:3', cast(null as time)) overlaps (time '23:59:59', time '1:2:3')");
            getTester().checkNull(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', cast(null as interval hour))");

            getTester().checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.TRUE);
            getTester().checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '2-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.FALSE);
            getTester().checkNull(
                "(timestamp '1-2-3 4:5:6', cast(null as interval day) ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");
            getTester().checkNull(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (cast(null as timestamp), interval '1 2:3:4.5' day to second)");
        }
    }

    @Test public void testLessThanOperator() {
        getTester().setFor(SqlStdOperatorTable.lessThanOperator);
        getTester().checkBoolean("1<2", Boolean.TRUE);
        getTester().checkBoolean("-1<1", Boolean.TRUE);
        getTester().checkBoolean("1<1", Boolean.FALSE);
        getTester().checkBoolean("2<1", Boolean.FALSE);
        getTester().checkBoolean("1.1<1.2", Boolean.TRUE);
        getTester().checkBoolean("-1.1<-1.2", Boolean.FALSE);
        getTester().checkBoolean("1.1<1.1", Boolean.FALSE);
        getTester().checkBoolean("cast(1.1 as real)<1", Boolean.FALSE);
        getTester().checkBoolean("cast(1.1 as real)<1.1", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(1.1 as real)<cast(1.2 as real)",
            Boolean.TRUE);
        getTester().checkBoolean("-1.1e-1<-1.2e-1", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(1.1 as real)<cast(1.1 as double)",
            Boolean.FALSE);
        getTester().checkBoolean("true<false", Boolean.FALSE);
        getTester().checkBoolean("true<true", Boolean.FALSE);
        getTester().checkBoolean("false<false", Boolean.FALSE);
        getTester().checkBoolean("false<true", Boolean.TRUE);
        getTester().checkNull("123<cast(null as bigint)");
        getTester().checkNull("cast(null as tinyint)<123");
        getTester().checkNull("cast(null as integer)<1.32");
    }

    @Test public void testLessThanOperatorInterval() {
        if (!DECIMAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day < interval '1' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day < interval '5' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2 2:2:2' day to second < interval '2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day < interval '2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day < interval '-2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day < interval '2' hour",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' minute < interval '2' hour",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' second < interval '2' minute",
            Boolean.TRUE);
        getTester().checkNull(
            "cast(null as interval hour) < interval '2' minute");
        getTester().checkNull(
            "interval '2:2' hour to minute < cast(null as interval second)");
    }

    @Test public void testLessThanOrEqualOperator() {
        getTester().setFor(SqlStdOperatorTable.lessThanOrEqualOperator);
        getTester().checkBoolean("1<=2", Boolean.TRUE);
        getTester().checkBoolean("1<=1", Boolean.TRUE);
        getTester().checkBoolean("-1<=1", Boolean.TRUE);
        getTester().checkBoolean("2<=1", Boolean.FALSE);
        getTester().checkBoolean("1.1<=1.2", Boolean.TRUE);
        getTester().checkBoolean("-1.1<=-1.2", Boolean.FALSE);
        getTester().checkBoolean("1.1<=1.1", Boolean.TRUE);
        getTester().checkBoolean("1.2<=1", Boolean.FALSE);
        getTester().checkBoolean("1<=cast(1e2 as real)", Boolean.TRUE);
        getTester().checkBoolean("1000<=cast(1e2 as real)", Boolean.FALSE);
        getTester().checkBoolean("1.2e1<=1e2", Boolean.TRUE);
        getTester().checkBoolean("1.2e1<=cast(1e2 as real)", Boolean.TRUE);
        getTester().checkBoolean("true<=false", Boolean.FALSE);
        getTester().checkBoolean("true<=true", Boolean.TRUE);
        getTester().checkBoolean("false<=false", Boolean.TRUE);
        getTester().checkBoolean("false<=true", Boolean.TRUE);
        getTester().checkNull("cast(null as real)<=cast(1 as real)");
        getTester().checkNull("cast(null as integer)<=3");
        getTester().checkNull("3<=cast(null as smallint)");
        getTester().checkNull("cast(null as integer)<=1.32");
    }

    @Test public void testLessThanOrEqualOperatorInterval() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day <= interval '1' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day <= interval '5' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2 2:2:2' day to second <= interval '2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day <= interval '2' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day <= interval '-2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' day <= interval '2' hour",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2' minute <= interval '2' hour",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' second <= interval '2' minute",
            Boolean.TRUE);
        getTester().checkNull(
            "cast(null as interval hour) <= interval '2' minute");
        getTester().checkNull(
            "interval '2:2' hour to minute <= cast(null as interval second)");
    }

    @Test public void testMinusOperator() {
        getTester().setFor(SqlStdOperatorTable.minusOperator);
        getTester().checkScalarExact("-2-1", "-3");
        getTester().checkScalarExact("-2-1-5", "-8");
        getTester().checkScalarExact("2-1", "1");
        getTester().checkScalarApprox(
            "cast(2.0 as double) -1",
            "DOUBLE NOT NULL",
            1,
            0);
        getTester().checkScalarApprox(
            "cast(1 as smallint)-cast(2.0 as real)",
            "REAL NOT NULL",
            -1,
            0);
        getTester().checkScalarApprox(
            "2.4-cast(2.0 as real)",
            "DOUBLE NOT NULL",
            0.4,
            0.00000001);
        getTester().checkScalarExact("1-2", "-1");
        getTester().checkScalarExact(
            "10.0 - 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "5.0");
        getTester().checkScalarExact(
            "19.68 - 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "15.48");
        getTester().checkNull("1e1-cast(null as double)");
        getTester().checkNull("cast(null as tinyint) - cast(null as smallint)");

        // TODO: Fix bug
        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            getTester().checkFails(
                "cast(100 as tinyint) - cast(-100 as tinyint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(-20000 as smallint) - cast(20000 as smallint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(1.5e9 as integer) - cast(-1.5e9 as integer)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(-5e18 as bigint) - cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(-5e8 as decimal(19,10)) - cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testMinusIntervalOperator() {
        getTester().setFor(SqlStdOperatorTable.minusOperator);
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "interval '2' day - interval '1' day",
            "+1",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "interval '2' day - interval '1' minute",
            "+1 23:59",
            "INTERVAL DAY TO MINUTE NOT NULL");
        getTester().checkScalar(
            "interval '2' year - interval '1' month",
            "+1-11",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkScalar(
            "interval '2' year - interval '1' month - interval '3' year",
            "-1-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkNull(
            "cast(null as interval day) + interval '2' hour");

        // Datetime minus interval
        getTester().checkScalar(
            "time '12:03:01' - interval '1:1' hour to minute",
            "11:02:01",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "date '2005-03-02' - interval '5' day",
            "2005-02-25",
            "DATE NOT NULL");
        getTester().checkScalar(
            "timestamp '2003-08-02 12:54:01' - interval '-4 2:4' day to minute",
            "2003-08-06 14:58:01",
            "TIMESTAMP(0) NOT NULL");

        // TODO: Tests with interval year months (not supported)
    }

    @Test public void testMinusDateOperator() {
        getTester().setFor(SqlStdOperatorTable.minusDateOperator);
        if (!enable) {
            return;
        }
        getTester().checkScalar(
            "(time '12:03:34' - time '11:57:23') minute to second",
            "+6:11",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        getTester().checkScalar(
            "(time '12:03:23' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        getTester().checkScalar(
            "(time '12:03:34' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        getTester().checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to second",
            "+2 00:06:11",
            "INTERVAL DAY TO SECOND NOT NULL");
        getTester().checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to hour",
            "+2 00",
            "INTERVAL DAY TO HOUR NOT NULL");
        getTester().checkScalar(
            "(date '2004-12-02' - date '2003-12-01') day",
            "+367",
            "INTERVAL DAY NOT NULL");
        getTester().checkNull(
            "(cast(null as date) - date '2003-12-01') day");

        // combine '<datetime> + <interval>' with '<datetime> - <datetime>'
        getTester().checkScalar(
            "timestamp '1969-04-29 0:0:0' +"
            + " (timestamp '2008-07-15 15:28:00' - "
            + "  timestamp '1969-04-29 0:0:0') day to second / 2",
            "1988-12-06 07:44:00",
            "TIMESTAMP(0) NOT NULL");

        getTester().checkScalar(
            "date '1969-04-29' +"
            + " (date '2008-07-15' - "
            + "  date '1969-04-29') day / 2",
            "1988-12-06",
            "DATE NOT NULL");

        getTester().checkScalar(
            "time '01:23:44' +"
            + " (time '15:28:00' - "
            + "  time '01:23:44') hour to second / 2",
            "08:25:52",
            "TIME(0) NOT NULL");

        if (Bug.Dt1684Fixed) {
            getTester().checkBoolean(
                "(date '1969-04-29' +"
                + " (CURRENT_DATE - "
                + "  date '1969-04-29') day / 2) is not null",
                Boolean.TRUE);
        }
        // TODO: Add tests for year month intervals (currently not supported)
    }

    @Test public void testMultiplyOperator() {
        getTester().setFor(SqlStdOperatorTable.multiplyOperator);
        getTester().checkScalarExact("2*3", "6");
        getTester().checkScalarExact("2*-3", "-6");
        getTester().checkScalarExact("+2*3", "6");
        getTester().checkScalarExact("2*0", "0");
        getTester().checkScalarApprox(
            "cast(2.0 as float)*3",
            "FLOAT NOT NULL",
            6,
            0);
        getTester().checkScalarApprox(
            "3*cast(2.0 as real)",
            "REAL NOT NULL",
            6,
            0);
        getTester().checkScalarApprox(
            "cast(2.0 as real)*3.2",
            "DOUBLE NOT NULL",
            6.4,
            0);
        getTester().checkScalarExact(
            "10.0 * 5.0",
            "DECIMAL(5, 2) NOT NULL",
            "50.00");
        getTester().checkScalarExact(
            "19.68 * 4.2",
            "DECIMAL(6, 3) NOT NULL",
            "82.656");
        getTester().checkNull("cast(1 as real)*cast(null as real)");
        getTester().checkNull("2e-3*cast(null as integer)");
        getTester().checkNull("cast(null as tinyint) * cast(4 as smallint)");

        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            getTester().checkFails(
                "cast(100 as tinyint) * cast(-2 as tinyint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(200 as smallint) * cast(200 as smallint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(1.5e9 as integer) * cast(-2 as integer)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(5e9 as bigint) * cast(2e9 as bigint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testMultiplyIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "interval '2:2' hour to minute * 3",
            "+6:06",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        getTester().checkScalar(
            "3 * 2 * interval '2:5:12' hour to second",
            "+12:31:12",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkNull(
            "interval '2' day * cast(null as bigint)");
        getTester().checkNull(
            "cast(null as interval month) * 2");
        if (todo) {
            getTester().checkScalar(
                "interval '3-2' year to month * 15e-1",
                "+04-09",
                "INTERVAL YEAR TO MONTH NOT NULL");
            getTester().checkScalar(
                "interval '3-4' year to month * 4.5",
                "+15-00",
                "INTERVAL YEAR TO MONTH NOT NULL");
        }
    }

    @Test public void testNotEqualsOperator() {
        getTester().setFor(SqlStdOperatorTable.notEqualsOperator);
        getTester().checkBoolean("1<>1", Boolean.FALSE);
        getTester().checkBoolean("'a'<>'A'", Boolean.TRUE);
        getTester().checkBoolean("1e0<>1e1", Boolean.TRUE);
        getTester().checkNull("'a'<>cast(null as varchar(1))");
    }

    @Test public void testNotEqualsOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkBoolean(
            "interval '2' day <> interval '1' day",
            Boolean.TRUE);
        getTester().checkBoolean(
            "interval '2' day <> interval '2' day",
            Boolean.FALSE);
        getTester().checkBoolean(
            "interval '2:2:2' hour to second <> interval '2' hour",
            Boolean.TRUE);
        getTester().checkNull(
            "cast(null as interval hour) <> interval '2' minute");

        // "!=" is not an acceptable alternative to "<>"
        getTester().checkFails(
            "1 ^!^= 1",
            "(?s).*Encountered: \"!\" \\(33\\).*",
            false);
    }

    @Test public void testOrOperator() {
        getTester().setFor(SqlStdOperatorTable.orOperator);
        getTester().checkBoolean("true or false", Boolean.TRUE);
        getTester().checkBoolean("false or false", Boolean.FALSE);
        getTester().checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
        getTester().checkNull("false or cast(null as boolean)");
    }

    @Test public void testOrOperatorLazy() {
        getTester().setFor(SqlStdOperatorTable.orOperator);

        // need to evaluate 2nd argument if first evaluates to null, therefore
        // get error
        getTester().check(
            "values 1 < cast(null as integer) or sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                null, invalidArgForPower, code2201f));

        // Do not need to evaluate 2nd argument if first evaluates to true.
        // In eager evaluation, get error;
        // lazy evaluation returns true;
        // both are valid.
        getTester().check(
            "values 1 < 2 or sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                Boolean.TRUE, invalidArgForPower, code2201f));

        // NULL OR FALSE --> NULL
        // In eager evaluation, get error;
        // lazy evaluation returns NULL;
        // both are valid.
        getTester().check(
            "values 1 < cast(null as integer) or sqrt(4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                null, invalidArgForPower, code2201f));

        // NULL OR TRUE --> TRUE
        getTester().checkBoolean(
            "1 < cast(null as integer) or sqrt(4) = 2", Boolean.TRUE);
    }

    @Test public void testPlusOperator() {
        getTester().setFor(SqlStdOperatorTable.plusOperator);
        getTester().checkScalarExact("1+2", "3");
        getTester().checkScalarExact("-1+2", "1");
        getTester().checkScalarExact("1+2+3", "6");
        getTester().checkScalarApprox(
            "1+cast(2.0 as double)",
            "DOUBLE NOT NULL",
            3,
            0);
        getTester().checkScalarApprox(
            "1+cast(2.0 as double)+cast(6.0 as float)",
            "DOUBLE NOT NULL",
            9,
            0);
        getTester().checkScalarExact(
            "10.0 + 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "15.0");
        getTester().checkScalarExact(
            "19.68 + 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "23.88");
        getTester().checkScalarExact(
            "19.68 + 4.2 + 6",
            "DECIMAL(13, 2) NOT NULL",
            "29.88");
        getTester().checkScalarApprox(
            "19.68 + cast(4.2 as float)",
            "DOUBLE NOT NULL",
            23.88,
            0.02);
        getTester().checkNull("cast(null as tinyint)+1");
        getTester().checkNull("1e-2+cast(null as double)");

        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            getTester().checkFails(
                "cast(100 as tinyint) + cast(100 as tinyint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(-20000 as smallint) + cast(-20000 as smallint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(1.5e9 as integer) + cast(1.5e9 as integer)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(5e18 as bigint) + cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(-5e18 as decimal(19,0)) + cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            getTester().checkFails(
                "cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testPlusIntervalOperator() {
        getTester().setFor(SqlStdOperatorTable.plusOperator);
        if (!INTERVAL) {
            return;
        }

        getTester().checkScalar(
            "interval '2' day + interval '1' day",
            "+3",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "interval '2' day + interval '1' minute",
            "+2 00:01",
            "INTERVAL DAY TO MINUTE NOT NULL");
        getTester().checkScalar(
            "interval '2' day + interval '5' minute + interval '-3' second",
            "+2 00:04:57",
            "INTERVAL DAY TO SECOND NOT NULL");
        getTester().checkScalar(
            "interval '2' year + interval '1' month",
            "+2-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkNull(
            "interval '2' year + cast(null as interval month)");

        // Datetime plus interval
        getTester().checkScalar(
            "time '12:03:01' + interval '1:1' hour to minute",
            "13:04:01",
            "TIME(0) NOT NULL");
        getTester().checkScalar(
            "interval '5' day + date '2005-03-02'",
            "2005-03-07",
            "DATE NOT NULL");
        getTester().checkScalar(
            "timestamp '2003-08-02 12:54:01' + interval '-4 2:4' day to minute",
            "2003-07-29 10:50:01",
            "TIMESTAMP(0) NOT NULL");

        // TODO: Tests with interval year months (not supported)
    }

    @Test public void testDescendingOperator() {
        getTester().setFor(SqlStdOperatorTable.descendingOperator, VM_EXPAND);
    }

    @Test public void testIsNotNullOperator() {
        getTester().setFor(SqlStdOperatorTable.isNotNullOperator);
        getTester().checkBoolean("true is not null", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as boolean) is not null",
            Boolean.FALSE);
    }

    @Test public void testIsNullOperator() {
        getTester().setFor(SqlStdOperatorTable.isNullOperator);
        getTester().checkBoolean("true is null", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as boolean) is null",
            Boolean.TRUE);
    }

    @Test public void testIsNotTrueOperator() {
        getTester().setFor(SqlStdOperatorTable.isNotTrueOperator);
        getTester().checkBoolean("true is not true", Boolean.FALSE);
        getTester().checkBoolean("false is not true", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as boolean) is not true",
            Boolean.TRUE);
        getTester().checkFails(
            "select ^'a string' is not true^ from (values (1))",
            "(?s)Cannot apply 'IS NOT TRUE' to arguments of type '<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): '<BOOLEAN> IS NOT TRUE'.*",
            false);
    }

    @Test public void testIsTrueOperator() {
        getTester().setFor(SqlStdOperatorTable.isTrueOperator);
        getTester().checkBoolean("true is true", Boolean.TRUE);
        getTester().checkBoolean("false is true", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as boolean) is true",
            Boolean.FALSE);
    }

    @Test public void testIsNotFalseOperator() {
        getTester().setFor(SqlStdOperatorTable.isNotFalseOperator);
        getTester().checkBoolean("false is not false", Boolean.FALSE);
        getTester().checkBoolean("true is not false", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as boolean) is not false",
            Boolean.TRUE);
    }

    @Test public void testIsFalseOperator() {
        getTester().setFor(SqlStdOperatorTable.isFalseOperator);
        getTester().checkBoolean("false is false", Boolean.TRUE);
        getTester().checkBoolean("true is false", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as boolean) is false",
            Boolean.FALSE);
    }

    @Test public void testIsNotUnknownOperator() {
        getTester().setFor(SqlStdOperatorTable.isNotUnknownOperator, VM_EXPAND);
        getTester().checkBoolean("false is not unknown", Boolean.TRUE);
        getTester().checkBoolean("true is not unknown", Boolean.TRUE);
        getTester().checkBoolean(
            "cast(null as boolean) is not unknown",
            Boolean.FALSE);
        getTester().checkBoolean("unknown is not unknown", Boolean.FALSE);
        getTester().checkFails(
            "^'abc' IS NOT UNKNOWN^",
            "(?s).*Cannot apply 'IS NOT UNKNOWN'.*",
            false);
    }

    @Test public void testIsUnknownOperator() {
        getTester().setFor(SqlStdOperatorTable.isUnknownOperator, VM_EXPAND);
        getTester().checkBoolean("false is unknown", Boolean.FALSE);
        getTester().checkBoolean("true is unknown", Boolean.FALSE);
        getTester().checkBoolean(
            "cast(null as boolean) is unknown",
            Boolean.TRUE);
        getTester().checkBoolean("unknown is unknown", Boolean.TRUE);
        getTester().checkFails(
            "0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4",
            "(?s).*Cannot apply 'IS UNKNOWN'.*",
            false);
    }

    @Test public void testIsASetOperator() {
        getTester().setFor(SqlStdOperatorTable.isASetOperator, VM_EXPAND);
    }

    @Test public void testExistsOperator() {
        getTester().setFor(SqlStdOperatorTable.existsOperator, VM_EXPAND);
    }

    @Test public void testNotOperator() {
        getTester().setFor(SqlStdOperatorTable.notOperator);
        getTester().checkBoolean("not true", Boolean.FALSE);
        getTester().checkBoolean("not false", Boolean.TRUE);
        getTester().checkBoolean("not unknown", null);
        getTester().checkNull("not cast(null as boolean)");
    }

    @Test public void testPrefixMinusOperator() {
        getTester().setFor(SqlStdOperatorTable.prefixMinusOperator);
        getTester().checkFails(
            "'a' + ^- 'b'^ + 'c'",
            "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*",
            false);
        getTester().checkScalarExact("-1", "-1");
        getTester().checkScalarExact(
            "-1.23",
            "DECIMAL(3, 2) NOT NULL",
            "-1.23");
        getTester().checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", -1, 0);
        getTester().checkNull("-cast(null as integer)");
        getTester().checkNull("-cast(null as tinyint)");
    }

    @Test public void testPrefixMinusOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "-interval '-6:2:8' hour to second",
            "+6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkScalar(
            "- -interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkScalar(
            "-interval '5' month",
            "-5",
            "INTERVAL MONTH NOT NULL");
        getTester().checkNull(
            "-cast(null as interval day to minute)");
    }

    @Test public void testPrefixPlusOperator() {
        getTester().setFor(SqlStdOperatorTable.prefixPlusOperator, VM_EXPAND);
        getTester().checkScalarExact("+1", "1");
        getTester().checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
        getTester().checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", 1, 0);
        getTester().checkNull("+cast(null as integer)");
        getTester().checkNull("+cast(null as tinyint)");
    }

    @Test public void testPrefixPlusOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "+interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkScalar(
            "++interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        if (Bug.Frg254Fixed) {
            getTester().checkScalar(
                "+interval '6:2:8.234' hour to second",
                "+06:02:08.234",
                "INTERVAL HOUR TO SECOND NOT NULL");
        }
        getTester().checkScalar(
            "+interval '5' month",
            "+5",
            "INTERVAL MONTH NOT NULL");
        getTester().checkNull(
            "+cast(null as interval day to minute)");
    }

    @Test public void testExplicitTableOperator() {
        getTester().setFor(
            SqlStdOperatorTable.explicitTableOperator,
            VM_EXPAND);
    }

    @Test public void testValuesOperator() {
        getTester().setFor(SqlStdOperatorTable.valuesOperator, VM_EXPAND);
        getTester().check(
            "select 'abc' from (values(true))",
            new AbstractSqlTester.StringTypeChecker("CHAR(3) NOT NULL"),
            "abc",
            0);
    }

    @Test public void testNotLikeOperator() {
        getTester().setFor(SqlStdOperatorTable.notLikeOperator, VM_EXPAND);
        getTester().checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    }

    @Test public void testLikeOperator() {
        getTester().setFor(SqlStdOperatorTable.likeOperator);
        getTester().checkBoolean("''  like ''", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'a'", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'b'", Boolean.FALSE);
        getTester().checkBoolean("'a' like 'A'", Boolean.FALSE);
        getTester().checkBoolean("'a' like 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'a' like '_a'", Boolean.FALSE);
        getTester().checkBoolean("'a' like '%a'", Boolean.TRUE);
        getTester().checkBoolean("'a' like '%a%'", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
        getTester().checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   like '_b'", Boolean.TRUE);
        getTester().checkBoolean("'abcd' like '_d'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    }

    @Test public void testNotSimilarToOperator() {
        getTester().setFor(SqlStdOperatorTable.notSimilarOperator, VM_EXPAND);
        getTester().checkBoolean("'ab' not similar to 'a_'", false);
        getTester().checkBoolean("'aabc' not similar to 'ab*c+d'", true);
        getTester().checkBoolean("'ab' not similar to 'a' || '_'", false);
        getTester().checkBoolean("'ab' not similar to 'ba_'", true);
        getTester().checkBoolean(
            "cast(null as varchar(2)) not similar to 'a_'",
            null);
        getTester().checkBoolean(
            "cast(null as varchar(3)) not similar to cast(null as char(2))",
            null);
    }

    @Test public void testSimilarToOperator() {
        getTester().setFor(SqlStdOperatorTable.similarOperator);

        // like LIKE
        getTester().checkBoolean("''  similar to ''", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'b'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to 'A'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to '_a'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to '%a'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
        getTester().checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
        getTester().checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

        // simple regular expressions
        // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
        getTester().checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
        getTester().checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

        // compound regular expressions
        // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
        getTester().checkBoolean(
            "'xy'      similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        getTester().checkBoolean(
            "'xccy'    similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        getTester().checkBoolean(
            "'xababcy' similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        getTester().checkBoolean(
            "'xbcy'    similar to 'x(ab|c)*y'",
            Boolean.FALSE);

        // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
        getTester().checkBoolean(
            "'xy'      similar to 'x(ab|c)+y'",
            Boolean.FALSE);
        getTester().checkBoolean(
            "'xccy'    similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        getTester().checkBoolean(
            "'xababcy' similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        getTester().checkBoolean(
            "'xbcy'    similar to 'x(ab|c)+y'",
            Boolean.FALSE);

        getTester().checkBoolean("'ab' similar to 'a%' ", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a%' ", Boolean.TRUE);
        getTester().checkBoolean("'abcd' similar to 'a_' ", Boolean.FALSE);
        getTester().checkBoolean("'abcd' similar to 'a%' ", Boolean.TRUE);
        getTester().checkBoolean("'1a' similar to '_a' ", Boolean.TRUE);
        getTester().checkBoolean("'123aXYZ' similar to '%a%'", Boolean.TRUE);

        getTester().checkBoolean(
            "'123aXYZ' similar to '_%_a%_' ",
            Boolean.TRUE);

        getTester().checkBoolean("'xy' similar to '(xy)' ", Boolean.TRUE);

        getTester().checkBoolean(
            "'abd' similar to '[ab][bcde]d' ",
            Boolean.TRUE);

        getTester().checkBoolean(
            "'bdd' similar to '[ab][bcde]d' ",
            Boolean.TRUE);

        getTester().checkBoolean("'abd' similar to '[ab]d' ", Boolean.FALSE);
        getTester().checkBoolean("'cd' similar to '[a-e]d' ", Boolean.TRUE);
        getTester().checkBoolean("'amy' similar to 'amy|fred' ", Boolean.TRUE);
        getTester().checkBoolean("'fred' similar to 'amy|fred' ", Boolean.TRUE);

        getTester().checkBoolean(
            "'mike' similar to 'amy|fred' ",
            Boolean.FALSE);

        getTester().checkBoolean("'acd' similar to 'ab*c+d' ", Boolean.TRUE);
        getTester().checkBoolean("'accccd' similar to 'ab*c+d' ", Boolean.TRUE);
        getTester().checkBoolean("'abd' similar to 'ab*c+d' ", Boolean.FALSE);
        getTester().checkBoolean("'aabc' similar to 'ab*c+d' ", Boolean.FALSE);
        getTester().checkBoolean("'abb' similar to 'a(b{3})' ", Boolean.FALSE);
        getTester().checkBoolean("'abbb' similar to 'a(b{3})' ", Boolean.TRUE);

        getTester().checkBoolean(
            "'abbbbb' similar to 'a(b{3})' ",
            Boolean.FALSE);

        getTester().checkBoolean(
            "'abbbbb' similar to 'ab{3,6}' ",
            Boolean.TRUE);

        getTester().checkBoolean(
            "'abbbbbbbb' similar to 'ab{3,6}' ",
            Boolean.FALSE);
        getTester().checkBoolean("'' similar to 'ab?' ", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to 'ab?' ", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a(b?)' ", Boolean.TRUE);
        getTester().checkBoolean("'ab' similar to 'ab?' ", Boolean.TRUE);
        getTester().checkBoolean("'ab' similar to 'a(b?)' ", Boolean.TRUE);
        getTester().checkBoolean("'abb' similar to 'ab?' ", Boolean.FALSE);

        getTester().checkBoolean(
            "'ab' similar to 'a\\_' ESCAPE '\\' ",
            Boolean.FALSE);

        getTester().checkBoolean(
            "'ab' similar to 'a\\%' ESCAPE '\\' ",
            Boolean.FALSE);

        getTester().checkBoolean(
            "'a_' similar to 'a\\_' ESCAPE '\\' ",
            Boolean.TRUE);

        getTester().checkBoolean(
            "'a%' similar to 'a\\%' ESCAPE '\\' ",
            Boolean.TRUE);

        getTester().checkBoolean(
            "'a(b{3})' similar to 'a(b{3})' ",
            Boolean.FALSE);

        getTester().checkBoolean(
            "'a(b{3})' similar to 'a\\(b\\{3\\}\\)' ESCAPE '\\' ",
            Boolean.TRUE);

        getTester().checkBoolean("'yd' similar to '[a-ey]d'", Boolean.TRUE);
        getTester().checkBoolean("'yd' similar to '[^a-ey]d'", Boolean.FALSE);
        getTester().checkBoolean("'yd' similar to '[^a-ex-z]d'", Boolean.FALSE);
        getTester().checkBoolean("'yd' similar to '[a-ex-z]d'", Boolean.TRUE);
        getTester().checkBoolean("'yd' similar to '[x-za-e]d'", Boolean.TRUE);
        getTester().checkBoolean("'yd' similar to '[^a-ey]?d'", Boolean.FALSE);
        getTester().checkBoolean("'yyyd' similar to '[a-ey]*d'", Boolean.TRUE);

         // range must be specified in []
        getTester().checkBoolean("'yd' similar to 'x-zd'", Boolean.FALSE);
        getTester().checkBoolean("'y' similar to 'x-z'", Boolean.FALSE);

        getTester().checkBoolean("'cd' similar to '([a-e])d'", Boolean.TRUE);
        getTester().checkBoolean("'xy' similar to 'x*?y'", Boolean.TRUE);
        getTester().checkBoolean("'y' similar to 'x*?y'", Boolean.TRUE);
        getTester().checkBoolean("'y' similar to '(x?)*y'", Boolean.TRUE);
        getTester().checkBoolean("'y' similar to 'x+?y'", Boolean.FALSE);

        getTester().checkBoolean("'y' similar to 'x?+y'", Boolean.TRUE);
        getTester().checkBoolean("'y' similar to 'x*+y'", Boolean.TRUE);

        // The following two tests throws exception(They probably should).
        // "Dangling meta character '*' near index 2"

        if (enable) {
            getTester().checkBoolean("'y' similar to 'x+*y'", Boolean.TRUE);
            getTester().checkBoolean("'y' similar to 'x?*y'", Boolean.TRUE);
        }

        // some negative tests
        getTester().checkFails(
            "'yd' similar to '[x-ze-a]d'",
            "Illegal character range near index 6\n"
            + "\\[x-ze-a\\]d\n"
            + "      \\^",
            true);   // illegal range

        getTester().checkFails(
            "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{,5}'",
            "Illegal repetition near index 20\n"
            + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
            + "                    \\^",
            true);

        getTester().checkFails(
            "'cd' similar to '[(a-e)]d' ",
            "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
            true);

        getTester().checkFails(
            "'yd' similar to '[(a-e)]d' ",
            "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
            true);

        // all the following tests wrong results due to missing functionality
        // or defect (FRG-375, 377).

        if (Bug.Frg375Fixed) {
            getTester().checkBoolean(
                "'cd' similar to '[a-e^c]d' ", Boolean.FALSE); // FRG-375
        }

        // following tests use regular character set identifiers.
        // Not implemented yet. FRG-377.
        if (Bug.Frg377Fixed) {
            getTester().checkBoolean(
                "'y' similar to '[:ALPHA:]*'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd32' similar to '[:LOWER:]{2}[:DIGIT:]*'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd32' similar to '[:ALNUM:]*'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd32' similar to '[:ALNUM:]*[:DIGIT:]?'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd32' similar to '[:ALNUM:]?[:DIGIT:]*'",
                Boolean.FALSE);

            getTester().checkBoolean(
                "'yd3223' similar to '([:LOWER:]{2})[:DIGIT:]{2,5}'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{2,}'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}||[:DIGIT:]{4}'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{3}'",
                Boolean.FALSE);

            getTester().checkBoolean(
                "'yd  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
                Boolean.FALSE);

            getTester().checkBoolean(
                "'YD  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
                Boolean.FALSE);

            getTester().checkBoolean(
                "'YD  3223' similar to "
                + "'[:UPPER:]{2}||[:WHITESPACE:]*[:DIGIT:]{4}'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'YD\t3223' similar to '[:UPPER:]{2}[:SPACE:]*[:DIGIT:]{4}'",
                Boolean.FALSE);

            getTester().checkBoolean(
                "'YD\t3223' similar to "
                + "'[:UPPER:]{2}[:WHITESPACE:]*[:DIGIT:]{4}'",
                Boolean.TRUE);

            getTester().checkBoolean(
                "'YD\t\t3223' similar to "
                + "'([:UPPER:]{2}[:WHITESPACE:]+)||[:DIGIT:]{4}'",
                Boolean.TRUE);
        }
    }

    @Test public void testEscapeOperator() {
        getTester().setFor(SqlStdOperatorTable.escapeOperator, VM_EXPAND);
    }

    @Test public void testConvertFunc() {
        getTester().setFor(
            SqlStdOperatorTable.convertFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testTranslateFunc() {
        getTester().setFor(
            SqlStdOperatorTable.translateFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testOverlayFunc() {
        getTester().setFor(SqlStdOperatorTable.overlayFunc);
        getTester().checkString(
            "overlay('ABCdef' placing 'abc' from 1)",
            "abcdef",
            "VARCHAR(9) NOT NULL");
        getTester().checkString(
            "overlay('ABCdef' placing 'abc' from 1 for 2)",
            "abcCdef",
            "VARCHAR(9) NOT NULL");
        if (!enable) {
            return;
        }
        getTester().checkString(
            "overlay(cast('ABCdef' as varchar(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef",
            "VARCHAR(15) NOT NULL");
        getTester().checkString(
            "overlay(cast('ABCdef' as char(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef    ",
            "VARCHAR(15) NOT NULL");
        getTester().checkNull(
            "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
        getTester().checkNull(
            "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

        if (false) {
            // hex strings not yet implemented in calc
            getTester().checkNull(
                "overlay(x'abc' placing x'abc' from cast(null as integer))");
        }
    }

    @Test public void testPositionFunc() {
        getTester().setFor(SqlStdOperatorTable.positionFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalarExact("position('b' in 'abc')", "2");
        getTester().checkScalarExact("position('' in 'abc')", "1");

        // FRG-211
        getTester().checkScalarExact("position('tra' in 'fdgjklewrtra')", "10");

        getTester().checkNull("position(cast(null as varchar(1)) in '0010')");
        getTester().checkNull("position('a' in cast(null as varchar(1)))");

        getTester().checkScalar(
            "position(cast('a' as char) in cast('bca' as varchar))",
            0,
            "INTEGER NOT NULL");
    }

    @Test public void testCharLengthFunc() {
        getTester().setFor(SqlStdOperatorTable.charLengthFunc);
        getTester().checkScalarExact("char_length('abc')", "3");
        getTester().checkNull("char_length(cast(null as varchar(1)))");
    }

    @Test public void testCharacterLengthFunc() {
        getTester().setFor(SqlStdOperatorTable.characterLengthFunc);
        getTester().checkScalarExact("CHARACTER_LENGTH('abc')", "3");
        getTester().checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    }

    @Test public void testUpperFunc() {
        getTester().setFor(SqlStdOperatorTable.upperFunc);
        getTester().checkString("upper('a')", "A", "CHAR(1) NOT NULL");
        getTester().checkString("upper('A')", "A", "CHAR(1) NOT NULL");
        getTester().checkString("upper('1')", "1", "CHAR(1) NOT NULL");
        getTester().checkString("upper('aa')", "AA", "CHAR(2) NOT NULL");
        getTester().checkNull("upper(cast(null as varchar(1)))");
    }

    @Test public void testLowerFunc() {
        getTester().setFor(SqlStdOperatorTable.lowerFunc);

        // SQL:2003 6.29.8 The type of lower is the type of its argument
        getTester().checkString("lower('A')", "a", "CHAR(1) NOT NULL");
        getTester().checkString("lower('a')", "a", "CHAR(1) NOT NULL");
        getTester().checkString("lower('1')", "1", "CHAR(1) NOT NULL");
        getTester().checkString("lower('AA')", "aa", "CHAR(2) NOT NULL");
        getTester().checkNull("lower(cast(null as varchar(1)))");
    }

    @Test public void testInitcapFunc() {
        // Note: the initcap function is an Oracle defined function and is not
        // defined in the SQL:2003 standard
        // todo: implement in fennel
        getTester().setFor(SqlStdOperatorTable.initcapFunc, VM_FENNEL);

        getTester().checkString("initcap('aA')", "Aa", "CHAR(2) NOT NULL");
        getTester().checkString("initcap('Aa')", "Aa", "CHAR(2) NOT NULL");
        getTester().checkString("initcap('1a')", "1a", "CHAR(2) NOT NULL");
        getTester().checkString(
            "initcap('ab cd Ef 12')",
            "Ab Cd Ef 12",
            "CHAR(11) NOT NULL");
        getTester().checkNull("initcap(cast(null as varchar(1)))");

        // dtbug 232
        getTester().checkFails(
            "^initcap(cast(null as date))^",
            "Cannot apply 'INITCAP' to arguments of type 'INITCAP\\(<DATE>\\)'\\. Supported form\\(s\\): 'INITCAP\\(<CHARACTER>\\)'",
            false);
    }

    @Test public void testPowerFunc() {
        getTester().setFor(SqlStdOperatorTable.powerFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox(
            "power(2,-2)",
            "DOUBLE NOT NULL",
            0.25,
            0);
        getTester().checkNull("power(cast(null as integer),2)");
        getTester().checkNull("power(2,cast(null as double))");

        // 'pow' is an obsolete form of the 'power' function
        getTester().checkFails(
            "^pow(2,-2)^",
            "No match found for function signature POW\\(<NUMERIC>, <NUMERIC>\\)",
            false);
    }

    @Test public void testSqrtFunc() {
        getTester().setFor(
            SqlStdOperatorTable.sqrtFunc, SqlTester.VmName.EXPAND);
        getTester().checkType("sqrt(2)", "DOUBLE NOT NULL");
        getTester().checkType("sqrt(cast(2 as float))", "DOUBLE NOT NULL");
        getTester().checkType(
            "sqrt(case when false then 2 else null end)", "DOUBLE");
        getTester().checkFails(
            "^sqrt('abc')^",
            "Cannot apply 'SQRT' to arguments of type 'SQRT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'SQRT\\(<NUMERIC>\\)'",
            false);
        getTester().checkScalarApprox(
            "sqrt(2)",
            "DOUBLE NOT NULL",
            1.4142d,
            0.0001d);
        getTester().checkNull("sqrt(cast(null as integer))");
        getTester().checkNull("sqrt(cast(null as double))");
    }

    @Test public void testExpFunc() {
        // todo: implement in fennel
        getTester().setFor(SqlStdOperatorTable.expFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox(
            "exp(2)",
            "DOUBLE NOT NULL",
            7.389056,
            0.000001);
        getTester().checkScalarApprox(
            "exp(-2)",
            "DOUBLE NOT NULL",
            0.1353,
            0.0001);
        getTester().checkNull("exp(cast(null as integer))");
        getTester().checkNull("exp(cast(null as double))");
    }

    @Test public void testModFunc() {
        getTester().setFor(SqlStdOperatorTable.modFunc);
        getTester().checkScalarExact("mod(4,2)", "0");
        getTester().checkScalarExact("mod(8,5)", "3");
        getTester().checkScalarExact("mod(-12,7)", "-5");
        getTester().checkScalarExact("mod(-12,-7)", "-5");
        getTester().checkScalarExact("mod(12,-7)", "5");
        getTester().checkScalarExact(
            "mod(cast(12 as tinyint), cast(-7 as tinyint))",
            "TINYINT NOT NULL",
            "5");

        getTester().checkScalarExact(
            "mod(cast(9 as decimal(2, 0)), 7)",
            "INTEGER NOT NULL",
            "2");
        getTester().checkScalarExact(
            "mod(7, cast(9 as decimal(2, 0)))",
            "DECIMAL(2, 0) NOT NULL",
            "7");
        getTester().checkScalarExact(
            "mod(cast(-9 as decimal(2, 0)), cast(7 as decimal(1, 0)))",
            "DECIMAL(1, 0) NOT NULL",
            "-2");
        getTester().checkNull("mod(cast(null as integer),2)");
        getTester().checkNull("mod(4,cast(null as tinyint))");
        getTester().checkNull("mod(4,cast(null as decimal(12,0)))");
    }

    @Test public void testModFuncDivByZero() {
        // The extra CASE expression is to fool Janino.  It does constant
        // reduction and will throw the divide by zero exception while
        // compiling the expression.  The test frame work would then issue
        // unexpected exception occurred during "validation".  You cannot
        // submit as non-runtime because the janino exception does not have
        // error position information and the framework is unhappy with that.
        getTester().checkFails(
            "mod(3,case 'a' when 'a' then 0 end)",
            divisionByZeroMessage,
            true);
    }

    @Test public void testLnFunc() {
        getTester().setFor(SqlStdOperatorTable.lnFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        getTester().checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            0.999999327,
            0.0000001);
        getTester().checkNull("ln(cast(null as tinyint))");
    }

    @Test public void testLogFunc() {
        getTester().setFor(SqlStdOperatorTable.log10Func);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox(
            "log10(10)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        getTester().checkScalarApprox(
            "log10(100.0)",
            "DOUBLE NOT NULL",
            2.0,
            0.000001);
        getTester().checkScalarApprox(
            "log10(cast(10e8 as double))",
            "DOUBLE NOT NULL",
            9.0,
            0.000001);
        getTester().checkScalarApprox(
            "log10(cast(10e2 as float))",
            "DOUBLE NOT NULL",
            3.0,
            0.000001);
        getTester().checkScalarApprox(
            "log10(cast(10e-3 as real))",
            "DOUBLE NOT NULL",
            -2.0,
            0.000001);
        getTester().checkNull("log10(cast(null as real))");
    }

    @Test public void testAbsFunc() {
        getTester().setFor(SqlStdOperatorTable.absFunc);

        if (!enable) {
            return;
        }
        getTester().checkScalarExact("abs(-1)", "1");
        getTester().checkScalarExact(
            "abs(cast(10 as TINYINT))",
            "TINYINT NOT NULL",
            "10");
        getTester().checkScalarExact(
            "abs(cast(-20 as SMALLINT))",
            "SMALLINT NOT NULL",
            "20");
        getTester().checkScalarExact(
            "abs(cast(-100 as INT))",
            "INTEGER NOT NULL",
            "100");
        getTester().checkScalarExact(
            "abs(cast(1000 as BIGINT))",
            "BIGINT NOT NULL",
            "1000");
        getTester().checkScalarExact(
            "abs(54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        getTester().checkScalarExact(
            "abs(-54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        getTester().checkScalarApprox(
            "abs(-9.32E-2)",
            "DOUBLE NOT NULL",
            0.0932,
            0);
        getTester().checkScalarApprox(
            "abs(cast(-3.5 as double))",
            "DOUBLE NOT NULL",
            3.5,
            0);
        getTester().checkScalarApprox(
            "abs(cast(-3.5 as float))",
            "FLOAT NOT NULL",
            3.5,
            0);
        getTester().checkScalarApprox(
            "abs(cast(3.5 as real))",
            "REAL NOT NULL",
            3.5,
            0);

        getTester().checkNull("abs(cast(null as double))");
    }

    @Test public void testAbsFuncIntervals() {
        if (!INTERVAL) {
           return;
        }
        getTester().checkScalar(
            "abs(interval '-2' day)",
            "+2",
            "INTERVAL DAY NOT NULL");
        getTester().checkScalar(
            "abs(interval '-5-03' year to month)",
            "+5-03",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkNull("abs(cast(null as interval hour))");
    }

    @Test public void testNullifFunc() {
        getTester().setFor(SqlStdOperatorTable.nullIfFunc, VM_EXPAND);
        getTester().checkNull("nullif(1,1)");
        if (!enable) {
            return;
        }
        getTester().checkScalarExact(
            "nullif(1.5, 13.56)",
            "DECIMAL(2, 1)",
            "1.5");
        getTester().checkScalarExact(
            "nullif(13.56, 1.5)",
            "DECIMAL(4, 2)",
            "13.56");
        getTester().checkScalarExact("nullif(1.5, 3)", "DECIMAL(2, 1)", "1.5");
        getTester().checkScalarExact("nullif(3, 1.5)", "INTEGER", "3");
        getTester().checkScalarApprox("nullif(1.5e0, 3e0)", "DOUBLE", 1.5, 0);
        getTester().checkScalarApprox(
            "nullif(1.5, cast(3e0 as REAL))",
            "DECIMAL(2, 1)",
            1.5,
            0);
        getTester().checkScalarExact("nullif(3, 1.5e0)", "INTEGER", "3");
        getTester().checkScalarExact(
            "nullif(3, cast(1.5e0 as REAL))",
            "INTEGER",
            "3");
        getTester().checkScalarApprox("nullif(1.5e0, 3.4)", "DOUBLE", 1.5, 0);
        getTester().checkScalarExact(
            "nullif(3.4, 1.5e0)",
            "DECIMAL(2, 1)",
            "3.4");
        getTester().checkString(
            "nullif('a','bc')",
            "a",
            "CHAR(1)");
        getTester().checkString(
            "nullif('a',cast(null as varchar(1)))",
            "a",
            "CHAR(1)");
        getTester().checkNull("nullif(cast(null as varchar(1)),'a')");
        getTester().checkNull("nullif(cast(null as numeric(4,3)), 4.3)");

        // Error message reflects the fact that Nullif is expanded before it is
        // validated (like a C macro). Not perfect, but good enough.
        getTester().checkFails(
            "1 + ^nullif(1, date '2005-8-4')^ + 2",
            "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*",
            false);

        getTester().checkFails(
            "1 + ^nullif(1, 2, 3)^ + 2",
            "Invalid number of arguments to function 'NULLIF'\\. Was expecting 2 arguments",
            false);
    }

    @Test public void testNullIfOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "nullif(interval '2' month, interval '3' year)",
            "+2",
            "INTERVAL MONTH");
        getTester().checkScalar(
            "nullif(interval '2 5' day to hour, interval '5' second)",
            "+2 05",
            "INTERVAL DAY TO HOUR");
        getTester().checkNull(
            "nullif(interval '3' day, interval '3' day)");
    }

    @Test public void testCoalesceFunc() {
        getTester().setFor(SqlStdOperatorTable.coalesceFunc, VM_EXPAND);
        getTester().checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
        getTester().checkScalarExact("coalesce(null,null,3)", "3");
        getTester().checkFails(
            "1 + ^coalesce('a', 'b', 1, null)^ + 2",
            "Illegal mixing of types in CASE or COALESCE statement",
            false);
    }

    @Test public void testUserFunc() {
        getTester().setFor(SqlStdOperatorTable.userFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentUserFunc() {
        getTester().setFor(SqlStdOperatorTable.currentUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testSessionUserFunc() {
        getTester().setFor(SqlStdOperatorTable.sessionUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testSystemUserFunc() {
        getTester().setFor(SqlStdOperatorTable.systemUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        String user = System.getProperty("user.name"); // e.g. "jhyde"
        getTester().checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentPathFunc() {
        getTester().setFor(SqlStdOperatorTable.currentPathFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentRoleFunc() {
        getTester().setFor(SqlStdOperatorTable.currentRoleFunc, VM_FENNEL);
        if (!enable) {
            return;
        }

        // By default, the CURRENT_ROLE function returns
        // the empty string because a role has to be set explicitly.
        getTester().checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testLocalTimeFunc() {
        getTester().setFor(SqlStdOperatorTable.localTimeFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
        getTester().checkFails(
            "^LOCALTIME()^",
            "No match found for function signature LOCALTIME\\(\\)",
            false);
        getTester().checkScalar(
            "LOCALTIME(1)",
            timePattern,
            "TIME(1) NOT NULL");

        getTester().checkScalar(
            "CAST(LOCALTIME AS VARCHAR(30))",
            Pattern.compile(
                currentTimeString(defaultTimeZone).substring(11)
                + "[0-9][0-9]:[0-9][0-9]"),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testLocalTimestampFunc() {
        getTester().setFor(SqlStdOperatorTable.localTimestampFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalar(
            "LOCALTIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        getTester().checkFails(
            "^LOCALTIMESTAMP()^",
            "No match found for function signature LOCALTIMESTAMP\\(\\)",
            false);
        getTester().checkFails(
            "LOCALTIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        getTester().checkScalar(
            "LOCALTIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");

        // Check that timestamp is being generated in the right timezone by
        // generating a specific timestamp.
        getTester().checkScalar(
            "CAST(LOCALTIMESTAMP AS VARCHAR(30))",
            Pattern.compile(
                currentTimeString(defaultTimeZone)
                + "[0-9][0-9]:[0-9][0-9]"),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testCurrentTimeFunc() {
        getTester().setFor(SqlStdOperatorTable.currentTimeFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalar(
            "CURRENT_TIME",
            timePattern,
            "TIME(0) NOT NULL");
        getTester().checkFails(
            "^CURRENT_TIME()^",
            "No match found for function signature CURRENT_TIME\\(\\)",
            false);
        getTester().checkScalar(
            "CURRENT_TIME(1)",
            timePattern,
            "TIME(1) NOT NULL");

        if (Bug.Fnl77Fixed) {
            // Currently works with Java calc, but fennel calc returns time in
            // GMT time zone.
            getTester().checkScalar(
                "CAST(CURRENT_TIME AS VARCHAR(30))",
                Pattern.compile(
                    currentTimeString(defaultTimeZone).substring(11)
                    + "[0-9][0-9]:[0-9][0-9]"),
                "VARCHAR(30) NOT NULL");
        }
    }

    @Test public void testCurrentTimestampFunc() {
        getTester().setFor(SqlStdOperatorTable.currentTimestampFunc);
        if (!enable) {
            return;
        }
        getTester().checkScalar(
            "CURRENT_TIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        getTester().checkFails(
            "^CURRENT_TIMESTAMP()^",
            "No match found for function signature CURRENT_TIMESTAMP\\(\\)",
            false);
        getTester().checkFails(
            "CURRENT_TIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        getTester().checkScalar(
            "CURRENT_TIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");

        // Check that timestamp is being generated in the right timezone by
        // generating a specific timestamp. We truncate to hours so that minor
        // delays don't generate false negatives.
        if (Bug.Fnl77Fixed) {
            // Currently works with Java calc, but fennel calc returns time in
            // GMT time zone.
            getTester().checkScalar(
                "CAST(CURRENT_TIMESTAMP AS VARCHAR(30))",
                Pattern.compile(
                    currentTimeString(defaultTimeZone)
                    + "[0-9][0-9]:[0-9][0-9]"),
                "VARCHAR(30) NOT NULL");
        }
    }

    /**
     * Returns a time string, in GMT, that will be valid for at least 2 minutes.
     *
     * <p>For example, at "2005-01-01 12:34:56 PST", returns "2005-01-01 20:".
     * At "2005-01-01 12:34:59 PST", waits a minute, then returns "2005-01-01
     * 21:".
     *
     * @param tz Time zone
     *
     * @return Time string
     */
    protected static String currentTimeString(TimeZone tz)
    {
        final Calendar calendar = getCalendarNotTooNear(Calendar.HOUR_OF_DAY);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:");
        sdf.setTimeZone(tz);
        return sdf.format(calendar.getTime());
    }

    @Test public void testCurrentDateFunc() {
        getTester().setFor(SqlStdOperatorTable.currentDateFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
        getTester().checkScalar(
            "(CURRENT_DATE - CURRENT_DATE) DAY",
            "+0",
            "INTERVAL DAY NOT NULL");
        getTester().checkBoolean("CURRENT_DATE IS NULL", false);
        getTester().checkFails(
            "^CURRENT_DATE()^",
            "No match found for function signature CURRENT_DATE\\(\\)",
            false);

        // Check the actual value.
        getTester().checkScalar(
            "CAST(CURRENT_DATE AS VARCHAR(30))",
            currentTimeString(defaultTimeZone).substring(0, 10),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testSubstringFunction() {
        getTester().setFor(SqlStdOperatorTable.substringFunc);
        getTester().checkString(
            "substring('abc' from 1 for 2)",
            "ab",
            "VARCHAR(3) NOT NULL");
        getTester().checkString(
            "substring('abc' from 2)",
            "bc",
            "VARCHAR(3) NOT NULL");

        if (Bug.Frg296Fixed) {
            // substring regexp not supported yet
            getTester().checkString(
                "substring('foobar' from '%#\"o_b#\"%' for'#')",
                "oob",
                "xx");
        }
        getTester().checkNull("substring(cast(null as varchar(1)),1,2)");
    }

    @Test public void testTrimFunc() {
        getTester().setFor(SqlStdOperatorTable.trimBothFunc);
        if (!enable) {
            return;
        }

        // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
        getTester().checkString(
            "trim('a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        getTester().checkString(
            "trim(both 'a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        getTester().checkString(
            "trim(leading 'a' from 'aAa')",
            "Aa",
            "VARCHAR(3) NOT NULL");
        getTester().checkString(
            "trim(trailing 'a' from 'aAa')",
            "aA",
            "VARCHAR(3) NOT NULL");
        getTester().checkNull("trim(cast(null as varchar(1)) from 'a')");
        getTester().checkNull("trim('a' from cast(null as varchar(1)))");

        if (Bug.Fnl3Fixed) {
            // SQL:2003 6.29.9: trim string must have length=1. Failure occurs
            // at runtime.
            //
            // TODO: Change message to "Invalid argument\(s\) for
            // 'TRIM' function".
            // The message should come from a resource file, and should still
            // have the SQL error code 22027.
            getTester().checkFails(
                "trim('xy' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
            getTester().checkFails(
                "trim('' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
        }
    }

    @Test public void testWindow() {
        getTester().setFor(
            SqlStdOperatorTable.windowOperator,
            VM_FENNEL,
            VM_JAVA);
        if (!enable) {
            return;
        }
        getTester().check(
            "select sum(1) over (order by x) from (select 1 as x, 2 as y from (values (true)))",
            new AbstractSqlTester.StringTypeChecker("INTEGER"),
            "1",
            0);
    }

    @Test public void testElementFunc() {
        getTester().setFor(
            SqlStdOperatorTable.elementFunc,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            getTester().checkString(
                "element(multiset['abc']))",
                "abc",
                "char(3) not null");
            getTester().checkNull("element(multiset[cast(null as integer)]))");
        }
    }

    @Test public void testCardinalityFunc() {
        getTester().setFor(
            SqlStdOperatorTable.cardinalityFunc,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            getTester().checkScalarExact(
                "cardinality(multiset[cast(null as integer),2]))",
                "2");
        }

        if (!enable) {
            return;
        }

        // applied to array
        getTester().checkScalarExact(
            "cardinality(array['foo', 'bar'])", "2");

        // applied to map
        getTester().checkScalarExact(
            "cardinality(map['foo', 1, 'bar', 2])", "2");
    }

    @Test public void testMemberOfOperator() {
        getTester().setFor(
            SqlStdOperatorTable.memberOfOperator,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            getTester().checkBoolean("1 member of multiset[1]", Boolean.TRUE);
            getTester().checkBoolean(
                "'2' member of multiset['1']",
                Boolean.FALSE);
            getTester().checkBoolean(
                "cast(null as double) member of multiset[cast(null as double)]",
                Boolean.TRUE);
            getTester().checkBoolean(
                "cast(null as double) member of multiset[1.1]",
                Boolean.FALSE);
            getTester().checkBoolean(
                "1.1 member of multiset[cast(null as double)]",
                Boolean.FALSE);
        }
    }

    @Test public void testCollectFunc() {
        getTester().setFor(
            SqlStdOperatorTable.collectFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testFusionFunc() {
        getTester().setFor(SqlStdOperatorTable.fusionFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testExtractFunc() {
        getTester().setFor(
            SqlStdOperatorTable.extractFunc,
            VM_FENNEL,
            VM_JAVA);

        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "extract(day from interval '2 3:4:5.678' day to second)",
            "2",
            "BIGINT NOT NULL");
        getTester().checkScalar(
            "extract(hour from interval '2 3:4:5.678' day to second)",
            "3",
            "BIGINT NOT NULL");
        getTester().checkScalar(
            "extract(minute from interval '2 3:4:5.678' day to second)",
            "4",
            "BIGINT NOT NULL");

        // TODO: Seconds should include precision
        getTester().checkScalar(
            "extract(second from interval '2 3:4:5.678' day to second)",
            "5",
            "BIGINT NOT NULL");
        getTester().checkScalar(
            "extract(year from interval '4-2' year to month)",
            "4",
            "BIGINT NOT NULL");
        getTester().checkScalar(
            "extract(month from interval '4-2' year to month)",
            "2",
            "BIGINT NOT NULL");
        getTester().checkNull(
            "extract(month from cast(null as interval year))");
    }

    @Test public void testArrayValueConstructor() {
        getTester().setFor(SqlStdOperatorTable.arrayValueConstructor);
        getTester().checkScalar(
            "Array['foo', 'bar']",
            "[foo, bar]",
            "CHAR(3) NOT NULL ARRAY NOT NULL");

        // empty array is illegal per SQL spec. presumably because one can't
        // infer type
        getTester().checkFails(
            "^Array[]^", "Require at least 1 argument", false);
    }

    @Test public void testItemOp() {
        getTester().setFor(SqlStdOperatorTable.itemOp);
        getTester().checkScalar(
            "ARRAY ['foo', 'bar'][1]", "foo", "CHAR(3) NOT NULL");
        getTester().checkScalar(
            "ARRAY ['foo', 'bar'][0]", null, "CHAR(3) NOT NULL");
        getTester().checkScalar(
            "ARRAY ['foo', 'bar'][2]", "bar", "CHAR(3) NOT NULL");
        getTester().checkScalar(
            "ARRAY ['foo', 'bar'][3]", null, "CHAR(3) NOT NULL");
        getTester().checkNull(
            "ARRAY ['foo', 'bar'][1 + CAST(NULL AS INTEGER)]");
        getTester().checkFails(
            "^ARRAY ['foo', 'bar']['baz']^",
            "Cannot apply 'ITEM' to arguments of type 'ITEM\\(<CHAR\\(3\\) ARRAY>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<VALUE>\\]",
            false);

        // Array of INTEGER NOT NULL is interesting because we might be tempted
        // to represent the result as Java "int".
        getTester().checkScalar(
            "ARRAY [2, 4, 6][2]", "4", "INTEGER NOT NULL");
        getTester().checkScalar(
            "ARRAY [2, 4, 6][4]", null, "INTEGER NOT NULL");

        // Map item
        getTester().checkScalarExact(
            "map['foo', 3, 'bar', 7]['bar']", "INTEGER NOT NULL", "7");
        getTester().checkScalarExact(
            "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['bar']", "INTEGER",
            "7");
        getTester().checkScalarExact(
            "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['baz']", "INTEGER",
            null);
    }

    @Test public void testMapValueConstructor() {
        getTester().setFor(SqlStdOperatorTable.mapValueConstructor, VM_JAVA);

        getTester().checkFails(
            "^Map[]^", "Map requires at least 2 arguments", false);

        getTester().checkFails(
            "^Map[1, 'x', 2]^",
            "Map requires an even number of arguments",
            false);

        getTester().checkFails(
            "^map[1, 1, 2, 'x']^",
            "Parameters must be of the same type", false);
        getTester().checkScalarExact(
            "map['washington', 1, 'obama', 44]",
            "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
            "{washington=1, obama=44}");
    }

    @Test public void testCeilFunc() {
        getTester().setFor(SqlStdOperatorTable.ceilFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", 11, 0);
        getTester().checkScalarApprox(
            "ceil(cast(-11.2e0 as real))",
            "REAL NOT NULL",
            -11,
            0);
        getTester().checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
        getTester().checkScalarExact(
            "ceil(1.3)", "DECIMAL(2, 0) NOT NULL", "2");
        getTester().checkScalarExact(
            "ceil(-1.7)", "DECIMAL(2, 0) NOT NULL", "-1");
        getTester().checkNull("ceiling(cast(null as decimal(2,0)))");
        getTester().checkNull("ceiling(cast(null as double))");
    }

    @Test public void testCeilFuncInterval() {
        if (!INTERVAL) {
           return;
        }
        getTester().checkScalar(
            "ceil(interval '3:4:5' hour to second)",
            "+4:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkScalar(
            "ceil(interval '-6.3' second)", "-6", "INTERVAL SECOND NOT NULL");
        getTester().checkScalar(
            "ceil(interval '5-1' year to month)",
            "+6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkScalar(
            "ceil(interval '-5-1' year to month)",
            "-5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkNull(
            "ceil(cast(null as interval year))");
    }

    @Test public void testFloorFunc() {
        getTester().setFor(SqlStdOperatorTable.floorFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        getTester().checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", 2, 0);
        getTester().checkScalarApprox(
            "floor(cast(-1.2e0 as real))",
            "REAL NOT NULL",
            -2,
            0);
        getTester().checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
        getTester().checkScalarExact(
            "floor(1.7)", "DECIMAL(2, 0) NOT NULL", "1");
        getTester().checkScalarExact(
            "floor(-1.7)", "DECIMAL(2, 0) NOT NULL", "-2");
        getTester().checkNull("floor(cast(null as decimal(2,0)))");
        getTester().checkNull("floor(cast(null as real))");
    }

    @Test public void testFloorFuncInterval() {
        if (!INTERVAL) {
            return;
        }
        getTester().checkScalar(
            "floor(interval '3:4:5' hour to second)",
            "+3:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        getTester().checkScalar(
            "floor(interval '-6.3' second)", "-7", "INTERVAL SECOND NOT NULL");
        getTester().checkScalar(
            "floor(interval '5-1' year to month)",
            "+5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkScalar(
            "floor(interval '-5-1' year to month)",
            "-6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        getTester().checkNull(
            "floor(cast(null as interval year))");
    }

    @Test public void testDenseRankFunc() {
        getTester().setFor(
            SqlStdOperatorTable.denseRankFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testPercentRankFunc() {
        getTester().setFor(
            SqlStdOperatorTable.percentRankFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testRankFunc() {
        getTester().setFor(SqlStdOperatorTable.rankFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testCumeDistFunc() {
        getTester().setFor(
            SqlStdOperatorTable.cumeDistFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testRowNumberFunc() {
        getTester().setFor(
            SqlStdOperatorTable.rowNumberFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testCountFunc() {
        getTester().setFor(SqlStdOperatorTable.countOperator, VM_EXPAND);
        getTester().checkType("count(*)", "BIGINT NOT NULL");
        getTester().checkType("count('name')", "BIGINT NOT NULL");
        getTester().checkType("count(1)", "BIGINT NOT NULL");
        getTester().checkType("count(1.2)", "BIGINT NOT NULL");
        getTester().checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
        getTester().checkFails(
            "^COUNT()^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
            false);
        getTester().checkFails(
            "^COUNT(1, 2)^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "1", "0" };
        getTester().checkAgg(
            "COUNT(x)",
            values,
            3,
            (double) 0);
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            2,
            (double) 0);
        getTester().checkAgg(
            "COUNT(DISTINCT x)",
            values,
            2,
            (double) 0);

        // string values -- note that empty string is not null
        final String [] stringValues =
        {
            "'a'", "CAST(NULL AS VARCHAR(1))", "''"
        };
        getTester().checkAgg(
            "COUNT(*)",
            stringValues,
            3,
            (double) 0);
        getTester().checkAgg(
            "COUNT(x)",
            stringValues,
            2,
            (double) 0);
        getTester().checkAgg(
            "COUNT(DISTINCT x)",
            stringValues,
            2,
            (double) 0);
        getTester().checkAgg(
            "COUNT(DISTINCT 123)",
            stringValues,
            1,
            (double) 0);
    }

    @Test public void testSumFunc() {
        getTester().setFor(SqlStdOperatorTable.sumOperator, VM_EXPAND);
        getTester().checkFails(
            "sum(^*^)", "Unknown identifier '\\*'", false);
        getTester().checkFails(
            "^sum('name')^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("sum(1)", "INTEGER NOT NULL");
        getTester().checkType("sum(1.2)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkType("sum(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkFails(
            "^sum()^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
            false);
        getTester().checkFails(
            "^sum(1, 2)^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
            false);
        getTester().checkFails(
            "^sum(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        getTester().checkAgg(
            "sum(x)",
            values,
            4,
            (double) 0);
        Object result1 = -3;
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "sum(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result1,
            (double) 0);
        Object result = -1;
        getTester().checkAgg(
            "sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result,
            (double) 0);
        getTester().checkAgg(
            "sum(DISTINCT x)",
            values,
            2,
            (double) 0);
    }

    @Test public void testAvgFunc() {
        getTester().setFor(SqlStdOperatorTable.avgOperator, VM_EXPAND);
        getTester().checkFails(
            "avg(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkFails(
            "^avg(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType("AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        if (!enable) {
            return;
        }
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        getTester().checkAgg(
            "AVG(x)", values, 2d, 0d);
        getTester().checkAgg(
            "AVG(DISTINCT x)", values, 1.5d, 0d);
        Object result = -1;
        getTester().checkAgg(
            "avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result,
            0d);
    }

    @Test public void testStddevPopFunc() {
        getTester().setFor(SqlStdOperatorTable.stddevPopOperator, VM_EXPAND);
        getTester().checkFails(
            "stddev_pop(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkFails(
            "^stddev_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_POP' to arguments of type 'STDDEV_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_POP\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("stddev_pop(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType(
            "stddev_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "stddev_pop(x)",
            values,
            1.414213562373095d, // verified on Oracle 10g
            0.000000000000001d);
        getTester().checkAgg(
            "stddev_pop(DISTINCT x)", // Oracle does not allow distinct
            values,
            1.5d,
            0d);
        getTester().checkAgg(
            "stddev_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            0,
            0d);
        // with one value
        getTester().checkAgg(
            "stddev_pop(x)",
            new String[] {"5"},
            0,
            0d);
        // with zero values
        getTester().checkAgg(
            "stddev_pop(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testStddevSampFunc() {
        getTester().setFor(SqlStdOperatorTable.stddevSampOperator, VM_EXPAND);
        getTester().checkFails(
            "stddev_samp(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkFails(
            "^stddev_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_SAMP' to arguments of type 'STDDEV_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_SAMP\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("stddev_samp(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType(
            "stddev_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "stddev_samp(x)",
            values,
            1.732050807568877d, // verified on Oracle 10g
            0.000000000000001d);
        getTester().checkAgg(
            "stddev_samp(DISTINCT x)", // Oracle does not allow distinct
            values,
            2.121320343559642d,
            0.000000000000001d);
        getTester().checkAgg(
            "stddev_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            null,
            0d);
        // with one value
        getTester().checkAgg(
            "stddev_samp(x)",
            new String[] {"5"},
            null,
            0d);
        // with zero values
        getTester().checkAgg(
            "stddev_samp(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testVarPopFunc() {
        getTester().setFor(SqlStdOperatorTable.varPopOperator, VM_EXPAND);
        getTester().checkFails(
            "var_pop(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkFails(
            "^var_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_POP' to arguments of type 'VAR_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_POP\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("var_pop(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType(
            "var_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "var_pop(x)",
            values,
            2d, // verified on Oracle 10g
            0d);
        getTester().checkAgg(
            "var_pop(DISTINCT x)", // Oracle does not allow distinct
            values,
            2.25d,
            0.0001d);
        getTester().checkAgg(
            "var_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            0,
            0d);
        // with one value
        getTester().checkAgg(
            "var_pop(x)",
            new String[] {"5"},
            0,
            0d);
        // with zero values
        getTester().checkAgg(
            "var_pop(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testVarSampFunc() {
        getTester().setFor(SqlStdOperatorTable.varSampOperator, VM_EXPAND);
        getTester().checkFails(
            "var_samp(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkFails(
            "^var_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_SAMP' to arguments of type 'VAR_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_SAMP\\(<NUMERIC>\\)'.*",
            false);
        getTester().checkType("var_samp(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType(
            "var_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "var_samp(x)", values, 3d, // verified on Oracle 10g
            0d);
        getTester().checkAgg(
            "var_samp(DISTINCT x)", // Oracle does not allow distinct
            values,
            4.5d,
            0.0001d);
        getTester().checkAgg(
            "var_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            null,
            0d);
        // with one value
        getTester().checkAgg(
            "var_samp(x)",
            new String[] {"5"},
            null,
            0d);
        // with zero values
        getTester().checkAgg(
            "var_samp(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testMinFunc() {
        getTester().setFor(SqlStdOperatorTable.minOperator, VM_EXPAND);
        getTester().checkFails(
            "min(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkType("min(1)", "INTEGER NOT NULL");
        getTester().checkType("min(1.2)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkType("min(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkFails(
            "^min()^",
            "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
            false);
        getTester().checkFails(
            "^min(1, 2)^",
            "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "min(x)",
            values,
            "0",
            0d);
        getTester().checkAgg(
            "min(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        getTester().checkAgg(
            "min(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        getTester().checkAgg(
            "min(DISTINCT x)",
            values,
            "0",
            0d);
    }

    @Test public void testMaxFunc() {
        getTester().setFor(SqlStdOperatorTable.maxOperator, VM_EXPAND);
        getTester().checkFails(
            "max(^*^)",
            "Unknown identifier '\\*'",
            false);
        getTester().checkType("max(1)", "INTEGER NOT NULL");
        getTester().checkType("max(1.2)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkType("max(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        getTester().checkFails(
            "^max()^",
            "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
            false);
        getTester().checkFails(
            "^max(1, 2)^",
            "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        if (!enable) {
            return;
        }
        getTester().checkAgg(
            "max(x)",
            values,
            "2",
            0d);
        getTester().checkAgg(
            "max(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        getTester().checkAgg(
            "max(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        getTester().checkAgg(
            "max(DISTINCT x)", values, "2", 0d);
    }

    @Test public void testLastValueFunc() {
        getTester().setFor(SqlStdOperatorTable.lastValueOperator, VM_EXPAND);
        final String [] values = { "0", "CAST(null AS INTEGER)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkWinAgg(
            "last_value(x)",
            values,
            "ROWS 3 PRECEDING",
            "INTEGER",
            Arrays.asList("3", "0"),
            0d);
        final String [] values2 = { "1.6", "1.2" };
        getTester().checkWinAgg(
            "last_value(x)",
            values2,
            "ROWS 3 PRECEDING",
            "DECIMAL(2, 1) NOT NULL",
            Arrays.asList("1.6", "1.2"),
            0d);
        final String [] values3 = { "'foo'", "'bar'", "'name'" };
        getTester().checkWinAgg(
            "last_value(x)",
            values3,
            "ROWS 3 PRECEDING",
            "CHAR(4) NOT NULL",
            Arrays.asList("foo ", "bar ", "name"),
            0d);
    }

    @Test public void testFirstValueFunc() {
        getTester().setFor(SqlStdOperatorTable.firstValueOperator, VM_EXPAND);
        final String [] values = { "0", "CAST(null AS INTEGER)", "3", "3" };
        if (!enable) {
            return;
        }
        getTester().checkWinAgg(
            "first_value(x)",
            values,
            "ROWS 3 PRECEDING",
            "INTEGER",
            Arrays.asList("0"),
            0d);
        final String [] values2 = { "1.6", "1.2" };
        getTester().checkWinAgg(
            "first_value(x)",
            values2,
            "ROWS 3 PRECEDING",
            "DECIMAL(2, 1) NOT NULL",
            Arrays.asList("1.6"),
            0d);
        final String [] values3 = { "'foo'", "'bar'", "'name'" };
        getTester().checkWinAgg(
            "first_value(x)",
            values3,
            "ROWS 3 PRECEDING",
            "CHAR(4) NOT NULL",
            Arrays.asList("foo "),
            0d);
    }

    /**
     * Tests that CAST fails when given a value just outside the valid range for
     * that type. For example,
     *
     * <ul>
     * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
     * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
     * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
     * precision.
     * </ul>
     */
    @Test public void testLiteralAtLimit() {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        if (!enable) {
            return;
        }
        for (BasicSqlType type : SqlLimitsTest.getTypes()) {
            for (Object o : getValues(type, true)) {
                SqlLiteral literal =
                    type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
                SqlString literalString =
                    literal.toSqlString(SqlDialect.DUMMY);
                final String expr =
                    "CAST(" + literalString
                    + " AS " + type + ")";
                if ((type.getSqlTypeName() == SqlTypeName.VARBINARY)
                    && !Bug.Frg283Fixed)
                {
                    continue;
                }
                try {
                    tester.checkType(
                        expr,
                        type.getFullTypeString());

                    if (type.getSqlTypeName() == SqlTypeName.BINARY) {
                        // Casting a string/binary values may change the value.
                        // For example, CAST(X'AB' AS BINARY(2)) yields
                        // X'AB00'.
                    } else {
                        tester.checkScalar(
                            expr + " = " + literalString,
                            true,
                            "BOOLEAN NOT NULL");
                    }
                } catch (Error e) {
                    System.out.println("Failed for expr=[" + expr + "]");
                    throw e;
                } catch (RuntimeException e) {
                    System.out.println("Failed for expr=[" + expr + "]");
                    throw e;
                }
            }
        }
    }

    /**
     * Tests that CAST fails when given a value just outside the valid range for
     * that type. For example,
     *
     * <ul>
     * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
     * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
     * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
     * precision.
     * </ul>
     */
    @Test public void testLiteralBeyondLimit() {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        for (BasicSqlType type : SqlLimitsTest.getTypes()) {
            for (Object o : getValues(type, false)) {
                SqlLiteral literal =
                    type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
                SqlString literalString =
                    literal.toSqlString(SqlDialect.DUMMY);

                if ((type.getSqlTypeName() == SqlTypeName.BIGINT)
                    || ((type.getSqlTypeName() == SqlTypeName.DECIMAL)
                        && (type.getPrecision() == 19)))
                {
                    // Values which are too large to be literals fail at
                    // validate time.
                    tester.checkFails(
                        "CAST(^" + literalString + "^ AS " + type + ")",
                        "Numeric literal '.*' out of range",
                        false);
                } else if (
                    (type.getSqlTypeName() == SqlTypeName.CHAR)
                    || (type.getSqlTypeName() == SqlTypeName.VARCHAR)
                    || (type.getSqlTypeName() == SqlTypeName.BINARY)
                    || (type.getSqlTypeName() == SqlTypeName.VARBINARY))
                {
                    // Casting overlarge string/binary values do not fail -
                    // they are truncated. See testCastTruncates().
                } else {
                    // Value outside legal bound should fail at runtime (not
                    // validate time).
                    //
                    // NOTE: Because Java and Fennel calcs give
                    // different errors, the pattern hedges its bets.
                    tester.checkFails(
                        "CAST(" + literalString + " AS " + type + ")",
                        "(?s).*(Overflow during calculation or cast\\.|Code=22003).*",
                        true);
                }
            }
        }
    }

    @Test public void testCastTruncates() {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        tester.checkScalar(
            "CAST('ABCD' AS CHAR(2))",
            "AB",
            "CHAR(2) NOT NULL");
        tester.checkScalar(
            "CAST('ABCD' AS VARCHAR(2))",
            "AB",
            "VARCHAR(2) NOT NULL");
        if (!enable) {
            return;
        }
        tester.checkScalar(
            "CAST(x'ABCDEF12' AS BINARY(2))",
            "ABCD",
            "BINARY(2) NOT NULL");

        if (Bug.Frg283Fixed) {
            tester.checkScalar(
                "CAST(x'ABCDEF12' AS VARBINARY(2))",
                "ABCD",
                "VARBINARY(2) NOT NULL");
        }

        tester.checkBoolean(
            "CAST(X'' AS BINARY(3)) = X'000000'",
            true);
        tester.checkBoolean(
            "CAST(X'' AS BINARY(3)) = X''",
            false);
    }

    private List<Object> getValues(BasicSqlType type, boolean inBound)
    {
        List<Object> values = new ArrayList<Object>();
        for (boolean sign : FalseTrue) {
            for (SqlTypeName.Limit limit : SqlTypeName.Limit.values()) {
                Object o = type.getLimit(sign, limit, !inBound);
                if (o == null) {
                    continue;
                }
                if (!values.contains(o)) {
                    values.add(o);
                }
            }
        }
        return values;
    }

    // TODO: Test other stuff

    /**
     * Result checker that considers a test to have succeeded if it throws an
     * exception that matches one of a list of patterns.
     */
    private static class ExceptionResultChecker
        implements SqlTester.ResultChecker
    {
        private final Pattern[] patterns;

        public ExceptionResultChecker(Pattern... patterns)
        {
            this.patterns = patterns;
        }

        public void checkResult(ResultSet result) throws Exception
        {
            Throwable thrown = null;
            try {
                result.next();
                fail("expected exception");
            } catch (SQLException e) {
                thrown = e;
            }
            final String stack = Util.getStackTrace(thrown);
            for (Pattern pattern : patterns) {
                if (pattern.matcher(stack).matches()) {
                    return;
                }
            }
            fail("Stack did not match any pattern; " + stack);
        }
    }

    /**
     * Result checker that considers a test to have succeeded if it returns a
     * particular value or throws an exception that matches one of a list of
     * patterns.
     *
     * <p>Sounds peculiar, but is necessary when eager and lazy behaviors are
     * both valid.
     */
    private static class ValueOrExceptionResultChecker
        implements SqlTester.ResultChecker
    {
        private final Object expected;
        private final Pattern[] patterns;

        public ValueOrExceptionResultChecker(
            Object expected, Pattern... patterns)
        {
            this.expected = expected;
            this.patterns = patterns;
        }

        public void checkResult(ResultSet result) throws Exception
        {
            Throwable thrown = null;
            try {
                if (!result.next()) {
                    // empty result is OK
                    return;
                }
                final Object actual = result.getObject(1);
                assertEquals(expected, actual);
            } catch (SQLException e) {
                thrown = e;
            }
            if (thrown != null) {
                final String stack = Util.getStackTrace(thrown);
                for (Pattern pattern : patterns) {
                    if (pattern.matcher(stack).matches()) {
                        return;
                    }
                }
                fail("Stack did not match any pattern; " + stack);
            }
        }
    }
}

// End SqlOperatorBaseTest.java
