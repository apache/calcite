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
import org.eigenbase.sql.validate.SqlConformance;
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
 * public void testSubstringFunc() {
 *     tester.setFor(SqlStdOperatorTable.substringFunc);
 *     tester.checkScalar("sin(0)", "0");
 *     tester.checkScalar("sin(1.5707)", "1");
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
 * <li>Also pay attention to precision/scale/length. For example, the maximum
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

    private final boolean enable;

    protected final SqlTester tester;

    //~ Constructors -----------------------------------------------------------

  /** Creates a SqlOperatorBaseTest.
   *
   * @param enable Whether to run "failing" tests.
   * @param tester Means to validate, execute various statements.
   */
    protected SqlOperatorBaseTest(boolean enable, SqlTester tester) {
        this.enable = enable;
        this.tester = tester;
        assert tester != null;
    }

  //~ Methods ----------------------------------------------------------------

    @Before public void setUp() throws Exception {
        tester.setFor(null);
    }

    //--- Tests -----------------------------------------------------------

    /** For development. Put any old code in here. */
    @Test public void testDummy() {
        checkCastToScalarOkay("'1'", "INTEGER", "1");
    }

    @Test public void testBetween() {
        tester.setFor(
            SqlStdOperatorTable.betweenOperator,
            SqlTester.VmName.EXPAND);
        tester.checkBoolean("2 between 1 and 3", Boolean.TRUE);
        tester.checkBoolean("2 between 3 and 2", Boolean.FALSE);
        tester.checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
        tester.checkBoolean("3 between 1 and 3", Boolean.TRUE);
        tester.checkBoolean("4 between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("1 between 4 and -3", Boolean.FALSE);
        tester.checkBoolean("1 between -1 and -3", Boolean.FALSE);
        tester.checkBoolean("1 between -1 and 3", Boolean.TRUE);
        tester.checkBoolean("1 between 1 and 1", Boolean.TRUE);
        tester.checkBoolean("1.5 between 1 and 3", Boolean.TRUE);
        tester.checkBoolean("1.2 between 1.1 and 1.3", Boolean.TRUE);
        tester.checkBoolean("1.5 between 2 and 3", Boolean.FALSE);
        tester.checkBoolean("1.5 between 1.6 and 1.7", Boolean.FALSE);
        tester.checkBoolean("1.2e1 between 1.1 and 1.3", Boolean.FALSE);
        tester.checkBoolean("1.2e0 between 1.1 and 1.3", Boolean.TRUE);
        tester.checkBoolean("1.5e0 between 2 and 3", Boolean.FALSE);
        tester.checkBoolean("1.5e0 between 2e0 and 3e0", Boolean.FALSE);
        tester.checkBoolean(
            "1.5e1 between 1.6e1 and 1.7e1",
            Boolean.FALSE);
        if (!enable) {
            return;
        }
        tester.checkBoolean("x'' between x'' and x''", Boolean.TRUE);
        tester.checkNull("cast(null as integer) between -1 and 2");
        tester.checkNull("1 between -1 and cast(null as integer)");
        tester.checkNull(
            "1 between cast(null as integer) and cast(null as integer)");
        tester.checkNull("1 between cast(null as integer) and 1");
    }

    @Test public void testNotBetween() {
        tester.setFor(SqlStdOperatorTable.notBetweenOperator, VM_EXPAND);
        tester.checkBoolean("2 not between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("3 not between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("4 not between 1 and 3", Boolean.TRUE);
        tester.checkBoolean(
            "1.2e0 not between 1.1 and 1.3",
            Boolean.FALSE);
        tester.checkBoolean("1.2e1 not between 1.1 and 1.3", Boolean.TRUE);
        tester.checkBoolean("1.5e0 not between 2 and 3", Boolean.TRUE);
        tester.checkBoolean("1.5e0 not between 2e0 and 3e0", Boolean.TRUE);
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
        tester.checkScalarApprox(
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
        tester.checkString(
            getCastString(value, targetType, false),
            expected,
            targetType + " NOT NULL");
    }

    private void checkCastToScalarOkay(
        String value,
        String targetType,
        String expected)
    {
        tester.checkScalarExact(
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
        tester.checkFails(
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
        tester.setFor(SqlStdOperatorTable.castFunc);

        // integer
        checkCastToString("123", "CHAR(3)", "123");
        checkCastToString("0", "CHAR", "0");
        checkCastToString("-123", "CHAR(4)", "-123");

        // decimal
        checkCastToString("123.4", "CHAR(5)", "123.4");
        checkCastToString("-0.0", "CHAR(2)", ".0");
        checkCastToString("-123.4", "CHAR(6)", "-123.4");

        tester.checkString(
            "cast(1.29 as varchar(10))",
            "1.29",
            "VARCHAR(10) NOT NULL");
        tester.checkString(
            "cast(.48 as varchar(10))",
            ".48",
            "VARCHAR(10) NOT NULL");
        tester.checkFails(
            "cast(2.523 as char(2))",
            stringTruncMessage,
            true);

        tester.checkString(
            "cast(-0.29 as varchar(10))",
            "-.29",
            "VARCHAR(10) NOT NULL");
        tester.checkString(
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

        tester.checkFails(
            "cast(1.3243232e0 as varchar(4))",
            stringTruncMessage,
            true);
        tester.checkFails(
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
        if (!(tester.isVm(VmName.FENNEL)) && INTERVAL) {
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
        tester.checkFails(
            "cast(true as char(3))",
            invalidCharMessage,
            true);
        tester.checkFails(
            "cast(false as char(4))",
            invalidCharMessage,
            true);
        tester.checkFails(
            "cast(true as varchar(3))",
            invalidCharMessage,
            true);
        tester.checkFails(
            "cast(false as varchar(4))",
            invalidCharMessage,
            true);
    }

    @Test public void testCastExactNumericLimits() {
        tester.setFor(SqlStdOperatorTable.castFunc);

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
        tester.setFor(SqlStdOperatorTable.castFunc);

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
        tester.checkScalarExact("cast('6543' as integer)", "6543");
        tester.checkScalarExact("cast(' -123 ' as int)", "-123");
        tester.checkScalarExact(
            "cast('654342432412312' as bigint)",
            "BIGINT NOT NULL",
            "654342432412312");
    }

    @Test public void testCastStringToDecimal() {
        tester.setFor(SqlStdOperatorTable.castFunc);
        if (!DECIMAL) {
            return;
        }
        // string to decimal
        tester.checkScalarExact(
            "cast('1.29' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        tester.checkScalarExact(
            "cast(' 1.25 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        tester.checkScalarExact(
            "cast('1.21' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.2");
        tester.checkScalarExact(
            "cast(' -1.29 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        tester.checkScalarExact(
            "cast('-1.25' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        tester.checkScalarExact(
            "cast(' -1.21 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");
        tester.checkFails(
            "cast(' -1.21e' as decimal(2,1))",
            invalidCharMessage,
            true);
    }

    @Test public void testCastIntervalToNumeric() {
        tester.setFor(SqlStdOperatorTable.castFunc);
        if (!INTERVAL) {
            return;
        }

        // interval to decimal
        tester.checkScalarExact(
            "cast(INTERVAL '1.29' second(1,2) as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        tester.checkScalarExact(
            "cast(INTERVAL '1.25' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        tester.checkScalarExact(
            "cast(INTERVAL '-1.29' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        tester.checkScalarExact(
            "cast(INTERVAL '-1.25' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        tester.checkScalarExact(
            "cast(INTERVAL '-1.21' second as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");
        tester.checkScalarExact(
            "cast(INTERVAL '5' minute as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "cast(INTERVAL '5' hour as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "cast(INTERVAL '5' day as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "cast(INTERVAL '5' month as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "cast(INTERVAL '5' year as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "cast(INTERVAL '-5' day as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-5.0");

        // Interval to bigint
        tester.checkScalarExact(
            "cast(INTERVAL '1.25' second as bigint)",
            "BIGINT NOT NULL",
            "1");
        tester.checkScalarExact(
            "cast(INTERVAL '-1.29' second(1,2) as bigint)",
            "BIGINT NOT NULL",
            "-1");
        tester.checkScalarExact(
            "cast(INTERVAL '5' day as bigint)",
            "BIGINT NOT NULL",
            "5");

        // Interval to integer
        tester.checkScalarExact(
            "cast(INTERVAL '1.25' second as integer)",
            "INTEGER NOT NULL",
            "1");
        tester.checkScalarExact(
            "cast(INTERVAL '-1.29' second(1,2) as integer)",
            "INTEGER NOT NULL",
            "-1");
        tester.checkScalarExact(
            "cast(INTERVAL '5' day as integer)",
            "INTEGER NOT NULL",
            "5");
    }

    @Test public void testCastToInterval() {
        tester.setFor(SqlStdOperatorTable.castFunc);
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "cast(5 as interval second)",
            "+5",
            "INTERVAL SECOND NOT NULL");
        tester.checkScalar(
            "cast(5 as interval minute)",
            "+5",
            "INTERVAL MINUTE NOT NULL");
        tester.checkScalar(
            "cast(5 as interval hour)",
            "+5",
            "INTERVAL HOUR NOT NULL");
        tester.checkScalar(
            "cast(5 as interval day)",
            "+5",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "cast(5 as interval month)",
            "+5",
            "INTERVAL MONTH NOT NULL");
        tester.checkScalar(
            "cast(5 as interval year)",
            "+5",
            "INTERVAL YEAR NOT NULL");
        tester.checkScalar(
            "cast(5.7 as interval day)",
            "+6",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "cast(-5.7 as interval day)",
            "-6",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "cast(3456 as interval month(4))",
            "+3456",
            "INTERVAL MONTH(4) NOT NULL");
        tester.checkScalar(
            "cast(-5723 as interval minute(4))",
            "-5723",
            "INTERVAL MINUTE(4) NOT NULL");
    }

    @Test public void testCastWithRoundingToScalar() {
        tester.setFor(SqlStdOperatorTable.castFunc);

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
        tester.checkFails(
            "cast(9.99 as decimal(2,1))",
            outOfRangeMessage,
            true);
    }

    @Test public void testCastDecimalToDoubleToInteger() {
        tester.setFor(SqlStdOperatorTable.castFunc);

        tester.checkScalarExact(
            "cast( cast(1.25 as double) as integer)",
            "1");
        tester.checkScalarExact(
            "cast( cast(-1.25 as double) as integer)",
            "-1");
        if (!enable) {
            return;
        }
        tester.checkScalarExact(
            "cast( cast(1.75 as double) as integer)",
            "2");
        tester.checkScalarExact(
            "cast( cast(-1.75 as double) as integer)",
            "-2");
        tester.checkScalarExact(
            "cast( cast(1.5 as double) as integer)",
            "2");
        tester.checkScalarExact(
            "cast( cast(-1.5 as double) as integer)",
            "-2");
    }

    @Test public void testCastApproxNumericLimits() {
        tester.setFor(SqlStdOperatorTable.castFunc);

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
        tester.setFor(SqlStdOperatorTable.castFunc);

        checkCastToApproxOkay("1", "DOUBLE", 1, 0);
        checkCastToApproxOkay("1.0", "DOUBLE", 1, 0);
        checkCastToApproxOkay("-2.3", "FLOAT", -2.3, 0.000001);
        checkCastToApproxOkay("'1'", "DOUBLE", 1, 0);
        checkCastToApproxOkay("'  -1e-37  '", "DOUBLE", -1e-37, 0);
        checkCastToApproxOkay("1e0", "DOUBLE", 1, 0);
        checkCastToApproxOkay("0e0", "REAL", 0, 0);
    }

    @Test public void testCastNull() {
        tester.setFor(SqlStdOperatorTable.castFunc);

        // null
        tester.checkNull("cast(null as integer)");
        if (DECIMAL) {
            tester.checkNull("cast(null as decimal(4,3))");
        }
        tester.checkNull("cast(null as double)");
        tester.checkNull("cast(null as varchar(10))");
        tester.checkNull("cast(null as char(10))");
        if (!enable) {
            return;
        }
        tester.checkNull("cast(null as date)");
        tester.checkNull("cast(null as time)");
        tester.checkNull("cast(null as timestamp)");
        if (INTERVAL) {
            tester.checkNull("cast(null as interval year to month)");
        tester.checkNull("cast(null as interval day to second(3))");
        }
        tester.checkNull("cast(null as boolean)");
    }

    @Test public void testCastDateTime() {
        // Test cast for date/time/timestamp
        tester.setFor(SqlStdOperatorTable.castFunc);

        tester.checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");

        tester.checkScalar(
            "cast(TIME '12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        // test rounding
        tester.checkScalar(
            "cast(TIME '12:42:25.9' as TIME)",
            "12:42:26",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            // test precision
            tester.checkScalar(
                "cast(TIME '12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        tester.checkScalar(
            "cast(DATE '1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // timestamp <-> time
        tester.checkScalar(
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

        tester.checkScalar(
            "cast(DATE '1945-02-24' as TIMESTAMP)",
            "1945-02-24 00:00:00.0",
            "TIMESTAMP(0) NOT NULL");

        if (!enable) {
            return;
        }

        // Note: Casting to time(0) should lose date info and fractional
        // seconds, then casting back to timestamp should initialize to
        // current_date.
        tester.checkScalar(
            "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME) as TIMESTAMP)",
            today + " 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        tester.checkScalar(
            "cast(TIME '12:42:25.34' as TIMESTAMP)",
            today + " 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        // timestamp <-> date
        tester.checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // Note: casting to Date discards Time fields
        tester.checkScalar(
            "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE) as TIMESTAMP)",
            "1945-02-24 00:00:00",
            "TIMESTAMP(0) NOT NULL");

        tester.checkScalar(
            "cast('12:42:25' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "cast('1:42:25' as TIME)",
            "01:42:25",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "cast('1:2:25' as TIME)",
            "01:02:25",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "cast('  12:42:25  ' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "cast('12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            tester.checkScalar(
                "cast('12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        tester.checkFails(
            "cast('nottime' as TIME)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('1241241' as TIME)",
            badDatetimeMessage,
            true);
        tester.checkFails(
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

        tester.checkScalar(
            "cast('1945-02-24 12:42:25' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");
        tester.checkScalar(
            "cast('1945-2-2 12:2:5' as TIMESTAMP)",
            "1945-02-02 12:02:05",
            "TIMESTAMP(0) NOT NULL");
        tester.checkScalar(
            "cast('  1945-02-24 12:42:25  ' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");
        tester.checkScalar(
            "cast('1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25",
            "TIMESTAMP(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            tester.checkScalar(
                "cast('1945-02-24 12:42:25.34' as TIMESTAMP(2))",
                "1945-02-24 12:42:25.34",
                "TIMESTAMP(2) NOT NULL");
        }
        tester.checkFails(
            "cast('nottime' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('1241241' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('1945-20-24 12:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('1945-01-24 25:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);

        // date <-> string
        checkCastToString("DATE '1945-02-24'", null, "1945-02-24");
        checkCastToString("DATE '1945-2-24'", null, "1945-02-24");

        tester.checkScalar(
            "cast('1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        tester.checkScalar(
            "cast('  1945-02-24  ' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        tester.checkFails(
            "cast('notdate' as DATE)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('52534253' as DATE)",
            badDatetimeMessage,
            true);
        tester.checkFails(
            "cast('1945-30-24' as DATE)",
            badDatetimeMessage,
            true);

        // cast null
        tester.checkNull("cast(null as date)");
        tester.checkNull("cast(null as timestamp)");
        tester.checkNull("cast(null as time)");
        tester.checkNull("cast(cast(null as varchar(10)) as time)");
        tester.checkNull("cast(cast(null as varchar(10)) as date)");
        tester.checkNull("cast(cast(null as varchar(10)) as timestamp)");
        tester.checkNull("cast(cast(null as date) as timestamp)");
        tester.checkNull("cast(cast(null as time) as timestamp)");
        tester.checkNull("cast(cast(null as timestamp) as date)");
        tester.checkNull("cast(cast(null as timestamp) as time)");
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
        tester.setFor(SqlStdOperatorTable.castFunc);

        // string to boolean
        tester.checkBoolean("cast('true' as boolean)", Boolean.TRUE);
        tester.checkBoolean("cast('false' as boolean)", Boolean.FALSE);
        tester.checkBoolean("cast('  trUe' as boolean)", Boolean.TRUE);
        tester.checkBoolean("cast('  fALse' as boolean)", Boolean.FALSE);
        tester.checkFails(
            "cast('unknown' as boolean)",
            invalidCharMessage,
            true);

        tester.checkBoolean(
            "cast(cast('true' as varchar(10))  as boolean)",
            Boolean.TRUE);
        tester.checkBoolean(
            "cast(cast('false' as varchar(10)) as boolean)",
            Boolean.FALSE);
        tester.checkFails(
            "cast(cast('blah' as varchar(10)) as boolean)",
            invalidCharMessage,
            true);
    }

    @Test public void testCase() {
        tester.setFor(SqlStdOperatorTable.caseOperator);
        tester.checkScalarExact("case when 'a'='a' then 1 end", "1");

        tester.checkString(
            "case 2 when 1 then 'a' when 2 then 'bcd' end",
            "bcd",
            "CHAR(3)");
        tester.checkString(
            "case 1 when 1 then 'a' when 2 then 'bcd' end",
            "a  ",
            "CHAR(3)");
        tester.checkString(
            "case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
            "a",
            "VARCHAR(3)");

        if (DECIMAL) {
        tester.checkScalarExact(
            "case 2 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "4.543");
        tester.checkScalarExact(
            "case 1 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "11.200");
        }
        tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
        tester.checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then cast(4 as bigint) else 3 end",
            "DOUBLE NOT NULL",
            11.2,
            0);
        tester.checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            11.2,
            0);
        tester.checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            4,
            0);
        tester.checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            11.2,
            0);
        tester.checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            4.543,
            0);
        tester.checkNull("case 'a' when 'b' then 1 end");
        if (!enable) {
            return;
        }
        tester.checkScalarExact(
            "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
            "2");

        if (todo) {
            tester.checkScalar(
                "case 1 when 1 then row(1,2) when 2 then row(2,3) end",
                "ROW(INTEGER NOT NULL, INTEGER NOT NULL)",
                "row(1,2)");
            tester.checkScalar(
                "case 1 when 1 then row('a','b') when 2 then row('ab','cd') end",
                "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)",
                "row('a ','b ')");
        }
        // TODO: Check case with multisets
    }

    @Test public void testCaseType() {
        tester.setFor(SqlStdOperatorTable.caseOperator);
        tester.checkType(
            "case 1 when 1 then current_timestamp else null end",
            "TIMESTAMP(0)");
        tester.checkType(
            "case 1 when 1 then current_timestamp else current_timestamp end",
            "TIMESTAMP(0) NOT NULL");
        tester.checkType(
            "case when true then current_timestamp else null end",
            "TIMESTAMP(0)");
        tester.checkType(
            "case when true then current_timestamp end",
            "TIMESTAMP(0)");
        tester.checkType(
            "case 'x' when 'a' then 3 when 'b' then null else 4.5 end",
            "DECIMAL(11, 1)");
    }

    /**
     * Tests support for JDBC functions.
     *
     * <p>See FRG-97 "Support for JDBC escape syntax is incomplete".
     */
    @Test public void testJdbcFn() {
        tester.setFor(new SqlJdbcFunctionCall("dummy"));

        // There follows one test for each function in appendix C of the JDBC
        // 3.0 specification. The test is 'if-false'd out if the function is
        // not implemented or is broken.

        // Numeric Functions
        if (!enable) {
            return;
        }
        tester.checkScalar("{fn ABS(-3)}", 3, "INTEGER NOT NULL");
        if (false) {
            tester.checkScalar("{fn ACOS(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn ASIN(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn ATAN(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn ATAN2(float1, float2)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn CEILING(-2.6)}", 2, "");
        }
        if (false) {
            tester.checkScalar("{fn COS(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn COT(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn DEGREES(number)}", null, "");
        }
        tester.checkScalarApprox(
            "{fn EXP(2)}",
            "DOUBLE NOT NULL",
            7.389,
            0.001);
        if (false) {
            tester.checkScalar("{fn FLOOR(2.6)}", 2, "DOUBLE NOT NULL");
        }
        tester.checkScalarApprox(
            "{fn LOG(10)}",
            "DOUBLE NOT NULL",
            2.30258,
            0.001);
        tester.checkScalarApprox(
            "{fn LOG10(100)}",
            "DOUBLE NOT NULL",
            2,
            0);
        tester.checkScalar("{fn MOD(19, 4)}", 3, "INTEGER NOT NULL");
        if (false) {
            tester.checkScalar("{fn PI()}", null, "");
        }
        tester.checkScalar("{fn POWER(2, 3)}", 8.0, "DOUBLE NOT NULL");
        if (false) {
            tester.checkScalar("{fn RADIANS(number)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn RAND(integer)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn ROUND(number, places)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn SIGN(number)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn SIN(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn SQRT(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn TAN(float)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn TRUNCATE(number, places)}", null, "");
        }

        // String Functions
        if (false) {
            tester.checkScalar("{fn ASCII(string)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn CHAR(code)}", null, "");
        }
        tester.checkScalar(
            "{fn CONCAT('foo', 'bar')}",
            "foobar",
            "CHAR(6) NOT NULL");
        if (false) {
            tester.checkScalar(
                "{fn DIFFERENCE(string1, string2)}",
                null,
                "");
        }

        // REVIEW: is this result correct? I think it should be "abcCdef"
        tester.checkScalar(
            "{fn INSERT('abc', 1, 2, 'ABCdef')}",
            "ABCdefc",
            "VARCHAR(9) NOT NULL");
        tester.checkScalar(
            "{fn LCASE('foo' || 'bar')}",
            "foobar",
            "CHAR(6) NOT NULL");
        if (false) {
            tester.checkScalar("{fn LEFT(string, count)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn LENGTH(string)}", null, "");
        }
        tester.checkScalar(
            "{fn LOCATE('ha', 'alphabet')}",
            4,
            "INTEGER NOT NULL");

        // only the 2 arg version of locate is implemented
        if (false) {
            tester.checkScalar(
                "{fn LOCATE(string1, string2[, start])}",
                null,
                "");
        }

        // ltrim is implemented but has a bug in arg checking
        if (false) {
            tester.checkScalar(
                "{fn LTRIM(' xxx  ')}",
                "xxx",
                "VARCHAR(6)");
        }
        if (false) {
            tester.checkScalar("{fn REPEAT(string, count)}", null, "");
        }
        if (false) {
            tester.checkScalar(
                "{fn REPLACE(string1, string2, string3)}",
                null,
                "");
        }
        if (false) {
            tester.checkScalar("{fn RIGHT(string, count)}", null, "");
        }

        // rtrim is implemented but has a bug in arg checking
        if (false) {
            tester.checkScalar(
                "{fn RTRIM(' xxx  ')}",
                "xxx",
                "VARCHAR(6)");
        }
        if (false) {
            tester.checkScalar("{fn SOUNDEX(string)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn SPACE(count)}", null, "");
        }
        tester.checkScalar(
            "{fn SUBSTRING('abcdef', 2, 3)}",
            "bcd",
            "VARCHAR(6) NOT NULL");
        tester.checkScalar("{fn UCASE('xxx')}", "XXX", "CHAR(3) NOT NULL");

        // Time and Date Functions
        tester.checkType("{fn CURDATE()}", "DATE NOT NULL");
        tester.checkType("{fn CURTIME()}", "TIME(0) NOT NULL");
        if (false) {
            tester.checkScalar("{fn DAYNAME(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn DAYOFMONTH(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn DAYOFWEEK(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn DAYOFYEAR(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn HOUR(time)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn MINUTE(time)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn MONTH(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn MONTHNAME(date)}", null, "");
        }
        tester.checkType("{fn NOW()}", "TIMESTAMP(0) NOT NULL");
        if (false) {
            tester.checkScalar("{fn QUARTER(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn SECOND(time)}", null, "");
        }
        if (false) {
            tester.checkScalar(
                "{fn TIMESTAMPADD(interval, count, timestamp)}",
                null,
                "");
        }
        if (false) {
            tester.checkScalar(
                "{fn TIMESTAMPDIFF(interval, timestamp1, timestamp2)}",
                null,
                "");
        }
        if (false) {
            tester.checkScalar("{fn WEEK(date)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn YEAR(date)}", null, "");
        }

        // System Functions
        if (false) {
            tester.checkScalar("{fn DATABASE()}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn IFNULL(expression, value)}", null, "");
        }
        if (false) {
            tester.checkScalar("{fn USER()}", null, "");
        }

        // Conversion Functions
        if (false) {
            tester.checkScalar("{fn CONVERT(value, SQLtype)}", null, "");
        }
    }

    @Test public void testSelect() {
        tester.setFor(SqlStdOperatorTable.selectOperator, VM_EXPAND);
        tester.check(
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
            tester.checkType(
                "SELECT *,(SELECT * FROM (VALUES(1))) FROM (VALUES(2))",
                "RecordType(INTEGER NOT NULL EXPR$0, INTEGER EXPR$1) NOT NULL");
            tester.checkType(
                "SELECT *,(SELECT * FROM (VALUES(CAST(10 as BIGINT)))) "
                + "FROM (VALUES(CAST(10 as bigint)))",
                "RecordType(BIGINT NOT NULL EXPR$0, BIGINT EXPR$1) NOT NULL");
            tester.checkType(
                " SELECT *,(SELECT * FROM (VALUES(10.5))) FROM (VALUES(10.5))",
                "RecordType(DECIMAL(3, 1) NOT NULL EXPR$0, DECIMAL(3, 1) EXPR$1) NOT NULL");
            tester.checkType(
                "SELECT *,(SELECT * FROM (VALUES('this is a char'))) "
                + "FROM (VALUES('this is a char too'))",
                "RecordType(CHAR(18) NOT NULL EXPR$0, CHAR(14) EXPR$1) NOT NULL");
            tester.checkType(
                "SELECT *,(SELECT * FROM (VALUES(true))) FROM (values(false))",
                "RecordType(BOOLEAN NOT NULL EXPR$0, BOOLEAN EXPR$1) NOT NULL");
            tester.checkType(
                " SELECT *,(SELECT * FROM (VALUES(cast('abcd' as varchar(10))))) "
                + "FROM (VALUES(CAST('abcd' as varchar(10))))",
                "RecordType(VARCHAR(10) NOT NULL EXPR$0, VARCHAR(10) EXPR$1) NOT NULL");
            tester.checkType(
                "SELECT *,"
                + "  (SELECT * FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))) "
                + "FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))",
                "RecordType(TIMESTAMP(0) NOT NULL EXPR$0, TIMESTAMP(0) EXPR$1) NOT NULL");
        }
    }

    @Test public void testLiteralChain() {
        tester.setFor(SqlStdOperatorTable.literalChainOperator, VM_EXPAND);
        tester.checkString(
            "'buttered'\n' toast'",
            "buttered toast",
            "CHAR(14) NOT NULL");
        tester.checkString(
            "'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye",
            "CHAR(18) NOT NULL");
        tester.checkString(
            "_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana",
            "CHAR(25) NOT NULL");
        if (!enable) {
            return;
        }
        tester.checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
        tester.checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
        tester.checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
    }

    @Test public void testRow() {
        tester.setFor(SqlStdOperatorTable.rowConstructor, VM_FENNEL);
    }

    @Test public void testAndOperator() {
        tester.setFor(SqlStdOperatorTable.andOperator);
        tester.checkBoolean("true and false", Boolean.FALSE);
        tester.checkBoolean("true and true", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as boolean) and false",
            Boolean.FALSE);
        tester.checkBoolean(
            "false and cast(null as boolean)",
            Boolean.FALSE);
        tester.checkNull("cast(null as boolean) and true");
        tester.checkBoolean("true and (not false)", Boolean.TRUE);
    }

    @Test public void testAndOperator2() {
        tester.checkBoolean(
            "case when false then unknown else true end and true",
            Boolean.TRUE);
        tester.checkBoolean(
            "case when false then cast(null as boolean) else true end and true",
            Boolean.TRUE);
        tester.checkBoolean(
            "case when false then null else true end and true",
            Boolean.TRUE);
    }

    @Test public void testAndOperatorLazy() {
        tester.setFor(SqlStdOperatorTable.andOperator);

        // lazy eval returns FALSE;
        // eager eval executes RHS of AND and throws;
        // both are valid
        tester.check(
            "values 1 > 2 and sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                Boolean.FALSE, invalidArgForPower, code2201f));
    }

    @Test public void testConcatOperator() {
        tester.setFor(SqlStdOperatorTable.concatOperator);
        tester.checkString(" 'a'||'b' ", "ab", "CHAR(2) NOT NULL");
        tester.checkNull(" 'a' || cast(null as char(2)) ");
        tester.checkNull(" cast(null as char(2)) || 'b' ");
        tester.checkNull(
            " cast(null as char(1)) || cast(null as char(2)) ");

        if (todo) {
            // not yet implemented
            tester.checkString(
                " x'f'||x'f' ",
                "X'FF",
                "BINARY(1) NOT NULL");
            tester.checkNull("x'ff' || cast(null as varbinary)");
        }
    }

    @Test public void testDivideOperator() {
        tester.setFor(SqlStdOperatorTable.divideOperator);
        tester.checkScalarExact("10 / 5", "2");
        tester.checkScalarExact("-10 / 5", "-2");
        tester.checkScalarExact("1 / 3", "0");
        tester.checkScalarApprox(
            " cast(10.0 as double) / 5",
            "DOUBLE NOT NULL",
            2.0,
            0);
        tester.checkScalarApprox(
            " cast(10.0 as real) / 5",
            "REAL NOT NULL",
            2.0,
            0);
        tester.checkScalarApprox(
            " 6.0 / cast(10.0 as real) ",
            "DOUBLE NOT NULL",
            0.6,
            0);
        tester.checkScalarExact(
            "10.0 / 5.0",
            "DECIMAL(9, 6) NOT NULL",
            "2");
        if (DECIMAL) {
            tester.checkScalarExact(
                "1.0 / 3.0",
                "DECIMAL(8, 6) NOT NULL",
                "0.333333");
            tester.checkScalarExact(
                "100.1 / 0.0001",
                "DECIMAL(14, 7) NOT NULL",
                "1001000.0000000");
            tester.checkScalarExact(
                "100.1 / 0.00000001",
                "DECIMAL(19, 8) NOT NULL",
                "10010000000.00000000");
        }
        tester.checkNull("1e1 / cast(null as float)");

        tester.checkFails(
            "100.1 / 0.00000000000000001",
            outOfRangeMessage,
            true);
    }

    @Test public void testDivideOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "interval '-2:2' hour to minute / 3",
            "-0:40",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        tester.checkScalar(
            "interval '2:5:12' hour to second / 2 / -3",
            "-0:20:52",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkNull(
            "interval '2' day / cast(null as bigint)");
        tester.checkNull(
            "cast(null as interval month) / 2");
        if (todo) {
            tester.checkScalar(
                "interval '3-3' year to month / 15e-1",
                "+02-02",
                "INTERVAL YEAR TO MONTH NOT NULL");
            tester.checkScalar(
                "interval '3-4' year to month / 4.5",
                "+00-08",
                "INTERVAL YEAR TO MONTH NOT NULL");
        }
    }

    @Test public void testEqualsOperator() {
        tester.setFor(SqlStdOperatorTable.equalsOperator);
        tester.checkBoolean("1=1", Boolean.TRUE);
        tester.checkBoolean("1=1.0", Boolean.TRUE);
        tester.checkBoolean("1.34=1.34", Boolean.TRUE);
        tester.checkBoolean("1=1.34", Boolean.FALSE);
        tester.checkBoolean("1e2=100e0", Boolean.TRUE);
        tester.checkBoolean("1e2=101", Boolean.FALSE);
        tester.checkBoolean(
            "cast(1e2 as real)=cast(101 as bigint)",
            Boolean.FALSE);
        tester.checkBoolean("'a'='b'", Boolean.FALSE);
        tester.checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        tester.checkBoolean(
            "cast('a ' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        tester.checkBoolean(
            "cast('a' as varchar(30))=cast('b' as varchar(30))",
            Boolean.FALSE);
        tester.checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(15))",
            Boolean.TRUE);
        tester.checkNull("cast(null as boolean)=cast(null as boolean)");
        tester.checkNull("cast(null as integer)=1");
        tester.checkNull("cast(null as varchar(10))='a'");
    }

    @Test public void testEqualsOperatorInterval() {
        // Intervals
        if (!INTERVAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day = interval '1' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day = interval '2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2:2:2' hour to second = interval '2' hour",
            Boolean.FALSE);
        tester.checkNull(
            "cast(null as interval hour) = interval '2' minute");
    }

    @Test public void testGreaterThanOperator() {
        tester.setFor(SqlStdOperatorTable.greaterThanOperator);
        tester.checkBoolean("1>2", Boolean.FALSE);
        tester.checkBoolean(
            "cast(-1 as TINYINT)>cast(1 as TINYINT)",
            Boolean.FALSE);
        tester.checkBoolean(
            "cast(1 as SMALLINT)>cast(1 as SMALLINT)",
            Boolean.FALSE);
        tester.checkBoolean("2>1", Boolean.TRUE);
        tester.checkBoolean("1.1>1.2", Boolean.FALSE);
        tester.checkBoolean("-1.1>-1.2", Boolean.TRUE);
        tester.checkBoolean("1.1>1.1", Boolean.FALSE);
        tester.checkBoolean("1.2>1", Boolean.TRUE);
        tester.checkBoolean("1.1e1>1.2e1", Boolean.FALSE);
        tester.checkBoolean(
            "cast(-1.1 as real) > cast(-1.2 as real)",
            Boolean.TRUE);
        tester.checkBoolean("1.1e2>1.1e2", Boolean.FALSE);
        tester.checkBoolean("1.2e0>1", Boolean.TRUE);
        tester.checkBoolean("cast(1.2e0 as real)>1", Boolean.TRUE);
        tester.checkBoolean("true>false", Boolean.TRUE);
        tester.checkBoolean("true>true", Boolean.FALSE);
        tester.checkBoolean("false>false", Boolean.FALSE);
        tester.checkBoolean("false>true", Boolean.FALSE);
        tester.checkNull("3.0>cast(null as double)");

        tester.checkBoolean(
            "DATE '2013-02-23' > DATE '1945-02-24'", Boolean.TRUE);
        tester.checkBoolean(
            "DATE '2013-02-23' > CAST(NULL AS DATE)", null);
    }

    @Test public void testGreaterThanOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day > interval '1' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day > interval '5' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2 2:2:2' day to second > interval '2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day > interval '2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day > interval '-2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day > interval '2' hour",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' minute > interval '2' hour",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' second > interval '2' minute",
            Boolean.FALSE);
        tester.checkNull(
            "cast(null as interval hour) > interval '2' minute");
        tester.checkNull(
            "interval '2:2' hour to minute > cast(null as interval second)");
    }

    @Test public void testIsDistinctFromOperator() {
        tester.setFor(
            SqlStdOperatorTable.isDistinctFromOperator,
            VM_EXPAND);
        tester.checkBoolean("1 is distinct from 1", Boolean.FALSE);
        if (!enable) {
            return;
        }
        tester.checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
        tester.checkBoolean("1 is distinct from 2", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as integer) is distinct from 2",
            Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as integer) is distinct from cast(null as integer)",
            Boolean.FALSE);
        tester.checkBoolean("1.23 is distinct from 1.23", Boolean.FALSE);
        tester.checkBoolean("1.23 is distinct from 5.23", Boolean.TRUE);
        tester.checkBoolean(
            "-23e0 is distinct from -2.3e1",
            Boolean.FALSE);

        // IS DISTINCT FROM not implemented for ROW yet
        if (false) {
            tester.checkBoolean(
                "row(1,1) is distinct from row(1,1)",
                true);
            tester.checkBoolean(
                "row(1,1) is distinct from row(1,2)",
                false);
        }

        // Intervals
        tester.checkBoolean(
            "interval '2' day is distinct from interval '1' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '10' hour is distinct from interval '10' hour",
            Boolean.FALSE);
    }

    @Test public void testIsNotDistinctFromOperator() {
        tester.setFor(
            SqlStdOperatorTable.isNotDistinctFromOperator,
            VM_EXPAND);
        tester.checkBoolean("1 is not distinct from 1", Boolean.TRUE);
        tester.checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
        tester.checkBoolean("1 is not distinct from 2", Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as integer) is not distinct from 2",
            Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as integer) is not distinct from cast(null as integer)",
            Boolean.TRUE);
        tester.checkBoolean(
            "1.23 is not distinct from 1.23",
            Boolean.TRUE);
        tester.checkBoolean(
            "1.23 is not distinct from 5.23",
            Boolean.FALSE);
        tester.checkBoolean(
            "-23e0 is not distinct from -2.3e1",
            Boolean.TRUE);

        // IS NOT DISTINCT FROM not implemented for ROW yet
        if (false) {
            tester.checkBoolean(
                "row(1,1) is not distinct from row(1,1)",
                false);
            tester.checkBoolean(
                "row(1,1) is not distinct from row(1,2)",
                true);
        }

        if (!enable) {
            return;
        }
        // Intervals
        tester.checkBoolean(
            "interval '2' day is not distinct from interval '1' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '10' hour is not distinct from interval '10' hour",
            Boolean.TRUE);
    }

    @Test public void testGreaterThanOrEqualOperator() {
        tester.setFor(SqlStdOperatorTable.greaterThanOrEqualOperator);
        tester.checkBoolean("1>=2", Boolean.FALSE);
        tester.checkBoolean("-1>=1", Boolean.FALSE);
        tester.checkBoolean("1>=1", Boolean.TRUE);
        tester.checkBoolean("2>=1", Boolean.TRUE);
        tester.checkBoolean("1.1>=1.2", Boolean.FALSE);
        tester.checkBoolean("-1.1>=-1.2", Boolean.TRUE);
        tester.checkBoolean("1.1>=1.1", Boolean.TRUE);
        tester.checkBoolean("1.2>=1", Boolean.TRUE);
        tester.checkBoolean("1.2e4>=1e5", Boolean.FALSE);
        tester.checkBoolean("1.2e4>=cast(1e5 as real)", Boolean.FALSE);
        tester.checkBoolean("1.2>=cast(1e5 as double)", Boolean.FALSE);
        tester.checkBoolean("120000>=cast(1e5 as real)", Boolean.TRUE);
        tester.checkBoolean("true>=false", Boolean.TRUE);
        tester.checkBoolean("true>=true", Boolean.TRUE);
        tester.checkBoolean("false>=false", Boolean.TRUE);
        tester.checkBoolean("false>=true", Boolean.FALSE);
        tester.checkNull("cast(null as real)>=999");
    }

    @Test public void testGreaterThanOrEqualOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day >= interval '1' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day >= interval '5' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2 2:2:2' day to second >= interval '2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day >= interval '2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day >= interval '-2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day >= interval '2' hour",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' minute >= interval '2' hour",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' second >= interval '2' minute",
            Boolean.FALSE);
        tester.checkNull(
            "cast(null as interval hour) >= interval '2' minute");
        tester.checkNull(
            "interval '2:2' hour to minute >= cast(null as interval second)");
    }

    @Test public void testInOperator() {
        tester.setFor(SqlStdOperatorTable.inOperator, VM_EXPAND);
        tester.checkBoolean("1 in (0, 1, 2)", true);
        tester.checkBoolean("3 in (0, 1, 2)", false);
        tester.checkBoolean("cast(null as integer) in (0, 1, 2)", null);
        tester.checkBoolean(
            "cast(null as integer) in (0, cast(null as integer), 2)",
            null);
        if (Bug.Frg327Fixed) {
            tester.checkBoolean(
                "cast(null as integer) in (0, null, 2)",
                null);
            tester.checkBoolean("1 in (0, null, 2)", null);
        }

        if (!enable) {
            return;
        }
        // AND has lower precedence than IN
        tester.checkBoolean("false and true in (false, false)", false);

        if (!Bug.TodoFixed) {
            return;
        }
        tester.checkFails(
            "'foo' in (^)^",
            "(?s).*Encountered \"\\)\" at .*",
            false);
    }

    @Test public void testNotInOperator() {
        tester.setFor(SqlStdOperatorTable.notInOperator, VM_EXPAND);
        tester.checkBoolean("1 not in (0, 1, 2)", false);
        tester.checkBoolean("3 not in (0, 1, 2)", true);
        if (!enable) {
            return;
        }
        tester.checkBoolean(
            "cast(null as integer) not in (0, 1, 2)",
            null);
        tester.checkBoolean(
            "cast(null as integer) not in (0, cast(null as integer), 2)",
            null);
        if (Bug.Frg327Fixed) {
            tester.checkBoolean(
                "cast(null as integer) not in (0, null, 2)",
                null);
            tester.checkBoolean("1 not in (0, null, 2)", null);
        }

        // AND has lower precedence than NOT IN
        tester.checkBoolean("true and false not in (true, true)", true);

        if (!Bug.TodoFixed) {
            return;
        }
        tester.checkFails(
            "'foo' not in (^)^",
            "(?s).*Encountered \"\\)\" at .*",
            false);
    }

    @Test public void testOverlapsOperator() {
        tester.setFor(SqlStdOperatorTable.overlapsOperator, VM_EXPAND);
        if (Bug.Frg187Fixed) {
            tester.checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)",
                Boolean.TRUE);
            tester.checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '4-5-6', interval '1' year)",
                Boolean.FALSE);
            tester.checkBoolean(
                "(date '1-2-3', date '4-5-6') overlaps (date '2-2-3', date '3-4-5')",
                Boolean.TRUE);
            tester.checkNull(
                "(cast(null as date), date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
            tester.checkNull(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', cast(null as date))");

            tester.checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')",
                Boolean.TRUE);
            tester.checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:2')",
                Boolean.FALSE);
            tester.checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', interval '2' hour)",
                Boolean.TRUE);
            tester.checkNull(
                "(time '1:2:3', cast(null as time)) overlaps (time '23:59:59', time '1:2:3')");
            tester.checkNull(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', cast(null as interval hour))");

            tester.checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.TRUE);
            tester.checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '2-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.FALSE);
            tester.checkNull(
                "(timestamp '1-2-3 4:5:6', cast(null as interval day) ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");
            tester.checkNull(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (cast(null as timestamp), interval '1 2:3:4.5' day to second)");
        }
    }

    @Test public void testLessThanOperator() {
        tester.setFor(SqlStdOperatorTable.lessThanOperator);
        tester.checkBoolean("1<2", Boolean.TRUE);
        tester.checkBoolean("-1<1", Boolean.TRUE);
        tester.checkBoolean("1<1", Boolean.FALSE);
        tester.checkBoolean("2<1", Boolean.FALSE);
        tester.checkBoolean("1.1<1.2", Boolean.TRUE);
        tester.checkBoolean("-1.1<-1.2", Boolean.FALSE);
        tester.checkBoolean("1.1<1.1", Boolean.FALSE);
        tester.checkBoolean("cast(1.1 as real)<1", Boolean.FALSE);
        tester.checkBoolean("cast(1.1 as real)<1.1", Boolean.FALSE);
        tester.checkBoolean(
            "cast(1.1 as real)<cast(1.2 as real)",
            Boolean.TRUE);
        tester.checkBoolean("-1.1e-1<-1.2e-1", Boolean.FALSE);
        tester.checkBoolean(
            "cast(1.1 as real)<cast(1.1 as double)",
            Boolean.FALSE);
        tester.checkBoolean("true<false", Boolean.FALSE);
        tester.checkBoolean("true<true", Boolean.FALSE);
        tester.checkBoolean("false<false", Boolean.FALSE);
        tester.checkBoolean("false<true", Boolean.TRUE);
        tester.checkNull("123<cast(null as bigint)");
        tester.checkNull("cast(null as tinyint)<123");
        tester.checkNull("cast(null as integer)<1.32");
    }

    @Test public void testLessThanOperatorInterval() {
        if (!DECIMAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day < interval '1' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day < interval '5' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2 2:2:2' day to second < interval '2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day < interval '2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day < interval '-2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day < interval '2' hour",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' minute < interval '2' hour",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' second < interval '2' minute",
            Boolean.TRUE);
        tester.checkNull(
            "cast(null as interval hour) < interval '2' minute");
        tester.checkNull(
            "interval '2:2' hour to minute < cast(null as interval second)");
    }

    @Test public void testLessThanOrEqualOperator() {
        tester.setFor(SqlStdOperatorTable.lessThanOrEqualOperator);
        tester.checkBoolean("1<=2", Boolean.TRUE);
        tester.checkBoolean("1<=1", Boolean.TRUE);
        tester.checkBoolean("-1<=1", Boolean.TRUE);
        tester.checkBoolean("2<=1", Boolean.FALSE);
        tester.checkBoolean("1.1<=1.2", Boolean.TRUE);
        tester.checkBoolean("-1.1<=-1.2", Boolean.FALSE);
        tester.checkBoolean("1.1<=1.1", Boolean.TRUE);
        tester.checkBoolean("1.2<=1", Boolean.FALSE);
        tester.checkBoolean("1<=cast(1e2 as real)", Boolean.TRUE);
        tester.checkBoolean("1000<=cast(1e2 as real)", Boolean.FALSE);
        tester.checkBoolean("1.2e1<=1e2", Boolean.TRUE);
        tester.checkBoolean("1.2e1<=cast(1e2 as real)", Boolean.TRUE);
        tester.checkBoolean("true<=false", Boolean.FALSE);
        tester.checkBoolean("true<=true", Boolean.TRUE);
        tester.checkBoolean("false<=false", Boolean.TRUE);
        tester.checkBoolean("false<=true", Boolean.TRUE);
        tester.checkNull("cast(null as real)<=cast(1 as real)");
        tester.checkNull("cast(null as integer)<=3");
        tester.checkNull("3<=cast(null as smallint)");
        tester.checkNull("cast(null as integer)<=1.32");
    }

    @Test public void testLessThanOrEqualOperatorInterval() {
        if (!INTERVAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day <= interval '1' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day <= interval '5' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2 2:2:2' day to second <= interval '2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day <= interval '2' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day <= interval '-2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' day <= interval '2' hour",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2' minute <= interval '2' hour",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' second <= interval '2' minute",
            Boolean.TRUE);
        tester.checkNull(
            "cast(null as interval hour) <= interval '2' minute");
        tester.checkNull(
            "interval '2:2' hour to minute <= cast(null as interval second)");
    }

    @Test public void testMinusOperator() {
        tester.setFor(SqlStdOperatorTable.minusOperator);
        tester.checkScalarExact("-2-1", "-3");
        tester.checkScalarExact("-2-1-5", "-8");
        tester.checkScalarExact("2-1", "1");
        tester.checkScalarApprox(
            "cast(2.0 as double) -1",
            "DOUBLE NOT NULL",
            1,
            0);
        tester.checkScalarApprox(
            "cast(1 as smallint)-cast(2.0 as real)",
            "REAL NOT NULL",
            -1,
            0);
        tester.checkScalarApprox(
            "2.4-cast(2.0 as real)",
            "DOUBLE NOT NULL",
            0.4,
            0.00000001);
        tester.checkScalarExact("1-2", "-1");
        tester.checkScalarExact(
            "10.0 - 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "5.0");
        tester.checkScalarExact(
            "19.68 - 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "15.48");
        tester.checkNull("1e1-cast(null as double)");
        tester.checkNull("cast(null as tinyint) - cast(null as smallint)");

        // TODO: Fix bug
        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            tester.checkFails(
                "cast(100 as tinyint) - cast(-100 as tinyint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(-20000 as smallint) - cast(20000 as smallint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(1.5e9 as integer) - cast(-1.5e9 as integer)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(-5e18 as bigint) - cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(-5e8 as decimal(19,10)) - cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testMinusIntervalOperator() {
        tester.setFor(SqlStdOperatorTable.minusOperator);
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "interval '2' day - interval '1' day",
            "+1",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "interval '2' day - interval '1' minute",
            "+1 23:59",
            "INTERVAL DAY TO MINUTE NOT NULL");
        tester.checkScalar(
            "interval '2' year - interval '1' month",
            "+1-11",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkScalar(
            "interval '2' year - interval '1' month - interval '3' year",
            "-1-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkNull(
            "cast(null as interval day) + interval '2' hour");

        // Datetime minus interval
        tester.checkScalar(
            "time '12:03:01' - interval '1:1' hour to minute",
            "11:02:01",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "date '2005-03-02' - interval '5' day",
            "2005-02-25",
            "DATE NOT NULL");
        tester.checkScalar(
            "timestamp '2003-08-02 12:54:01' - interval '-4 2:4' day to minute",
            "2003-08-06 14:58:01",
            "TIMESTAMP(0) NOT NULL");

        // TODO: Tests with interval year months (not supported)
    }

    @Test public void testMinusDateOperator() {
        tester.setFor(SqlStdOperatorTable.minusDateOperator);
        if (!enable) {
            return;
        }
        tester.checkScalar(
            "(time '12:03:34' - time '11:57:23') minute to second",
            "+6:11",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        tester.checkScalar(
            "(time '12:03:23' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        tester.checkScalar(
            "(time '12:03:34' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        tester.checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to second",
            "+2 00:06:11",
            "INTERVAL DAY TO SECOND NOT NULL");
        tester.checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to hour",
            "+2 00",
            "INTERVAL DAY TO HOUR NOT NULL");
        tester.checkScalar(
            "(date '2004-12-02' - date '2003-12-01') day",
            "+367",
            "INTERVAL DAY NOT NULL");
        tester.checkNull(
            "(cast(null as date) - date '2003-12-01') day");

        // combine '<datetime> + <interval>' with '<datetime> - <datetime>'
        tester.checkScalar(
            "timestamp '1969-04-29 0:0:0' +"
            + " (timestamp '2008-07-15 15:28:00' - "
            + "  timestamp '1969-04-29 0:0:0') day to second / 2",
            "1988-12-06 07:44:00",
            "TIMESTAMP(0) NOT NULL");

        tester.checkScalar(
            "date '1969-04-29' +"
            + " (date '2008-07-15' - "
            + "  date '1969-04-29') day / 2",
            "1988-12-06",
            "DATE NOT NULL");

        tester.checkScalar(
            "time '01:23:44' +"
            + " (time '15:28:00' - "
            + "  time '01:23:44') hour to second / 2",
            "08:25:52",
            "TIME(0) NOT NULL");

        if (Bug.Dt1684Fixed) {
            tester.checkBoolean(
                "(date '1969-04-29' +"
                + " (CURRENT_DATE - "
                + "  date '1969-04-29') day / 2) is not null",
                Boolean.TRUE);
        }
        // TODO: Add tests for year month intervals (currently not supported)
    }

    @Test public void testMultiplyOperator() {
        tester.setFor(SqlStdOperatorTable.multiplyOperator);
        tester.checkScalarExact("2*3", "6");
        tester.checkScalarExact("2*-3", "-6");
        tester.checkScalarExact("+2*3", "6");
        tester.checkScalarExact("2*0", "0");
        tester.checkScalarApprox(
            "cast(2.0 as float)*3",
            "FLOAT NOT NULL",
            6,
            0);
        tester.checkScalarApprox(
            "3*cast(2.0 as real)",
            "REAL NOT NULL",
            6,
            0);
        tester.checkScalarApprox(
            "cast(2.0 as real)*3.2",
            "DOUBLE NOT NULL",
            6.4,
            0);
        tester.checkScalarExact(
            "10.0 * 5.0",
            "DECIMAL(5, 2) NOT NULL",
            "50.00");
        tester.checkScalarExact(
            "19.68 * 4.2",
            "DECIMAL(6, 3) NOT NULL",
            "82.656");
        tester.checkNull("cast(1 as real)*cast(null as real)");
        tester.checkNull("2e-3*cast(null as integer)");
        tester.checkNull("cast(null as tinyint) * cast(4 as smallint)");

        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            tester.checkFails(
                "cast(100 as tinyint) * cast(-2 as tinyint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(200 as smallint) * cast(200 as smallint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(1.5e9 as integer) * cast(-2 as integer)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(5e9 as bigint) * cast(2e9 as bigint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testMultiplyIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "interval '2:2' hour to minute * 3",
            "+6:06",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        tester.checkScalar(
            "3 * 2 * interval '2:5:12' hour to second",
            "+12:31:12",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkNull(
            "interval '2' day * cast(null as bigint)");
        tester.checkNull(
            "cast(null as interval month) * 2");
        if (todo) {
            tester.checkScalar(
                "interval '3-2' year to month * 15e-1",
                "+04-09",
                "INTERVAL YEAR TO MONTH NOT NULL");
            tester.checkScalar(
                "interval '3-4' year to month * 4.5",
                "+15-00",
                "INTERVAL YEAR TO MONTH NOT NULL");
        }
    }

    @Test public void testNotEqualsOperator() {
        tester.setFor(SqlStdOperatorTable.notEqualsOperator);
        tester.checkBoolean("1<>1", Boolean.FALSE);
        tester.checkBoolean("'a'<>'A'", Boolean.TRUE);
        tester.checkBoolean("1e0<>1e1", Boolean.TRUE);
        tester.checkNull("'a'<>cast(null as varchar(1))");
    }

    @Test public void testNotEqualsOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkBoolean(
            "interval '2' day <> interval '1' day",
            Boolean.TRUE);
        tester.checkBoolean(
            "interval '2' day <> interval '2' day",
            Boolean.FALSE);
        tester.checkBoolean(
            "interval '2:2:2' hour to second <> interval '2' hour",
            Boolean.TRUE);
        tester.checkNull(
            "cast(null as interval hour) <> interval '2' minute");

        // "!=" is not an acceptable alternative to "<>"
        tester.checkFails(
            "1 ^!^= 1",
            "(?s).*Encountered: \"!\" \\(33\\).*",
            false);
    }

    @Test public void testOrOperator() {
        tester.setFor(SqlStdOperatorTable.orOperator);
        tester.checkBoolean("true or false", Boolean.TRUE);
        tester.checkBoolean("false or false", Boolean.FALSE);
        tester.checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
        tester.checkNull("false or cast(null as boolean)");
    }

    @Test public void testOrOperatorLazy() {
        tester.setFor(SqlStdOperatorTable.orOperator);

        // need to evaluate 2nd argument if first evaluates to null, therefore
        // get error
        tester.check(
            "values 1 < cast(null as integer) or sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                null, invalidArgForPower, code2201f));

        // Do not need to evaluate 2nd argument if first evaluates to true.
        // In eager evaluation, get error;
        // lazy evaluation returns true;
        // both are valid.
        tester.check(
            "values 1 < 2 or sqrt(-4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                Boolean.TRUE, invalidArgForPower, code2201f));

        // NULL OR FALSE --> NULL
        // In eager evaluation, get error;
        // lazy evaluation returns NULL;
        // both are valid.
        tester.check(
            "values 1 < cast(null as integer) or sqrt(4) = -2",
            AbstractSqlTester.BooleanTypeChecker,
            new ValueOrExceptionResultChecker(
                null, invalidArgForPower, code2201f));

        // NULL OR TRUE --> TRUE
        tester.checkBoolean(
            "1 < cast(null as integer) or sqrt(4) = 2", Boolean.TRUE);
    }

    @Test public void testPlusOperator() {
        tester.setFor(SqlStdOperatorTable.plusOperator);
        tester.checkScalarExact("1+2", "3");
        tester.checkScalarExact("-1+2", "1");
        tester.checkScalarExact("1+2+3", "6");
        tester.checkScalarApprox(
            "1+cast(2.0 as double)",
            "DOUBLE NOT NULL",
            3,
            0);
        tester.checkScalarApprox(
            "1+cast(2.0 as double)+cast(6.0 as float)",
            "DOUBLE NOT NULL",
            9,
            0);
        tester.checkScalarExact(
            "10.0 + 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "15.0");
        tester.checkScalarExact(
            "19.68 + 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "23.88");
        tester.checkScalarExact(
            "19.68 + 4.2 + 6",
            "DECIMAL(13, 2) NOT NULL",
            "29.88");
        tester.checkScalarApprox(
            "19.68 + cast(4.2 as float)",
            "DOUBLE NOT NULL",
            23.88,
            0.02);
        tester.checkNull("cast(null as tinyint)+1");
        tester.checkNull("1e-2+cast(null as double)");

        if (Bug.Fnl25Fixed) {
            // Should throw out of range error
            tester.checkFails(
                "cast(100 as tinyint) + cast(100 as tinyint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(-20000 as smallint) + cast(-20000 as smallint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(1.5e9 as integer) + cast(1.5e9 as integer)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(5e18 as bigint) + cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(-5e18 as decimal(19,0)) + cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            tester.checkFails(
                "cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    @Test public void testPlusIntervalOperator() {
        tester.setFor(SqlStdOperatorTable.plusOperator);
        if (!INTERVAL) {
            return;
        }

        tester.checkScalar(
            "interval '2' day + interval '1' day",
            "+3",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "interval '2' day + interval '1' minute",
            "+2 00:01",
            "INTERVAL DAY TO MINUTE NOT NULL");
        tester.checkScalar(
            "interval '2' day + interval '5' minute + interval '-3' second",
            "+2 00:04:57",
            "INTERVAL DAY TO SECOND NOT NULL");
        tester.checkScalar(
            "interval '2' year + interval '1' month",
            "+2-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkNull(
            "interval '2' year + cast(null as interval month)");

        // Datetime plus interval
        tester.checkScalar(
            "time '12:03:01' + interval '1:1' hour to minute",
            "13:04:01",
            "TIME(0) NOT NULL");
        tester.checkScalar(
            "interval '5' day + date '2005-03-02'",
            "2005-03-07",
            "DATE NOT NULL");
        tester.checkScalar(
            "timestamp '2003-08-02 12:54:01' + interval '-4 2:4' day to minute",
            "2003-07-29 10:50:01",
            "TIMESTAMP(0) NOT NULL");

        // TODO: Tests with interval year months (not supported)
    }

    @Test public void testDescendingOperator() {
        tester.setFor(SqlStdOperatorTable.descendingOperator, VM_EXPAND);
    }

    @Test public void testIsNotNullOperator() {
        tester.setFor(SqlStdOperatorTable.isNotNullOperator);
        tester.checkBoolean("true is not null", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as boolean) is not null",
            Boolean.FALSE);
    }

    @Test public void testIsNullOperator() {
        tester.setFor(SqlStdOperatorTable.isNullOperator);
        tester.checkBoolean("true is null", Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as boolean) is null",
            Boolean.TRUE);
    }

    @Test public void testIsNotTrueOperator() {
        tester.setFor(SqlStdOperatorTable.isNotTrueOperator);
        tester.checkBoolean("true is not true", Boolean.FALSE);
        tester.checkBoolean("false is not true", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as boolean) is not true",
            Boolean.TRUE);
        tester.checkFails(
            "select ^'a string' is not true^ from (values (1))",
            "(?s)Cannot apply 'IS NOT TRUE' to arguments of type '<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): '<BOOLEAN> IS NOT TRUE'.*",
            false);
    }

    @Test public void testIsTrueOperator() {
        tester.setFor(SqlStdOperatorTable.isTrueOperator);
        tester.checkBoolean("true is true", Boolean.TRUE);
        tester.checkBoolean("false is true", Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as boolean) is true",
            Boolean.FALSE);
    }

    @Test public void testIsNotFalseOperator() {
        tester.setFor(SqlStdOperatorTable.isNotFalseOperator);
        tester.checkBoolean("false is not false", Boolean.FALSE);
        tester.checkBoolean("true is not false", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as boolean) is not false",
            Boolean.TRUE);
    }

    @Test public void testIsFalseOperator() {
        tester.setFor(SqlStdOperatorTable.isFalseOperator);
        tester.checkBoolean("false is false", Boolean.TRUE);
        tester.checkBoolean("true is false", Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as boolean) is false",
            Boolean.FALSE);
    }

    @Test public void testIsNotUnknownOperator() {
        tester.setFor(SqlStdOperatorTable.isNotUnknownOperator, VM_EXPAND);
        tester.checkBoolean("false is not unknown", Boolean.TRUE);
        tester.checkBoolean("true is not unknown", Boolean.TRUE);
        tester.checkBoolean(
            "cast(null as boolean) is not unknown",
            Boolean.FALSE);
        tester.checkBoolean("unknown is not unknown", Boolean.FALSE);
        tester.checkFails(
            "^'abc' IS NOT UNKNOWN^",
            "(?s).*Cannot apply 'IS NOT UNKNOWN'.*",
            false);
    }

    @Test public void testIsUnknownOperator() {
        tester.setFor(SqlStdOperatorTable.isUnknownOperator, VM_EXPAND);
        tester.checkBoolean("false is unknown", Boolean.FALSE);
        tester.checkBoolean("true is unknown", Boolean.FALSE);
        tester.checkBoolean(
            "cast(null as boolean) is unknown",
            Boolean.TRUE);
        tester.checkBoolean("unknown is unknown", Boolean.TRUE);
        tester.checkFails(
            "0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4",
            "(?s).*Cannot apply 'IS UNKNOWN'.*",
            false);
    }

    @Test public void testIsASetOperator() {
        tester.setFor(SqlStdOperatorTable.isASetOperator, VM_EXPAND);
    }

    @Test public void testExistsOperator() {
        tester.setFor(SqlStdOperatorTable.existsOperator, VM_EXPAND);
    }

    @Test public void testNotOperator() {
        tester.setFor(SqlStdOperatorTable.notOperator);
        tester.checkBoolean("not true", Boolean.FALSE);
        tester.checkBoolean("not false", Boolean.TRUE);
        tester.checkBoolean("not unknown", null);
        tester.checkNull("not cast(null as boolean)");
    }

    @Test public void testPrefixMinusOperator() {
        tester.setFor(SqlStdOperatorTable.prefixMinusOperator);
        tester.checkFails(
            "'a' + ^- 'b'^ + 'c'",
            "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*",
            false);
        tester.checkScalarExact("-1", "-1");
        tester.checkScalarExact(
            "-1.23",
            "DECIMAL(3, 2) NOT NULL",
            "-1.23");
        tester.checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", -1, 0);
        tester.checkNull("-cast(null as integer)");
        tester.checkNull("-cast(null as tinyint)");
    }

    @Test public void testPrefixMinusOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "-interval '-6:2:8' hour to second",
            "+6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkScalar(
            "- -interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkScalar(
            "-interval '5' month",
            "-5",
            "INTERVAL MONTH NOT NULL");
        tester.checkNull(
            "-cast(null as interval day to minute)");
    }

    @Test public void testPrefixPlusOperator() {
        tester.setFor(SqlStdOperatorTable.prefixPlusOperator, VM_EXPAND);
        tester.checkScalarExact("+1", "1");
        tester.checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
        tester.checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", 1, 0);
        tester.checkNull("+cast(null as integer)");
        tester.checkNull("+cast(null as tinyint)");
    }

    @Test public void testPrefixPlusOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "+interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkScalar(
            "++interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        if (Bug.Frg254Fixed) {
            tester.checkScalar(
                "+interval '6:2:8.234' hour to second",
                "+06:02:08.234",
                "INTERVAL HOUR TO SECOND NOT NULL");
        }
        tester.checkScalar(
            "+interval '5' month",
            "+5",
            "INTERVAL MONTH NOT NULL");
        tester.checkNull(
            "+cast(null as interval day to minute)");
    }

    @Test public void testExplicitTableOperator() {
        tester.setFor(
            SqlStdOperatorTable.explicitTableOperator,
            VM_EXPAND);
    }

    @Test public void testValuesOperator() {
        tester.setFor(SqlStdOperatorTable.valuesOperator, VM_EXPAND);
        tester.check(
            "select 'abc' from (values(true))",
            new AbstractSqlTester.StringTypeChecker("CHAR(3) NOT NULL"),
            "abc",
            0);
    }

    @Test public void testNotLikeOperator() {
        tester.setFor(SqlStdOperatorTable.notLikeOperator, VM_EXPAND);
        tester.checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    }

    @Test public void testLikeOperator() {
        tester.setFor(SqlStdOperatorTable.likeOperator);
        tester.checkBoolean("''  like ''", Boolean.TRUE);
        tester.checkBoolean("'a' like 'a'", Boolean.TRUE);
        tester.checkBoolean("'a' like 'b'", Boolean.FALSE);
        tester.checkBoolean("'a' like 'A'", Boolean.FALSE);
        tester.checkBoolean("'a' like 'a_'", Boolean.FALSE);
        tester.checkBoolean("'a' like '_a'", Boolean.FALSE);
        tester.checkBoolean("'a' like '%a'", Boolean.TRUE);
        tester.checkBoolean("'a' like '%a%'", Boolean.TRUE);
        tester.checkBoolean("'a' like 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
        tester.checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
        tester.checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   like '_b'", Boolean.TRUE);
        tester.checkBoolean("'abcd' like '_d'", Boolean.FALSE);
        tester.checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    }

    @Test public void testNotSimilarToOperator() {
        tester.setFor(SqlStdOperatorTable.notSimilarOperator, VM_EXPAND);
        tester.checkBoolean("'ab' not similar to 'a_'", false);
        tester.checkBoolean("'aabc' not similar to 'ab*c+d'", true);
        tester.checkBoolean("'ab' not similar to 'a' || '_'", false);
        tester.checkBoolean("'ab' not similar to 'ba_'", true);
        tester.checkBoolean(
            "cast(null as varchar(2)) not similar to 'a_'",
            null);
        tester.checkBoolean(
            "cast(null as varchar(3)) not similar to cast(null as char(2))",
            null);
    }

    @Test public void testSimilarToOperator() {
        tester.setFor(SqlStdOperatorTable.similarOperator);

        // like LIKE
        tester.checkBoolean("''  similar to ''", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'b'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to 'A'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to '_a'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to '%a'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
        tester.checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
        tester.checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
        tester.checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
        tester.checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

        // simple regular expressions
        // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
        tester.checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
        tester.checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

        // compound regular expressions
        // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
        tester.checkBoolean(
            "'xy'      similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        tester.checkBoolean(
            "'xccy'    similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        tester.checkBoolean(
            "'xababcy' similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        tester.checkBoolean(
            "'xbcy'    similar to 'x(ab|c)*y'",
            Boolean.FALSE);

        // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
        tester.checkBoolean(
            "'xy'      similar to 'x(ab|c)+y'",
            Boolean.FALSE);
        tester.checkBoolean(
            "'xccy'    similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        tester.checkBoolean(
            "'xababcy' similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        tester.checkBoolean(
            "'xbcy'    similar to 'x(ab|c)+y'",
            Boolean.FALSE);

        tester.checkBoolean("'ab' similar to 'a%' ", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a%' ", Boolean.TRUE);
        tester.checkBoolean("'abcd' similar to 'a_' ", Boolean.FALSE);
        tester.checkBoolean("'abcd' similar to 'a%' ", Boolean.TRUE);
        tester.checkBoolean("'1a' similar to '_a' ", Boolean.TRUE);
        tester.checkBoolean("'123aXYZ' similar to '%a%'", Boolean.TRUE);

        tester.checkBoolean(
            "'123aXYZ' similar to '_%_a%_' ",
            Boolean.TRUE);

        tester.checkBoolean("'xy' similar to '(xy)' ", Boolean.TRUE);

        tester.checkBoolean(
            "'abd' similar to '[ab][bcde]d' ",
            Boolean.TRUE);

        tester.checkBoolean(
            "'bdd' similar to '[ab][bcde]d' ",
            Boolean.TRUE);

        tester.checkBoolean("'abd' similar to '[ab]d' ", Boolean.FALSE);
        tester.checkBoolean("'cd' similar to '[a-e]d' ", Boolean.TRUE);
        tester.checkBoolean("'amy' similar to 'amy|fred' ", Boolean.TRUE);
        tester.checkBoolean("'fred' similar to 'amy|fred' ", Boolean.TRUE);

        tester.checkBoolean(
            "'mike' similar to 'amy|fred' ",
            Boolean.FALSE);

        tester.checkBoolean("'acd' similar to 'ab*c+d' ", Boolean.TRUE);
        tester.checkBoolean("'accccd' similar to 'ab*c+d' ", Boolean.TRUE);
        tester.checkBoolean("'abd' similar to 'ab*c+d' ", Boolean.FALSE);
        tester.checkBoolean("'aabc' similar to 'ab*c+d' ", Boolean.FALSE);
        tester.checkBoolean("'abb' similar to 'a(b{3})' ", Boolean.FALSE);
        tester.checkBoolean("'abbb' similar to 'a(b{3})' ", Boolean.TRUE);

        tester.checkBoolean(
            "'abbbbb' similar to 'a(b{3})' ",
            Boolean.FALSE);

        tester.checkBoolean(
            "'abbbbb' similar to 'ab{3,6}' ",
            Boolean.TRUE);

        tester.checkBoolean(
            "'abbbbbbbb' similar to 'ab{3,6}' ",
            Boolean.FALSE);
        tester.checkBoolean("'' similar to 'ab?' ", Boolean.FALSE);
        tester.checkBoolean("'a' similar to 'ab?' ", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a(b?)' ", Boolean.TRUE);
        tester.checkBoolean("'ab' similar to 'ab?' ", Boolean.TRUE);
        tester.checkBoolean("'ab' similar to 'a(b?)' ", Boolean.TRUE);
        tester.checkBoolean("'abb' similar to 'ab?' ", Boolean.FALSE);

        tester.checkBoolean(
            "'ab' similar to 'a\\_' ESCAPE '\\' ",
            Boolean.FALSE);

        tester.checkBoolean(
            "'ab' similar to 'a\\%' ESCAPE '\\' ",
            Boolean.FALSE);

        tester.checkBoolean(
            "'a_' similar to 'a\\_' ESCAPE '\\' ",
            Boolean.TRUE);

        tester.checkBoolean(
            "'a%' similar to 'a\\%' ESCAPE '\\' ",
            Boolean.TRUE);

        tester.checkBoolean(
            "'a(b{3})' similar to 'a(b{3})' ",
            Boolean.FALSE);

        tester.checkBoolean(
            "'a(b{3})' similar to 'a\\(b\\{3\\}\\)' ESCAPE '\\' ",
            Boolean.TRUE);

        tester.checkBoolean("'yd' similar to '[a-ey]d'", Boolean.TRUE);
        tester.checkBoolean("'yd' similar to '[^a-ey]d'", Boolean.FALSE);
        tester.checkBoolean("'yd' similar to '[^a-ex-z]d'", Boolean.FALSE);
        tester.checkBoolean("'yd' similar to '[a-ex-z]d'", Boolean.TRUE);
        tester.checkBoolean("'yd' similar to '[x-za-e]d'", Boolean.TRUE);
        tester.checkBoolean("'yd' similar to '[^a-ey]?d'", Boolean.FALSE);
        tester.checkBoolean("'yyyd' similar to '[a-ey]*d'", Boolean.TRUE);

         // range must be specified in []
        tester.checkBoolean("'yd' similar to 'x-zd'", Boolean.FALSE);
        tester.checkBoolean("'y' similar to 'x-z'", Boolean.FALSE);

        tester.checkBoolean("'cd' similar to '([a-e])d'", Boolean.TRUE);
        tester.checkBoolean("'xy' similar to 'x*?y'", Boolean.TRUE);
        tester.checkBoolean("'y' similar to 'x*?y'", Boolean.TRUE);
        tester.checkBoolean("'y' similar to '(x?)*y'", Boolean.TRUE);
        tester.checkBoolean("'y' similar to 'x+?y'", Boolean.FALSE);

        tester.checkBoolean("'y' similar to 'x?+y'", Boolean.TRUE);
        tester.checkBoolean("'y' similar to 'x*+y'", Boolean.TRUE);

        // The following two tests throws exception(They probably should).
        // "Dangling meta character '*' near index 2"

        if (enable) {
            tester.checkBoolean("'y' similar to 'x+*y'", Boolean.TRUE);
            tester.checkBoolean("'y' similar to 'x?*y'", Boolean.TRUE);
        }

        // some negative tests
        tester.checkFails(
            "'yd' similar to '[x-ze-a]d'",
            "Illegal character range near index 6\n"
            + "\\[x-ze-a\\]d\n"
            + "      \\^",
            true);   // illegal range

        tester.checkFails(
            "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{,5}'",
            "Illegal repetition near index 20\n"
            + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
            + "                    \\^",
            true);

        tester.checkFails(
            "'cd' similar to '[(a-e)]d' ",
            "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
            true);

        tester.checkFails(
            "'yd' similar to '[(a-e)]d' ",
            "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
            true);

        // all the following tests wrong results due to missing functionality
        // or defect (FRG-375, 377).

        if (Bug.Frg375Fixed) {
            tester.checkBoolean(
                "'cd' similar to '[a-e^c]d' ", Boolean.FALSE); // FRG-375
        }

        // following tests use regular character set identifiers.
        // Not implemented yet. FRG-377.
        if (Bug.Frg377Fixed) {
            tester.checkBoolean(
                "'y' similar to '[:ALPHA:]*'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd32' similar to '[:LOWER:]{2}[:DIGIT:]*'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd32' similar to '[:ALNUM:]*'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd32' similar to '[:ALNUM:]*[:DIGIT:]?'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd32' similar to '[:ALNUM:]?[:DIGIT:]*'",
                Boolean.FALSE);

            tester.checkBoolean(
                "'yd3223' similar to '([:LOWER:]{2})[:DIGIT:]{2,5}'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{2,}'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}||[:DIGIT:]{4}'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{3}'",
                Boolean.FALSE);

            tester.checkBoolean(
                "'yd  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
                Boolean.FALSE);

            tester.checkBoolean(
                "'YD  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
                Boolean.FALSE);

            tester.checkBoolean(
                "'YD  3223' similar to "
                + "'[:UPPER:]{2}||[:WHITESPACE:]*[:DIGIT:]{4}'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'YD\t3223' similar to '[:UPPER:]{2}[:SPACE:]*[:DIGIT:]{4}'",
                Boolean.FALSE);

            tester.checkBoolean(
                "'YD\t3223' similar to "
                + "'[:UPPER:]{2}[:WHITESPACE:]*[:DIGIT:]{4}'",
                Boolean.TRUE);

            tester.checkBoolean(
                "'YD\t\t3223' similar to "
                + "'([:UPPER:]{2}[:WHITESPACE:]+)||[:DIGIT:]{4}'",
                Boolean.TRUE);
        }
    }

    @Test public void testEscapeOperator() {
        tester.setFor(SqlStdOperatorTable.escapeOperator, VM_EXPAND);
    }

    @Test public void testConvertFunc() {
        tester.setFor(
            SqlStdOperatorTable.convertFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testTranslateFunc() {
        tester.setFor(
            SqlStdOperatorTable.translateFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testOverlayFunc() {
        tester.setFor(SqlStdOperatorTable.overlayFunc);
        tester.checkString(
            "overlay('ABCdef' placing 'abc' from 1)",
            "abcdef",
            "VARCHAR(9) NOT NULL");
        tester.checkString(
            "overlay('ABCdef' placing 'abc' from 1 for 2)",
            "abcCdef",
            "VARCHAR(9) NOT NULL");
        if (!enable) {
            return;
        }
        tester.checkString(
            "overlay(cast('ABCdef' as varchar(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef",
            "VARCHAR(15) NOT NULL");
        tester.checkString(
            "overlay(cast('ABCdef' as char(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef    ",
            "VARCHAR(15) NOT NULL");
        tester.checkNull(
            "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
        tester.checkNull(
            "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

        if (false) {
            // hex strings not yet implemented in calc
            tester.checkNull(
                "overlay(x'abc' placing x'abc' from cast(null as integer))");
        }
    }

    @Test public void testPositionFunc() {
        tester.setFor(SqlStdOperatorTable.positionFunc);
        if (!enable) {
            return;
        }
        tester.checkScalarExact("position('b' in 'abc')", "2");
        tester.checkScalarExact("position('' in 'abc')", "1");

        // FRG-211
        tester.checkScalarExact("position('tra' in 'fdgjklewrtra')", "10");

        tester.checkNull("position(cast(null as varchar(1)) in '0010')");
        tester.checkNull("position('a' in cast(null as varchar(1)))");

        tester.checkScalar(
            "position(cast('a' as char) in cast('bca' as varchar))",
            0,
            "INTEGER NOT NULL");
    }

    @Test public void testCharLengthFunc() {
        tester.setFor(SqlStdOperatorTable.charLengthFunc);
        tester.checkScalarExact("char_length('abc')", "3");
        tester.checkNull("char_length(cast(null as varchar(1)))");
    }

    @Test public void testCharacterLengthFunc() {
        tester.setFor(SqlStdOperatorTable.characterLengthFunc);
        tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
        tester.checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    }

    @Test public void testUpperFunc() {
        tester.setFor(SqlStdOperatorTable.upperFunc);
        tester.checkString("upper('a')", "A", "CHAR(1) NOT NULL");
        tester.checkString("upper('A')", "A", "CHAR(1) NOT NULL");
        tester.checkString("upper('1')", "1", "CHAR(1) NOT NULL");
        tester.checkString("upper('aa')", "AA", "CHAR(2) NOT NULL");
        tester.checkNull("upper(cast(null as varchar(1)))");
    }

    @Test public void testLowerFunc() {
        tester.setFor(SqlStdOperatorTable.lowerFunc);

        // SQL:2003 6.29.8 The type of lower is the type of its argument
        tester.checkString("lower('A')", "a", "CHAR(1) NOT NULL");
        tester.checkString("lower('a')", "a", "CHAR(1) NOT NULL");
        tester.checkString("lower('1')", "1", "CHAR(1) NOT NULL");
        tester.checkString("lower('AA')", "aa", "CHAR(2) NOT NULL");
        tester.checkNull("lower(cast(null as varchar(1)))");
    }

    @Test public void testInitcapFunc() {
        // Note: the initcap function is an Oracle defined function and is not
        // defined in the SQL:2003 standard
        // todo: implement in fennel
        tester.setFor(SqlStdOperatorTable.initcapFunc, VM_FENNEL);

        tester.checkString("initcap('aA')", "Aa", "CHAR(2) NOT NULL");
        tester.checkString("initcap('Aa')", "Aa", "CHAR(2) NOT NULL");
        tester.checkString("initcap('1a')", "1a", "CHAR(2) NOT NULL");
        tester.checkString(
            "initcap('ab cd Ef 12')",
            "Ab Cd Ef 12",
            "CHAR(11) NOT NULL");
        tester.checkNull("initcap(cast(null as varchar(1)))");

        // dtbug 232
        tester.checkFails(
            "^initcap(cast(null as date))^",
            "Cannot apply 'INITCAP' to arguments of type 'INITCAP\\(<DATE>\\)'\\. Supported form\\(s\\): 'INITCAP\\(<CHARACTER>\\)'",
            false);
    }

    @Test public void testPowerFunc() {
        tester.setFor(SqlStdOperatorTable.powerFunc);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox(
            "power(2,-2)",
            "DOUBLE NOT NULL",
            0.25,
            0);
        tester.checkNull("power(cast(null as integer),2)");
        tester.checkNull("power(2,cast(null as double))");

        // 'pow' is an obsolete form of the 'power' function
        tester.checkFails(
            "^pow(2,-2)^",
            "No match found for function signature POW\\(<NUMERIC>, <NUMERIC>\\)",
            false);
    }

    @Test public void testSqrtFunc() {
        tester.setFor(
            SqlStdOperatorTable.sqrtFunc, SqlTester.VmName.EXPAND);
        tester.checkType("sqrt(2)", "DOUBLE NOT NULL");
        tester.checkType("sqrt(cast(2 as float))", "DOUBLE NOT NULL");
        tester.checkType(
            "sqrt(case when false then 2 else null end)", "DOUBLE");
        tester.checkFails(
            "^sqrt('abc')^",
            "Cannot apply 'SQRT' to arguments of type 'SQRT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'SQRT\\(<NUMERIC>\\)'",
            false);
        tester.checkScalarApprox(
            "sqrt(2)",
            "DOUBLE NOT NULL",
            1.4142d,
            0.0001d);
        tester.checkNull("sqrt(cast(null as integer))");
        tester.checkNull("sqrt(cast(null as double))");
    }

    @Test public void testExpFunc() {
        // todo: implement in fennel
        tester.setFor(SqlStdOperatorTable.expFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox(
            "exp(2)",
            "DOUBLE NOT NULL",
            7.389056,
            0.000001);
        tester.checkScalarApprox(
            "exp(-2)",
            "DOUBLE NOT NULL",
            0.1353,
            0.0001);
        tester.checkNull("exp(cast(null as integer))");
        tester.checkNull("exp(cast(null as double))");
    }

    @Test public void testModFunc() {
        tester.setFor(SqlStdOperatorTable.modFunc);
        tester.checkScalarExact("mod(4,2)", "0");
        tester.checkScalarExact("mod(8,5)", "3");
        tester.checkScalarExact("mod(-12,7)", "-5");
        tester.checkScalarExact("mod(-12,-7)", "-5");
        tester.checkScalarExact("mod(12,-7)", "5");
        tester.checkScalarExact(
            "mod(cast(12 as tinyint), cast(-7 as tinyint))",
            "TINYINT NOT NULL",
            "5");

        tester.checkScalarExact(
            "mod(cast(9 as decimal(2, 0)), 7)",
            "INTEGER NOT NULL",
            "2");
        tester.checkScalarExact(
            "mod(7, cast(9 as decimal(2, 0)))",
            "DECIMAL(2, 0) NOT NULL",
            "7");
        tester.checkScalarExact(
            "mod(cast(-9 as decimal(2, 0)), cast(7 as decimal(1, 0)))",
            "DECIMAL(1, 0) NOT NULL",
            "-2");
        tester.checkNull("mod(cast(null as integer),2)");
        tester.checkNull("mod(4,cast(null as tinyint))");
        tester.checkNull("mod(4,cast(null as decimal(12,0)))");
    }

    @Test public void testModFuncDivByZero() {
        // The extra CASE expression is to fool Janino.  It does constant
        // reduction and will throw the divide by zero exception while
        // compiling the expression.  The test frame work would then issue
        // unexpected exception occurred during "validation".  You cannot
        // submit as non-runtime because the janino exception does not have
        // error position information and the framework is unhappy with that.
        tester.checkFails(
            "mod(3,case 'a' when 'a' then 0 end)",
            divisionByZeroMessage,
            true);
    }

    @Test public void testLnFunc() {
        tester.setFor(SqlStdOperatorTable.lnFunc);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        tester.checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            0.999999327,
            0.0000001);
        tester.checkNull("ln(cast(null as tinyint))");
    }

    @Test public void testLogFunc() {
        tester.setFor(SqlStdOperatorTable.log10Func);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox(
            "log10(10)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        tester.checkScalarApprox(
            "log10(100.0)",
            "DOUBLE NOT NULL",
            2.0,
            0.000001);
        tester.checkScalarApprox(
            "log10(cast(10e8 as double))",
            "DOUBLE NOT NULL",
            9.0,
            0.000001);
        tester.checkScalarApprox(
            "log10(cast(10e2 as float))",
            "DOUBLE NOT NULL",
            3.0,
            0.000001);
        tester.checkScalarApprox(
            "log10(cast(10e-3 as real))",
            "DOUBLE NOT NULL",
            -2.0,
            0.000001);
        tester.checkNull("log10(cast(null as real))");
    }

    @Test public void testAbsFunc() {
        tester.setFor(SqlStdOperatorTable.absFunc);

        if (!enable) {
            return;
        }
        tester.checkScalarExact("abs(-1)", "1");
        tester.checkScalarExact(
            "abs(cast(10 as TINYINT))",
            "TINYINT NOT NULL",
            "10");
        tester.checkScalarExact(
            "abs(cast(-20 as SMALLINT))",
            "SMALLINT NOT NULL",
            "20");
        tester.checkScalarExact(
            "abs(cast(-100 as INT))",
            "INTEGER NOT NULL",
            "100");
        tester.checkScalarExact(
            "abs(cast(1000 as BIGINT))",
            "BIGINT NOT NULL",
            "1000");
        tester.checkScalarExact(
            "abs(54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        tester.checkScalarExact(
            "abs(-54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        tester.checkScalarApprox(
            "abs(-9.32E-2)",
            "DOUBLE NOT NULL",
            0.0932,
            0);
        tester.checkScalarApprox(
            "abs(cast(-3.5 as double))",
            "DOUBLE NOT NULL",
            3.5,
            0);
        tester.checkScalarApprox(
            "abs(cast(-3.5 as float))",
            "FLOAT NOT NULL",
            3.5,
            0);
        tester.checkScalarApprox(
            "abs(cast(3.5 as real))",
            "REAL NOT NULL",
            3.5,
            0);

        tester.checkNull("abs(cast(null as double))");
    }

    @Test public void testAbsFuncIntervals() {
        if (!INTERVAL) {
           return;
        }
        tester.checkScalar(
            "abs(interval '-2' day)",
            "+2",
            "INTERVAL DAY NOT NULL");
        tester.checkScalar(
            "abs(interval '-5-03' year to month)",
            "+5-03",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkNull("abs(cast(null as interval hour))");
    }

    @Test public void testNullifFunc() {
        tester.setFor(SqlStdOperatorTable.nullIfFunc, VM_EXPAND);
        tester.checkNull("nullif(1,1)");
        if (!enable) {
            return;
        }
        tester.checkScalarExact(
            "nullif(1.5, 13.56)",
            "DECIMAL(2, 1)",
            "1.5");
        tester.checkScalarExact(
            "nullif(13.56, 1.5)",
            "DECIMAL(4, 2)",
            "13.56");
        tester.checkScalarExact("nullif(1.5, 3)", "DECIMAL(2, 1)", "1.5");
        tester.checkScalarExact("nullif(3, 1.5)", "INTEGER", "3");
        tester.checkScalarApprox("nullif(1.5e0, 3e0)", "DOUBLE", 1.5, 0);
        tester.checkScalarApprox(
            "nullif(1.5, cast(3e0 as REAL))",
            "DECIMAL(2, 1)",
            1.5,
            0);
        tester.checkScalarExact("nullif(3, 1.5e0)", "INTEGER", "3");
        tester.checkScalarExact(
            "nullif(3, cast(1.5e0 as REAL))",
            "INTEGER",
            "3");
        tester.checkScalarApprox("nullif(1.5e0, 3.4)", "DOUBLE", 1.5, 0);
        tester.checkScalarExact(
            "nullif(3.4, 1.5e0)",
            "DECIMAL(2, 1)",
            "3.4");
        tester.checkString(
            "nullif('a','bc')",
            "a",
            "CHAR(1)");
        tester.checkString(
            "nullif('a',cast(null as varchar(1)))",
            "a",
            "CHAR(1)");
        tester.checkNull("nullif(cast(null as varchar(1)),'a')");
        tester.checkNull("nullif(cast(null as numeric(4,3)), 4.3)");

        // Error message reflects the fact that Nullif is expanded before it is
        // validated (like a C macro). Not perfect, but good enough.
        tester.checkFails(
            "1 + ^nullif(1, date '2005-8-4')^ + 2",
            "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*",
            false);

        tester.checkFails(
            "1 + ^nullif(1, 2, 3)^ + 2",
            "Invalid number of arguments to function 'NULLIF'\\. Was expecting 2 arguments",
            false);
    }

    @Test public void testNullIfOperatorIntervals() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "nullif(interval '2' month, interval '3' year)",
            "+2",
            "INTERVAL MONTH");
        tester.checkScalar(
            "nullif(interval '2 5' day to hour, interval '5' second)",
            "+2 05",
            "INTERVAL DAY TO HOUR");
        tester.checkNull(
            "nullif(interval '3' day, interval '3' day)");
    }

    @Test public void testCoalesceFunc() {
        tester.setFor(SqlStdOperatorTable.coalesceFunc, VM_EXPAND);
        tester.checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
        tester.checkScalarExact("coalesce(null,null,3)", "3");
        tester.checkFails(
            "1 + ^coalesce('a', 'b', 1, null)^ + 2",
            "Illegal mixing of types in CASE or COALESCE statement",
            false);
    }

    @Test public void testUserFunc() {
        tester.setFor(SqlStdOperatorTable.userFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentUserFunc() {
        tester.setFor(SqlStdOperatorTable.currentUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testSessionUserFunc() {
        tester.setFor(SqlStdOperatorTable.sessionUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testSystemUserFunc() {
        tester.setFor(SqlStdOperatorTable.systemUserFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        String user = System.getProperty("user.name"); // e.g. "jhyde"
        tester.checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentPathFunc() {
        tester.setFor(SqlStdOperatorTable.currentPathFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testCurrentRoleFunc() {
        tester.setFor(SqlStdOperatorTable.currentRoleFunc, VM_FENNEL);
        if (!enable) {
            return;
        }

        // By default, the CURRENT_ROLE function returns
        // the empty string because a role has to be set explicitly.
        tester.checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
    }

    @Test public void testLocalTimeFunc() {
        tester.setFor(SqlStdOperatorTable.localTimeFunc);
        if (!enable) {
            return;
        }
        tester.checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
        tester.checkFails(
            "^LOCALTIME()^",
            "No match found for function signature LOCALTIME\\(\\)",
            false);
        tester.checkScalar(
            "LOCALTIME(1)",
            timePattern,
            "TIME(1) NOT NULL");

        tester.checkScalar(
            "CAST(LOCALTIME AS VARCHAR(30))",
            Pattern.compile(
                currentTimeString(defaultTimeZone).substring(11)
                + "[0-9][0-9]:[0-9][0-9]"),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testLocalTimestampFunc() {
        tester.setFor(SqlStdOperatorTable.localTimestampFunc);
        if (!enable) {
            return;
        }
        tester.checkScalar(
            "LOCALTIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        tester.checkFails(
            "^LOCALTIMESTAMP()^",
            "No match found for function signature LOCALTIMESTAMP\\(\\)",
            false);
        tester.checkFails(
            "LOCALTIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        tester.checkScalar(
            "LOCALTIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");

        // Check that timestamp is being generated in the right timezone by
        // generating a specific timestamp.
        tester.checkScalar(
            "CAST(LOCALTIMESTAMP AS VARCHAR(30))",
            Pattern.compile(
                currentTimeString(defaultTimeZone)
                + "[0-9][0-9]:[0-9][0-9]"),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testCurrentTimeFunc() {
        tester.setFor(SqlStdOperatorTable.currentTimeFunc);
        if (!enable) {
            return;
        }
        tester.checkScalar(
            "CURRENT_TIME",
            timePattern,
            "TIME(0) NOT NULL");
        tester.checkFails(
            "^CURRENT_TIME()^",
            "No match found for function signature CURRENT_TIME\\(\\)",
            false);
        tester.checkScalar(
            "CURRENT_TIME(1)",
            timePattern,
            "TIME(1) NOT NULL");

        if (Bug.Fnl77Fixed) {
            // Currently works with Java calc, but fennel calc returns time in
            // GMT time zone.
            tester.checkScalar(
                "CAST(CURRENT_TIME AS VARCHAR(30))",
                Pattern.compile(
                    currentTimeString(defaultTimeZone).substring(11)
                    + "[0-9][0-9]:[0-9][0-9]"),
                "VARCHAR(30) NOT NULL");
        }
    }

    @Test public void testCurrentTimestampFunc() {
        tester.setFor(SqlStdOperatorTable.currentTimestampFunc);
        if (!enable) {
            return;
        }
        tester.checkScalar(
            "CURRENT_TIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        tester.checkFails(
            "^CURRENT_TIMESTAMP()^",
            "No match found for function signature CURRENT_TIMESTAMP\\(\\)",
            false);
        tester.checkFails(
            "CURRENT_TIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        tester.checkScalar(
            "CURRENT_TIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");

        // Check that timestamp is being generated in the right timezone by
        // generating a specific timestamp. We truncate to hours so that minor
        // delays don't generate false negatives.
        if (Bug.Fnl77Fixed) {
            // Currently works with Java calc, but fennel calc returns time in
            // GMT time zone.
            tester.checkScalar(
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
        tester.setFor(SqlStdOperatorTable.currentDateFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
        tester.checkScalar(
            "(CURRENT_DATE - CURRENT_DATE) DAY",
            "+0",
            "INTERVAL DAY NOT NULL");
        tester.checkBoolean("CURRENT_DATE IS NULL", false);
        tester.checkFails(
            "^CURRENT_DATE()^",
            "No match found for function signature CURRENT_DATE\\(\\)",
            false);

        // Check the actual value.
        tester.checkScalar(
            "CAST(CURRENT_DATE AS VARCHAR(30))",
            currentTimeString(defaultTimeZone).substring(0, 10),
            "VARCHAR(30) NOT NULL");
    }

    @Test public void testSubstringFunction() {
        tester.setFor(SqlStdOperatorTable.substringFunc);
        tester.checkString(
            "substring('abc' from 1 for 2)",
            "ab",
            "VARCHAR(3) NOT NULL");
        tester.checkString(
            "substring('abc' from 2)",
            "bc",
            "VARCHAR(3) NOT NULL");

        if (Bug.Frg296Fixed) {
            // substring regexp not supported yet
            tester.checkString(
                "substring('foobar' from '%#\"o_b#\"%' for'#')",
                "oob",
                "xx");
        }
        tester.checkNull("substring(cast(null as varchar(1)),1,2)");
    }

    @Test public void testTrimFunc() {
        tester.setFor(SqlStdOperatorTable.trimBothFunc);
        if (!enable) {
            return;
        }

        // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
        tester.checkString(
            "trim('a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        tester.checkString(
            "trim(both 'a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        tester.checkString(
            "trim(leading 'a' from 'aAa')",
            "Aa",
            "VARCHAR(3) NOT NULL");
        tester.checkString(
            "trim(trailing 'a' from 'aAa')",
            "aA",
            "VARCHAR(3) NOT NULL");
        tester.checkNull("trim(cast(null as varchar(1)) from 'a')");
        tester.checkNull("trim('a' from cast(null as varchar(1)))");

        if (Bug.Fnl3Fixed) {
            // SQL:2003 6.29.9: trim string must have length=1. Failure occurs
            // at runtime.
            //
            // TODO: Change message to "Invalid argument\(s\) for
            // 'TRIM' function".
            // The message should come from a resource file, and should still
            // have the SQL error code 22027.
            tester.checkFails(
                "trim('xy' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
            tester.checkFails(
                "trim('' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
        }
    }

    @Test public void testWindow() {
        tester.setFor(
            SqlStdOperatorTable.windowOperator,
            VM_FENNEL,
            VM_JAVA);
        if (!enable) {
            return;
        }
        tester.check(
            "select sum(1) over (order by x) from (select 1 as x, 2 as y from (values (true)))",
            new AbstractSqlTester.StringTypeChecker("INTEGER"),
            "1",
            0);
    }

    @Test public void testElementFunc() {
        tester.setFor(
            SqlStdOperatorTable.elementFunc,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            tester.checkString(
                "element(multiset['abc']))",
                "abc",
                "char(3) not null");
            tester.checkNull("element(multiset[cast(null as integer)]))");
        }
    }

    @Test public void testCardinalityFunc() {
        tester.setFor(
            SqlStdOperatorTable.cardinalityFunc,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            tester.checkScalarExact(
                "cardinality(multiset[cast(null as integer),2]))",
                "2");
        }

        if (!enable) {
            return;
        }

        // applied to array
        tester.checkScalarExact(
            "cardinality(array['foo', 'bar'])", "2");

        // applied to map
        tester.checkScalarExact(
            "cardinality(map['foo', 1, 'bar', 2])", "2");
    }

    @Test public void testMemberOfOperator() {
        tester.setFor(
            SqlStdOperatorTable.memberOfOperator,
            VM_FENNEL,
            VM_JAVA);
        if (todo) {
            tester.checkBoolean("1 member of multiset[1]", Boolean.TRUE);
            tester.checkBoolean(
                "'2' member of multiset['1']",
                Boolean.FALSE);
            tester.checkBoolean(
                "cast(null as double) member of multiset[cast(null as double)]",
                Boolean.TRUE);
            tester.checkBoolean(
                "cast(null as double) member of multiset[1.1]",
                Boolean.FALSE);
            tester.checkBoolean(
                "1.1 member of multiset[cast(null as double)]",
                Boolean.FALSE);
        }
    }

    @Test public void testCollectFunc() {
        tester.setFor(
            SqlStdOperatorTable.collectFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testFusionFunc() {
        tester.setFor(SqlStdOperatorTable.fusionFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testExtractFunc() {
        tester.setFor(
            SqlStdOperatorTable.extractFunc,
            VM_FENNEL,
            VM_JAVA);

        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "extract(day from interval '2 3:4:5.678' day to second)",
            "2",
            "BIGINT NOT NULL");
        tester.checkScalar(
            "extract(hour from interval '2 3:4:5.678' day to second)",
            "3",
            "BIGINT NOT NULL");
        tester.checkScalar(
            "extract(minute from interval '2 3:4:5.678' day to second)",
            "4",
            "BIGINT NOT NULL");

        // TODO: Seconds should include precision
        tester.checkScalar(
            "extract(second from interval '2 3:4:5.678' day to second)",
            "5",
            "BIGINT NOT NULL");
        tester.checkScalar(
            "extract(year from interval '4-2' year to month)",
            "4",
            "BIGINT NOT NULL");
        tester.checkScalar(
            "extract(month from interval '4-2' year to month)",
            "2",
            "BIGINT NOT NULL");
        tester.checkNull(
            "extract(month from cast(null as interval year))");
    }

    @Test public void testArrayValueConstructor() {
        tester.setFor(SqlStdOperatorTable.arrayValueConstructor);
        tester.checkScalar(
            "Array['foo', 'bar']",
            "[foo, bar]",
            "CHAR(3) NOT NULL ARRAY NOT NULL");

        // empty array is illegal per SQL spec. presumably because one can't
        // infer type
        tester.checkFails(
            "^Array[]^", "Require at least 1 argument", false);
    }

    @Test public void testItemOp() {
        tester.setFor(SqlStdOperatorTable.itemOp);
        tester.checkScalar(
            "ARRAY ['foo', 'bar'][1]", "foo", "CHAR(3) NOT NULL");
        tester.checkScalar(
            "ARRAY ['foo', 'bar'][0]", null, "CHAR(3) NOT NULL");
        tester.checkScalar(
            "ARRAY ['foo', 'bar'][2]", "bar", "CHAR(3) NOT NULL");
        tester.checkScalar(
            "ARRAY ['foo', 'bar'][3]", null, "CHAR(3) NOT NULL");
        tester.checkNull(
            "ARRAY ['foo', 'bar'][1 + CAST(NULL AS INTEGER)]");
        tester.checkFails(
            "^ARRAY ['foo', 'bar']['baz']^",
            "Cannot apply 'ITEM' to arguments of type 'ITEM\\(<CHAR\\(3\\) ARRAY>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<VALUE>\\]",
            false);

        // Array of INTEGER NOT NULL is interesting because we might be tempted
        // to represent the result as Java "int".
        tester.checkScalar(
            "ARRAY [2, 4, 6][2]", "4", "INTEGER NOT NULL");
        tester.checkScalar(
            "ARRAY [2, 4, 6][4]", null, "INTEGER NOT NULL");

        // Map item
        tester.checkScalarExact(
            "map['foo', 3, 'bar', 7]['bar']", "INTEGER NOT NULL", "7");
        tester.checkScalarExact(
            "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['bar']", "INTEGER",
            "7");
        tester.checkScalarExact(
            "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['baz']", "INTEGER",
            null);
    }

    @Test public void testMapValueConstructor() {
        tester.setFor(SqlStdOperatorTable.mapValueConstructor, VM_JAVA);

        tester.checkFails(
            "^Map[]^", "Map requires at least 2 arguments", false);

        tester.checkFails(
            "^Map[1, 'x', 2]^",
            "Map requires an even number of arguments",
            false);

        tester.checkFails(
            "^map[1, 1, 2, 'x']^",
            "Parameters must be of the same type", false);
        tester.checkScalarExact(
            "map['washington', 1, 'obama', 44]",
            "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
            "{washington=1, obama=44}");
    }

    @Test public void testCeilFunc() {
        tester.setFor(SqlStdOperatorTable.ceilFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", 11, 0);
        tester.checkScalarApprox(
            "ceil(cast(-11.2e0 as real))",
            "REAL NOT NULL",
            -11,
            0);
        tester.checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
        tester.checkScalarExact(
            "ceil(1.3)", "DECIMAL(2, 0) NOT NULL", "2");
        tester.checkScalarExact(
            "ceil(-1.7)", "DECIMAL(2, 0) NOT NULL", "-1");
        tester.checkNull("ceiling(cast(null as decimal(2,0)))");
        tester.checkNull("ceiling(cast(null as double))");
    }

    @Test public void testCeilFuncInterval() {
        if (!INTERVAL) {
           return;
        }
        tester.checkScalar(
            "ceil(interval '3:4:5' hour to second)",
            "+4:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkScalar(
            "ceil(interval '-6.3' second)", "-6", "INTERVAL SECOND NOT NULL");
        tester.checkScalar(
            "ceil(interval '5-1' year to month)",
            "+6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkScalar(
            "ceil(interval '-5-1' year to month)",
            "-5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkNull(
            "ceil(cast(null as interval year))");
    }

    @Test public void testFloorFunc() {
        tester.setFor(SqlStdOperatorTable.floorFunc, VM_FENNEL);
        if (!enable) {
            return;
        }
        tester.checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", 2, 0);
        tester.checkScalarApprox(
            "floor(cast(-1.2e0 as real))",
            "REAL NOT NULL",
            -2,
            0);
        tester.checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
        tester.checkScalarExact(
            "floor(1.7)", "DECIMAL(2, 0) NOT NULL", "1");
        tester.checkScalarExact(
            "floor(-1.7)", "DECIMAL(2, 0) NOT NULL", "-2");
        tester.checkNull("floor(cast(null as decimal(2,0)))");
        tester.checkNull("floor(cast(null as real))");
    }

    @Test public void testFloorFuncInterval() {
        if (!INTERVAL) {
            return;
        }
        tester.checkScalar(
            "floor(interval '3:4:5' hour to second)",
            "+3:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        tester.checkScalar(
            "floor(interval '-6.3' second)", "-7", "INTERVAL SECOND NOT NULL");
        tester.checkScalar(
            "floor(interval '5-1' year to month)",
            "+5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkScalar(
            "floor(interval '-5-1' year to month)",
            "-6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        tester.checkNull(
            "floor(cast(null as interval year))");
    }

    @Test public void testDenseRankFunc() {
        tester.setFor(
            SqlStdOperatorTable.denseRankFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testPercentRankFunc() {
        tester.setFor(
            SqlStdOperatorTable.percentRankFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testRankFunc() {
        tester.setFor(SqlStdOperatorTable.rankFunc, VM_FENNEL, VM_JAVA);
    }

    @Test public void testCumeDistFunc() {
        tester.setFor(
            SqlStdOperatorTable.cumeDistFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testRowNumberFunc() {
        tester.setFor(
            SqlStdOperatorTable.rowNumberFunc,
            VM_FENNEL,
            VM_JAVA);
    }

    @Test public void testCountFunc() {
        tester.setFor(SqlStdOperatorTable.countOperator, VM_EXPAND);
        tester.checkType("count(*)", "BIGINT NOT NULL");
        tester.checkType("count('name')", "BIGINT NOT NULL");
        tester.checkType("count(1)", "BIGINT NOT NULL");
        tester.checkType("count(1.2)", "BIGINT NOT NULL");
        tester.checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
        tester.checkFails(
            "^COUNT()^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
            false);
        tester.checkFails(
            "^COUNT(1, 2)^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "1", "0" };
        tester.checkAgg(
            "COUNT(x)",
            values,
            3,
            (double) 0);
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            2,
            (double) 0);
        tester.checkAgg(
            "COUNT(DISTINCT x)",
            values,
            2,
            (double) 0);

        // string values -- note that empty string is not null
        final String [] stringValues =
        {
            "'a'", "CAST(NULL AS VARCHAR(1))", "''"
        };
        tester.checkAgg(
            "COUNT(*)",
            stringValues,
            3,
            (double) 0);
        tester.checkAgg(
            "COUNT(x)",
            stringValues,
            2,
            (double) 0);
        tester.checkAgg(
            "COUNT(DISTINCT x)",
            stringValues,
            2,
            (double) 0);
        tester.checkAgg(
            "COUNT(DISTINCT 123)",
            stringValues,
            1,
            (double) 0);
    }

    @Test public void testSumFunc() {
        tester.setFor(SqlStdOperatorTable.sumOperator, VM_EXPAND);
        tester.checkFails(
            "sum(^*^)", "Unknown identifier '\\*'", false);
        tester.checkFails(
            "^sum('name')^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("sum(1)", "INTEGER NOT NULL");
        tester.checkType("sum(1.2)", "DECIMAL(2, 1) NOT NULL");
        tester.checkType("sum(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        tester.checkFails(
            "^sum()^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
            false);
        tester.checkFails(
            "^sum(1, 2)^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
            false);
        tester.checkFails(
            "^sum(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        tester.checkAgg(
            "sum(x)",
            values,
            4,
            (double) 0);
        Object result1 = -3;
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "sum(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result1,
            (double) 0);
        Object result = -1;
        tester.checkAgg(
            "sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result,
            (double) 0);
        tester.checkAgg(
            "sum(DISTINCT x)",
            values,
            2,
            (double) 0);
    }

    @Test public void testAvgFunc() {
        tester.setFor(SqlStdOperatorTable.avgOperator, VM_EXPAND);
        tester.checkFails(
            "avg(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkFails(
            "^avg(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
        tester.checkType("AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        if (!enable) {
            return;
        }
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        tester.checkAgg(
            "AVG(x)", values, 2d, 0d);
        tester.checkAgg(
            "AVG(DISTINCT x)", values, 1.5d, 0d);
        Object result = -1;
        tester.checkAgg(
            "avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            result,
            0d);
    }

    @Test public void testStddevPopFunc() {
        tester.setFor(SqlStdOperatorTable.stddevPopOperator, VM_EXPAND);
        tester.checkFails(
            "stddev_pop(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkFails(
            "^stddev_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_POP' to arguments of type 'STDDEV_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_POP\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("stddev_pop(CAST(NULL AS INTEGER))", "INTEGER");
        tester.checkType(
            "stddev_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "stddev_pop(x)",
            values,
            1.414213562373095d, // verified on Oracle 10g
            0.000000000000001d);
        tester.checkAgg(
            "stddev_pop(DISTINCT x)", // Oracle does not allow distinct
            values,
            1.5d,
            0d);
        tester.checkAgg(
            "stddev_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            0,
            0d);
        // with one value
        tester.checkAgg(
            "stddev_pop(x)",
            new String[] {"5"},
            0,
            0d);
        // with zero values
        tester.checkAgg(
            "stddev_pop(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testStddevSampFunc() {
        tester.setFor(SqlStdOperatorTable.stddevSampOperator, VM_EXPAND);
        tester.checkFails(
            "stddev_samp(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkFails(
            "^stddev_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_SAMP' to arguments of type 'STDDEV_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_SAMP\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("stddev_samp(CAST(NULL AS INTEGER))", "INTEGER");
        tester.checkType(
            "stddev_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "stddev_samp(x)",
            values,
            1.732050807568877d, // verified on Oracle 10g
            0.000000000000001d);
        tester.checkAgg(
            "stddev_samp(DISTINCT x)", // Oracle does not allow distinct
            values,
            2.121320343559642d,
            0.000000000000001d);
        tester.checkAgg(
            "stddev_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            null,
            0d);
        // with one value
        tester.checkAgg(
            "stddev_samp(x)",
            new String[] {"5"},
            null,
            0d);
        // with zero values
        tester.checkAgg(
            "stddev_samp(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testVarPopFunc() {
        tester.setFor(SqlStdOperatorTable.varPopOperator, VM_EXPAND);
        tester.checkFails(
            "var_pop(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkFails(
            "^var_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_POP' to arguments of type 'VAR_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_POP\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("var_pop(CAST(NULL AS INTEGER))", "INTEGER");
        tester.checkType(
            "var_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "var_pop(x)",
            values,
            2d, // verified on Oracle 10g
            0d);
        tester.checkAgg(
            "var_pop(DISTINCT x)", // Oracle does not allow distinct
            values,
            2.25d,
            0.0001d);
        tester.checkAgg(
            "var_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            0,
            0d);
        // with one value
        tester.checkAgg(
            "var_pop(x)",
            new String[] {"5"},
            0,
            0d);
        // with zero values
        tester.checkAgg(
            "var_pop(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testVarSampFunc() {
        tester.setFor(SqlStdOperatorTable.varSampOperator, VM_EXPAND);
        tester.checkFails(
            "var_samp(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkFails(
            "^var_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_SAMP' to arguments of type 'VAR_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_SAMP\\(<NUMERIC>\\)'.*",
            false);
        tester.checkType("var_samp(CAST(NULL AS INTEGER))", "INTEGER");
        tester.checkType(
            "var_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String [] values = { "0", "CAST(null AS FLOAT)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "var_samp(x)", values, 3d, // verified on Oracle 10g
            0d);
        tester.checkAgg(
            "var_samp(DISTINCT x)", // Oracle does not allow distinct
            values,
            4.5d,
            0.0001d);
        tester.checkAgg(
            "var_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            null,
            0d);
        // with one value
        tester.checkAgg(
            "var_samp(x)",
            new String[] {"5"},
            null,
            0d);
        // with zero values
        tester.checkAgg(
            "var_samp(x)",
            new String[] {},
            null,
            0d);
    }

    @Test public void testMinFunc() {
        tester.setFor(SqlStdOperatorTable.minOperator, VM_EXPAND);
        tester.checkFails(
            "min(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkType("min(1)", "INTEGER NOT NULL");
        tester.checkType("min(1.2)", "DECIMAL(2, 1) NOT NULL");
        tester.checkType("min(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        tester.checkFails(
            "^min()^",
            "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
            false);
        tester.checkFails(
            "^min(1, 2)^",
            "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "min(x)",
            values,
            "0",
            0d);
        tester.checkAgg(
            "min(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        tester.checkAgg(
            "min(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        tester.checkAgg(
            "min(DISTINCT x)",
            values,
            "0",
            0d);
    }

    @Test public void testMaxFunc() {
        tester.setFor(SqlStdOperatorTable.maxOperator, VM_EXPAND);
        tester.checkFails(
            "max(^*^)",
            "Unknown identifier '\\*'",
            false);
        tester.checkType("max(1)", "INTEGER NOT NULL");
        tester.checkType("max(1.2)", "DECIMAL(2, 1) NOT NULL");
        tester.checkType("max(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        tester.checkFails(
            "^max()^",
            "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
            false);
        tester.checkFails(
            "^max(1, 2)^",
            "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
            false);
        final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
        if (!enable) {
            return;
        }
        tester.checkAgg(
            "max(x)",
            values,
            "2",
            0d);
        tester.checkAgg(
            "max(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        tester.checkAgg(
            "max(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values,
            "-1",
            0d);
        tester.checkAgg(
            "max(DISTINCT x)", values, "2", 0d);
    }

    @Test public void testLastValueFunc() {
        tester.setFor(SqlStdOperatorTable.lastValueOperator, VM_EXPAND);
        final String [] values = { "0", "CAST(null AS INTEGER)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkWinAgg(
            "last_value(x)",
            values,
            "ROWS 3 PRECEDING",
            "INTEGER",
            Arrays.asList("3", "0"),
            0d);
        final String [] values2 = { "1.6", "1.2" };
        tester.checkWinAgg(
            "last_value(x)",
            values2,
            "ROWS 3 PRECEDING",
            "DECIMAL(2, 1) NOT NULL",
            Arrays.asList("1.6", "1.2"),
            0d);
        final String [] values3 = { "'foo'", "'bar'", "'name'" };
        tester.checkWinAgg(
            "last_value(x)",
            values3,
            "ROWS 3 PRECEDING",
            "CHAR(4) NOT NULL",
            Arrays.asList("foo ", "bar ", "name"),
            0d);
    }

    @Test public void testFirstValueFunc() {
        tester.setFor(SqlStdOperatorTable.firstValueOperator, VM_EXPAND);
        final String [] values = { "0", "CAST(null AS INTEGER)", "3", "3" };
        if (!enable) {
            return;
        }
        tester.checkWinAgg(
            "first_value(x)",
            values,
            "ROWS 3 PRECEDING",
            "INTEGER",
            Arrays.asList("0"),
            0d);
        final String [] values2 = { "1.6", "1.2" };
        tester.checkWinAgg(
            "first_value(x)",
            values2,
            "ROWS 3 PRECEDING",
            "DECIMAL(2, 1) NOT NULL",
            Arrays.asList("1.6"),
            0d);
        final String [] values3 = { "'foo'", "'bar'", "'name'" };
        tester.checkWinAgg(
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

    /** Creates a {@link org.eigenbase.sql.test.SqlTester} based on a JDBC
     * connection. */
    public static SqlTester tester(Connection connection) {
        return new TesterImpl(connection);
    }

    /** Implementation of {@link org.eigenbase.sql.test.SqlTester} based on a
     * JDBC connection. */
    protected static class TesterImpl extends SqlValidatorTestCase.TesterImpl {
        final Connection connection;

        public TesterImpl(Connection connection) {
            super(SqlConformance.Default);
            this.connection = connection;
        }

        @Override public void check(
            String query,
            TypeChecker typeChecker,
            ResultChecker resultChecker)
        {
            super.check(
                query,
                typeChecker,
                resultChecker);
            Statement statement = null;
            try {
                statement = connection.createStatement();
                final ResultSet resultSet =
                    statement.executeQuery(query);
                resultChecker.checkResult(resultSet);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }

        public void close() {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

// End SqlOperatorBaseTest.java
