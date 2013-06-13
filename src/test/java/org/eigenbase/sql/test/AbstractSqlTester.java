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

import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Pattern;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import org.junit.Assert;

import static org.junit.Assert.*;


/**
 * Abstract implementation of {@link SqlTester}. A derived class only needs to
 * implement {@link #check}, {@link #checkColumnType} and {@link #checkFails}.
 */
public abstract class AbstractSqlTester
    implements SqlTester
{
    //~ Static fields/initializers ---------------------------------------------

    // ~ Constants ------------------------------------------------------------
    public static final TypeChecker IntegerTypeChecker =
        new SqlTypeChecker(SqlTypeName.INTEGER);

    public static final TypeChecker BooleanTypeChecker =
        new SqlTypeChecker(SqlTypeName.BOOLEAN);

    /**
     * Checker which allows any type.
     */
    public static final TypeChecker AnyTypeChecker =
        new TypeChecker() {
            public void checkType(RelDataType type)
            {
            }
        };

    //~ Instance fields --------------------------------------------------------

    // ~ Members --------------------------------------------------------------

    private SqlOperator operator;

    //~ Methods ----------------------------------------------------------------

    public void setFor(SqlOperator operator, VmName ... unimplementedVmNames)
    {
        if ((operator != null) && (this.operator != null)) {
            throw new AssertionError("isFor() called twice");
        }
        this.operator = operator;
    }

    public void checkAgg(
        String expr,
        String [] inputValues,
        Object result,
        double delta)
    {
        String query = generateAggQuery(expr, inputValues);
        check(query, AnyTypeChecker, result, delta);
    }

    public void checkWinAgg(
        String expr,
        String [] inputValues,
        String windowSpec,
        String type,
        Object result,
        double delta)
    {
        // Windowed aggregation is not implemented in eigenbase. We cannot
        // evaluate the query to check results. The best we can do is to check
        // the type.
        String query = generateWinAggQuery(expr, windowSpec, inputValues);
        checkColumnType(query, type);
    }

    /**
     * Helper function to get the string representation of a RelDataType
     * (include precision/scale but no charset or collation)
     *
     * @param sqlType Type
     *
     * @return String representation of type
     */
    public static String getTypeString(RelDataType sqlType)
    {
        switch (sqlType.getSqlTypeName()) {
        case VARCHAR:
            String actual = "VARCHAR(" + sqlType.getPrecision() + ")";
            return sqlType.isNullable() ? actual : (actual + " NOT NULL");
        case CHAR:
            actual = "CHAR(" + sqlType.getPrecision() + ")";
            return sqlType.isNullable() ? actual : (actual + " NOT NULL");
        default:

            // Get rid of the verbose charset/collation stuff.
            // TODO: There's probably a better way to do this.
            final String s = sqlType.getFullTypeString();
            return s.replace(
                " CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"",
                "");
        }
    }

    public static String generateAggQuery(String expr, String [] inputValues)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ").append(expr).append(" FROM ");
        if (inputValues.length == 0) {
            buf.append("(VALUES 1) AS t(x) WHERE false");
        } else {
            buf.append("(");
            for (int i = 0; i < inputValues.length; i++) {
                if (i > 0) {
                    buf.append(" UNION ALL ");
                }
                buf.append("SELECT ");
                String inputValue = inputValues[i];
                buf.append(inputValue).append(" AS x FROM (VALUES (1))");
            }
            buf.append(")");
        }
        return buf.toString();
    }

    public static String generateWinAggQuery(
        String expr,
        String windowSpec,
        String [] inputValues)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ").append(expr).append(" OVER (").append(windowSpec)
        .append(") FROM (");
        for (int i = 0; i < inputValues.length; i++) {
            if (i > 0) {
                buf.append(" UNION ALL ");
            }
            buf.append("SELECT ");
            String inputValue = inputValues[i];
            buf.append(inputValue).append(" AS x FROM (VALUES (1))");
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * Helper method which converts a scalar expression into a SQL query.
     *
     * <p>By default, "expr" becomes "VALUES (expr)". Derived classes may
     * override.
     *
     * @param expression Expression
     *
     * @return Query that returns expression
     */
    protected String buildQuery(String expression)
    {
        return "values (" + expression + ")";
    }

    public void checkType(
        String expression,
        String type)
    {
        checkColumnType(buildQuery(expression), type);
    }

    public void checkScalarExact(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, IntegerTypeChecker, result, 0);
    }

    public void checkScalarExact(
        String expression,
        String expectedType,
        String result)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(sql, typeChecker, result, 0);
    }

    public void checkScalarApprox(
        String expression,
        String expectedType,
        double expectedResult,
        double delta)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(
            sql,
            typeChecker,
            expectedResult,
            delta);
    }

    public void checkBoolean(
        String expression,
        Boolean result)
    {
        String sql = buildQuery(expression);
        if (null == result) {
            checkNull(expression);
        } else {
            check(
                sql,
                BooleanTypeChecker,
                result.toString(),
                0);
        }
    }

    public void checkString(
        String expression,
        String result,
        String expectedType)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(sql, typeChecker, result, 0);
    }

    public void checkNull(String expression)
    {
        String sql = buildQuery(expression);
        check(sql, AnyTypeChecker, null, 0);
    }

    public void checkScalar(
        String expression,
        Object result,
        String resultType)
    {
        checkType(expression, resultType);
        check(
            buildQuery(expression),
            AnyTypeChecker,
            result,
            0);
    }

    /**
     * Returns the operator this test is for. Throws if no operator has been
     * set.
     *
     * @return the operator this test is for, never null
     */
    protected SqlOperator getFor()
    {
        assertNotNull("Must call setFor()", operator);
        return operator;
    }

    /**
     * Compares the first column of a result set against a String-valued
     * reference set, disregarding order entirely.
     *
     * @param resultSet Result set
     * @param refSet Expected results
     *
     * @throws Exception .
     */
    public static void compareResultSet(
        ResultSet resultSet,
        Set<String> refSet)
        throws Exception
    {
        Set<String> actualSet = new HashSet<String>();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        Assert.assertEquals(refSet, actualSet);
    }

    /**
     * Compares the first column of a result set against a pattern. The result
     * set must return exactly one row.
     *
     * @param resultSet Result set
     * @param pattern Expected pattern
     */
    public static void compareResultSetWithPattern(
        ResultSet resultSet,
        Pattern pattern)
        throws Exception
    {
        if (!resultSet.next()) {
            Assert.fail("Query returned 0 rows, expected 1");
        }
        String actual = resultSet.getString(1);
        if (resultSet.next()) {
            Assert.fail("Query returned 2 or more rows, expected 1");
        }
        if (!pattern.matcher(actual).matches()) {
            Assert.fail(
                "Query returned '" + actual + "', expected '"
                + pattern.pattern() + "'");
        }
    }

    /**
     * Compares the first column of a result set against a numeric result,
     * within a given tolerance. The result set must return exactly one row.
     *
     * @param resultSet Result set
     * @param expected Expected result
     * @param delta Tolerance
     */
    public static void compareResultSetWithDelta(
        ResultSet resultSet,
        double expected,
        double delta)
        throws Exception
    {
        if (!resultSet.next()) {
            Assert.fail("Query returned 0 rows, expected 1");
        }
        double actual = resultSet.getDouble(1);
        if (resultSet.next()) {
            Assert.fail("Query returned 2 or more rows, expected 1");
        }
        if ((actual < (expected - delta)) || (actual > (expected + delta))) {
            Assert.fail(
                "Query returned " + actual
                + ", expected " + expected
                + ((delta == 0) ? "" : ("+/-" + delta)));
        }
    }

    /**
     * Compares the first column of a result set against a String-valued
     * reference set, taking order into account.
     *
     * @param resultSet Result set
     * @param refList Expected results
     *
     * @throws Exception .
     */
    public static void compareResultList(
        ResultSet resultSet,
        List<String> refList)
        throws Exception
    {
        List<String> actualSet = new ArrayList<String>();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        Assert.assertEquals(refList, actualSet);
    }

    /**
     * Compares the columns of a result set against several String-valued
     * reference lists, taking order into account.
     *
     * @param resultSet Result set
     * @param refLists vararg of List<String>. The first list is compared to the
     * first column, the second list to the second column and so on
     */
    public static void compareResultLists(
        ResultSet resultSet,
        List<String>... refLists)
        throws Exception
    {
        int numExpectedColumns = refLists.length;

        Assert.assertTrue(numExpectedColumns > 0);

        Assert.assertTrue(
            resultSet.getMetaData().getColumnCount() >= numExpectedColumns);

        int numExpectedRows = -1;

        List<List<String>> actualLists = new ArrayList<List<String>>();
        for (int i = 0; i < numExpectedColumns; i++) {
            actualLists.add(new ArrayList<String>());

            if (i == 0) {
                numExpectedRows = refLists[i].size();
            } else {
                Assert.assertEquals(
                    "num rows differ across ref lists",
                    numExpectedRows,
                    refLists[i].size());
            }
        }

        while (resultSet.next()) {
            for (int i = 0; i < numExpectedColumns; i++) {
                String s = resultSet.getString(i + 1);

                actualLists.get(i).add(s);
            }
        }
        resultSet.close();

        for (int i = 0; i < numExpectedColumns; i++) {
            Assert.assertEquals(
                "column mismatch in column " + (i + 1),
                refLists[i],
                actualLists.get(i));
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Checks that a type matches a given SQL type. Does not care about
     * nullability.
     */
    private static class SqlTypeChecker
        implements TypeChecker
    {
        private final SqlTypeName typeName;

        SqlTypeChecker(SqlTypeName typeName)
        {
            this.typeName = typeName;
        }

        public void checkType(RelDataType type)
        {
            assertEquals(
                typeName.toString(),
                type.toString());
        }
    }

    /**
     * Type checker which compares types to a specified string.
     *
     * <p>The string contains "NOT NULL" constraints, but does not contain
     * collations and charsets. For example,
     *
     * <ul>
     * <li><code>INTEGER NOT NULL</code></li>
     * <li><code>BOOLEAN</code></li>
     * <li><code>DOUBLE NOT NULL MULTISET NOT NULL</code></li>
     * <li><code>CHAR(3) NOT NULL</code></li>
     * <li><code>RecordType(INTEGER X, VARCHAR(10) Y)</code></li>
     * </ul>
     */
    public static class StringTypeChecker
        implements TypeChecker
    {
        private final String expected;

        public StringTypeChecker(String expected)
        {
            this.expected = expected;
        }

        public void checkType(RelDataType type)
        {
            String actual = getTypeString(type);
            assertEquals(expected, actual);
        }
    }

    public static ResultChecker createChecker(Object result, double delta)
    {
        if (result instanceof Pattern) {
            return new PatternResultChecker((Pattern) result);
        } else if (delta != 0) {
            Assert.assertTrue(result instanceof Number);
            return new ApproximateResultChecker((Number) result, delta);
        } else {
            Set<String> refSet = new HashSet<String>();
            if (result == null) {
                refSet.add(null);
            } else if (result instanceof Collection) {
                refSet.addAll((Collection<String>) result);
            } else {
                refSet.add(result.toString());
            }
            return new RefSetResultChecker(refSet);
        }
    }

    /**
     * Result checker that checks a result against a regular expression.
     */
    public static class PatternResultChecker implements ResultChecker
    {
        private final Pattern pattern;

        public PatternResultChecker(Pattern pattern)
        {
            this.pattern = pattern;
        }

        public void checkResult(ResultSet resultSet) throws Exception
        {
            compareResultSetWithPattern(resultSet, pattern);
        }
    }

    /**
     * Result checker that checks a result against an expected value. A delta
     * value is used for approximate values (double and float).
     */
    public static class ApproximateResultChecker implements ResultChecker
    {
        private final Number expected;
        private final double delta;

        public ApproximateResultChecker(Number expected, double delta)
        {
            this.expected = expected;
            this.delta = delta;
        }

        public void checkResult(ResultSet resultSet) throws Exception
        {
            compareResultSetWithDelta(
                resultSet,
                expected.doubleValue(),
                delta);
        }
    }

    /**
     * Result checker that checks a result against a list of expected strings.
     */
    public static class RefSetResultChecker implements ResultChecker
    {
        private final Set<String> expected;

        public RefSetResultChecker(Set<String> expected)
        {
            this.expected = expected;
        }

        public void checkResult(ResultSet resultSet) throws Exception
        {
            compareResultSet(resultSet, expected);
        }
    }
}

// End AbstractSqlTester.java
