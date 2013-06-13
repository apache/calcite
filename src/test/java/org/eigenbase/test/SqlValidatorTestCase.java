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
package org.eigenbase.test;

import java.nio.charset.*;

import java.util.List;
import java.util.regex.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import static org.junit.Assert.*;


/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add <code>
 * testXxx()</code> methods, to test more functionality.
 *
 * <p>Second, it can override the {@link #getTester} method to return a
 * different implementation of the {@link Tester} object. This encapsulates the
 * differences between test environments, for example, which SQL parser or
 * validator to use.</p>
 */
public class SqlValidatorTestCase {
    //~ Static fields/initializers ---------------------------------------------

    protected static final String NL = System.getProperty("line.separator");

    private static final Pattern lineColPattern =
        Pattern.compile("At line ([0-9]+), column ([0-9]+)");

    private static final Pattern lineColTwicePattern =
        Pattern.compile(
            "(?s)From line ([0-9]+), column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)");

    //~ Instance fields --------------------------------------------------------

    protected final Tester tester;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a test case.
     */
    public SqlValidatorTestCase(SqlConformance conformance) {
        if (conformance == null) {
            conformance = SqlConformance.Default;
        }
        this.tester = getTester(conformance);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a tester. Derived classes should override this method to run the
     * same set of tests in a different testing environment.
     *
     * @param conformance Language version tests should check compatibility with
     */
    public Tester getTester(SqlConformance conformance)
    {
        return new TesterImpl(conformance);
    }

    public void check(String sql)
    {
        tester.assertExceptionIsThrown(sql, null);
    }

    public void checkExp(String sql)
    {
        tester.assertExceptionIsThrown(
            TesterImpl.buildQuery(sql),
            null);
    }

    /**
     * Checks that a SQL query gives a particular error, or succeeds if {@code
     * expected} is null.
     */
    public final void checkFails(
        String sql,
        String expected)
    {
        tester.assertExceptionIsThrown(sql, expected);
    }

    /**
     * Checks that a SQL expression gives a particular error.
     */
    public final void checkExpFails(
        String sql,
        String expected)
    {
        tester.assertExceptionIsThrown(
            TesterImpl.buildQuery(sql),
            expected);
    }

    /**
     * Checks that a SQL expression gives a particular error, and that the
     * location of the error is the whole expression.
     */
    public final void checkWholeExpFails(
        String sql,
        String expected)
    {
        assert sql.indexOf('^') < 0;
        checkExpFails("^" + sql + "^", expected);
    }

    public void checkExpType(
        String sql,
        String expected)
    {
        checkColumnType(
            TesterImpl.buildQuery(sql),
            expected);
    }

    /**
     * Checks that a query returns a single column, and that the column has the
     * expected type. For example,
     *
     * <blockquote><code>checkColumnType("SELECT empno FROM Emp", "INTEGER NOT
     * NULL");</code></blockquote>
     *
     * @param sql Query
     * @param expected Expected type, including nullability
     */
    public void checkColumnType(
        String sql,
        String expected)
    {
        tester.checkColumnType(sql, expected);
    }

    /**
     * Checks that a query returns a row of the expected type. For example,
     *
     * <blockquote><code>checkResultType("select empno, name from emp","{EMPNO
     * INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}");</code></blockquote>
     *
     * @param sql Query
     * @param expected Expected row type
     */
    public void checkResultType(
        String sql,
        String expected)
    {
        tester.checkResultType(sql, expected);
    }

    /**
     * Checks that the first column returned by a query has the expected type.
     * For example,
     *
     * <blockquote><code>checkQueryType("SELECT empno FROM Emp", "INTEGER NOT
     * NULL");</code></blockquote>
     *
     * @param sql Query
     * @param expected Expected type, including nullability
     */
    public void checkIntervalConv(
        String sql,
        String expected)
    {
        tester.checkIntervalConv(
            TesterImpl.buildQuery(sql),
            expected);
    }

    protected final void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern)
    {
        tester.assertExceptionIsThrown(sql, expectedMsgPattern);
    }

    public void checkCharset(
        String sql,
        Charset expectedCharset)
    {
        tester.checkCharset(sql, expectedCharset);
    }

    public void checkCollation(
        String sql,
        String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility)
    {
        tester.checkCollation(sql, expectedCollationName, expectedCoercibility);
    }

    /**
     * Checks whether an exception matches the expected pattern. If <code>
     * sap</code> contains an error location, checks this too.
     *
     * @param ex Exception thrown
     * @param expectedMsgPattern Expected pattern
     * @param sap Query and (optional) position in query
     */
    public static void checkEx(
        Throwable ex,
        String expectedMsgPattern,
        SqlParserUtil.StringAndPos sap)
    {
        if (null == ex) {
            if (expectedMsgPattern == null) {
                // No error expected, and no error happened.
                return;
            } else {
                throw new AssertionError(
                    "Expected query to throw exception, but it did not; "
                    + "query [" + sap.sql
                    + "]; expected [" + expectedMsgPattern + "]");
            }
        }
        Throwable actualException = ex;
        String actualMessage = actualException.getMessage();
        int actualLine = -1;
        int actualColumn = -1;
        int actualEndLine = 100;
        int actualEndColumn = 99;

        // Search for an EigenbaseContextException somewhere in the stack.
        EigenbaseContextException ece = null;
        for (Throwable x = ex; x != null; x = x.getCause()) {
            if (x instanceof EigenbaseContextException) {
                ece = (EigenbaseContextException) x;
                break;
            }
            if (x.getCause() == x) {
                break;
            }
        }

        // Search for a SqlParseException -- with its position set -- somewhere
        // in the stack.
        SqlParseException spe = null;
        for (Throwable x = ex; x != null; x = x.getCause()) {
            if ((x instanceof SqlParseException)
                && (((SqlParseException) x).getPos() != null))
            {
                spe = (SqlParseException) x;
                break;
            }
            if (x.getCause() == x) {
                break;
            }
        }

        if (ece != null) {
            actualLine = ece.getPosLine();
            actualColumn = ece.getPosColumn();
            actualEndLine = ece.getEndPosLine();
            actualEndColumn = ece.getEndPosColumn();
            if (ece.getCause() != null) {
                actualException = ece.getCause();
                actualMessage = actualException.getMessage();
            }
        } else if (spe != null) {
            actualLine = spe.getPos().getLineNum();
            actualColumn = spe.getPos().getColumnNum();
            actualEndLine = spe.getPos().getEndLineNum();
            actualEndColumn = spe.getPos().getEndColumnNum();
            if (spe.getCause() != null) {
                actualException = spe.getCause();
                actualMessage = actualException.getMessage();
            }
        } else {
            final String message = ex.getMessage();
            if (message != null) {
                Matcher matcher = lineColTwicePattern.matcher(message);
                if (matcher.matches()) {
                    actualLine = Integer.parseInt(matcher.group(1));
                    actualColumn = Integer.parseInt(matcher.group(2));
                    actualEndLine = Integer.parseInt(matcher.group(3));
                    actualEndColumn = Integer.parseInt(matcher.group(4));
                    actualMessage = matcher.group(5);
                } else {
                    matcher = lineColPattern.matcher(message);
                    if (matcher.matches()) {
                        actualLine = Integer.parseInt(matcher.group(1));
                        actualColumn = Integer.parseInt(matcher.group(2));
                    }
                }
            }
        }

        if (null == expectedMsgPattern) {
            if (null != actualException) {
                actualException.printStackTrace();
                fail(
                    "Validator threw unexpected exception"
                    + "; query [" + sap.sql
                    + "]; exception [" + actualMessage
                    + "]; pos [line " + actualLine
                    + " col " + actualColumn
                    + " thru line " + actualLine
                    + " col " + actualColumn + "]");
            }
        } else if (null != expectedMsgPattern) {
            if (null == actualException) {
                fail(
                    "Expected validator to throw "
                    + "exception, but it did not; query [" + sap.sql
                    + "]; expected [" + expectedMsgPattern + "]");
            } else {
                String sqlWithCarets;
                if ((actualColumn <= 0)
                    || (actualLine <= 0)
                    || (actualEndColumn <= 0)
                    || (actualEndLine <= 0))
                {
                    if (sap.pos != null) {
                        throw new AssertionError(
                            "Expected error to have position,"
                            + " but actual error did not: "
                            + " actual pos [line " + actualLine
                            + " col " + actualColumn
                            + " thru line " + actualEndLine
                            + " col " + actualEndColumn + "]");
                    }
                    sqlWithCarets = sap.sql;
                } else {
                    sqlWithCarets =
                        SqlParserUtil.addCarets(
                            sap.sql,
                            actualLine,
                            actualColumn,
                            actualEndLine,
                            actualEndColumn + 1);
                    if (sap.pos == null) {
                        throw new AssertionError(
                            "Actual error had a position, but expected error"
                            + " did not. Add error position carets to sql:\n"
                            + sqlWithCarets);
                    }
                }
                if ((actualMessage == null)
                    || !actualMessage.matches(expectedMsgPattern))
                {
                    actualException.printStackTrace();
                    final String actualJavaRegexp =
                        (actualMessage == null) ? "null"
                        : TestUtil.quoteForJava(
                            TestUtil.quotePattern(actualMessage));
                    fail(
                        "Validator threw different "
                        + "exception than expected; query [" + sap.sql
                        + "];" + NL
                        + " expected pattern [" + expectedMsgPattern
                        + "];" + NL
                        + " actual [" + actualMessage
                        + "];" + NL
                        + " actual as java regexp [" + actualJavaRegexp
                        + "]; pos [" + actualLine
                        + " col " + actualColumn
                        + " thru line " + actualEndLine
                        + " col " + actualEndColumn
                        + "]; sql [" + sqlWithCarets + "]");
                } else if (
                    (sap.pos != null)
                    && ((actualLine != sap.pos.getLineNum())
                        || (actualColumn != sap.pos.getColumnNum())
                        || (actualEndLine != sap.pos.getEndLineNum())
                        || (actualEndColumn != sap.pos.getEndColumnNum())))
                {
                    fail(
                        "Validator threw expected "
                        + "exception [" + actualMessage
                        + "];\nbut at pos [line " + actualLine
                        + " col " + actualColumn
                        + " thru line " + actualEndLine
                        + " col " + actualEndColumn
                        + "];\nsql [" + sqlWithCarets + "]");
                }
            }
        }
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Encapsulates differences between test environments, for example, which
     * SQL parser or validator to use.
     *
     * <p>It contains a mock schema with <code>EMP</code> and <code>DEPT</code>
     * tables, which can run without having to start up Farrago.
     */
    public interface Tester
    {
        SqlNode parseQuery(String sql)
            throws SqlParseException;

        SqlNode parseAndValidate(SqlValidator validator, String sql);

        SqlValidator getValidator();

        /**
         * Checks that a query is valid, or, if invalid, throws the right
         * message at the right location.
         *
         * <p>If <code>expectedMsgPattern</code> is null, the query must
         * succeed.
         *
         * <p>If <code>expectedMsgPattern</code> is not null, the query must
         * fail, and give an error location of (expectedLine, expectedColumn)
         * through (expectedEndLine, expectedEndColumn).
         *
         * @param sql SQL statement
         * @param expectedMsgPattern If this parameter is null the query must be
         * valid for the test to pass; If this parameter is not null the query
         * must be malformed and the message given must match the pattern
         */
        void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern);

        /**
         * Returns the data type of the sole column of a SQL query.
         *
         * <p>For example, <code>getResultType("VALUES (1")</code> returns
         * <code>INTEGER</code>.
         *
         * <p>Fails if query returns more than one column.
         *
         * @see #getResultType(String)
         */
        RelDataType getColumnType(String sql);

        /**
         * Returns the data type of the row returned by a SQL query.
         *
         * <p>For example, <code>getResultType("VALUES (1, 'foo')")</code>
         * returns <code>RecordType(INTEGER EXPR$0, CHAR(3) EXPR#1)</code>.
         */
        RelDataType getResultType(String sql);

        void checkCollation(
            String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility);

        void checkCharset(
            String sql,
            Charset expectedCharset);

        /**
         * Checks that a query returns one column of an expected type. For
         * example, <code>checkType("VALUES (1 + 2)", "INTEGER NOT
         * NULL")</code>.
         */
        void checkColumnType(
            String sql,
            String expected);

        /**
         * Given a SQL query, returns a list of the origins of each result
         * field.
         *
         * @param sql SQL query
         * @param fieldOriginList Field origin list, e.g.
         *   "{(CATALOG.SALES.EMP.EMPNO, null)}"
         */
        void checkFieldOrigin(String sql, String fieldOriginList);

        /**
         * Checks that a query gets rewritten to an expected form.
         *
         * @param validator validator to use; null for default
         * @param query query to test
         * @param expectedRewrite expected SQL text after rewrite and unparse
         */
        void checkRewrite(
            SqlValidator validator,
            String query,
            String expectedRewrite);

        /**
         * Checks that a query returns one column of an expected type. For
         * example, <code>checkType("select empno, name from emp""{EMPNO INTEGER
         * NOT NULL, NAME VARCHAR(10) NOT NULL}")</code>.
         */
        void checkResultType(
            String sql,
            String expected);

        /**
         * Checks if the interval value conversion to milliseconds is valid. For
         * example, <code>checkIntervalConv(VALUES (INTERVAL '1' Minute),
         * "60000")</code>.
         */
        void checkIntervalConv(
            String sql,
            String expected);

        /**
         * Given a SQL query, returns the monotonicity of the first item in the
         * SELECT clause.
         *
         * @param sql SQL query
         *
         * @return Monotonicity
         */
        SqlMonotonicity getMonotonicity(String sql);

        SqlConformance getConformance();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link org.eigenbase.test.SqlValidatorTestCase.Tester}
     * which talks to a mock catalog.
     *
     * <p>It is also a pure-Java implementation of the {@link SqlTester} used by
     * {@link SqlOperatorBaseTest}. It can parse and validate queries, but it
     * does not invoke Farrago, so it is very fast but cannot execute functions.
     */
    public static class TesterImpl
        implements Tester,
            SqlTester
    {
        protected final SqlOperatorTable opTab;
        protected final SqlConformance conformance;

        public TesterImpl(SqlConformance conformance)
        {
            assert conformance != null;
            this.conformance = conformance;
            this.opTab = createOperatorTable();
        }

        public SqlConformance getConformance()
        {
            return conformance;
        }

        protected SqlOperatorTable createOperatorTable()
        {
            MockSqlOperatorTable opTab =
                new MockSqlOperatorTable(SqlStdOperatorTable.instance());
            MockSqlOperatorTable.addRamp(opTab);
            return opTab;
        }

        public SqlValidator getValidator()
        {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
            return SqlValidatorUtil.newValidator(
                opTab,
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        public void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern)
        {
            SqlValidator validator;
            SqlNode sqlNode;
            SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
            try {
                sqlNode = parseQuery(sap.sql);
                validator = getValidator();
            } catch (SqlParseException e) {
                String errMessage = e.getMessage();
                if (expectedMsgPattern == null) {
                    e.printStackTrace();
                    throw new AssertionError(
                        "Error while parsing query [" + sap.sql + "]");
                } else if (
                    (null == errMessage)
                    || !errMessage.matches(expectedMsgPattern))
                {
                    e.printStackTrace();
                    throw new AssertionError(
                        "Error did not match expected ["
                        + expectedMsgPattern + "] while parsing query ["
                        + sap.sql + "]");
                }
                return;
            } catch (Throwable e) {
                e.printStackTrace();
                throw new AssertionError(
                    "Error while parsing query [" + sap.sql + "]");
            }

            Throwable thrown = null;
            try {
                validator.validate(sqlNode);
            } catch (Throwable ex) {
                thrown = ex;
            }

            checkEx(thrown, expectedMsgPattern, sap);
        }

        public RelDataType getColumnType(String sql)
        {
            RelDataType rowType = getResultType(sql);
            final RelDataTypeField [] fields = rowType.getFields();
            assertEquals("expected query to return 1 field", 1, fields.length);
            RelDataType actualType = fields[0].getType();
            return actualType;
        }

        public RelDataType getResultType(String sql)
        {
            SqlValidator validator = getValidator();
            SqlNode n = parseAndValidate(validator, sql);

            RelDataType rowType = validator.getValidatedNodeType(n);
            return rowType;
        }

        public SqlNode parseAndValidate(SqlValidator validator, String sql)
        {
            if (validator == null) {
                validator = getValidator();
            }
            SqlNode sqlNode;
            try {
                sqlNode = parseQuery(sql);
            } catch (SqlParseException e) {
                throw new RuntimeException(
                    "Error while parsing query [" + sql + "]", e);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new AssertionError(
                    "Error while parsing query [" + sql + "]");
            }
            return validator.validate(sqlNode);
        }

        public SqlNode parseQuery(String sql)
            throws SqlParseException
        {
            SqlParser parser = new SqlParser(sql);
            return parser.parseQuery();
        }

        public void checkColumnType(String sql, String expected)
        {
            RelDataType actualType = getColumnType(sql);
            String actual = AbstractSqlTester.getTypeString(actualType);
            assertEquals(expected, actual);
        }

        public void checkFieldOrigin(String sql, String fieldOriginList)
        {
            SqlValidator validator = getValidator();
            SqlNode n = parseAndValidate(validator, sql);
            final List<List<String>> list = validator.getFieldOrigins(n);
            final StringBuilder buf = new StringBuilder("{");
            int i = 0;
            for (List<String> strings : list) {
                if (i++ > 0) {
                    buf.append(", ");
                }
                if (strings == null) {
                    buf.append("null");
                } else {
                    int j = 0;
                    for (String s : strings) {
                        if (j++ > 0) {
                            buf.append('.');
                        }
                        buf.append(s);
                    }
                }
            }
            buf.append("}");
            assertEquals(fieldOriginList, buf.toString());
        }

        public void checkResultType(String sql, String expected)
        {
            RelDataType actualType = getResultType(sql);
            String actual = AbstractSqlTester.getTypeString(actualType);
            assertEquals(expected, actual);
        }

        public void checkIntervalConv(String sql, String expected)
        {
            SqlValidator validator = getValidator();
            final SqlCall n = (SqlCall) parseAndValidate(validator, sql);

            SqlNode node = null;
            for (int i = 0; i < n.getOperands().length; i++) {
                node = n.getOperands()[i];
                if (node instanceof SqlCall) {
                    if (node.getKind() == SqlKind.AS) {
                        node = ((SqlCall) node).operands[0];
                    }
                    node = ((SqlCall) node).getOperands()[0];
                    break;
                }
            }

            SqlIntervalLiteral.IntervalValue interval =
                (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) node)
                .getValue();
            long l =
                interval.getIntervalQualifier().isYearMonth()
                ? SqlParserUtil.intervalToMonths(interval)
                : SqlParserUtil.intervalToMillis(interval);
            String actual = l + "";
            assertEquals(expected, actual);
        }

        public void checkType(String expression, String type)
        {
            checkColumnType(
                buildQuery(expression),
                type);
        }

        public void checkCollation(
            String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility)
        {
            RelDataType actualType = getColumnType(buildQuery(sql));
            SqlCollation collation = actualType.getCollation();

            assertEquals(expectedCollationName, collation.getCollationName());
            assertEquals(expectedCoercibility, collation.getCoercibility());
        }

        public void checkCharset(
            String sql,
            Charset expectedCharset)
        {
            RelDataType actualType = getColumnType(buildQuery(sql));
            Charset actualCharset = actualType.getCharset();

            if (!expectedCharset.equals(actualCharset)) {
                fail(
                    NL + "Expected=" + expectedCharset.name() + NL
                    + "  actual=" + actualCharset.name());
            }
        }

        // SqlTester methods

        public void setFor(
            SqlOperator operator,
            VmName ... unimplementedVmNames)
        {
            // do nothing
        }

        public void checkAgg(
            String expr,
            String [] inputValues,
            Object result,
            double delta)
        {
            String query =
                AbstractSqlTester.generateAggQuery(expr, inputValues);
            check(query, AbstractSqlTester.AnyTypeChecker, result, delta);
        }

        public void checkWinAgg(
            String expr,
            String [] inputValues,
            String windowSpec,
            String type,
            Object result,
            double delta)
        {
            String query =
                AbstractSqlTester.generateWinAggQuery(
                    expr,
                    windowSpec,
                    inputValues);
            check(query, AbstractSqlTester.AnyTypeChecker, result, delta);
        }

        public void checkScalar(
            String expression,
            Object result,
            String resultType)
        {
            checkType(expression, resultType);
            check(
                buildQuery(expression),
                AbstractSqlTester.AnyTypeChecker,
                result,
                0);
        }

        public void checkScalarExact(
            String expression,
            String result)
        {
            String sql = buildQuery(expression);
            check(sql, AbstractSqlTester.IntegerTypeChecker, result, 0);
        }

        public void checkScalarExact(
            String expression,
            String expectedType,
            String result)
        {
            String sql = buildQuery(expression);
            TypeChecker typeChecker =
                new AbstractSqlTester.StringTypeChecker(expectedType);
            check(sql, typeChecker, result, 0);
        }

        public void checkScalarApprox(
            String expression,
            String expectedType,
            double expectedResult,
            double delta)
        {
            String sql = buildQuery(expression);
            TypeChecker typeChecker =
                new AbstractSqlTester.StringTypeChecker(expectedType);
            check(
                sql,
                typeChecker,
                new Double(expectedResult),
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
                    AbstractSqlTester.BooleanTypeChecker,
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
            TypeChecker typeChecker =
                new AbstractSqlTester.StringTypeChecker(expectedType);
            check(sql, typeChecker, result, 0);
        }

        public void checkNull(String expression)
        {
            String sql = buildQuery(expression);
            check(sql, AbstractSqlTester.AnyTypeChecker, null, 0);
        }

        public final void check(
            String query,
            TypeChecker typeChecker,
            Object result,
            double delta)
        {
            check(
                query,
                typeChecker,
                AbstractSqlTester.createChecker(result, delta));
        }

        public void check(
            String query,
            TypeChecker typeChecker,
            ResultChecker resultChecker)
        {
            // This implementation does NOT check the result!
            // (It can't because we're pure Java.)
            // All it does is check the return type.

            // Parse and validate. There should be no errors.
            RelDataType actualType = getColumnType(query);

            // Check result type.
            typeChecker.checkType(actualType);
        }

        public void checkRewrite(
            SqlValidator validator,
            String query,
            String expectedRewrite)
        {
            SqlNode rewrittenNode = parseAndValidate(validator, query);
            String actualRewrite =
                rewrittenNode.toSqlString(SqlDialect.DUMMY, false).getSql();
            TestUtil.assertEqualsVerbose(expectedRewrite, actualRewrite);
        }

        public void checkFails(
            String expression,
            String expectedError,
            boolean runtime)
        {
            if (runtime) {
                // We need to test that the expression fails at runtime.
                // Ironically, that means that it must succeed at prepare time.
                SqlValidator validator = getValidator();
                final String sql = buildQuery(expression);
                SqlNode n = parseAndValidate(validator, sql);
                assertNotNull(n);
            } else {
                assertExceptionIsThrown(
                    buildQuery(expression),
                    expectedError);
            }
        }

        public SqlMonotonicity getMonotonicity(String sql)
        {
            final SqlValidator validator = getValidator();
            final SqlNode node = parseAndValidate(validator, sql);
            final SqlSelect select = (SqlSelect) node;
            final SqlNode selectItem0 = select.getSelectList().get(0);
            final SqlValidatorScope scope = validator.getSelectScope(select);
            return selectItem0.getMonotonicity(scope);
        }

        private static String buildQuery(String expression)
        {
            return "values (" + expression + ")";
        }

        public boolean isVm(VmName vmName)
        {
            return false;
        }
    }
}

// End SqlValidatorTestCase.java
