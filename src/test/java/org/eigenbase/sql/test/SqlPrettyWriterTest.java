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

import java.io.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.test.*;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link SqlPrettyWriter}.
 *
 * <p>You must provide the system property "source.dir".
 */
public class SqlPrettyWriterTest {
    //~ Static fields/initializers ---------------------------------------------

    public static final String NL = System.getProperty("line.separator");

    //~ Constructors -----------------------------------------------------------

    public SqlPrettyWriterTest() {
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Helper methods -------------------------------------------------------

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(SqlPrettyWriterTest.class);
    }

    /**
     * Parses a SQL query. To use a different parser, override this method.
     */
    protected SqlNode parseQuery(String sql)
    {
        SqlNode node;
        try {
            node = new SqlParser(sql).parseQuery();
        } catch (SqlParseException e) {
            String message =
                "Received error while parsing SQL '" + sql
                + "'; error is:" + NL + e.toString();
            throw new AssertionError(message);
        }
        return node;
    }

    protected void assertPrintsTo(
        boolean newlines,
        final String sql,
        String expected)
    {
        final SqlNode node = parseQuery(sql);
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setAlwaysUseParentheses(false);
        if (newlines) {
            prettyWriter.setCaseClausesOnNewLines(true);
        }
        String actual = prettyWriter.format(node);
        getDiffRepos().assertEquals("formatted", expected, actual);

        // Now parse the result, and make sure it is structurally equivalent
        // to the original.
        final String actual2 = actual.replaceAll("`", "\"");
        final SqlNode node2 = parseQuery(actual2);
        assertTrue(node.equalsDeep(node2, true));
    }

    protected void assertExprPrintsTo(
        boolean newlines,
        final String sql,
        String expected)
    {
        final SqlCall valuesCall = (SqlCall) parseQuery("VALUES (" + sql + ")");
        final SqlCall rowCall = (SqlCall) valuesCall.getOperands()[0];
        final SqlNode node = rowCall.getOperands()[0];
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setAlwaysUseParentheses(false);
        if (newlines) {
            prettyWriter.setCaseClausesOnNewLines(true);
        }
        String actual = prettyWriter.format(node);
        getDiffRepos().assertEquals("formatted", expected, actual);

        // Now parse the result, and make sure it is structurally equivalent
        // to the original.
        final String actual2 = actual.replaceAll("`", "\"");
        final SqlNode valuesCall2 = parseQuery("VALUES (" + actual2 + ")");
        assertTrue(valuesCall.equalsDeep(valuesCall2, true));
    }

    // ~ Tests ----------------------------------------------------------------

    protected void checkSimple(
        SqlPrettyWriter prettyWriter,
        String expectedDesc,
        String expected)
        throws Exception
    {
        final SqlNode node =
            parseQuery(
                "select x as a, b as b, c as c, d,"
                + " 'mixed-Case string',"
                + " unquotedCamelCaseId,"
                + " \"quoted id\" "
                + "from"
                + " (select *"
                + " from t"
                + " where x = y and a > 5"
                + " group by z, zz"
                + " window w as (partition by c),"
                + "  w1 as (partition by c,d order by a, b"
                + "   range between interval '2:2' hour to minute preceding"
                + "    and interval '1' day following)) "
                + "order by gg");

        // Describe settings
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        prettyWriter.describe(pw, true);
        pw.flush();
        String desc = sw.toString();
        getDiffRepos().assertEquals("desc", expectedDesc, desc);

        // Format
        String actual = prettyWriter.format(node);
        getDiffRepos().assertEquals("formatted", expected, actual);
    }

    @Test public void testDefault() throws Exception {
      final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testIndent8() throws Exception {
      final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setIndentation(8);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testClausesNotOnNewLine() throws Exception {
      final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setClauseStartsLine(false);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testSelectListItemsOnSeparateLines() throws Exception {
      final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setSelectListItemsOnSeparateLines(true);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testSelectListExtraIndentFlag() throws Exception {
      final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setSelectListItemsOnSeparateLines(true);
        prettyWriter.setSelectListExtraIndentFlag(false);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testKeywordsLowerCase() throws Exception {
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setKeywordsLowerCase(true);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testParenthesizeAllExprs() throws Exception {
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setAlwaysUseParentheses(true);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testOnlyQuoteIdentifiersWhichNeedIt() throws Exception {
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setQuoteAllIdentifiers(false);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    @Test public void testDamiansSubqueryStyle() throws Exception {
        // Note that ( is at the indent, SELECT is on the same line, and ) is
        // below it.
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setSubqueryStyle(SqlWriter.SubqueryStyle.Black);
        checkSimple(prettyWriter, "${desc}", "${formatted}");
    }

    // test disabled because default SQL parser cannot parse DDL
    public void _testExplain()
    {
        assertPrintsTo(false, "explain select * from t", "foo");
    }

    @Test public void testCase() {
        // Note that CASE is rewritten to the searched form. Wish it weren't
        // so, but that's beyond the control of the pretty-printer.
        assertExprPrintsTo(
            true,
            "case 1 when 2 + 3 then 4 when case a when b then c else d end then 6 else 7 end",
            "CASE" + NL
            + "WHEN 1 = 2 + 3" + NL
            + "THEN 4" + NL
            + "WHEN 1 = CASE" + NL
            + "        WHEN `A` = `B`" + NL // todo: indent should be 4 not 8
            + "        THEN `C`" + NL
            + "        ELSE `D`" + NL
            + "        END" + NL
            + "THEN 6" + NL
            + "ELSE 7" + NL
            + "END");
    }

    @Test public void testCase2() {
        assertExprPrintsTo(
            false,
            "case 1 when 2 + 3 then 4 when case a when b then c else d end then 6 else 7 end",
            "CASE WHEN 1 = 2 + 3 THEN 4 WHEN 1 = CASE WHEN `A` = `B` THEN `C` ELSE `D` END THEN 6 ELSE 7 END");
    }

    @Test public void testBetween() {
        assertExprPrintsTo(
            true,
            "x not between symmetric y and z",
            "`X` NOT BETWEEN SYMMETRIC `Y` AND `Z`"); // todo: remove leading

        // space
    }

    @Test public void testCast() {
        assertExprPrintsTo(
            true,
            "cast(x + y as decimal(5, 10))",
            "CAST(`X` + `Y` AS DECIMAL(5, 10))");
    }

    @Test public void testLiteralChain() {
        assertExprPrintsTo(
            true,
            "'x' /* comment */ 'y'" + NL
            + "  'z' ",
            "'x'" + NL + "'y'" + NL + "'z'");
    }

    @Test public void testOverlaps() {
        assertExprPrintsTo(
            true,
            "(x,xx) overlaps (y,yy) or x is not null",
            "(`X`, `XX`) OVERLAPS (`Y`, `YY`) OR `X` IS NOT NULL");
    }

    @Test public void testUnion() {
        assertPrintsTo(
            true,
            "select * from t "
            + "union select * from ("
            + "  select * from u "
            + "  union select * from v) "
            + "union select * from w "
            + "order by a, b",

            // todo: SELECT should not be indended from UNION, like this:
            // UNION
            //     SELECT *
            //     FROM `W`

            "${formatted}");
    }

    @Test public void testMultiset() {
        assertPrintsTo(
            false,
            "values (multiset (select * from t))",
            "${formatted}");
    }

    @Test public void testInnerJoin() {
        assertPrintsTo(
            true,
            "select * from x inner join y on x.k=y.k",
            "${formatted}");
    }

    @Test public void testWhereListItemsOnSeparateLinesOr() throws Exception {
        checkPrettySeparateLines(
            "select x"
            + " from y"
            + " where h is not null and i < j"
            + " or ((a or b) is true) and d not in (f,g)"
            + " or x <> z");
    }

    @Test public void testWhereListItemsOnSeparateLinesAnd() throws Exception {
        checkPrettySeparateLines(
            "select x"
            + " from y"
            + " where h is not null and (i < j"
            + " or ((a or b) is true)) and (d not in (f,g)"
            + " or v <> ((w * x) + y) * z)");
    }

    private void checkPrettySeparateLines(String sql)
    {
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlDialect.DUMMY);
        prettyWriter.setSelectListItemsOnSeparateLines(true);
        prettyWriter.setSelectListExtraIndentFlag(false);

        final SqlNode node = parseQuery(sql);

        // Describe settings
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        prettyWriter.describe(pw, true);
        pw.flush();
        String desc = sw.toString();
        getDiffRepos().assertEquals("desc", "${desc}", desc);
        prettyWriter.setWhereListItemsOnSeparateLines(true);

        // Format
        String actual = prettyWriter.format(node);
        getDiffRepos().assertEquals("formatted", "${formatted}", actual);
    }
}

// End SqlPrettyWriterTest.java
