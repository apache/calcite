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
package org.eigenbase.sql.parser;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.impl.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * A <code>SqlParserTest</code> is a unit-test for {@link SqlParser the SQL
 * parser}.
 */
public class SqlParserTest {
    //~ Static fields/initializers ---------------------------------------------

    private static final String ANY = "(?s).*";

    //~ Constructors -----------------------------------------------------------

    public SqlParserTest() {
    }

    //~ Methods ----------------------------------------------------------------

    // Helper functions -------------------------------------------------------

    protected Tester getTester() {
        return new TesterImpl();
    }

    protected void check(
        String sql,
        String expected)
    {
        getTester().check(sql, expected);
    }

    protected SqlNode parseStmt(String sql)
        throws SqlParseException
    {
        return new SqlParser(sql).parseStmt();
    }

    protected void checkExp(
        String sql,
        String expected)
    {
        getTester().checkExp(sql, expected);
    }

    protected SqlNode parseExpression(String sql)
        throws SqlParseException
    {
        return new SqlParser(sql).parseExpression();
    }

    protected SqlParserImpl getParserImpl() {
        return new SqlParser("").getParserImpl();
    }

    protected void checkExpSame(String sql)
    {
        checkExp(sql, sql);
    }

    protected void checkFails(
        String sql,
        String expectedMsgPattern)
    {
        getTester().checkFails(sql, expectedMsgPattern);
    }

    /**
     * Tests that an expression throws an exception which matches the given
     * pattern.
     */
    protected void checkExpFails(
        String sql,
        String expectedMsgPattern)
    {
        getTester().checkExpFails(sql, expectedMsgPattern);
    }

    /**
     * Tests that when there is an error, non-reserved keywords such as "A",
     * "ABSOLUTE" (which naturally arise whenever a production uses
     * "&lt;IDENTIFIER&gt;") are removed, but reserved words such as "AND"
     * remain.
     */
    @Test public void testExceptionCleanup() {
        checkFails(
            "select 0.5e1^.1^ from sales.emps",
            "(?s).*Encountered \".1\" at line 1, column 13.\n"
            + "Was expecting one of:\n"
            + "    \"FROM\" ...\n"
            + "    \",\" ...\n"
            + "    \"AS\" ...\n"
            + "    <IDENTIFIER> ...\n"
            + "    <QUOTED_IDENTIFIER> ...\n"
            + ".*");
    }

    @Test public void testInvalidToken() {
        // Causes problems to the test infrastructure because the token mgr
        // throws a java.lang.Error. The usual case is that the parser throws
        // an exception.
        checkFails(
            "values (a^#^b)",
            "Lexical error at line 1, column 10\\.  Encountered: \"#\" \\(35\\), after : \"\"");
    }

    public void _testDerivedColumnList() {
        check("select * from emp (empno, gender) where true", "foo");
    }

    public void _testDerivedColumnListInJoin() {
        check(
            "select * from emp as e (empno, gender) join dept (deptno, dname) on emp.deptno = dept.deptno",
            "foo");
    }

    public void _testDerivedColumnListNoAs() {
        check("select * from emp e (empno, gender) where true", "foo");
    }

    public void _testDerivedColumnListWithAlias() {
        check("select * from emp as e (empno, gender) where true", "foo");
    }

    // jdbc syntax
    public void _testEmbeddedCall() {
        checkExp("{call foo(?, ?)}", "foo");
    }

    public void _testEmbeddedFunction() {
        checkExp("{? = call bar (?, ?)}", "foo");
    }

    @Test public void testColumnAliasWithAs() {
        check(
            "select 1 as foo from emp",
            "SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
    }

    @Test public void testColumnAliasWithoutAs() {
        check(
            "select 1 foo from emp",
            "SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
    }

    @Test public void testEmbeddedDate() {
        checkExp("{d '1998-10-22'}", "DATE '1998-10-22'");
    }

    @Test public void testEmbeddedTime() {
        checkExp("{t '16:22:34'}", "TIME '16:22:34'");
    }

    @Test public void testEmbeddedTimestamp() {
        checkExp(
            "{ts '1998-10-22 16:22:34'}",
            "TIMESTAMP '1998-10-22 16:22:34'");
    }

    @Test public void testNot() {
        check(
            "select not true, not false, not null, not unknown from t",
            "SELECT (NOT TRUE), (NOT FALSE), (NOT NULL), (NOT UNKNOWN)\n"
            + "FROM `T`");
    }

    @Test public void testBooleanPrecedenceAndAssociativity() {
        check(
            "select * from t where true and false",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE AND FALSE)");

        check(
            "select * from t where null or unknown and unknown",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (NULL OR (UNKNOWN AND UNKNOWN))");

        check(
            "select * from t where true and (true or true) or false",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE AND (TRUE OR TRUE)) OR FALSE)");

        check(
            "select * from t where 1 and true",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (1 AND TRUE)");
    }

    @Test public void testIsBooleans() {
        String [] inOuts = { "NULL", "TRUE", "FALSE", "UNKNOWN" };

        for (String inOut : inOuts) {
            check(
                "select * from t where nOt fAlSe Is " + inOut,
                "SELECT *\n"
                + "FROM `T`\n"
                + "WHERE ((NOT FALSE) IS " + inOut + ")");

            check(
                "select * from t where c1=1.1 IS NOT " + inOut,
                "SELECT *\n"
                + "FROM `T`\n"
                + "WHERE ((`C1` = 1.1) IS NOT " + inOut + ")");
        }
    }

    @Test public void testIsBooleanPrecedenceAndAssociativity() {
        check(
            "select * from t where x is unknown is not unknown",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` IS UNKNOWN) IS NOT UNKNOWN)");

        check(
            "select 1 from t where not true is unknown",
            "SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE ((NOT TRUE) IS UNKNOWN)");

        check(
            "select * from t where x is unknown is not unknown is false is not false"
            + " is true is not true is null is not null",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((((((((`X` IS UNKNOWN) IS NOT UNKNOWN) IS FALSE) IS NOT FALSE) IS TRUE) IS NOT TRUE) IS NULL) IS NOT NULL)");

        // combine IS postfix operators with infix (AND) and prefix (NOT) ops
        check(
            "select * from t where x is unknown is false and x is unknown is true or not y is unknown is not null",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((((`X` IS UNKNOWN) IS FALSE) AND ((`X` IS UNKNOWN) IS TRUE)) OR (((NOT `Y`) IS UNKNOWN) IS NOT NULL))");
    }

    @Test public void testEqualNotEqual() {
        checkExp("'abc'=123", "('abc' = 123)");
        checkExp("'abc'<>123", "('abc' <> 123)");
        checkExp("'abc'<>123='def'<>456", "((('abc' <> 123) = 'def') <> 456)");
        checkExp(
            "'abc'<>123=('def'<>456)",
            "(('abc' <> 123) = ('def' <> 456))");
    }

    @Test public void testBangEqualIsBad() {
        // Quoth www.ocelot.ca:
        //   "Other relators besides '=' are what you'd expect if
        //   you've used any programming language: > and >= and < and <=. The
        //   only potential point of confusion is that the operator for 'not
        //   equals' is <> as in BASIC. There are many texts which will tell
        //   you that != is SQL's not-equals operator; those texts are false;
        //   it's one of those unstampoutable urban myths."
        checkFails(
            "'abc'^!^=123",
            "Lexical error at line 1, column 6\\.  Encountered: \"!\" \\(33\\), after : \"\"");
    }

    @Test public void testBetween() {
        check(
            "select * from t where price between 1 and 2",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND 2)");

        check(
            "select * from t where price between symmetric 1 and 2",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN SYMMETRIC 1 AND 2)");

        check(
            "select * from t where price not between symmetric 1 and 2",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` NOT BETWEEN SYMMETRIC 1 AND 2)");

        check(
            "select * from t where price between ASYMMETRIC 1 and 2+2*2",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND (2 + (2 * 2)))");

        check(
            "select * from t where price > 5 and price not between 1 + 2 and 3 * 4 AnD price is null",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (((`PRICE` > 5) AND (`PRICE` NOT BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) AND (`PRICE` IS NULL))");

        check(
            "select * from t where price > 5 and price between 1 + 2 and 3 * 4 + price is null",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND ((3 * 4) + `PRICE`)) IS NULL))");

        check(
            "select * from t where price > 5 and price between 1 + 2 and 3 * 4 or price is null",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (((`PRICE` > 5) AND (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) OR (`PRICE` IS NULL))");

        check(
            "values a between c and d and e and f between g and h",
            "(VALUES (ROW((((`A` BETWEEN ASYMMETRIC `C` AND `D`) AND `E`) AND (`F` BETWEEN ASYMMETRIC `G` AND `H`)))))");

        checkFails(
            "values a between b or c^",
            ".*BETWEEN operator has no terminating AND");

        checkFails(
            "values a ^between^",
            "(?s).*Encountered \"between <EOF>\" at line 1, column 10.*");

        checkFails(
            "values a between symmetric 1^",
            ".*BETWEEN operator has no terminating AND");

        // precedence of BETWEEN is higher than AND and OR, but lower than '+'
        check(
            "values a between b and c + 2 or d and e",
            "(VALUES (ROW(((`A` BETWEEN ASYMMETRIC `B` AND (`C` + 2)) OR (`D` AND `E`)))))");

        // '=' and BETWEEN have same precedence, and are left-assoc
        check(
            "values x = a between b and c = d = e",
            "(VALUES (ROW(((((`X` = `A`) BETWEEN ASYMMETRIC `B` AND `C`) = `D`) = `E`))))");

        // AND doesn't match BETWEEN if it's between parentheses!
        check(
            "values a between b or (c and d) or e and f",
            "(VALUES (ROW((`A` BETWEEN ASYMMETRIC ((`B` OR (`C` AND `D`)) OR `E`) AND `F`))))");
    }

    @Test public void testOperateOnColumn() {
        check(
            "select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t",
            "SELECT (`C1` * 1), (`C2` + 2), (`C3` / 3), (`C4` - 4), (`C5` * `C4`)\n"
            + "FROM `T`");
    }

    @Test public void testRow() {
        check(
            "select t.r.\"EXPR$1\", t.r.\"EXPR$0\" from (select (1,2) r from sales.depts) t",
            "SELECT `T`.`R`.`EXPR$1`, `T`.`R`.`EXPR$0`\n"
            + "FROM (SELECT (ROW(1, 2)) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

        check(
            "select t.r.\"EXPR$1\".\"EXPR$2\" "
            + "from (select ((1,2),(3,4,5)) r from sales.depts) t",
            "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

        check(
            "select t.r.\"EXPR$1\".\"EXPR$2\" "
            + "from (select ((1,2),(3,4,5,6)) r from sales.depts) t",
            "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5, 6)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");
    }

    @Test public void testOverlaps() {
        checkExp(
            "(x,xx) overlaps (y,yy)",
            "((`X`, `XX`) OVERLAPS (`Y`, `YY`))");

        checkExp(
            "(x,xx) overlaps (y,yy) or false",
            "(((`X`, `XX`) OVERLAPS (`Y`, `YY`)) OR FALSE)");

        checkExp(
            "true and not (x,xx) overlaps (y,yy) or false",
            "((TRUE AND (NOT ((`X`, `XX`) OVERLAPS (`Y`, `YY`)))) OR FALSE)");

        checkExpFails(
            "^(x,xx,xxx) overlaps (y,yy)^ or false",
            "(?s).*Illegal overlaps expression.*");

        checkExpFails(
            "true or ^(x,xx,xxx) overlaps (y,yy,yyy)^ or false",
            "(?s).*Illegal overlaps expression.*");

        checkExpFails(
            "^(x,xx) overlaps (y,yy,yyy)^ or false",
            "(?s).*Illegal overlaps expression.*");
    }

    @Test public void testIsDistinctFrom() {
        check(
            "select x is distinct from y from t",
            "SELECT (`X` IS DISTINCT FROM `Y`)\n"
            + "FROM `T`");

        check(
            "select * from t where x is distinct from y",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM `Y`)");

        check(
            "select * from t where x is distinct from (4,5,6)",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

        check(
            "select * from t where true is distinct from true",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS DISTINCT FROM TRUE)");

        check(
            "select * from t where true is distinct from true is true",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE IS DISTINCT FROM TRUE) IS TRUE)");
    }

    @Test public void testIsNotDistinct() {
        check(
            "select x is not distinct from y from t",
            "SELECT (`X` IS NOT DISTINCT FROM `Y`)\n"
            + "FROM `T`");

        check(
            "select * from t where true is not distinct from true",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS NOT DISTINCT FROM TRUE)");
    }

    @Test public void testCast() {
        checkExp("cast(x as boolean)", "CAST(`X` AS BOOLEAN)");
        checkExp("cast(x as integer)", "CAST(`X` AS INTEGER)");
        checkExp("cast(x as varchar(1))", "CAST(`X` AS VARCHAR(1))");
        checkExp("cast(x as date)", "CAST(`X` AS DATE)");
        checkExp("cast(x as time)", "CAST(`X` AS TIME)");
        checkExp("cast(x as timestamp)", "CAST(`X` AS TIMESTAMP)");
        checkExp("cast(x as time(0))", "CAST(`X` AS TIME(0))");
        checkExp("cast(x as timestamp(0))", "CAST(`X` AS TIMESTAMP(0))");
        checkExp("cast(x as decimal(1,1))", "CAST(`X` AS DECIMAL(1, 1))");
        checkExp("cast(x as char(1))", "CAST(`X` AS CHAR(1))");
        checkExp("cast(x as binary(1))", "CAST(`X` AS BINARY(1))");
        checkExp("cast(x as varbinary(1))", "CAST(`X` AS VARBINARY(1))");
        checkExp("cast(x as tinyint)", "CAST(`X` AS TINYINT)");
        checkExp("cast(x as smallint)", "CAST(`X` AS SMALLINT)");
        checkExp("cast(x as bigint)", "CAST(`X` AS BIGINT)");
        checkExp("cast(x as real)", "CAST(`X` AS REAL)");
        checkExp("cast(x as double)", "CAST(`X` AS DOUBLE)");
        checkExp("cast(x as decimal)", "CAST(`X` AS DECIMAL)");
        checkExp("cast(x as decimal(0))", "CAST(`X` AS DECIMAL(0))");
        checkExp("cast(x as decimal(1,2))", "CAST(`X` AS DECIMAL(1, 2))");

        checkExp("cast('foo' as bar)", "CAST('foo' AS `BAR`)");
    }

    @Test public void testCastFails() {
    }

    @Test public void testLikeAndSimilar() {
        check(
            "select * from t where x like '%abc%'",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` LIKE '%abc%')");

        check(
            "select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

        // LIKE has higher precedence than AND
        check(
            "select * from t where price > 5 and x+2*2 like y*3+2 escape (select*from t)",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) LIKE ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

        check(
            "values a and b like c",
            "(VALUES (ROW((`A` AND (`B` LIKE `C`)))))");

        // LIKE has higher precedence than AND
        check(
            "values a and b like c escape d and e",
            "(VALUES (ROW(((`A` AND (`B` LIKE `C` ESCAPE `D`)) AND `E`))))");

        // LIKE has same precedence as '='; LIKE is right-assoc, '=' is left
        check(
            "values a = b like c = d",
            "(VALUES (ROW(((`A` = `B`) LIKE (`C` = `D`)))))");

        // Nested LIKE
        check(
            "values a like b like c escape d",
            "(VALUES (ROW((`A` LIKE (`B` LIKE `C` ESCAPE `D`)))))");
        check(
            "values a like b like c escape d and false",
            "(VALUES (ROW(((`A` LIKE (`B` LIKE `C` ESCAPE `D`)) AND FALSE))))");
        check(
            "values a like b like c like d escape e escape f",
            "(VALUES (ROW((`A` LIKE (`B` LIKE (`C` LIKE `D` ESCAPE `E`) ESCAPE `F`)))))");

        // Mixed LIKE and SIMILAR TO
        check(
            "values a similar to b like c similar to d escape e escape f",
            "(VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`)))))");

        // FIXME should fail at "escape"
        checkFails(
            "select * from t ^where^ escape 'e'",
            "(?s).*Encountered \"where escape\" at .*");

        // LIKE with +
        check(
            "values a like b + c escape d",
            "(VALUES (ROW((`A` LIKE (`B` + `C`) ESCAPE `D`))))");

        // LIKE with ||
        check(
            "values a like b || c escape d",
            "(VALUES (ROW((`A` LIKE (`B` || `C`) ESCAPE `D`))))");

        // ESCAPE with no expression
        // FIXME should fail at "escape"
        checkFails(
            "values a ^like^ escape d",
            "(?s).*Encountered \"like escape\" at .*");

        // ESCAPE with no expression
        checkFails(
            "values a like b || c ^escape^ and false",
            "(?s).*Encountered \"escape and\" at line 1, column 22.*");

        // basic SIMILAR TO
        check(
            "select * from t where x similar to '%abc%'",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` SIMILAR TO '%abc%')");

        check(
            "select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

        // SIMILAR TO has higher precedence than AND
        check(
            "select * from t where price > 5 and x+2*2 SIMILAR TO y*3+2 escape (select*from t)",
            "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) SIMILAR TO ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

        // Mixed LIKE and SIMILAR TO
        check(
            "values a similar to b like c similar to d escape e escape f",
            "(VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`)))))");

        // SIMILAR TO with subquery
        check(
            "values a similar to (select * from t where a like b escape c) escape d",
            "(VALUES (ROW((`A` SIMILAR TO (SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`A` LIKE `B` ESCAPE `C`)) ESCAPE `D`))))");
    }

    @Test public void testFoo() {
    }

    @Test public void testArthimeticOperators() {
        checkExp("1-2+3*4/5/6-7", "(((1 - 2) + (((3 * 4) / 5) / 6)) - 7)");
        checkExp("power(2,3)", "POWER(2, 3)");
        checkExp("aBs(-2.3e-2)", "ABS(-2.3E-2)");
        checkExp("MOD(5             ,\t\f\r\n2)", "MOD(5, 2)");
        checkExp("ln(5.43  )", "LN(5.43)");
        checkExp("log10(- -.2  )", "LOG10((- -0.2))");
    }

    @Test public void testExists() {
        check(
            "select * from dept where exists (select 1 from emp where emp.deptno = dept.deptno)",
            "SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE (EXISTS (SELECT 1\n"
            + "FROM `EMP`\n"
            + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)))");
    }

    @Test public void testExistsInWhere() {
        check(
            "select * from emp where 1 = 2 and exists (select 1 from dept) and 3 = 4",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (((1 = 2) AND (EXISTS (SELECT 1\n"
            + "FROM `DEPT`))) AND (3 = 4))");
    }

    @Test public void testFromWithAs() {
        check(
            "select 1 from emp as e where 1",
            "SELECT 1\n"
            + "FROM `EMP` AS `E`\n"
            + "WHERE 1");
    }

    @Test public void testConcat() {
        checkExp("'a' || 'b'", "('a' || 'b')");
    }

    @Test public void testReverseSolidus() {
        checkExp("'\\'", "'\\'");
    }

    @Test public void testSubstring() {
        checkExp("substring('a' \n  FROM \t  1)", "SUBSTRING('a' FROM 1)");
        checkExp("substring('a' FROM 1 FOR 3)", "SUBSTRING('a' FROM 1 FOR 3)");
        checkExp(
            "substring('a' FROM 'reg' FOR '\\')",
            "SUBSTRING('a' FROM 'reg' FOR '\\')");

        checkExp(
            "substring('a', 'reg', '\\')",
            "SUBSTRING('a' FROM 'reg' FOR '\\')");
        checkExp("substring('a', 1, 2)", "SUBSTRING('a' FROM 1 FOR 2)");
        checkExp("substring('a' , 1)", "SUBSTRING('a' FROM 1)");
    }

    @Test public void testFunction() {
        check(
            "select substring('Eggs and ham', 1, 3 + 2) || ' benedict' from emp",
            "SELECT (SUBSTRING('Eggs and ham' FROM 1 FOR (3 + 2)) || ' benedict')\n"
            + "FROM `EMP`");
        checkExp(
            "log10(1)\r\n+power(2, mod(\r\n3\n\t\t\f\n,ln(4))*log10(5)-6*log10(7/abs(8)+9))*power(10,11)",
            "(LOG10(1) + (POWER(2, ((MOD(3, LN(4)) * LOG10(5)) - (6 * LOG10(((7 / ABS(8)) + 9))))) * POWER(10, 11)))");
    }

    @Test public void testFunctionWithDistinct() {
        checkExp("count(DISTINCT 1)", "COUNT(DISTINCT 1)");
        checkExp("count(ALL 1)", "COUNT(ALL 1)");
        checkExp("count(1)", "COUNT(1)");
        check(
            "select count(1), count(distinct 2) from emp",
            "SELECT COUNT(1), COUNT(DISTINCT 2)\n"
            + "FROM `EMP`");
    }

    @Test public void testFunctionInFunction() {
        checkExp("ln(power(2,2))", "LN(POWER(2, 2))");
    }

    @Test public void testGroup() {
        check(
            "select deptno, min(foo) as x from emp group by deptno, gender",
            "SELECT `DEPTNO`, MIN(`FOO`) AS `X`\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`, `GENDER`");
    }

    @Test public void testGroupEmpty() {
        check(
            "select count(*) from emp group by ()",
            "SELECT COUNT(*)\n"
            + "FROM `EMP`\n"
            + "GROUP BY ()");

        check(
            "select count(*) from emp group by () having 1 = 2 order by 3",
            "SELECT COUNT(*)\n"
            + "FROM `EMP`\n"
            + "GROUP BY ()\n"
            + "HAVING (1 = 2)\n"
            + "ORDER BY 3");

        checkFails(
            "select 1 from emp group by ()^,^ x",
            "(?s)Encountered \\\",\\\" at .*");

        checkFails(
            "select 1 from emp group by x, ^(^)",
            "(?s)Encountered \"\\( \\)\" at .*");

        // parentheses do not an empty GROUP BY make
        check(
            "select 1 from emp group by (empno + deptno)",
            "SELECT 1\n"
            + "FROM `EMP`\n"
            + "GROUP BY (`EMPNO` + `DEPTNO`)");
    }

    @Test public void testHavingAfterGroup() {
        check(
            "select deptno from emp group by deptno, emp having count(*) > 5 and 1 = 2 order by 5, 2",
            "SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`, `EMP`\n"
            + "HAVING ((COUNT(*) > 5) AND (1 = 2))\n"
            + "ORDER BY 5, 2");
    }

    @Test public void testHavingBeforeGroupFails() {
        checkFails(
            "select deptno from emp having count(*) > 5 and deptno < 4 ^group^ by deptno, emp",
            "(?s).*Encountered \"group\" at .*");
    }

    @Test public void testHavingNoGroup() {
        check(
            "select deptno from emp having count(*) > 5",
            "SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "HAVING (COUNT(*) > 5)");
    }

    @Test public void testIdentifier() {
        checkExp("ab", "`AB`");
        checkExp("     \"a  \"\" b!c\"", "`a  \" b!c`");
        checkExp("\"x`y`z\"", "`x``y``z`");
    }

    @Test public void testInList() {
        check(
            "select * from emp where deptno in (10, 20) and gender = 'F'",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`DEPTNO` IN (10, 20)) AND (`GENDER` = 'F'))");
    }

    @Test public void testInListEmptyFails() {
        checkFails(
            "select * from emp where deptno in (^)^ and gender = 'F'",
            "(?s).*Encountered \"\\)\" at line 1, column 36\\..*");
    }

    @Test public void testInQuery() {
        check(
            "select * from emp where deptno in (select deptno from dept)",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`))");
    }

    /** Tricky for the parser - looks like "IN (scalar, scalar)" but isn't. */
    @Test public void testInQueryWithComma() {
        check(
            "select * from emp where deptno in (select deptno from dept group by 1, 2)",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "GROUP BY 1, 2))");
    }

    @Test public void testInSetop() {
        check(
            "select * from emp where deptno in ((select deptno from dept union select * from dept)"
            + "except select * from dept) and false",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`DEPTNO` IN ((SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `DEPT`)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `DEPT`)) AND FALSE)");
    }

    @Test public void testUnion() {
        check(
            "select * from a union select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a union all select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a union distinct select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
    }

    @Test public void testUnionOrder() {
        check(
            "select a, b from t "
            + "union all "
            + "select x, y from u "
            + "order by 1 asc, 2 desc",
            "(SELECT `A`, `B`\n"
            + "FROM `T`\n"
            + "UNION ALL\n"
            + "SELECT `X`, `Y`\n"
            + "FROM `U`)\n"
            + "ORDER BY 1, 2 DESC");
    }

    @Test public void testUnionOfNonQueryFails() {
        checkFails(
            "select 1 from emp union ^2^ + 5",
            "Non-query expression encountered in illegal context");
    }

    /**
     * In modern SQL, a query can occur almost everywhere that an expression
     * can. This test tests the few exceptions.
     */
    @Test public void testQueryInIllegalContext() {
        checkFails(
            "select 0, multiset[^(^select * from emp), 2] from dept",
            "Query expression encountered in illegal context");
        checkFails(
            "select 0, multiset[1, ^(^select * from emp), 2, 3] from dept",
            "Query expression encountered in illegal context");
    }

    @Test public void testExcept() {
        check(
            "select * from a except select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a except all select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a except distinct select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    }

    @Test public void testIntersect() {
        check(
            "select * from a intersect select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a intersect all select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
        check(
            "select * from a intersect distinct select * from a",
            "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    }

    @Test public void testJoinCross() {
        check(
            "select * from a as a2 cross join b",
            "SELECT *\n"
            + "FROM `A` AS `A2`\n"
            + "CROSS JOIN `B`");
    }

    @Test public void testJoinOn() {
        check(
            "select * from a left join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
    }

    @Test public void testJoinOnParentheses() {
        if (!Bug.TodoFixed) {
            return;
        }
        check(
            "select * from a\n"
            + " left join (b join c as c1 on 1 = 1) on 2 = 2\n"
            + "where 3 = 3",
            "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` INNER JOIN `C` AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
    }

    /** Same as {@link #testJoinOnParentheses()} but fancy aliases. */
    @Test public void testJoinOnParenthesesPlus() {
        if (!Bug.TodoFixed) {
            return;
        }
        check(
            "select * from a\n"
            + " left join (b as b1 (x, y) join (select * from c) c1 on 1 = 1) on 2 = 2\n"
            + "where 3 = 3",
            "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` AS `B1` (`X`, `Y`) INNER JOIN (SELECT *\n"
            + "FROM `C`) AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
    }

    @Test public void testExplicitTableInJoin() {
        check(
            "select * from a left join (table b) on 2 = 2 where 3 = 3",
            "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (TABLE `B`) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
    }

    @Test public void testSubqueryInJoin() {
        if (!Bug.TodoFixed) {
            return;
        }
        check(
            "select * from (select * from a cross join b) as ab\n"
            + " left join ((table c) join d on 2 = 2) on 3 = 3\n"
            + " where 4 = 4",
            "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `A`\n"
            + "CROSS JOIN `B`) AS `AB`\n"
            + "LEFT JOIN ((TABLE `C`) INNER JOIN `D` ON (2 = 2)) ON (3 = 3)\n"
            + "WHERE (4 = 4)");
    }

    @Test public void testOuterJoinNoiseWord() {
        check(
            "select * from a left outer join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
    }

    @Test public void testJoinQuery() {
        check(
            "select * from a join (select * from b) as b2 on true",
            "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `B`) AS `B2` ON TRUE");
    }

    @Test public void testFullInnerJoinFails() {
        // cannot have more than one of INNER, FULL, LEFT, RIGHT, CROSS
        checkFails(
            "select * from a ^full^ inner join b",
            "(?s).*Encountered \"full inner\" at line 1, column 17.*");
    }

    @Test public void testFullOuterJoin() {
        // OUTER is an optional extra to LEFT, RIGHT, or FULL
        check(
            "select * from a full outer join b",
            "SELECT *\n"
            + "FROM `A`\n"
            + "FULL JOIN `B`");
    }

    @Test public void testInnerOuterJoinFails() {
        checkFails(
            "select * from a ^inner^ outer join b",
            "(?s).*Encountered \"inner outer\" at line 1, column 17.*");
    }

    public void _testJoinAssociativity() {
        // joins are left-associative
        // 1. no parens needed
        check(
            "select * from (a natural left join b) left join c on b.c1 = c.c1",
            "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

        // 2. parens needed
        check(
            "select * from a natural left join (b left join c on b.c1 = c.c1)",
            "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

        // 3. same as 1
        check(
            "select * from a natural left join b left join c on b.c1 = c.c1",
            "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");
    }

    // Note: "select * from a natural cross join b" is actually illegal SQL
    // ("cross" is the only join type which cannot be modified with the
    // "natural") but the parser allows it; we and catch it at validate time
    @Test public void testNaturalCrossJoin() {
        check(
            "select * from a natural cross join b",
            "SELECT *\n"
            + "FROM `A`\n"
            + "NATURAL CROSS JOIN `B`");
    }

    @Test public void testJoinUsing() {
        check(
            "select * from a join b using (x)",
            "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`)");
        checkFails(
            "select * from a join b using (^)^ where c = d",
            "(?s).*Encountered \"[)]\" at line 1, column 31.*");
    }

    @Test public void testTableSample() {
        check(
            "select * from ("
            + "  select * "
            + "  from emp "
            + "  join dept on emp.deptno = dept.deptno"
            + "  where gender = 'F'"
            + "  order by sal) tablesample substitute('medium')",
            "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`\n"
            + "INNER JOIN `DEPT` ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)\n"
            + "WHERE (`GENDER` = 'F')\n"
            + "ORDER BY `SAL`) TABLESAMPLE SUBSTITUTE('MEDIUM')");

        check(
            "select * "
            + "from emp as x tablesample substitute('medium') "
            + "join dept tablesample substitute('lar' /* split */ 'ge') on x.deptno = dept.deptno",
            "SELECT *\n"
            + "FROM `EMP` AS `X` TABLESAMPLE SUBSTITUTE('MEDIUM')\n"
            + "INNER JOIN `DEPT` TABLESAMPLE SUBSTITUTE('LARGE') ON (`X`.`DEPTNO` = `DEPT`.`DEPTNO`)");

        check(
            "select * "
            + "from emp as x tablesample bernoulli(50)",
            "SELECT *\n"
            + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0)");
    }

    @Test public void testLiteral() {
        checkExpSame("'foo'");
        checkExpSame("100");
        check(
            "select 1 as one, 'x' as x, null as n from emp",
            "SELECT 1 AS `ONE`, 'x' AS `X`, NULL AS `N`\n"
            + "FROM `EMP`");

        // Even though it looks like a date, it's just a string.
        checkExp("'2004-06-01'", "'2004-06-01'");
        checkExp("-.25", "-0.25");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55'");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55.900'");
        checkExp(
            "TIMESTAMP '2004-06-01 15:55:55.1234'",
            "TIMESTAMP '2004-06-01 15:55:55.123'");
        checkExp(
            "TIMESTAMP '2004-06-01 15:55:55.1236'",
            "TIMESTAMP '2004-06-01 15:55:55.124'");
        checkExp(
            "TIMESTAMP '2004-06-01 15:55:55.9999'",
            "TIMESTAMP '2004-06-01 15:55:56.000'");
        checkExpSame("NULL");
    }

    @Test public void testContinuedLiteral() {
        checkExp(
            "'abba'\n'abba'",
            "'abba'\n'abba'");
        checkExp(
            "'abba'\n'0001'",
            "'abba'\n'0001'");
        checkExp(
            "N'yabba'\n'dabba'\n'doo'",
            "_ISO-8859-1'yabba'\n'dabba'\n'doo'");
        checkExp(
            "_iso-8859-1'yabba'\n'dabba'\n'don''t'",
            "_ISO-8859-1'yabba'\n'dabba'\n'don''t'");

        checkExp(
            "x'01aa'\n'03ff'",
            "X'01AA'\n'03FF'");

        // a bad hexstring
        checkFails(
            "x'01aa'\n^'vvvv'^",
            "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    }

    @Test public void testMixedFrom() {
        // REVIEW: Is this syntax even valid?
        check(
            "select * from a join b using (x), c join d using (y)",
            "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`),\n"
            + "`C`\n"
            + "INNER JOIN `D` USING (`Y`)");
    }

    @Test public void testMixedStar() {
        check(
            "select emp.*, 1 as foo from emp, dept",
            "SELECT `EMP`.*, 1 AS `FOO`\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
    }

    @Test public void testNotExists() {
        check(
            "select * from dept where not not exists (select * from emp) and true",
            "SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE ((NOT (NOT (EXISTS (SELECT *\n"
            + "FROM `EMP`)))) AND TRUE)");
    }

    @Test public void testOrder() {
        check(
            "select * from emp order by empno, gender desc, deptno asc, empno asc, name desc",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`, `GENDER` DESC, `DEPTNO`, `EMPNO`, `NAME` DESC");
    }

    @Test public void testOrderNullsFirst() {
        check(
            "select * from emp order by gender desc nulls last, deptno asc nulls first, empno nulls last",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `GENDER` DESC NULLS LAST, `DEPTNO` NULLS FIRST, `EMPNO` NULLS LAST");
    }

    @Test public void testOrderInternal() {
        check(
            "(select * from emp order by empno) union select * from emp",
            "((SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`)\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `EMP`)");

        check(
            "select * from (select * from t order by x, y) where a = b",
            "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `T`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "WHERE (`A` = `B`)");
    }

    @Test public void testOrderIllegalInExpression() {
        check(
            "select (select 1 from foo order by x,y) from t where a = b",
            "SELECT (SELECT 1\n"
            + "FROM `FOO`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "FROM `T`\n"
            + "WHERE (`A` = `B`)");
        checkFails(
            "select (1 ^order^ by x, y) from t where a = b",
            "ORDER BY unexpected");
    }

    @Test public void testSqlInlineComment() {
        check(
            "select 1 from t --this is a comment\n",
            "SELECT 1\n"
            + "FROM `T`");
        check(
            "select 1 from t--\n",
            "SELECT 1\n"
            + "FROM `T`");
        check(
            "select 1 from t--this is a comment\n"
            + "where a>b-- this is comment\n",
            "SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE (`A` > `B`)");
    }

    @Test public void testMultilineComment() {
        // on single line
        check(
            "select 1 /* , 2 */, 3 from t",
            "SELECT 1, 3\n"
            + "FROM `T`");

        // on several lines
        check(
            "select /* 1,\n"
            + " 2, \n"
            + " */ 3 from t",
            "SELECT 3\n"
            + "FROM `T`");

        // stuff inside comment
        check(
            "values ( /** 1, 2 + ** */ 3)",
            "(VALUES (ROW(3)))");

        // comment in string is preserved
        check(
            "values ('a string with /* a comment */ in it')",
            "(VALUES (ROW('a string with /* a comment */ in it')))");

        // SQL:2003, 5.2, syntax rule # 8 "There shall be no <separator>
        // separating the <minus sign>s of a <simple comment introducer>".

        check(
            "values (- -1\n"
            + ")",
            "(VALUES (ROW((- -1))))");

        check(
            "values (--1+\n"
            + "2)",
            "(VALUES (ROW(2)))");

        // end of multiline commment without start
        if (Bug.Frg73Fixed) {
            checkFails("values (1 */ 2)", "xx");
        }

        // SQL:2003, 5.2, syntax rule #10 "Within a <bracket comment context>,
        // any <solidus> immediately followed by an <asterisk> without any
        // intervening <separator> shall be considered to be the <bracketed
        // comment introducer> for a <separator> that is a <bracketed
        // comment>".

        // comment inside a comment
        // Spec is unclear what should happen, but currently it crashes the
        // parser, and that's bad
        if (Bug.Frg73Fixed) {
            check("values (1 + /* comment /* inner comment */ */ 2)", "xx");
        }

        // single-line comment inside multiline comment is illegal
        //
        // SQL-2003, 5.2: "Note 63 - Conforming programs should not place
        // <simple comment> within a <bracketed comment> because if such a
        // <simple comment> contains the sequence of characeters "*/" without
        // a preceding "/*" in the same <simple comment>, it will prematurely
        // terminate the containing <bracketed comment>.
        if (Bug.Frg73Fixed) {
            checkFails(
                "values /* multiline contains -- singline */ \n"
                + " (1)",
                "xxx");
        }

        // non-terminated multiline comment inside singleline comment
        if (Bug.Frg73Fixed) {
            // Test should fail, and it does, but it should give "*/" as the
            // erroneous token.
            checkFails(
                "values ( -- rest of line /* a comment  \n"
                + " 1, ^*/^ 2)",
                "Encountered \"/\\*\" at");
        }

        check(
            "values (1 + /* comment -- rest of line\n"
            + " rest of comment */ 2)",
            "(VALUES (ROW((1 + 2))))");

        // multiline comment inside singleline comment
        check(
            "values -- rest of line /* a comment */ \n"
            + "(1)",
            "(VALUES (ROW(1)))");

        // non-terminated multiline comment inside singleline comment
        check(
            "values -- rest of line /* a comment  \n"
            + "(1)",
            "(VALUES (ROW(1)))");

        // even if comment abuts the tokens at either end, it becomes a space
        check(
            "values ('abc'/* a comment*/'def')",
            "(VALUES (ROW('abc'\n'def')))");

        // comment which starts as soon as it has begun
        check(
            "values /**/ (1)",
            "(VALUES (ROW(1)))");
    }

    // expressions
    @Test public void testParseNumber() {
        // Exacts
        checkExp("1", "1");
        checkExp("+1.", "1");
        checkExp("-1", "-1");
        checkExp("- -1", "(- -1)");
        checkExp("1.0", "1.0");
        checkExp("-3.2", "-3.2");
        checkExp("1.", "1");
        checkExp(".1", "0.1");
        checkExp("2500000000", "2500000000");
        checkExp("5000000000", "5000000000");

        // Approximates
        checkExp("1e1", "1E1");
        checkExp("+1e1", "1E1");
        checkExp("1.1e1", "1.1E1");
        checkExp("1.1e+1", "1.1E1");
        checkExp("1.1e-1", "1.1E-1");
        checkExp("+1.1e-1", "1.1E-1");
        checkExp("1.E3", "1E3");
        checkExp("1.e-3", "1E-3");
        checkExp("1.e+3", "1E3");
        checkExp(".5E3", "5E2");
        checkExp("+.5e3", "5E2");
        checkExp("-.5E3", "-5E2");
        checkExp(".5e-32", "5E-33");

        // Mix integer/decimals/approx
        checkExp("3. + 2", "(3 + 2)");
        checkExp("1++2+3", "((1 + 2) + 3)");
        checkExp("1- -2", "(1 - -2)");
        checkExp(
            "1++2.3e-4++.5e-6++.7++8",
            "((((1 + 2.3E-4) + 5E-7) + 0.7) + 8)");
        checkExp(
            "1- -2.3e-4 - -.5e-6  -\n"
            + "-.7++8",
            "((((1 - -2.3E-4) - -5E-7) - -0.7) + 8)");
        checkExp("1+-2.*-3.e-1/-4", "(1 + ((-2 * -3E-1) / -4))");
    }

    @Test public void testParseNumberFails() {
        checkFails(
            "SELECT 0.5e1^.1^ from t",
            "(?s).*Encountered .*\\.1.* at line 1.*");
    }

    @Test public void testMinusPrefixInExpression() {
        checkExp("-(1+2)", "(- (1 + 2))");
    }

    // operator precedence
    @Test public void testPrecedence0() {
        checkExp("1 + 2 * 3 * 4 + 5", "((1 + ((2 * 3) * 4)) + 5)");
    }

    @Test public void testPrecedence1() {
        checkExp("1 + 2 * (3 * (4 + 5))", "(1 + (2 * (3 * (4 + 5))))");
    }

    @Test public void testPrecedence2() {
        checkExp("- - 1", "(- -1)"); // two prefices
    }

    @Test public void testPrecedence3() {
        checkExp("- 1 is null", "(-1 IS NULL)"); // prefix vs. postfix
    }

    @Test public void testPrecedence4() {
        checkExp("1 - -2", "(1 - -2)"); // infix, prefix '-'
    }

    @Test public void testPrecedence5() {
        checkExp("1++2", "(1 + 2)"); // infix, prefix '+'
        checkExp("1+ +2", "(1 + 2)"); // infix, prefix '+'
    }

    @Test public void testPrecedenceSetOps() {
        check(
            "select * from a union "
            + "select * from b intersect "
            + "select * from c intersect "
            + "select * from d except "
            + "select * from e except "
            + "select * from f union "
            + "select * from g",
            "((((SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "((SELECT *\n"
            + "FROM `B`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `C`)\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `D`))\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `E`)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `F`)\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `G`)");
    }

    @Test public void testQueryInFrom() {
        // one query with 'as', the other without
        check(
            "select * from (select * from emp) as e join (select * from dept) d",
            "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`) AS `E`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `DEPT`) AS `D`");
    }

    @Test public void testQuotesInString() {
        checkExp("'a''b'", "'a''b'");
        checkExp("'''x'", "'''x'");
        checkExp("''", "''");
        checkExp(
            "'Quoted strings aren''t \"hard\"'",
            "'Quoted strings aren''t \"hard\"'");
    }

    @Test public void testScalarQueryInWhere() {
        check(
            "select * from emp where 3 = (select count(*) from dept where dept.deptno = emp.deptno)",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (3 = (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)))");
    }

    @Test public void testScalarQueryInSelect() {
        check(
            "select x, (select count(*) from dept where dept.deptno = emp.deptno) from emp",
            "SELECT `X`, (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`))\n"
            + "FROM `EMP`");
    }

    @Test public void testSelectList() {
        check(
            "select * from emp, dept",
            "SELECT *\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
    }

    @Test public void testSelectList3() {
        check(
            "select 1, emp.*, 2 from emp",
            "SELECT 1, `EMP`.*, 2\n"
            + "FROM `EMP`");
    }

    @Test public void testSelectList4() {
        checkFails(
            "select ^from^ emp",
            "(?s).*Encountered \"from\" at line .*");
    }

    @Test public void testStar() {
        check(
            "select * from emp",
            "SELECT *\n"
            + "FROM `EMP`");
    }

    @Test public void testSelectDistinct() {
        check(
            "select distinct foo from bar",
            "SELECT DISTINCT `FOO`\n"
            + "FROM `BAR`");
    }

    @Test public void testSelectAll() {
        // "unique" is the default -- so drop the keyword
        check(
            "select * from (select all foo from bar) as xyz",
            "SELECT *\n"
            + "FROM (SELECT ALL `FOO`\n"
            + "FROM `BAR`) AS `XYZ`");
    }

    @Test public void testWhere() {
        check(
            "select * from emp where empno > 5 and gender = 'F'",
            "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`EMPNO` > 5) AND (`GENDER` = 'F'))");
    }

    @Test public void testNestedSelect() {
        check(
            "select * from (select * from emp)",
            "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`)");
    }

    @Test public void testValues() {
        check("values(1,'two')", "(VALUES (ROW(1, 'two')))");
    }

    @Test public void testValuesExplicitRow() {
        check("values row(1,'two')", "(VALUES (ROW(1, 'two')))");
    }

    @Test public void testFromValues() {
        check(
            "select * from (values(1,'two'), 3, (4, 'five'))",
            "SELECT *\n"
            + "FROM (VALUES (ROW(1, 'two')), (ROW(3)), (ROW(4, 'five')))");
    }

    @Test public void testFromValuesWithoutParens() {
        checkFails(
            "select 1 from ^values^('x')",
            "Encountered \"values\" at line 1, column 15\\.\n"
            + "Was expecting one of:\n"
            + "    <IDENTIFIER> \\.\\.\\.\n"
            + "    <QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <UNICODE_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    \"LATERAL\" \\.\\.\\.\n"
            + "    \"\\(\" \\.\\.\\.\n"
            + "    \"UNNEST\" \\.\\.\\.\n"
            + "    \"TABLE\" \\.\\.\\.\n"
            + "    ");
    }

    @Test public void testEmptyValues() {
        checkFails(
            "select * from (values^(^))",
            "(?s).*Encountered \"\\( \\)\" at .*");
    }

    @Test public void testExplicitTable() {
        check("table emp", "(TABLE `EMP`)");

        // FIXME should fail at "123"
        checkFails(
            "^table^ 123",
            "(?s)Encountered \"table 123\" at line 1, column 1\\.\n.*");
    }

    @Test public void testExplicitTableOrdered() {
        check(
            "table emp order by name",
            "(TABLE `EMP`)\n"
            + "ORDER BY `NAME`");
    }

    @Test public void testSelectFromExplicitTable() {
        check(
            "select * from (table emp)",
            "SELECT *\n"
            + "FROM (TABLE `EMP`)");
    }

    @Test public void testSelectFromBareExplicitTableFails() {
        // FIXME should fail at "emp"
        checkFails(
            "select * from ^table^ emp",
            "(?s).*Encountered \"table emp\" at .*");

        checkFails(
            "select * from (^table^ (select empno from emp))",
            "(?s)Encountered \"table \\(\".*");
    }

    @Test public void testCollectionTable() {
        check(
            "select * from table(ramp(3, 4))",
            "SELECT *\n"
            + "FROM TABLE(`RAMP`(3, 4))");
    }

    @Test public void testCollectionTableWithCursorParam() {
        check(
            "select * from table(dedup(cursor(select * from emps),'name'))",
            "SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), 'name'))");
    }

    @Test public void testCollectionTableWithColumnListParam() {
        check(
            "select * from table(dedup(cursor(select * from emps),"
            + "row(empno, name)))",
            "SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), (ROW(`EMPNO`, `NAME`))))");
    }

    @Test public void testIllegalCursors() {
        checkFails(
            "select ^cursor^(select * from emps) from emps",
            "CURSOR expression encountered in illegal context");
        checkFails(
            "call p(^cursor^(select * from emps))",
            "CURSOR expression encountered in illegal context");
        checkFails(
            "select f(^cursor^(select * from emps)) from emps",
            "CURSOR expression encountered in illegal context");
    }

    @Test public void testExplain() {
        check(
            "explain plan for select * from emps",
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
    }

    @Test public void testExplainWithImpl() {
        check(
            "explain plan with implementation for select * from emps",
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
    }

    @Test public void testExplainWithoutImpl() {
        check(
            "explain plan without implementation for select * from emps",
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITHOUT IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
    }

    @Test public void testExplainWithType() {
        check(
            "explain plan with type for (values (true))",
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH TYPE FOR\n"
            + "(VALUES (ROW(TRUE)))");
    }

    @Test public void testInsertSelect() {
        check(
            "insert into emps select * from emps",
            "INSERT INTO `EMPS`\n"
            + "(SELECT *\n"
            + "FROM `EMPS`)");
    }

    @Test public void testInsertUnion() {
        check(
            "insert into emps select * from emps1 union select * from emps2",
            "INSERT INTO `EMPS`\n"
            + "(SELECT *\n"
            + "FROM `EMPS1`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `EMPS2`)");
    }

    @Test public void testInsertValues() {
        check(
            "insert into emps values (1,'Fredkin')",
            "INSERT INTO `EMPS`\n"
            + "(VALUES (ROW(1, 'Fredkin')))");
    }

    @Test public void testInsertColumnList() {
        check(
            "insert into emps(x,y) select * from emps",
            "INSERT INTO `EMPS` (`X`, `Y`)\n"
            + "(SELECT *\n"
            + "FROM `EMPS`)");
    }

    @Test public void testExplainInsert() {
        check(
            "explain plan for insert into emps1 select * from emps2",
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
            + "INSERT INTO `EMPS1`\n"
            + "(SELECT *\n"
            + "FROM `EMPS2`)");
    }

    @Test public void testDelete() {
        check("delete from emps", "DELETE FROM `EMPS`");
    }

    @Test public void testDeleteWhere() {
        check(
            "delete from emps where empno=12",
            "DELETE FROM `EMPS`\n"
            + "WHERE (`EMPNO` = 12)");
    }

    @Test public void testMergeSelectSource() {
        check(
            "merge into emps e "
            + "using (select * from tempemps where deptno is null) t "
            + "on e.empno = t.empno "
            + "when matched then update "
            + "set name = t.name, deptno = t.deptno, salary = t.salary * .1 "
            + "when not matched then insert (name, dept, salary) "
            + "values(t.name, 10, t.salary * .15)",

            "MERGE INTO `EMPS` AS `E`\n"
            + "USING (SELECT *\n"
            + "FROM `TEMPEMPS`\n"
            + "WHERE (`DEPTNO` IS NULL)) AS `T`\n"
            + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
            + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`\n"
            + ", `DEPTNO` = `T`.`DEPTNO`\n"
            + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
            + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
            + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))");
    }

    @Test public void testMergeTableRefSource() {
        check(
            "merge into emps e "
            + "using tempemps as t "
            + "on e.empno = t.empno "
            + "when matched then update "
            + "set name = t.name, deptno = t.deptno, salary = t.salary * .1 "
            + "when not matched then insert (name, dept, salary) "
            + "values(t.name, 10, t.salary * .15)",

            "MERGE INTO `EMPS` AS `E`\n"
            + "USING `TEMPEMPS` AS `T`\n"
            + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
            + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`\n"
            + ", `DEPTNO` = `T`.`DEPTNO`\n"
            + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
            + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
            + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))");
    }

    @Test public void testBitStringNotImplemented() {
        // Bit-string is longer part of the SQL standard. We do not support it.
        checkFails(
            "select B^'1011'^ || 'foobar' from (values (true))",
            "(?s).*Encountered \"\\\\'1011\\\\'\" at line 1, column 9.*");
    }

    @Test public void testHexAndBinaryString() {
        checkExp("x''=X'2'", "(X'' = X'2')");
        checkExp("x'fffff'=X''", "(X'FFFFF' = X'')");
        checkExp(
            "x'1' \t\t\f\r \n"
            + "'2'--hi this is a comment'FF'\r\r\t\f \n"
            + "'34'",
            "X'1'\n'2'\n'34'");
        checkExp(
            "x'1' \t\t\f\r \n"
            + "'000'--\n"
            + "'01'",
            "X'1'\n'000'\n'01'");
        checkExp(
            "x'1234567890abcdef'=X'fFeEdDcCbBaA'",
            "(X'1234567890ABCDEF' = X'FFEEDDCCBBAA')");

        // Check the inital zeroes don't get trimmed somehow
        checkExp("x'001'=X'000102'", "(X'001' = X'000102')");
    }

    @Test public void testHexAndBinaryStringFails() {
        checkFails(
            "select ^x'FeedGoats'^ from t",
            "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
        checkFails(
            "select ^x'abcdefG'^ from t",
            "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
        checkFails(
            "select x'1' ^x'2'^ from t",
            "(?s).*Encountered .x.*2.* at line 1, column 13.*");

        // valid syntax, but should fail in the validator
        check(
            "select x'1' '2' from t",
            "SELECT X'1'\n"
            + "'2'\n"
            + "FROM `T`");
    }

    @Test public void testStringLiteral() {
        checkExp("_latin1'hi'", "_LATIN1'hi'");
        checkExp(
            "N'is it a plane? no it''s superman!'",
            "_ISO-8859-1'is it a plane? no it''s superman!'");
        checkExp("n'lowercase n'", "_ISO-8859-1'lowercase n'");
        checkExp("'boring string'", "'boring string'");
        checkExp("_iSo-8859-1'bye'", "_ISO-8859-1'bye'");
        checkExp(
            "'three' \n ' blind'\n' mice'",
            "'three'\n' blind'\n' mice'");
        checkExp(
            "'three' -- comment \n ' blind'\n' mice'",
            "'three'\n' blind'\n' mice'");
        checkExp(
            "N'bye' \t\r\f\f\n' bye'",
            "_ISO-8859-1'bye'\n' bye'");
        checkExp(
            "_iso-8859-1'bye' \n\n--\n-- this is a comment\n' bye'",
            "_ISO-8859-1'bye'\n' bye'");

        // newline in string literal
        checkExp("'foo\rbar'", "'foo\rbar'");
        checkExp("'foo\nbar'", "'foo\nbar'");
        checkExp("'foo\r\nbar'", "'foo\r\nbar'");
    }

    @Test public void testStringLiteralFails() {
        checkFails(
            "select N ^'space'^",
            "(?s).*Encountered .*space.* at line 1, column ...*");
        checkFails(
            "select _latin1 \n^'newline'^",
            "(?s).*Encountered.*newline.* at line 2, column ...*");
        checkFails(
            "select ^_unknown-charset''^ from (values(true))",
            "Unknown character set 'unknown-charset'");

        // valid syntax, but should give a validator error
        check(
            "select N'1' '2' from t",
            "SELECT _ISO-8859-1'1'\n'2'\n"
            + "FROM `T`");
    }

    @Test public void testStringLiteralChain() {
        final String fooBar =
            "'foo'\n"
            + "'bar'";
        final String fooBarBaz =
            "'foo'\n"
            + "'bar'\n"
            + "'baz'";
        checkExp("   'foo'\r'bar'", fooBar);
        checkExp("   'foo'\r\n'bar'", fooBar);
        checkExp("   'foo'\r\n\r\n'bar'  \n   'baz'", fooBarBaz);
        checkExp("   'foo' /* a comment */ 'bar'", fooBar);
        checkExp("   'foo' -- a comment\r\n 'bar'", fooBar);

        // String literals not separated by comment or newline are OK in
        // parser, should fail in validator.
        checkExp("   'foo' 'bar'", fooBar);
    }

    @Test public void testCaseExpression() {
        // implicit simple "ELSE NULL" case
        checkExp(
            "case \t col1 when 1 then 'one' end",
            "(CASE WHEN (`COL1` = 1) THEN 'one' ELSE NULL END)");

        // implicit searched "ELSE NULL" case
        checkExp(
            "case when nbr is false then 'one' end",
            "(CASE WHEN (`NBR` IS FALSE) THEN 'one' ELSE NULL END)");

        // multiple WHENs
        checkExp(
            "case col1 when \n1.2 then 'one' when 2 then 'two' else 'three' end",
            "(CASE WHEN (`COL1` = 1.2) THEN 'one' WHEN (`COL1` = 2) THEN 'two' ELSE 'three' END)");

        // subqueries as case expression operands
        checkExp(
            "case (select * from emp) when 1 then 2 end",
            "(CASE WHEN ((SELECT *\n"
            + "FROM `EMP`) = 1) THEN 2 ELSE NULL END)");
        checkExp(
            "case 1 when (select * from emp) then 2 end",
            "(CASE WHEN (1 = (SELECT *\n"
            + "FROM `EMP`)) THEN 2 ELSE NULL END)");
        checkExp(
            "case 1 when 2 then (select * from emp) end",
            "(CASE WHEN (1 = 2) THEN (SELECT *\n"
            + "FROM `EMP`) ELSE NULL END)");
        checkExp(
            "case 1 when 2 then 3 else (select * from emp) end",
            "(CASE WHEN (1 = 2) THEN 3 ELSE (SELECT *\n"
            + "FROM `EMP`) END)");
    }

    @Test public void testCaseExpressionFails() {
        // Missing 'END'
        checkFails(
            "select case col1 when 1 then 'one' ^from^ t",
            "(?s).*from.*");

        // Wrong 'WHEN'
        checkFails(
            "select case col1 ^when1^ then 'one' end from t",
            "(?s).*when1.*");
    }

    @Test public void testNullIf() {
        checkExp(
            "nullif(v1,v2)",
            "NULLIF(`V1`, `V2`)");
        checkExpFails(
            "1 ^+^ nullif + 3",
            "(?s)Encountered \"\\+ nullif \\+\" at line 1, column 3.*");
    }

    @Test public void testCoalesce() {
        checkExp(
            "coalesce(v1)",
            "COALESCE(`V1`)");
        checkExp(
            "coalesce(v1,v2)",
            "COALESCE(`V1`, `V2`)");
        checkExp(
            "coalesce(v1,v2,v3)",
            "COALESCE(`V1`, `V2`, `V3`)");
    }

    @Test public void testLiteralCollate() {
        if (!Bug.Frg78Fixed) {
            return;
        }

        checkExp(
            "'string' collate latin1$sv_SE$mega_strength",
            "'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
        checkExp(
            "'a long '\n'string' collate latin1$sv_SE$mega_strength",
            "'a long ' 'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
        checkExp(
            "x collate iso-8859-6$ar_LB$1",
            "`X` COLLATE ISO-8859-6$ar_LB$1");
        checkExp(
            "x.y.z collate shift_jis$ja_JP$2",
            "`X`.`Y`.`Z` COLLATE SHIFT_JIS$ja_JP$2");
        checkExp(
            "'str1'='str2' collate latin1$sv_SE",
            "('str1' = 'str2' COLLATE ISO-8859-1$sv_SE$primary)");
        checkExp(
            "'str1' collate latin1$sv_SE>'str2'",
            "('str1' COLLATE ISO-8859-1$sv_SE$primary > 'str2')");
        checkExp(
            "'str1' collate latin1$sv_SE<='str2' collate latin1$sv_FI",
            "('str1' COLLATE ISO-8859-1$sv_SE$primary <= 'str2' COLLATE ISO-8859-1$sv_FI$primary)");
    }

    @Test public void testCharLength() {
        checkExp("char_length('string')", "CHAR_LENGTH('string')");
        checkExp("character_length('string')", "CHARACTER_LENGTH('string')");
    }

    @Test public void testPosition() {
        checkExp(
            "posiTion('mouse' in 'house')",
            "POSITION('mouse' IN 'house')");
    }

    // check date/time functions.
    @Test public void testTimeDate() {
        // CURRENT_TIME - returns time w/ timezone
        checkExp("CURRENT_TIME(3)", "CURRENT_TIME(3)");

        // checkFails("SELECT CURRENT_TIME() FROM foo", "SELECT CURRENT_TIME()
        // FROM `FOO`");
        checkExp("CURRENT_TIME", "`CURRENT_TIME`");
        checkExp("CURRENT_TIME(x+y)", "CURRENT_TIME((`X` + `Y`))");

        // LOCALTIME returns time w/o TZ
        checkExp("LOCALTIME(3)", "LOCALTIME(3)");

        // checkFails("SELECT LOCALTIME() FROM foo", "SELECT LOCALTIME() FROM
        // `FOO`");
        checkExp("LOCALTIME", "`LOCALTIME`");
        checkExp("LOCALTIME(x+y)", "LOCALTIME((`X` + `Y`))");

        // LOCALTIMESTAMP - returns timestamp w/o TZ
        checkExp("LOCALTIMESTAMP(3)", "LOCALTIMESTAMP(3)");

        // checkFails("SELECT LOCALTIMESTAMP() FROM foo", "SELECT
        // LOCALTIMESTAMP() FROM `FOO`");
        checkExp("LOCALTIMESTAMP", "`LOCALTIMESTAMP`");
        checkExp("LOCALTIMESTAMP(x+y)", "LOCALTIMESTAMP((`X` + `Y`))");

        // CURRENT_DATE - returns DATE
        checkExp("CURRENT_DATE(3)", "CURRENT_DATE(3)");

        // checkFails("SELECT CURRENT_DATE() FROM foo", "SELECT CURRENT_DATE()
        // FROM `FOO`");
        checkExp("CURRENT_DATE", "`CURRENT_DATE`");

        // checkFails("SELECT CURRENT_DATE(x+y) FROM foo", "CURRENT_DATE((`X` +
        // `Y`))"); CURRENT_TIMESTAMP - returns timestamp w/ TZ
        checkExp("CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)");

        // checkFails("SELECT CURRENT_TIMESTAMP() FROM foo", "SELECT
        // CURRENT_TIMESTAMP() FROM `FOO`");
        checkExp("CURRENT_TIMESTAMP", "`CURRENT_TIMESTAMP`");
        checkExp("CURRENT_TIMESTAMP(x+y)", "CURRENT_TIMESTAMP((`X` + `Y`))");

        // Date literals
        checkExp("DATE '2004-12-01'", "DATE '2004-12-01'");
        checkExp("TIME '12:01:01'", "TIME '12:01:01'");
        checkExp("TIME '12:01:01.'", "TIME '12:01:01'");
        checkExp("TIME '12:01:01.000'", "TIME '12:01:01.000'");
        checkExp("TIME '12:01:01.001'", "TIME '12:01:01.001'");
        checkExp(
            "TIMESTAMP '2004-12-01 12:01:01'",
            "TIMESTAMP '2004-12-01 12:01:01'");
        checkExp(
            "TIMESTAMP '2004-12-01 12:01:01.1'",
            "TIMESTAMP '2004-12-01 12:01:01.1'");
        checkExp(
            "TIMESTAMP '2004-12-01 12:01:01.'",
            "TIMESTAMP '2004-12-01 12:01:01'");
        checkExpSame("TIMESTAMP '2004-12-01 12:01:01.1'");

        // Failures.
        checkFails("^DATE '12/21/99'^", "(?s).*Illegal DATE literal.*");
        checkFails("^TIME '1230:33'^", "(?s).*Illegal TIME literal.*");
        checkFails("^TIME '12:00:00 PM'^", "(?s).*Illegal TIME literal.*");
        checkFails(
            "^TIMESTAMP '12-21-99, 12:30:00'^",
            "(?s).*Illegal TIMESTAMP literal.*");
    }

    /**
     * Tests for casting to/from date/time types.
     */
    @Test public void testDateTimeCast() {
        //   checkExp("CAST(DATE '2001-12-21' AS CHARACTER VARYING)",
        // "CAST(2001-12-21)");
        checkExp("CAST('2001-12-21' AS DATE)", "CAST('2001-12-21' AS DATE)");
        checkExp("CAST(12 AS DATE)", "CAST(12 AS DATE)");
        checkFails(
            "CAST('2000-12-21' AS DATE ^NOT^ NULL)",
            "(?s).*Encountered \"NOT\" at line 1, column 27.*");
        checkFails(
            "CAST('foo' as ^1^)",
            "(?s).*Encountered \"1\" at line 1, column 15.*");
        checkExp(
            "Cast(DATE '2004-12-21' AS VARCHAR(10))",
            "CAST(DATE '2004-12-21' AS VARCHAR(10))");
    }

    @Test public void testTrim() {
        checkExp(
            "trim('mustache' FROM 'beard')",
            "TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp("trim('mustache')", "TRIM(BOTH ' ' FROM 'mustache')");
        checkExp(
            "trim(TRAILING FROM 'mustache')",
            "TRIM(TRAILING ' ' FROM 'mustache')");
        checkExp(
            "trim(bOth 'mustache' FROM 'beard')",
            "TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp(
            "trim( lEaDing       'mustache' FROM 'beard')",
            "TRIM(LEADING 'mustache' FROM 'beard')");
        checkExp(
            "trim(\r\n\ttrailing\n  'mustache' FROM 'beard')",
            "TRIM(TRAILING 'mustache' FROM 'beard')");
        checkExp(
            "trim (coalesce(cast(null as varchar(2)))||"
            + "' '||coalesce('junk ',''))",
            "TRIM(BOTH ' ' FROM ((COALESCE(CAST(NULL AS VARCHAR(2))) || "
            + "' ') || COALESCE('junk ', '')))");

        checkFails(
            "trim(^from^ 'beard')",
            "(?s).*'FROM' without operands preceding it is illegal.*");
    }

    @Test public void testConvertAndTranslate() {
        checkExp(
            "convert('abc' using conversion)",
            "CONVERT('abc' USING `CONVERSION`)");
        checkExp(
            "translate('abc' using lazy_translation)",
            "TRANSLATE('abc' USING `LAZY_TRANSLATION`)");
    }

    @Test public void testOverlay() {
        checkExp(
            "overlay('ABCdef' placing 'abc' from 1)",
            "OVERLAY('ABCdef' PLACING 'abc' FROM 1)");
        checkExp(
            "overlay('ABCdef' placing 'abc' from 1 for 3)",
            "OVERLAY('ABCdef' PLACING 'abc' FROM 1 FOR 3)");
    }

    @Test public void testJdbcFunctionCall() {
        checkExp("{fn apa(1,'1')}", "{fn APA(1, '1') }");
        checkExp("{ Fn apa(log10(ln(1))+2)}", "{fn APA((LOG10(LN(1)) + 2)) }");
        checkExp("{fN apa(*)}", "{fn APA(*) }");
        checkExp("{   FN\t\r\n apa()}", "{fn APA() }");
        checkExp("{fn insert()}", "{fn INSERT() }");
    }

    @Test public void testWindowReference() {
        checkExp("sum(sal) over (w)", "(SUM(`SAL`) OVER (`W`))");

        // Only 1 window reference allowed
        checkExpFails(
            "sum(sal) over (w ^w1^ partition by deptno)",
            "(?s)Encountered \"w1\" at.*");
    }

    @Test public void testWindowInSubquery() {
        check(
            "select * from ( select sum(x) over w, sum(y) over w from s window w as (range interval '1' minute preceding))",
            "SELECT *\n"
            + "FROM (SELECT (SUM(`X`) OVER `W`), (SUM(`Y`) OVER `W`)\n"
            + "FROM `S`\n"
            + "WINDOW `W` AS (RANGE INTERVAL '1' MINUTE PRECEDING))");
    }

    @Test public void testWindowSpec() {
        // Correct syntax
        check(
            "select count(z) over w as foo from Bids window w as (partition by y + yy, yyy order by x rows between 2 preceding and 2 following)",
            "SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (PARTITION BY (`Y` + `YY`), `YYY` ORDER BY `X` ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)");

        check(
            "select count(*) over w from emp window w as (rows 2 preceding)",
            "SELECT (COUNT(*) OVER `W`)\n"
            + "FROM `EMP`\n"
            + "WINDOW `W` AS (ROWS 2 PRECEDING)");

        // Chained string literals are valid syntax. They are unlikely to be
        // semantically valid, because intervals are usually numeric or
        // datetime.
        check(
            "select count(*) over w from emp window w as (\n"
            + "  rows 'foo' 'bar'\n"
            + "       'baz' preceding)",
            "SELECT (COUNT(*) OVER `W`)\n"
            + "FROM `EMP`\n"
            + "WINDOW `W` AS (ROWS 'foobarbaz' PRECEDING)");

        // Partition clause out of place. Found after ORDER BY
        checkFails(
            "select count(z) over w as foo \n"
            + "from Bids window w as (partition by y order by x ^partition^ by y)",
            "(?s).*Encountered \"partition\".*");
        checkFails(
            "select count(z) over w as foo from Bids window w as (order by x ^partition^ by y)",
            "(?s).*Encountered \"partition\".*");

        // Cannot partition by subquery
        checkFails(
            "select sum(a) over (partition by ^(^select 1 from t), x) from t2",
            "Query expression encountered in illegal context");

        // AND is required in BETWEEN clause of window frame
        checkFails(
            "select sum(x) over (order by x range between unbounded preceding ^unbounded^ following)",
            "(?s).*Encountered \"unbounded\".*");

        // WINDOW keyword is not permissible.
        // FIXME should fail at "window"
        checkFails(
            "select sum(x) ^over^ window (order by x) from bids",
            "(?s).*Encountered \"over window\".*");

        // ORDER BY must be before Frame spec
        checkFails(
            "select sum(x) over (rows 2 preceding ^order^ by x) from emp",
            "(?s).*Encountered \"order\".*");
    }

    @Test public void testWindowSpecPartial() {
        // ALLOW PARTIAL is the default, and is omitted when the statement is
        // unparsed.
        check(
            "select sum(x) over (order by x allow partial) from bids",
            "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

        check(
            "select sum(x) over (order by x) from bids",
            "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

        check(
            "select sum(x) over (order by x disallow partial) from bids",
            "SELECT (SUM(`X`) OVER (ORDER BY `X` DISALLOW PARTIAL))\n"
            + "FROM `BIDS`");

        check(
            "select sum(x) over (order by x) from bids",
            "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");
    }

    @Test public void testAs() {
        // AS is optional for column aliases
        check(
            "select x y from t",
            "SELECT `X` AS `Y`\n"
            + "FROM `T`");

        check(
            "select x AS y from t",
            "SELECT `X` AS `Y`\n"
            + "FROM `T`");
        check(
            "select sum(x) y from t group by z",
            "SELECT SUM(`X`) AS `Y`\n"
            + "FROM `T`\n"
            + "GROUP BY `Z`");

        // Even after OVER
        check(
            "select count(z) over w foo from Bids window w as (order by x)",
            "SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (ORDER BY `X`)");

        // AS is optional for table correlation names
        final String expected =
            "SELECT `X`\n"
            + "FROM `T` AS `T1`";
        check("select x from t as t1", expected);
        check("select x from t t1", expected);

        // AS is required in WINDOW declaration
        checkFails(
            "select sum(x) over w from bids window w ^(order by x)",
            "(?s).*Encountered \"\\(\".*");

        // Error if OVER and AS are in wrong order
        checkFails(
            "select count(*) as foo ^over^ w from Bids window w (order by x)",
            "(?s).*Encountered \"over\".*");
    }

    @Test public void testAsAliases() {
        check(
            "select x from t as t1 (a, b) where foo",
            "SELECT `X`\n"
            + "FROM `T` AS `T1` (`A`, `B`)\n"
            + "WHERE `FOO`");

        check(
            "select x from (values (1, 2), (3, 4)) as t1 (\"a\", b) where \"a\" > b",
            "SELECT `X`\n"
            + "FROM (VALUES (ROW(1, 2)), (ROW(3, 4))) AS `T1` (`a`, `B`)\n"
            + "WHERE (`a` > `B`)");

        // must have at least one column
        checkFails(
            "select x from (values (1, 2), (3, 4)) as t1 ^(^)",
            "(?s).*Encountered \"\\( \\)\" at .*");

        // cannot have expressions
        checkFails(
            "select x from t as t1 (x ^+^ y)",
            "(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");

        // cannot have compound identifiers
        checkFails(
            "select x from t as t1 (x^.^y)",
            "(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");
    }

    @Test public void testOver() {
        checkExp(
            "sum(sal) over ()",
            "(SUM(`SAL`) OVER ())");
        checkExp(
            "sum(sal) over (partition by x, y)",
            "(SUM(`SAL`) OVER (PARTITION BY `X`, `Y`))");
        checkExp(
            "sum(sal) over (order by x desc, y asc)",
            "(SUM(`SAL`) OVER (ORDER BY `X` DESC, `Y`))");
        checkExp(
            "sum(sal) over (rows 5 preceding)",
            "(SUM(`SAL`) OVER (ROWS 5 PRECEDING))");
        checkExp(
            "sum(sal) over (range between interval '1' second preceding and interval '1' second following)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND INTERVAL '1' SECOND FOLLOWING))");
        checkExp(
            "sum(sal) over (range between interval '1:03' hour preceding and interval '2' minute following)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1:03' HOUR PRECEDING AND INTERVAL '2' MINUTE FOLLOWING))");
        checkExp(
            "sum(sal) over (range between interval '5' day preceding and current row)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '5' DAY PRECEDING AND CURRENT ROW))");
        checkExp(
            "sum(sal) over (range interval '5' day preceding)",
            "(SUM(`SAL`) OVER (RANGE INTERVAL '5' DAY PRECEDING))");
        checkExp(
            "sum(sal) over (range between unbounded preceding and current row)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))");
        checkExp(
            "sum(sal) over (range unbounded preceding)",
            "(SUM(`SAL`) OVER (RANGE UNBOUNDED PRECEDING))");
        checkExp(
            "sum(sal) over (range between current row and unbounded preceding)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING))");
        checkExp(
            "sum(sal) over (range between current row and unbounded following)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING))");
        checkExp(
            "sum(sal) over (range between 6 preceding and interval '1:03' hour preceding)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN 6 PRECEDING AND INTERVAL '1:03' HOUR PRECEDING))");
        checkExp(
            "sum(sal) over (range between interval '1' second following and interval '5' day following)",
            "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND FOLLOWING AND INTERVAL '5' DAY FOLLOWING))");
    }

    @Test public void testElementFunc() {
        checkExp("element(a)", "ELEMENT(`A`)");
    }

    @Test public void testCardinalityFunc() {
        checkExp("cardinality(a)", "CARDINALITY(`A`)");
    }

    @Test public void testMemberOf() {
        checkExp("a member of b", "(`A` MEMBER OF `B`)");
        checkExp(
            "a member of multiset[b]",
            "(`A` MEMBER OF (MULTISET [`B`]))");
    }

    @Test public void testSubMultisetrOf() {
        checkExp("a submultiset of b", "(`A` SUBMULTISET OF `B`)");
    }

    @Test public void testIsASet() {
        checkExp("b is a set", "(`B` IS A SET)");
        checkExp("a is a set", "(`A` IS A SET)");
    }

    @Test public void testMultiset() {
        checkExp("multiset[1]", "(MULTISET [1])");
        checkExp("multiset[1,2.3]", "(MULTISET [1, 2.3])");
        checkExp("multiset[1,    '2']", "(MULTISET [1, '2'])");
        checkExp("multiset[ROW(1,2)]", "(MULTISET [(ROW(1, 2))])");
        checkExp(
            "multiset[ROW(1,2),ROW(3,4)]",
            "(MULTISET [(ROW(1, 2)), (ROW(3, 4))])");

        checkExp(
            "multiset(select*from T)",
            "(MULTISET ((SELECT *\n"
            + "FROM `T`)))");
    }

    @Test public void testMultisetUnion() {
        checkExp("a multiset union b", "(`A` MULTISET UNION `B`)");
        checkExp("a multiset union all b", "(`A` MULTISET UNION ALL `B`)");
        checkExp("a multiset union distinct b", "(`A` MULTISET UNION `B`)");
    }

    @Test public void testMultisetExcept() {
        checkExp("a multiset EXCEPT b", "(`A` MULTISET EXCEPT `B`)");
        checkExp("a multiset EXCEPT all b", "(`A` MULTISET EXCEPT ALL `B`)");
        checkExp("a multiset EXCEPT distinct b", "(`A` MULTISET EXCEPT `B`)");
    }

    @Test public void testMultisetIntersect() {
        checkExp("a multiset INTERSECT b", "(`A` MULTISET INTERSECT `B`)");
        checkExp(
            "a multiset INTERSECT all b",
            "(`A` MULTISET INTERSECT ALL `B`)");
        checkExp(
            "a multiset INTERSECT distinct b",
            "(`A` MULTISET INTERSECT `B`)");
    }

    @Test public void testMultisetMixed() {
        checkExp(
            "multiset[1] MULTISET union b",
            "((MULTISET [1]) MULTISET UNION `B`)");
        checkExp(
            "a MULTISET union b multiset intersect c multiset except d multiset union e",
            "(((`A` MULTISET UNION (`B` MULTISET INTERSECT `C`)) MULTISET EXCEPT `D`) MULTISET UNION `E`)");
    }

    @Test public void testMapElement() {
        checkExp("a['foo']", "`A` ['foo']");
        checkExp("a['x' || 'y']", "`A` [('x' || 'y')]");
        checkExp("a['foo'] ['bar']", "`A` ['foo'] ['bar']");
        checkExp("a['foo']['bar']", "`A` ['foo'] ['bar']");
    }

    @Test public void testArrayElement() {
        checkExp("a[1]", "`A` [1]");
        checkExp("a[b[1]]", "`A` [`B` [1]]");
        checkExp("a[b[1 + 2] + 3]", "`A` [(`B` [(1 + 2)] + 3)]");
    }

    @Test public void testArrayValueConstructor() {
        checkExp("array[1, 2]", "(ARRAY [1, 2])");
        checkExp("array [1, 2]", "(ARRAY [1, 2])"); // with space

        // parser allows empty array; validator will reject it
        checkExp("array[]", "(ARRAY [])");
        checkExp(
            "array[(1, 'a'), (2, 'b')]",
            "(ARRAY [(ROW(1, 'a')), (ROW(2, 'b'))])");
    }

    @Test public void testMapValueConstructor() {
        checkExp("map[1, 'x', 2, 'y']", "(MAP [1, 'x', 2, 'y'])");
        checkExp("map [1, 'x', 2, 'y']", "(MAP [1, 'x', 2, 'y'])");
        checkExp("map[]", "(MAP [])");
    }

    /**
     * Runs tests for INTERVAL... YEAR that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalYearPositive() {
        // default precision
        checkExp(
            "interval '1' year",
            "INTERVAL '1' YEAR");
        checkExp(
            "interval '99' year",
            "INTERVAL '99' YEAR");

        // explicit precision equal to default
        checkExp(
            "interval '1' year(2)",
            "INTERVAL '1' YEAR(2)");
        checkExp(
            "interval '99' year(2)",
            "INTERVAL '99' YEAR(2)");

        // max precision
        checkExp(
            "interval '2147483647' year(10)",
            "INTERVAL '2147483647' YEAR(10)");

        // min precision
        checkExp(
            "interval '0' year(1)",
            "INTERVAL '0' YEAR(1)");

        // alternate precision
        checkExp(
            "interval '1234' year(4)",
            "INTERVAL '1234' YEAR(4)");

        // sign
        checkExp(
            "interval '+1' year",
            "INTERVAL '+1' YEAR");
        checkExp(
            "interval '-1' year",
            "INTERVAL '-1' YEAR");
        checkExp(
            "interval +'1' year",
            "INTERVAL '1' YEAR");
        checkExp(
            "interval +'+1' year",
            "INTERVAL '+1' YEAR");
        checkExp(
            "interval +'-1' year",
            "INTERVAL '-1' YEAR");
        checkExp(
            "interval -'1' year",
            "INTERVAL -'1' YEAR");
        checkExp(
            "interval -'+1' year",
            "INTERVAL -'+1' YEAR");
        checkExp(
            "interval -'-1' year",
            "INTERVAL -'-1' YEAR");
    }

    /**
     * Runs tests for INTERVAL... YEAR TO MONTH that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalYearToMonthPositive() {
        // default precision
        checkExp(
            "interval '1-2' year to month",
            "INTERVAL '1-2' YEAR TO MONTH");
        checkExp(
            "interval '99-11' year to month",
            "INTERVAL '99-11' YEAR TO MONTH");
        checkExp(
            "interval '99-0' year to month",
            "INTERVAL '99-0' YEAR TO MONTH");

        // explicit precision equal to default
        checkExp(
            "interval '1-2' year(2) to month",
            "INTERVAL '1-2' YEAR(2) TO MONTH");
        checkExp(
            "interval '99-11' year(2) to month",
            "INTERVAL '99-11' YEAR(2) TO MONTH");
        checkExp(
            "interval '99-0' year(2) to month",
            "INTERVAL '99-0' YEAR(2) TO MONTH");

        // max precision
        checkExp(
            "interval '2147483647-11' year(10) to month",
            "INTERVAL '2147483647-11' YEAR(10) TO MONTH");

        // min precision
        checkExp(
            "interval '0-0' year(1) to month",
            "INTERVAL '0-0' YEAR(1) TO MONTH");

        // alternate precision
        checkExp(
            "interval '2006-2' year(4) to month",
            "INTERVAL '2006-2' YEAR(4) TO MONTH");

        // sign
        checkExp(
            "interval '-1-2' year to month",
            "INTERVAL '-1-2' YEAR TO MONTH");
        checkExp(
            "interval '+1-2' year to month",
            "INTERVAL '+1-2' YEAR TO MONTH");
        checkExp(
            "interval +'1-2' year to month",
            "INTERVAL '1-2' YEAR TO MONTH");
        checkExp(
            "interval +'-1-2' year to month",
            "INTERVAL '-1-2' YEAR TO MONTH");
        checkExp(
            "interval +'+1-2' year to month",
            "INTERVAL '+1-2' YEAR TO MONTH");
        checkExp(
            "interval -'1-2' year to month",
            "INTERVAL -'1-2' YEAR TO MONTH");
        checkExp(
            "interval -'-1-2' year to month",
            "INTERVAL -'-1-2' YEAR TO MONTH");
        checkExp(
            "interval -'+1-2' year to month",
            "INTERVAL -'+1-2' YEAR TO MONTH");
    }

    /**
     * Runs tests for INTERVAL... MONTH that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMonthPositive() {
        // default precision
        checkExp(
            "interval '1' month",
            "INTERVAL '1' MONTH");
        checkExp(
            "interval '99' month",
            "INTERVAL '99' MONTH");

        // explicit precision equal to default
        checkExp(
            "interval '1' month(2)",
            "INTERVAL '1' MONTH(2)");
        checkExp(
            "interval '99' month(2)",
            "INTERVAL '99' MONTH(2)");

        // max precision
        checkExp(
            "interval '2147483647' month(10)",
            "INTERVAL '2147483647' MONTH(10)");

        // min precision
        checkExp(
            "interval '0' month(1)",
            "INTERVAL '0' MONTH(1)");

        // alternate precision
        checkExp(
            "interval '1234' month(4)",
            "INTERVAL '1234' MONTH(4)");

        // sign
        checkExp(
            "interval '+1' month",
            "INTERVAL '+1' MONTH");
        checkExp(
            "interval '-1' month",
            "INTERVAL '-1' MONTH");
        checkExp(
            "interval +'1' month",
            "INTERVAL '1' MONTH");
        checkExp(
            "interval +'+1' month",
            "INTERVAL '+1' MONTH");
        checkExp(
            "interval +'-1' month",
            "INTERVAL '-1' MONTH");
        checkExp(
            "interval -'1' month",
            "INTERVAL -'1' MONTH");
        checkExp(
            "interval -'+1' month",
            "INTERVAL -'+1' MONTH");
        checkExp(
            "interval -'-1' month",
            "INTERVAL -'-1' MONTH");
    }

    /**
     * Runs tests for INTERVAL... DAY that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayPositive() {
        // default precision
        checkExp(
            "interval '1' day",
            "INTERVAL '1' DAY");
        checkExp(
            "interval '99' day",
            "INTERVAL '99' DAY");

        // explicit precision equal to default
        checkExp(
            "interval '1' day(2)",
            "INTERVAL '1' DAY(2)");
        checkExp(
            "interval '99' day(2)",
            "INTERVAL '99' DAY(2)");

        // max precision
        checkExp(
            "interval '2147483647' day(10)",
            "INTERVAL '2147483647' DAY(10)");

        // min precision
        checkExp(
            "interval '0' day(1)",
            "INTERVAL '0' DAY(1)");

        // alternate precision
        checkExp(
            "interval '1234' day(4)",
            "INTERVAL '1234' DAY(4)");

        // sign
        checkExp(
            "interval '+1' day",
            "INTERVAL '+1' DAY");
        checkExp(
            "interval '-1' day",
            "INTERVAL '-1' DAY");
        checkExp(
            "interval +'1' day",
            "INTERVAL '1' DAY");
        checkExp(
            "interval +'+1' day",
            "INTERVAL '+1' DAY");
        checkExp(
            "interval +'-1' day",
            "INTERVAL '-1' DAY");
        checkExp(
            "interval -'1' day",
            "INTERVAL -'1' DAY");
        checkExp(
            "interval -'+1' day",
            "INTERVAL -'+1' DAY");
        checkExp(
            "interval -'-1' day",
            "INTERVAL -'-1' DAY");
    }

    /**
     * Runs tests for INTERVAL... DAY TO HOUR that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayToHourPositive() {
        // default precision
        checkExp(
            "interval '1 2' day to hour",
            "INTERVAL '1 2' DAY TO HOUR");
        checkExp(
            "interval '99 23' day to hour",
            "INTERVAL '99 23' DAY TO HOUR");
        checkExp(
            "interval '99 0' day to hour",
            "INTERVAL '99 0' DAY TO HOUR");

        // explicit precision equal to default
        checkExp(
            "interval '1 2' day(2) to hour",
            "INTERVAL '1 2' DAY(2) TO HOUR");
        checkExp(
            "interval '99 23' day(2) to hour",
            "INTERVAL '99 23' DAY(2) TO HOUR");
        checkExp(
            "interval '99 0' day(2) to hour",
            "INTERVAL '99 0' DAY(2) TO HOUR");

        // max precision
        checkExp(
            "interval '2147483647 23' day(10) to hour",
            "INTERVAL '2147483647 23' DAY(10) TO HOUR");

        // min precision
        checkExp(
            "interval '0 0' day(1) to hour",
            "INTERVAL '0 0' DAY(1) TO HOUR");

        // alternate precision
        checkExp(
            "interval '2345 2' day(4) to hour",
            "INTERVAL '2345 2' DAY(4) TO HOUR");

        // sign
        checkExp(
            "interval '-1 2' day to hour",
            "INTERVAL '-1 2' DAY TO HOUR");
        checkExp(
            "interval '+1 2' day to hour",
            "INTERVAL '+1 2' DAY TO HOUR");
        checkExp(
            "interval +'1 2' day to hour",
            "INTERVAL '1 2' DAY TO HOUR");
        checkExp(
            "interval +'-1 2' day to hour",
            "INTERVAL '-1 2' DAY TO HOUR");
        checkExp(
            "interval +'+1 2' day to hour",
            "INTERVAL '+1 2' DAY TO HOUR");
        checkExp(
            "interval -'1 2' day to hour",
            "INTERVAL -'1 2' DAY TO HOUR");
        checkExp(
            "interval -'-1 2' day to hour",
            "INTERVAL -'-1 2' DAY TO HOUR");
        checkExp(
            "interval -'+1 2' day to hour",
            "INTERVAL -'+1 2' DAY TO HOUR");
    }

    /**
     * Runs tests for INTERVAL... DAY TO MINUTE that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayToMinutePositive() {
        // default precision
        checkExp(
            "interval '1 2:3' day to minute",
            "INTERVAL '1 2:3' DAY TO MINUTE");
        checkExp(
            "interval '99 23:59' day to minute",
            "INTERVAL '99 23:59' DAY TO MINUTE");
        checkExp(
            "interval '99 0:0' day to minute",
            "INTERVAL '99 0:0' DAY TO MINUTE");

        // explicit precision equal to default
        checkExp(
            "interval '1 2:3' day(2) to minute",
            "INTERVAL '1 2:3' DAY(2) TO MINUTE");
        checkExp(
            "interval '99 23:59' day(2) to minute",
            "INTERVAL '99 23:59' DAY(2) TO MINUTE");
        checkExp(
            "interval '99 0:0' day(2) to minute",
            "INTERVAL '99 0:0' DAY(2) TO MINUTE");

        // max precision
        checkExp(
            "interval '2147483647 23:59' day(10) to minute",
            "INTERVAL '2147483647 23:59' DAY(10) TO MINUTE");

        // min precision
        checkExp(
            "interval '0 0:0' day(1) to minute",
            "INTERVAL '0 0:0' DAY(1) TO MINUTE");

        // alternate precision
        checkExp(
            "interval '2345 6:7' day(4) to minute",
            "INTERVAL '2345 6:7' DAY(4) TO MINUTE");

        // sign
        checkExp(
            "interval '-1 2:3' day to minute",
            "INTERVAL '-1 2:3' DAY TO MINUTE");
        checkExp(
            "interval '+1 2:3' day to minute",
            "INTERVAL '+1 2:3' DAY TO MINUTE");
        checkExp(
            "interval +'1 2:3' day to minute",
            "INTERVAL '1 2:3' DAY TO MINUTE");
        checkExp(
            "interval +'-1 2:3' day to minute",
            "INTERVAL '-1 2:3' DAY TO MINUTE");
        checkExp(
            "interval +'+1 2:3' day to minute",
            "INTERVAL '+1 2:3' DAY TO MINUTE");
        checkExp(
            "interval -'1 2:3' day to minute",
            "INTERVAL -'1 2:3' DAY TO MINUTE");
        checkExp(
            "interval -'-1 2:3' day to minute",
            "INTERVAL -'-1 2:3' DAY TO MINUTE");
        checkExp(
            "interval -'+1 2:3' day to minute",
            "INTERVAL -'+1 2:3' DAY TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... DAY TO SECOND that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayToSecondPositive() {
        // default precision
        checkExp(
            "interval '1 2:3:4' day to second",
            "INTERVAL '1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval '99 23:59:59' day to second",
            "INTERVAL '99 23:59:59' DAY TO SECOND");
        checkExp(
            "interval '99 0:0:0' day to second",
            "INTERVAL '99 0:0:0' DAY TO SECOND");
        checkExp(
            "interval '99 23:59:59.999999' day to second",
            "INTERVAL '99 23:59:59.999999' DAY TO SECOND");
        checkExp(
            "interval '99 0:0:0.0' day to second",
            "INTERVAL '99 0:0:0.0' DAY TO SECOND");

        // explicit precision equal to default
        checkExp(
            "interval '1 2:3:4' day(2) to second",
            "INTERVAL '1 2:3:4' DAY(2) TO SECOND");
        checkExp(
            "interval '99 23:59:59' day(2) to second",
            "INTERVAL '99 23:59:59' DAY(2) TO SECOND");
        checkExp(
            "interval '99 0:0:0' day(2) to second",
            "INTERVAL '99 0:0:0' DAY(2) TO SECOND");
        checkExp(
            "interval '99 23:59:59.999999' day to second(6)",
            "INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)");
        checkExp(
            "interval '99 0:0:0.0' day to second(6)",
            "INTERVAL '99 0:0:0.0' DAY TO SECOND(6)");

        // max precision
        checkExp(
            "interval '2147483647 23:59:59' day(10) to second",
            "INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND");
        checkExp(
            "interval '2147483647 23:59:59.999999999' day(10) to second(9)",
            "INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)");

        // min precision
        checkExp(
            "interval '0 0:0:0' day(1) to second",
            "INTERVAL '0 0:0:0' DAY(1) TO SECOND");
        checkExp(
            "interval '0 0:0:0.0' day(1) to second(1)",
            "INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)");

        // alternate precision
        checkExp(
            "interval '2345 6:7:8' day(4) to second",
            "INTERVAL '2345 6:7:8' DAY(4) TO SECOND");
        checkExp(
            "interval '2345 6:7:8.9012' day(4) to second(4)",
            "INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)");

        // sign
        checkExp(
            "interval '-1 2:3:4' day to second",
            "INTERVAL '-1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval '+1 2:3:4' day to second",
            "INTERVAL '+1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval +'1 2:3:4' day to second",
            "INTERVAL '1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval +'-1 2:3:4' day to second",
            "INTERVAL '-1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval +'+1 2:3:4' day to second",
            "INTERVAL '+1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval -'1 2:3:4' day to second",
            "INTERVAL -'1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval -'-1 2:3:4' day to second",
            "INTERVAL -'-1 2:3:4' DAY TO SECOND");
        checkExp(
            "interval -'+1 2:3:4' day to second",
            "INTERVAL -'+1 2:3:4' DAY TO SECOND");
    }

    /**
     * Runs tests for INTERVAL... HOUR that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourPositive() {
        // default precision
        checkExp(
            "interval '1' hour",
            "INTERVAL '1' HOUR");
        checkExp(
            "interval '99' hour",
            "INTERVAL '99' HOUR");

        // explicit precision equal to default
        checkExp(
            "interval '1' hour(2)",
            "INTERVAL '1' HOUR(2)");
        checkExp(
            "interval '99' hour(2)",
            "INTERVAL '99' HOUR(2)");

        // max precision
        checkExp(
            "interval '2147483647' hour(10)",
            "INTERVAL '2147483647' HOUR(10)");

        // min precision
        checkExp(
            "interval '0' hour(1)",
            "INTERVAL '0' HOUR(1)");

        // alternate precision
        checkExp(
            "interval '1234' hour(4)",
            "INTERVAL '1234' HOUR(4)");

        // sign
        checkExp(
            "interval '+1' hour",
            "INTERVAL '+1' HOUR");
        checkExp(
            "interval '-1' hour",
            "INTERVAL '-1' HOUR");
        checkExp(
            "interval +'1' hour",
            "INTERVAL '1' HOUR");
        checkExp(
            "interval +'+1' hour",
            "INTERVAL '+1' HOUR");
        checkExp(
            "interval +'-1' hour",
            "INTERVAL '-1' HOUR");
        checkExp(
            "interval -'1' hour",
            "INTERVAL -'1' HOUR");
        checkExp(
            "interval -'+1' hour",
            "INTERVAL -'+1' HOUR");
        checkExp(
            "interval -'-1' hour",
            "INTERVAL -'-1' HOUR");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO MINUTE that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourToMinutePositive() {
        // default precision
        checkExp(
            "interval '2:3' hour to minute",
            "INTERVAL '2:3' HOUR TO MINUTE");
        checkExp(
            "interval '23:59' hour to minute",
            "INTERVAL '23:59' HOUR TO MINUTE");
        checkExp(
            "interval '99:0' hour to minute",
            "INTERVAL '99:0' HOUR TO MINUTE");

        // explicit precision equal to default
        checkExp(
            "interval '2:3' hour(2) to minute",
            "INTERVAL '2:3' HOUR(2) TO MINUTE");
        checkExp(
            "interval '23:59' hour(2) to minute",
            "INTERVAL '23:59' HOUR(2) TO MINUTE");
        checkExp(
            "interval '99:0' hour(2) to minute",
            "INTERVAL '99:0' HOUR(2) TO MINUTE");

        // max precision
        checkExp(
            "interval '2147483647:59' hour(10) to minute",
            "INTERVAL '2147483647:59' HOUR(10) TO MINUTE");

        // min precision
        checkExp(
            "interval '0:0' hour(1) to minute",
            "INTERVAL '0:0' HOUR(1) TO MINUTE");

        // alternate precision
        checkExp(
            "interval '2345:7' hour(4) to minute",
            "INTERVAL '2345:7' HOUR(4) TO MINUTE");

        // sign
        checkExp(
            "interval '-1:3' hour to minute",
            "INTERVAL '-1:3' HOUR TO MINUTE");
        checkExp(
            "interval '+1:3' hour to minute",
            "INTERVAL '+1:3' HOUR TO MINUTE");
        checkExp(
            "interval +'2:3' hour to minute",
            "INTERVAL '2:3' HOUR TO MINUTE");
        checkExp(
            "interval +'-2:3' hour to minute",
            "INTERVAL '-2:3' HOUR TO MINUTE");
        checkExp(
            "interval +'+2:3' hour to minute",
            "INTERVAL '+2:3' HOUR TO MINUTE");
        checkExp(
            "interval -'2:3' hour to minute",
            "INTERVAL -'2:3' HOUR TO MINUTE");
        checkExp(
            "interval -'-2:3' hour to minute",
            "INTERVAL -'-2:3' HOUR TO MINUTE");
        checkExp(
            "interval -'+2:3' hour to minute",
            "INTERVAL -'+2:3' HOUR TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO SECOND that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourToSecondPositive() {
        // default precision
        checkExp(
            "interval '2:3:4' hour to second",
            "INTERVAL '2:3:4' HOUR TO SECOND");
        checkExp(
            "interval '23:59:59' hour to second",
            "INTERVAL '23:59:59' HOUR TO SECOND");
        checkExp(
            "interval '99:0:0' hour to second",
            "INTERVAL '99:0:0' HOUR TO SECOND");
        checkExp(
            "interval '23:59:59.999999' hour to second",
            "INTERVAL '23:59:59.999999' HOUR TO SECOND");
        checkExp(
            "interval '99:0:0.0' hour to second",
            "INTERVAL '99:0:0.0' HOUR TO SECOND");

        // explicit precision equal to default
        checkExp(
            "interval '2:3:4' hour(2) to second",
            "INTERVAL '2:3:4' HOUR(2) TO SECOND");
        checkExp(
            "interval '99:59:59' hour(2) to second",
            "INTERVAL '99:59:59' HOUR(2) TO SECOND");
        checkExp(
            "interval '99:0:0' hour(2) to second",
            "INTERVAL '99:0:0' HOUR(2) TO SECOND");
        checkExp(
            "interval '23:59:59.999999' hour to second(6)",
            "INTERVAL '23:59:59.999999' HOUR TO SECOND(6)");
        checkExp(
            "interval '99:0:0.0' hour to second(6)",
            "INTERVAL '99:0:0.0' HOUR TO SECOND(6)");

        // max precision
        checkExp(
            "interval '2147483647:59:59' hour(10) to second",
            "INTERVAL '2147483647:59:59' HOUR(10) TO SECOND");
        checkExp(
            "interval '2147483647:59:59.999999999' hour(10) to second(9)",
            "INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)");

        // min precision
        checkExp(
            "interval '0:0:0' hour(1) to second",
            "INTERVAL '0:0:0' HOUR(1) TO SECOND");
        checkExp(
            "interval '0:0:0.0' hour(1) to second(1)",
            "INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)");

        // alternate precision
        checkExp(
            "interval '2345:7:8' hour(4) to second",
            "INTERVAL '2345:7:8' HOUR(4) TO SECOND");
        checkExp(
            "interval '2345:7:8.9012' hour(4) to second(4)",
            "INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)");

        // sign
        checkExp(
            "interval '-2:3:4' hour to second",
            "INTERVAL '-2:3:4' HOUR TO SECOND");
        checkExp(
            "interval '+2:3:4' hour to second",
            "INTERVAL '+2:3:4' HOUR TO SECOND");
        checkExp(
            "interval +'2:3:4' hour to second",
            "INTERVAL '2:3:4' HOUR TO SECOND");
        checkExp(
            "interval +'-2:3:4' hour to second",
            "INTERVAL '-2:3:4' HOUR TO SECOND");
        checkExp(
            "interval +'+2:3:4' hour to second",
            "INTERVAL '+2:3:4' HOUR TO SECOND");
        checkExp(
            "interval -'2:3:4' hour to second",
            "INTERVAL -'2:3:4' HOUR TO SECOND");
        checkExp(
            "interval -'-2:3:4' hour to second",
            "INTERVAL -'-2:3:4' HOUR TO SECOND");
        checkExp(
            "interval -'+2:3:4' hour to second",
            "INTERVAL -'+2:3:4' HOUR TO SECOND");
    }

    /**
     * Runs tests for INTERVAL... MINUTE that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMinutePositive() {
        // default precision
        checkExp(
            "interval '1' minute",
            "INTERVAL '1' MINUTE");
        checkExp(
            "interval '99' minute",
            "INTERVAL '99' MINUTE");

        // explicit precision equal to default
        checkExp(
            "interval '1' minute(2)",
            "INTERVAL '1' MINUTE(2)");
        checkExp(
            "interval '99' minute(2)",
            "INTERVAL '99' MINUTE(2)");

        // max precision
        checkExp(
            "interval '2147483647' minute(10)",
            "INTERVAL '2147483647' MINUTE(10)");

        // min precision
        checkExp(
            "interval '0' minute(1)",
            "INTERVAL '0' MINUTE(1)");

        // alternate precision
        checkExp(
            "interval '1234' minute(4)",
            "INTERVAL '1234' MINUTE(4)");

        // sign
        checkExp(
            "interval '+1' minute",
            "INTERVAL '+1' MINUTE");
        checkExp(
            "interval '-1' minute",
            "INTERVAL '-1' MINUTE");
        checkExp(
            "interval +'1' minute",
            "INTERVAL '1' MINUTE");
        checkExp(
            "interval +'+1' minute",
            "INTERVAL '+1' MINUTE");
        checkExp(
            "interval +'+1' minute",
            "INTERVAL '+1' MINUTE");
        checkExp(
            "interval -'1' minute",
            "INTERVAL -'1' MINUTE");
        checkExp(
            "interval -'+1' minute",
            "INTERVAL -'+1' MINUTE");
        checkExp(
            "interval -'-1' minute",
            "INTERVAL -'-1' MINUTE");
    }

    /**
     * Runs tests for INTERVAL... MINUTE TO SECOND that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMinuteToSecondPositive() {
        // default precision
        checkExp(
            "interval '2:4' minute to second",
            "INTERVAL '2:4' MINUTE TO SECOND");
        checkExp(
            "interval '59:59' minute to second",
            "INTERVAL '59:59' MINUTE TO SECOND");
        checkExp(
            "interval '99:0' minute to second",
            "INTERVAL '99:0' MINUTE TO SECOND");
        checkExp(
            "interval '59:59.999999' minute to second",
            "INTERVAL '59:59.999999' MINUTE TO SECOND");
        checkExp(
            "interval '99:0.0' minute to second",
            "INTERVAL '99:0.0' MINUTE TO SECOND");

        // explicit precision equal to default
        checkExp(
            "interval '2:4' minute(2) to second",
            "INTERVAL '2:4' MINUTE(2) TO SECOND");
        checkExp(
            "interval '59:59' minute(2) to second",
            "INTERVAL '59:59' MINUTE(2) TO SECOND");
        checkExp(
            "interval '99:0' minute(2) to second",
            "INTERVAL '99:0' MINUTE(2) TO SECOND");
        checkExp(
            "interval '99:59.999999' minute to second(6)",
            "INTERVAL '99:59.999999' MINUTE TO SECOND(6)");
        checkExp(
            "interval '99:0.0' minute to second(6)",
            "INTERVAL '99:0.0' MINUTE TO SECOND(6)");

        // max precision
        checkExp(
            "interval '2147483647:59' minute(10) to second",
            "INTERVAL '2147483647:59' MINUTE(10) TO SECOND");
        checkExp(
            "interval '2147483647:59.999999999' minute(10) to second(9)",
            "INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)");

        // min precision
        checkExp(
            "interval '0:0' minute(1) to second",
            "INTERVAL '0:0' MINUTE(1) TO SECOND");
        checkExp(
            "interval '0:0.0' minute(1) to second(1)",
            "INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)");

        // alternate precision
        checkExp(
            "interval '2345:8' minute(4) to second",
            "INTERVAL '2345:8' MINUTE(4) TO SECOND");
        checkExp(
            "interval '2345:7.8901' minute(4) to second(4)",
            "INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)");

        // sign
        checkExp(
            "interval '-3:4' minute to second",
            "INTERVAL '-3:4' MINUTE TO SECOND");
        checkExp(
            "interval '+3:4' minute to second",
            "INTERVAL '+3:4' MINUTE TO SECOND");
        checkExp(
            "interval +'3:4' minute to second",
            "INTERVAL '3:4' MINUTE TO SECOND");
        checkExp(
            "interval +'-3:4' minute to second",
            "INTERVAL '-3:4' MINUTE TO SECOND");
        checkExp(
            "interval +'+3:4' minute to second",
            "INTERVAL '+3:4' MINUTE TO SECOND");
        checkExp(
            "interval -'3:4' minute to second",
            "INTERVAL -'3:4' MINUTE TO SECOND");
        checkExp(
            "interval -'-3:4' minute to second",
            "INTERVAL -'-3:4' MINUTE TO SECOND");
        checkExp(
            "interval -'+3:4' minute to second",
            "INTERVAL -'+3:4' MINUTE TO SECOND");
    }

    /**
     * Runs tests for INTERVAL... SECOND that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalSecondPositive() {
        // default precision
        checkExp(
            "interval '1' second",
            "INTERVAL '1' SECOND");
        checkExp(
            "interval '99' second",
            "INTERVAL '99' SECOND");

        // explicit precision equal to default
        checkExp(
            "interval '1' second(2)",
            "INTERVAL '1' SECOND(2)");
        checkExp(
            "interval '99' second(2)",
            "INTERVAL '99' SECOND(2)");
        checkExp(
            "interval '1' second(2,6)",
            "INTERVAL '1' SECOND(2, 6)");
        checkExp(
            "interval '99' second(2,6)",
            "INTERVAL '99' SECOND(2, 6)");

        // max precision
        checkExp(
            "interval '2147483647' second(10)",
            "INTERVAL '2147483647' SECOND(10)");
        checkExp(
            "interval '2147483647.999999999' second(9,9)",
            "INTERVAL '2147483647.999999999' SECOND(9, 9)");

        // min precision
        checkExp(
            "interval '0' second(1)",
            "INTERVAL '0' SECOND(1)");
        checkExp(
            "interval '0.0' second(1,1)",
            "INTERVAL '0.0' SECOND(1, 1)");

        // alternate precision
        checkExp(
            "interval '1234' second(4)",
            "INTERVAL '1234' SECOND(4)");
        checkExp(
            "interval '1234.56789' second(4,5)",
            "INTERVAL '1234.56789' SECOND(4, 5)");

        // sign
        checkExp(
            "interval '+1' second",
            "INTERVAL '+1' SECOND");
        checkExp(
            "interval '-1' second",
            "INTERVAL '-1' SECOND");
        checkExp(
            "interval +'1' second",
            "INTERVAL '1' SECOND");
        checkExp(
            "interval +'+1' second",
            "INTERVAL '+1' SECOND");
        checkExp(
            "interval +'-1' second",
            "INTERVAL '-1' SECOND");
        checkExp(
            "interval -'1' second",
            "INTERVAL -'1' SECOND");
        checkExp(
            "interval -'+1' second",
            "INTERVAL -'+1' SECOND");
        checkExp(
            "interval -'-1' second",
            "INTERVAL -'-1' SECOND");
    }

    /**
     * Runs tests for INTERVAL... YEAR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalYearFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' YEAR",
            "INTERVAL '-' YEAR");
        checkExp(
            "INTERVAL '1-2' YEAR",
            "INTERVAL '1-2' YEAR");
        checkExp(
            "INTERVAL '1.2' YEAR",
            "INTERVAL '1.2' YEAR");
        checkExp(
            "INTERVAL '1 2' YEAR",
            "INTERVAL '1 2' YEAR");
        checkExp(
            "INTERVAL '1-2' YEAR(2)",
            "INTERVAL '1-2' YEAR(2)");
        checkExp(
            "INTERVAL 'bogus text' YEAR",
            "INTERVAL 'bogus text' YEAR");

        // negative field values
        checkExp(
            "INTERVAL '--1' YEAR",
            "INTERVAL '--1' YEAR");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' YEAR",
            "INTERVAL '100' YEAR");
        checkExp(
            "INTERVAL '100' YEAR(2)",
            "INTERVAL '100' YEAR(2)");
        checkExp(
            "INTERVAL '1000' YEAR(3)",
            "INTERVAL '1000' YEAR(3)");
        checkExp(
            "INTERVAL '-1000' YEAR(3)",
            "INTERVAL '-1000' YEAR(3)");
        checkExp(
            "INTERVAL '2147483648' YEAR(10)",
            "INTERVAL '2147483648' YEAR(10)");
        checkExp(
            "INTERVAL '-2147483648' YEAR(10)",
            "INTERVAL '-2147483648' YEAR(10)");

        // precision > maximum
        checkExp(
            "INTERVAL '1' YEAR(11)",
            "INTERVAL '1' YEAR(11)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' YEAR(0)",
            "INTERVAL '0' YEAR(0)");
    }

    /**
     * Runs tests for INTERVAL... YEAR TO MONTH that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalYearToMonthFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' YEAR TO MONTH",
            "INTERVAL '-' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1' YEAR TO MONTH",
            "INTERVAL '1' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1:2' YEAR TO MONTH",
            "INTERVAL '1:2' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1.2' YEAR TO MONTH",
            "INTERVAL '1.2' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1 2' YEAR TO MONTH",
            "INTERVAL '1 2' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1:2' YEAR(2) TO MONTH",
            "INTERVAL '1:2' YEAR(2) TO MONTH");
        checkExp(
            "INTERVAL 'bogus text' YEAR TO MONTH",
            "INTERVAL 'bogus text' YEAR TO MONTH");

        // negative field values
        checkExp(
            "INTERVAL '--1-2' YEAR TO MONTH",
            "INTERVAL '--1-2' YEAR TO MONTH");
        checkExp(
            "INTERVAL '1--2' YEAR TO MONTH",
            "INTERVAL '1--2' YEAR TO MONTH");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100-0' YEAR TO MONTH",
            "INTERVAL '100-0' YEAR TO MONTH");
        checkExp(
            "INTERVAL '100-0' YEAR(2) TO MONTH",
            "INTERVAL '100-0' YEAR(2) TO MONTH");
        checkExp(
            "INTERVAL '1000-0' YEAR(3) TO MONTH",
            "INTERVAL '1000-0' YEAR(3) TO MONTH");
        checkExp(
            "INTERVAL '-1000-0' YEAR(3) TO MONTH",
            "INTERVAL '-1000-0' YEAR(3) TO MONTH");
        checkExp(
            "INTERVAL '2147483648-0' YEAR(10) TO MONTH",
            "INTERVAL '2147483648-0' YEAR(10) TO MONTH");
        checkExp(
            "INTERVAL '-2147483648-0' YEAR(10) TO MONTH",
            "INTERVAL '-2147483648-0' YEAR(10) TO MONTH");
        checkExp(
            "INTERVAL '1-12' YEAR TO MONTH",
            "INTERVAL '1-12' YEAR TO MONTH");

        // precision > maximum
        checkExp(
            "INTERVAL '1-1' YEAR(11) TO MONTH",
            "INTERVAL '1-1' YEAR(11) TO MONTH");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0-0' YEAR(0) TO MONTH",
            "INTERVAL '0-0' YEAR(0) TO MONTH");
    }

    /**
     * Runs tests for INTERVAL... MONTH that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalMonthFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' MONTH",
            "INTERVAL '-' MONTH");
        checkExp(
            "INTERVAL '1-2' MONTH",
            "INTERVAL '1-2' MONTH");
        checkExp(
            "INTERVAL '1.2' MONTH",
            "INTERVAL '1.2' MONTH");
        checkExp(
            "INTERVAL '1 2' MONTH",
            "INTERVAL '1 2' MONTH");
        checkExp(
            "INTERVAL '1-2' MONTH(2)",
            "INTERVAL '1-2' MONTH(2)");
        checkExp(
            "INTERVAL 'bogus text' MONTH",
            "INTERVAL 'bogus text' MONTH");

        // negative field values
        checkExp(
            "INTERVAL '--1' MONTH",
            "INTERVAL '--1' MONTH");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' MONTH",
            "INTERVAL '100' MONTH");
        checkExp(
            "INTERVAL '100' MONTH(2)",
            "INTERVAL '100' MONTH(2)");
        checkExp(
            "INTERVAL '1000' MONTH(3)",
            "INTERVAL '1000' MONTH(3)");
        checkExp(
            "INTERVAL '-1000' MONTH(3)",
            "INTERVAL '-1000' MONTH(3)");
        checkExp(
            "INTERVAL '2147483648' MONTH(10)",
            "INTERVAL '2147483648' MONTH(10)");
        checkExp(
            "INTERVAL '-2147483648' MONTH(10)",
            "INTERVAL '-2147483648' MONTH(10)");

        // precision > maximum
        checkExp(
            "INTERVAL '1' MONTH(11)",
            "INTERVAL '1' MONTH(11)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' MONTH(0)",
            "INTERVAL '0' MONTH(0)");
    }

    /**
     * Runs tests for INTERVAL... DAY that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalDayFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' DAY",
            "INTERVAL '-' DAY");
        checkExp(
            "INTERVAL '1-2' DAY",
            "INTERVAL '1-2' DAY");
        checkExp(
            "INTERVAL '1.2' DAY",
            "INTERVAL '1.2' DAY");
        checkExp(
            "INTERVAL '1 2' DAY",
            "INTERVAL '1 2' DAY");
        checkExp(
            "INTERVAL '1:2' DAY",
            "INTERVAL '1:2' DAY");
        checkExp(
            "INTERVAL '1-2' DAY(2)",
            "INTERVAL '1-2' DAY(2)");
        checkExp(
            "INTERVAL 'bogus text' DAY",
            "INTERVAL 'bogus text' DAY");

        // negative field values
        checkExp(
            "INTERVAL '--1' DAY",
            "INTERVAL '--1' DAY");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' DAY",
            "INTERVAL '100' DAY");
        checkExp(
            "INTERVAL '100' DAY(2)",
            "INTERVAL '100' DAY(2)");
        checkExp(
            "INTERVAL '1000' DAY(3)",
            "INTERVAL '1000' DAY(3)");
        checkExp(
            "INTERVAL '-1000' DAY(3)",
            "INTERVAL '-1000' DAY(3)");
        checkExp(
            "INTERVAL '2147483648' DAY(10)",
            "INTERVAL '2147483648' DAY(10)");
        checkExp(
            "INTERVAL '-2147483648' DAY(10)",
            "INTERVAL '-2147483648' DAY(10)");

        // precision > maximum
        checkExp(
            "INTERVAL '1' DAY(11)",
            "INTERVAL '1' DAY(11)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' DAY(0)",
            "INTERVAL '0' DAY(0)");
    }

    /**
     * Runs tests for INTERVAL... DAY TO HOUR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalDayToHourFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' DAY TO HOUR",
            "INTERVAL '-' DAY TO HOUR");
        checkExp(
            "INTERVAL '1' DAY TO HOUR",
            "INTERVAL '1' DAY TO HOUR");
        checkExp(
            "INTERVAL '1:2' DAY TO HOUR",
            "INTERVAL '1:2' DAY TO HOUR");
        checkExp(
            "INTERVAL '1.2' DAY TO HOUR",
            "INTERVAL '1.2' DAY TO HOUR");
        checkExp(
            "INTERVAL '1 x' DAY TO HOUR",
            "INTERVAL '1 x' DAY TO HOUR");
        checkExp(
            "INTERVAL ' ' DAY TO HOUR",
            "INTERVAL ' ' DAY TO HOUR");
        checkExp(
            "INTERVAL '1:2' DAY(2) TO HOUR",
            "INTERVAL '1:2' DAY(2) TO HOUR");
        checkExp(
            "INTERVAL 'bogus text' DAY TO HOUR",
            "INTERVAL 'bogus text' DAY TO HOUR");

        // negative field values
        checkExp(
            "INTERVAL '--1 1' DAY TO HOUR",
            "INTERVAL '--1 1' DAY TO HOUR");
        checkExp(
            "INTERVAL '1 -1' DAY TO HOUR",
            "INTERVAL '1 -1' DAY TO HOUR");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100 0' DAY TO HOUR",
            "INTERVAL '100 0' DAY TO HOUR");
        checkExp(
            "INTERVAL '100 0' DAY(2) TO HOUR",
            "INTERVAL '100 0' DAY(2) TO HOUR");
        checkExp(
            "INTERVAL '1000 0' DAY(3) TO HOUR",
            "INTERVAL '1000 0' DAY(3) TO HOUR");
        checkExp(
            "INTERVAL '-1000 0' DAY(3) TO HOUR",
            "INTERVAL '-1000 0' DAY(3) TO HOUR");
        checkExp(
            "INTERVAL '2147483648 0' DAY(10) TO HOUR",
            "INTERVAL '2147483648 0' DAY(10) TO HOUR");
        checkExp(
            "INTERVAL '-2147483648 0' DAY(10) TO HOUR",
            "INTERVAL '-2147483648 0' DAY(10) TO HOUR");
        checkExp(
            "INTERVAL '1 24' DAY TO HOUR",
            "INTERVAL '1 24' DAY TO HOUR");

        // precision > maximum
        checkExp(
            "INTERVAL '1 1' DAY(11) TO HOUR",
            "INTERVAL '1 1' DAY(11) TO HOUR");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0 0' DAY(0) TO HOUR",
            "INTERVAL '0 0' DAY(0) TO HOUR");
    }

    /**
     * Runs tests for INTERVAL... DAY TO MINUTE that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalDayToMinuteFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL ' :' DAY TO MINUTE",
            "INTERVAL ' :' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1' DAY TO MINUTE",
            "INTERVAL '1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 2' DAY TO MINUTE",
            "INTERVAL '1 2' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1:2' DAY TO MINUTE",
            "INTERVAL '1:2' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1.2' DAY TO MINUTE",
            "INTERVAL '1.2' DAY TO MINUTE");
        checkExp(
            "INTERVAL 'x 1:1' DAY TO MINUTE",
            "INTERVAL 'x 1:1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 x:1' DAY TO MINUTE",
            "INTERVAL '1 x:1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:x' DAY TO MINUTE",
            "INTERVAL '1 1:x' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:2:3' DAY TO MINUTE",
            "INTERVAL '1 1:2:3' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:1:1.2' DAY TO MINUTE",
            "INTERVAL '1 1:1:1.2' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:2:3' DAY(2) TO MINUTE",
            "INTERVAL '1 1:2:3' DAY(2) TO MINUTE");
        checkExp(
            "INTERVAL '1 1' DAY(2) TO MINUTE",
            "INTERVAL '1 1' DAY(2) TO MINUTE");
        checkExp(
            "INTERVAL 'bogus text' DAY TO MINUTE",
            "INTERVAL 'bogus text' DAY TO MINUTE");

        // negative field values
        checkExp(
            "INTERVAL '--1 1:1' DAY TO MINUTE",
            "INTERVAL '--1 1:1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 -1:1' DAY TO MINUTE",
            "INTERVAL '1 -1:1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:-1' DAY TO MINUTE",
            "INTERVAL '1 1:-1' DAY TO MINUTE");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100 0' DAY TO MINUTE",
            "INTERVAL '100 0' DAY TO MINUTE");
        checkExp(
            "INTERVAL '100 0' DAY(2) TO MINUTE",
            "INTERVAL '100 0' DAY(2) TO MINUTE");
        checkExp(
            "INTERVAL '1000 0' DAY(3) TO MINUTE",
            "INTERVAL '1000 0' DAY(3) TO MINUTE");
        checkExp(
            "INTERVAL '-1000 0' DAY(3) TO MINUTE",
            "INTERVAL '-1000 0' DAY(3) TO MINUTE");
        checkExp(
            "INTERVAL '2147483648 0' DAY(10) TO MINUTE",
            "INTERVAL '2147483648 0' DAY(10) TO MINUTE");
        checkExp(
            "INTERVAL '-2147483648 0' DAY(10) TO MINUTE",
            "INTERVAL '-2147483648 0' DAY(10) TO MINUTE");
        checkExp(
            "INTERVAL '1 24:1' DAY TO MINUTE",
            "INTERVAL '1 24:1' DAY TO MINUTE");
        checkExp(
            "INTERVAL '1 1:60' DAY TO MINUTE",
            "INTERVAL '1 1:60' DAY TO MINUTE");

        // precision > maximum
        checkExp(
            "INTERVAL '1 1' DAY(11) TO MINUTE",
            "INTERVAL '1 1' DAY(11) TO MINUTE");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0 0' DAY(0) TO MINUTE",
            "INTERVAL '0 0' DAY(0) TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... DAY TO SECOND that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalDayToSecondFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL ' ::' DAY TO SECOND",
            "INTERVAL ' ::' DAY TO SECOND");
        checkExp(
            "INTERVAL ' ::.' DAY TO SECOND",
            "INTERVAL ' ::.' DAY TO SECOND");
        checkExp(
            "INTERVAL '1' DAY TO SECOND",
            "INTERVAL '1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 2' DAY TO SECOND",
            "INTERVAL '1 2' DAY TO SECOND");
        checkExp(
            "INTERVAL '1:2' DAY TO SECOND",
            "INTERVAL '1:2' DAY TO SECOND");
        checkExp(
            "INTERVAL '1.2' DAY TO SECOND",
            "INTERVAL '1.2' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' DAY TO SECOND",
            "INTERVAL '1 1:2' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:2:x' DAY TO SECOND",
            "INTERVAL '1 1:2:x' DAY TO SECOND");
        checkExp(
            "INTERVAL '1:2:3' DAY TO SECOND",
            "INTERVAL '1:2:3' DAY TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.2' DAY TO SECOND",
            "INTERVAL '1:1:1.2' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' DAY(2) TO SECOND",
            "INTERVAL '1 1:2' DAY(2) TO SECOND");
        checkExp(
            "INTERVAL '1 1' DAY(2) TO SECOND",
            "INTERVAL '1 1' DAY(2) TO SECOND");
        checkExp(
            "INTERVAL 'bogus text' DAY TO SECOND",
            "INTERVAL 'bogus text' DAY TO SECOND");
        checkExp(
            "INTERVAL '2345 6:7:8901' DAY TO SECOND(4)",
            "INTERVAL '2345 6:7:8901' DAY TO SECOND(4)");

        // negative field values
        checkExp(
            "INTERVAL '--1 1:1:1' DAY TO SECOND",
            "INTERVAL '--1 1:1:1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 -1:1:1' DAY TO SECOND",
            "INTERVAL '1 -1:1:1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:-1:1' DAY TO SECOND",
            "INTERVAL '1 1:-1:1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:1:-1' DAY TO SECOND",
            "INTERVAL '1 1:1:-1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:1:1.-1' DAY TO SECOND",
            "INTERVAL '1 1:1:1.-1' DAY TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100 0' DAY TO SECOND",
            "INTERVAL '100 0' DAY TO SECOND");
        checkExp(
            "INTERVAL '100 0' DAY(2) TO SECOND",
            "INTERVAL '100 0' DAY(2) TO SECOND");
        checkExp(
            "INTERVAL '1000 0' DAY(3) TO SECOND",
            "INTERVAL '1000 0' DAY(3) TO SECOND");
        checkExp(
            "INTERVAL '-1000 0' DAY(3) TO SECOND",
            "INTERVAL '-1000 0' DAY(3) TO SECOND");
        checkExp(
            "INTERVAL '2147483648 0' DAY(10) TO SECOND",
            "INTERVAL '2147483648 0' DAY(10) TO SECOND");
        checkExp(
            "INTERVAL '-2147483648 0' DAY(10) TO SECOND",
            "INTERVAL '-2147483648 0' DAY(10) TO SECOND");
        checkExp(
            "INTERVAL '1 24:1:1' DAY TO SECOND",
            "INTERVAL '1 24:1:1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:60:1' DAY TO SECOND",
            "INTERVAL '1 1:60:1' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:1:60' DAY TO SECOND",
            "INTERVAL '1 1:1:60' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:1:1.0000001' DAY TO SECOND",
            "INTERVAL '1 1:1:1.0000001' DAY TO SECOND");
        checkExp(
            "INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)",
            "INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)");

        // precision > maximum
        checkExp(
            "INTERVAL '1 1' DAY(11) TO SECOND",
            "INTERVAL '1 1' DAY(11) TO SECOND");
        checkExp(
            "INTERVAL '1 1' DAY TO SECOND(10)",
            "INTERVAL '1 1' DAY TO SECOND(10)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0 0:0:0' DAY(0) TO SECOND",
            "INTERVAL '0 0:0:0' DAY(0) TO SECOND");
        checkExp(
            "INTERVAL '0 0:0:0' DAY TO SECOND(0)",
            "INTERVAL '0 0:0:0' DAY TO SECOND(0)");
    }

    /**
     * Runs tests for INTERVAL... HOUR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalHourFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' HOUR",
            "INTERVAL '-' HOUR");
        checkExp(
            "INTERVAL '1-2' HOUR",
            "INTERVAL '1-2' HOUR");
        checkExp(
            "INTERVAL '1.2' HOUR",
            "INTERVAL '1.2' HOUR");
        checkExp(
            "INTERVAL '1 2' HOUR",
            "INTERVAL '1 2' HOUR");
        checkExp(
            "INTERVAL '1:2' HOUR",
            "INTERVAL '1:2' HOUR");
        checkExp(
            "INTERVAL '1-2' HOUR(2)",
            "INTERVAL '1-2' HOUR(2)");
        checkExp(
            "INTERVAL 'bogus text' HOUR",
            "INTERVAL 'bogus text' HOUR");

        // negative field values
        checkExp(
            "INTERVAL '--1' HOUR",
            "INTERVAL '--1' HOUR");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' HOUR",
            "INTERVAL '100' HOUR");
        checkExp(
            "INTERVAL '100' HOUR(2)",
            "INTERVAL '100' HOUR(2)");
        checkExp(
            "INTERVAL '1000' HOUR(3)",
            "INTERVAL '1000' HOUR(3)");
        checkExp(
            "INTERVAL '-1000' HOUR(3)",
            "INTERVAL '-1000' HOUR(3)");
        checkExp(
            "INTERVAL '2147483648' HOUR(10)",
            "INTERVAL '2147483648' HOUR(10)");
        checkExp(
            "INTERVAL '-2147483648' HOUR(10)",
            "INTERVAL '-2147483648' HOUR(10)");

        // negative field values
        checkExp(
            "INTERVAL '--1' HOUR",
            "INTERVAL '--1' HOUR");

        // precision > maximum
        checkExp(
            "INTERVAL '1' HOUR(11)",
            "INTERVAL '1' HOUR(11)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' HOUR(0)",
            "INTERVAL '0' HOUR(0)");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO MINUTE that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalHourToMinuteFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL ':' HOUR TO MINUTE",
            "INTERVAL ':' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1' HOUR TO MINUTE",
            "INTERVAL '1' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1:x' HOUR TO MINUTE",
            "INTERVAL '1:x' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1.2' HOUR TO MINUTE",
            "INTERVAL '1.2' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1 2' HOUR TO MINUTE",
            "INTERVAL '1 2' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1:2:3' HOUR TO MINUTE",
            "INTERVAL '1:2:3' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1 2' HOUR(2) TO MINUTE",
            "INTERVAL '1 2' HOUR(2) TO MINUTE");
        checkExp(
            "INTERVAL 'bogus text' HOUR TO MINUTE",
            "INTERVAL 'bogus text' HOUR TO MINUTE");

        // negative field values
        checkExp(
            "INTERVAL '--1:1' HOUR TO MINUTE",
            "INTERVAL '--1:1' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '1:-1' HOUR TO MINUTE",
            "INTERVAL '1:-1' HOUR TO MINUTE");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100:0' HOUR TO MINUTE",
            "INTERVAL '100:0' HOUR TO MINUTE");
        checkExp(
            "INTERVAL '100:0' HOUR(2) TO MINUTE",
            "INTERVAL '100:0' HOUR(2) TO MINUTE");
        checkExp(
            "INTERVAL '1000:0' HOUR(3) TO MINUTE",
            "INTERVAL '1000:0' HOUR(3) TO MINUTE");
        checkExp(
            "INTERVAL '-1000:0' HOUR(3) TO MINUTE",
            "INTERVAL '-1000:0' HOUR(3) TO MINUTE");
        checkExp(
            "INTERVAL '2147483648:0' HOUR(10) TO MINUTE",
            "INTERVAL '2147483648:0' HOUR(10) TO MINUTE");
        checkExp(
            "INTERVAL '-2147483648:0' HOUR(10) TO MINUTE",
            "INTERVAL '-2147483648:0' HOUR(10) TO MINUTE");
        checkExp(
            "INTERVAL '1:24' HOUR TO MINUTE",
            "INTERVAL '1:24' HOUR TO MINUTE");

        // precision > maximum
        checkExp(
            "INTERVAL '1:1' HOUR(11) TO MINUTE",
            "INTERVAL '1:1' HOUR(11) TO MINUTE");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0:0' HOUR(0) TO MINUTE",
            "INTERVAL '0:0' HOUR(0) TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO SECOND that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalHourToSecondFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '::' HOUR TO SECOND",
            "INTERVAL '::' HOUR TO SECOND");
        checkExp(
            "INTERVAL '::.' HOUR TO SECOND",
            "INTERVAL '::.' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1' HOUR TO SECOND",
            "INTERVAL '1' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1 2' HOUR TO SECOND",
            "INTERVAL '1 2' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:2' HOUR TO SECOND",
            "INTERVAL '1:2' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1.2' HOUR TO SECOND",
            "INTERVAL '1.2' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' HOUR TO SECOND",
            "INTERVAL '1 1:2' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:2:x' HOUR TO SECOND",
            "INTERVAL '1:2:x' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:x:3' HOUR TO SECOND",
            "INTERVAL '1:x:3' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.x' HOUR TO SECOND",
            "INTERVAL '1:1:1.x' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' HOUR(2) TO SECOND",
            "INTERVAL '1 1:2' HOUR(2) TO SECOND");
        checkExp(
            "INTERVAL '1 1' HOUR(2) TO SECOND",
            "INTERVAL '1 1' HOUR(2) TO SECOND");
        checkExp(
            "INTERVAL 'bogus text' HOUR TO SECOND",
            "INTERVAL 'bogus text' HOUR TO SECOND");
        checkExp(
            "INTERVAL '6:7:8901' HOUR TO SECOND(4)",
            "INTERVAL '6:7:8901' HOUR TO SECOND(4)");

        // negative field values
        checkExp(
            "INTERVAL '--1:1:1' HOUR TO SECOND",
            "INTERVAL '--1:1:1' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:-1:1' HOUR TO SECOND",
            "INTERVAL '1:-1:1' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:-1' HOUR TO SECOND",
            "INTERVAL '1:1:-1' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.-1' HOUR TO SECOND",
            "INTERVAL '1:1:1.-1' HOUR TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100:0:0' HOUR TO SECOND",
            "INTERVAL '100:0:0' HOUR TO SECOND");
        checkExp(
            "INTERVAL '100:0:0' HOUR(2) TO SECOND",
            "INTERVAL '100:0:0' HOUR(2) TO SECOND");
        checkExp(
            "INTERVAL '1000:0:0' HOUR(3) TO SECOND",
            "INTERVAL '1000:0:0' HOUR(3) TO SECOND");
        checkExp(
            "INTERVAL '-1000:0:0' HOUR(3) TO SECOND",
            "INTERVAL '-1000:0:0' HOUR(3) TO SECOND");
        checkExp(
            "INTERVAL '2147483648:0:0' HOUR(10) TO SECOND",
            "INTERVAL '2147483648:0:0' HOUR(10) TO SECOND");
        checkExp(
            "INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND",
            "INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND");
        checkExp(
            "INTERVAL '1:60:1' HOUR TO SECOND",
            "INTERVAL '1:60:1' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:60' HOUR TO SECOND",
            "INTERVAL '1:1:60' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.0000001' HOUR TO SECOND",
            "INTERVAL '1:1:1.0000001' HOUR TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.0001' HOUR TO SECOND(3)",
            "INTERVAL '1:1:1.0001' HOUR TO SECOND(3)");

        // precision > maximum
        checkExp(
            "INTERVAL '1:1:1' HOUR(11) TO SECOND",
            "INTERVAL '1:1:1' HOUR(11) TO SECOND");
        checkExp(
            "INTERVAL '1:1:1' HOUR TO SECOND(10)",
            "INTERVAL '1:1:1' HOUR TO SECOND(10)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0:0:0' HOUR(0) TO SECOND",
            "INTERVAL '0:0:0' HOUR(0) TO SECOND");
        checkExp(
            "INTERVAL '0:0:0' HOUR TO SECOND(0)",
            "INTERVAL '0:0:0' HOUR TO SECOND(0)");
    }

    /**
     * Runs tests for INTERVAL... MINUTE that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalMinuteFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL '-' MINUTE",
            "INTERVAL '-' MINUTE");
        checkExp(
            "INTERVAL '1-2' MINUTE",
            "INTERVAL '1-2' MINUTE");
        checkExp(
            "INTERVAL '1.2' MINUTE",
            "INTERVAL '1.2' MINUTE");
        checkExp(
            "INTERVAL '1 2' MINUTE",
            "INTERVAL '1 2' MINUTE");
        checkExp(
            "INTERVAL '1:2' MINUTE",
            "INTERVAL '1:2' MINUTE");
        checkExp(
            "INTERVAL '1-2' MINUTE(2)",
            "INTERVAL '1-2' MINUTE(2)");
        checkExp(
            "INTERVAL 'bogus text' MINUTE",
            "INTERVAL 'bogus text' MINUTE");

        // negative field values
        checkExp(
            "INTERVAL '--1' MINUTE",
            "INTERVAL '--1' MINUTE");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' MINUTE",
            "INTERVAL '100' MINUTE");
        checkExp(
            "INTERVAL '100' MINUTE(2)",
            "INTERVAL '100' MINUTE(2)");
        checkExp(
            "INTERVAL '1000' MINUTE(3)",
            "INTERVAL '1000' MINUTE(3)");
        checkExp(
            "INTERVAL '-1000' MINUTE(3)",
            "INTERVAL '-1000' MINUTE(3)");
        checkExp(
            "INTERVAL '2147483648' MINUTE(10)",
            "INTERVAL '2147483648' MINUTE(10)");
        checkExp(
            "INTERVAL '-2147483648' MINUTE(10)",
            "INTERVAL '-2147483648' MINUTE(10)");

        // precision > maximum
        checkExp(
            "INTERVAL '1' MINUTE(11)",
            "INTERVAL '1' MINUTE(11)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' MINUTE(0)",
            "INTERVAL '0' MINUTE(0)");
    }

    /**
     * Runs tests for INTERVAL... MINUTE TO SECOND that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalMinuteToSecondFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL ':' MINUTE TO SECOND",
            "INTERVAL ':' MINUTE TO SECOND");
        checkExp(
            "INTERVAL ':.' MINUTE TO SECOND",
            "INTERVAL ':.' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1' MINUTE TO SECOND",
            "INTERVAL '1' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1 2' MINUTE TO SECOND",
            "INTERVAL '1 2' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1.2' MINUTE TO SECOND",
            "INTERVAL '1.2' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' MINUTE TO SECOND",
            "INTERVAL '1 1:2' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:x' MINUTE TO SECOND",
            "INTERVAL '1:x' MINUTE TO SECOND");
        checkExp(
            "INTERVAL 'x:3' MINUTE TO SECOND",
            "INTERVAL 'x:3' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:1.x' MINUTE TO SECOND",
            "INTERVAL '1:1.x' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1 1:2' MINUTE(2) TO SECOND",
            "INTERVAL '1 1:2' MINUTE(2) TO SECOND");
        checkExp(
            "INTERVAL '1 1' MINUTE(2) TO SECOND",
            "INTERVAL '1 1' MINUTE(2) TO SECOND");
        checkExp(
            "INTERVAL 'bogus text' MINUTE TO SECOND",
            "INTERVAL 'bogus text' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '7:8901' MINUTE TO SECOND(4)",
            "INTERVAL '7:8901' MINUTE TO SECOND(4)");

        // negative field values
        checkExp(
            "INTERVAL '--1:1' MINUTE TO SECOND",
            "INTERVAL '--1:1' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:-1' MINUTE TO SECOND",
            "INTERVAL '1:-1' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:1.-1' MINUTE TO SECOND",
            "INTERVAL '1:1.-1' MINUTE TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkExp(
            "INTERVAL '100:0' MINUTE TO SECOND",
            "INTERVAL '100:0' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '100:0' MINUTE(2) TO SECOND",
            "INTERVAL '100:0' MINUTE(2) TO SECOND");
        checkExp(
            "INTERVAL '1000:0' MINUTE(3) TO SECOND",
            "INTERVAL '1000:0' MINUTE(3) TO SECOND");
        checkExp(
            "INTERVAL '-1000:0' MINUTE(3) TO SECOND",
            "INTERVAL '-1000:0' MINUTE(3) TO SECOND");
        checkExp(
            "INTERVAL '2147483648:0' MINUTE(10) TO SECOND",
            "INTERVAL '2147483648:0' MINUTE(10) TO SECOND");
        checkExp(
            "INTERVAL '-2147483648:0' MINUTE(10) TO SECOND",
            "INTERVAL '-2147483648:0' MINUTE(10) TO SECOND");
        checkExp(
            "INTERVAL '1:60' MINUTE TO SECOND",
            "INTERVAL '1:60' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:1.0000001' MINUTE TO SECOND",
            "INTERVAL '1:1.0000001' MINUTE TO SECOND");
        checkExp(
            "INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)",
            "INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)");

        // precision > maximum
        checkExp(
            "INTERVAL '1:1' MINUTE(11) TO SECOND",
            "INTERVAL '1:1' MINUTE(11) TO SECOND");
        checkExp(
            "INTERVAL '1:1' MINUTE TO SECOND(10)",
            "INTERVAL '1:1' MINUTE TO SECOND(10)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0:0' MINUTE(0) TO SECOND",
            "INTERVAL '0:0' MINUTE(0) TO SECOND");
        checkExp(
            "INTERVAL '0:0' MINUTE TO SECOND(0)",
            "INTERVAL '0:0' MINUTE TO SECOND(0)");
    }

    /**
     * Runs tests for INTERVAL... SECOND that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlValidatorTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXFailsValidation() tests.
     */
    public void subTestIntervalSecondFailsValidation() {
        // Qualifier - field mismatches
        checkExp(
            "INTERVAL ':' SECOND",
            "INTERVAL ':' SECOND");
        checkExp(
            "INTERVAL '.' SECOND",
            "INTERVAL '.' SECOND");
        checkExp(
            "INTERVAL '1-2' SECOND",
            "INTERVAL '1-2' SECOND");
        checkExp(
            "INTERVAL '1.x' SECOND",
            "INTERVAL '1.x' SECOND");
        checkExp(
            "INTERVAL 'x.1' SECOND",
            "INTERVAL 'x.1' SECOND");
        checkExp(
            "INTERVAL '1 2' SECOND",
            "INTERVAL '1 2' SECOND");
        checkExp(
            "INTERVAL '1:2' SECOND",
            "INTERVAL '1:2' SECOND");
        checkExp(
            "INTERVAL '1-2' SECOND(2)",
            "INTERVAL '1-2' SECOND(2)");
        checkExp(
            "INTERVAL 'bogus text' SECOND",
            "INTERVAL 'bogus text' SECOND");

        // negative field values
        checkExp(
            "INTERVAL '--1' SECOND",
            "INTERVAL '--1' SECOND");
        checkExp(
            "INTERVAL '1.-1' SECOND",
            "INTERVAL '1.-1' SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkExp(
            "INTERVAL '100' SECOND",
            "INTERVAL '100' SECOND");
        checkExp(
            "INTERVAL '100' SECOND(2)",
            "INTERVAL '100' SECOND(2)");
        checkExp(
            "INTERVAL '1000' SECOND(3)",
            "INTERVAL '1000' SECOND(3)");
        checkExp(
            "INTERVAL '-1000' SECOND(3)",
            "INTERVAL '-1000' SECOND(3)");
        checkExp(
            "INTERVAL '2147483648' SECOND(10)",
            "INTERVAL '2147483648' SECOND(10)");
        checkExp(
            "INTERVAL '-2147483648' SECOND(10)",
            "INTERVAL '-2147483648' SECOND(10)");
        checkExp(
            "INTERVAL '1.0000001' SECOND",
            "INTERVAL '1.0000001' SECOND");
        checkExp(
            "INTERVAL '1.0000001' SECOND(2)",
            "INTERVAL '1.0000001' SECOND(2)");
        checkExp(
            "INTERVAL '1.0001' SECOND(2, 3)",
            "INTERVAL '1.0001' SECOND(2, 3)");
        checkExp(
            "INTERVAL '1.000000001' SECOND(2, 9)",
            "INTERVAL '1.000000001' SECOND(2, 9)");

        // precision > maximum
        checkExp(
            "INTERVAL '1' SECOND(11)",
            "INTERVAL '1' SECOND(11)");
        checkExp(
            "INTERVAL '1.1' SECOND(1, 10)",
            "INTERVAL '1.1' SECOND(1, 10)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExp(
            "INTERVAL '0' SECOND(0)",
            "INTERVAL '0' SECOND(0)");
        checkExp(
            "INTERVAL '0' SECOND(1, 0)",
            "INTERVAL '0' SECOND(1, 0)");
    }

    /**
     * Runs tests for each of the thirteen different main types of INTERVAL
     * qualifiers (YEAR, YEAR TO MONTH, etc.) Tests in this section fall into
     * two categories:
     *
     * <ul>
     * <li>xxxPositive: tests that should pass parser and validator</li>
     * <li>xxxFailsValidation: tests that should pass parser but fail validator
     * </li>
     * </ul>
     *
     * A substantially identical set of tests exists in SqlValidatorTest, and
     * any changes here should be synchronized there.
     */
    @Test public void testIntervalLiterals() {
        subTestIntervalYearPositive();
        subTestIntervalYearToMonthPositive();
        subTestIntervalMonthPositive();
        subTestIntervalDayPositive();
        subTestIntervalDayToHourPositive();
        subTestIntervalDayToMinutePositive();
        subTestIntervalDayToSecondPositive();
        subTestIntervalHourPositive();
        subTestIntervalHourToMinutePositive();
        subTestIntervalHourToSecondPositive();
        subTestIntervalMinutePositive();
        subTestIntervalMinuteToSecondPositive();
        subTestIntervalSecondPositive();

        subTestIntervalYearFailsValidation();
        subTestIntervalYearToMonthFailsValidation();
        subTestIntervalMonthFailsValidation();
        subTestIntervalDayFailsValidation();
        subTestIntervalDayToHourFailsValidation();
        subTestIntervalDayToMinuteFailsValidation();
        subTestIntervalDayToSecondFailsValidation();
        subTestIntervalHourFailsValidation();
        subTestIntervalHourToMinuteFailsValidation();
        subTestIntervalHourToSecondFailsValidation();
        subTestIntervalMinuteFailsValidation();
        subTestIntervalMinuteToSecondFailsValidation();
        subTestIntervalSecondFailsValidation();
    }

    @Test public void testUnparseableIntervalQualifiers() {
        // No qualifier
        checkExpFails(
            "interval '1^'^",
            "Encountered \"<EOF>\" at line 1, column 12\\.\n"
            + "Was expecting one of:\n"
            + "    \"YEAR\" \\.\\.\\.\n"
            + "    \"MONTH\" \\.\\.\\.\n"
            + "    \"DAY\" \\.\\.\\.\n"
            + "    \"HOUR\" \\.\\.\\.\n"
            + "    \"MINUTE\" \\.\\.\\.\n"
            + "    \"SECOND\" \\.\\.\\.\n"
            + "    ");

        // illegal qualifiers, no precision in either field
        checkExpFails(
            "interval '1' year ^to^ year",
            "(?s)Encountered \"to year\" at line 1, column 19.\n"
            + "Was expecting one of:\n"
            + "    <EOF> \n"
            + "    \"NOT\" \\.\\.\\..*");
        checkExpFails("interval '1-2' year ^to^ day", ANY);
        checkExpFails("interval '1-2' year ^to^ hour", ANY);
        checkExpFails("interval '1-2' year ^to^ minute", ANY);
        checkExpFails("interval '1-2' year ^to^ second", ANY);

        checkExpFails("interval '1-2' month ^to^ year", ANY);
        checkExpFails("interval '1-2' month ^to^ month", ANY);
        checkExpFails("interval '1-2' month ^to^ day", ANY);
        checkExpFails("interval '1-2' month ^to^ hour", ANY);
        checkExpFails("interval '1-2' month ^to^ minute", ANY);
        checkExpFails("interval '1-2' month ^to^ second", ANY);

        checkExpFails("interval '1-2' day ^to^ year", ANY);
        checkExpFails("interval '1-2' day ^to^ month", ANY);
        checkExpFails("interval '1-2' day ^to^ day", ANY);

        checkExpFails("interval '1-2' hour ^to^ year", ANY);
        checkExpFails("interval '1-2' hour ^to^ month", ANY);
        checkExpFails("interval '1-2' hour ^to^ day", ANY);
        checkExpFails("interval '1-2' hour ^to^ hour", ANY);

        checkExpFails("interval '1-2' minute ^to^ year", ANY);
        checkExpFails("interval '1-2' minute ^to^ month", ANY);
        checkExpFails("interval '1-2' minute ^to^ day", ANY);
        checkExpFails("interval '1-2' minute ^to^ hour", ANY);
        checkExpFails("interval '1-2' minute ^to^ minute", ANY);

        checkExpFails("interval '1-2' second ^to^ year", ANY);
        checkExpFails("interval '1-2' second ^to^ month", ANY);
        checkExpFails("interval '1-2' second ^to^ day", ANY);
        checkExpFails("interval '1-2' second ^to^ hour", ANY);
        checkExpFails("interval '1-2' second ^to^ minute", ANY);
        checkExpFails("interval '1-2' second ^to^ second", ANY);

        // illegal qualifiers, including precision in start field
        checkExpFails("interval '1' year(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ day", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ hour", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ minute", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ second", ANY);

        checkExpFails("interval '1-2' month(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ month", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ day", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ hour", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ minute", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ second", ANY);

        checkExpFails("interval '1-2' day(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' day(3) ^to^ month", ANY);

        checkExpFails("interval '1-2' hour(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' hour(3) ^to^ month", ANY);
        checkExpFails("interval '1-2' hour(3) ^to^ day", ANY);

        checkExpFails("interval '1-2' minute(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ month", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ day", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ hour", ANY);

        checkExpFails("interval '1-2' second(3) ^to^ year", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ month", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ day", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ hour", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ minute", ANY);

        // illegal qualfiers, including precision in end field
        checkExpFails("interval '1' year ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' year to month^(^2)", ANY);
        checkExpFails("interval '1-2' year ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' year ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' year ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' year ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' year ^to^ second(2,6)", ANY);

        checkExpFails("interval '1-2' month ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' month ^to^ second(2,6)", ANY);

        checkExpFails("interval '1-2' day ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' day ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' day ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' day to hour^(^2)", ANY);
        checkExpFails("interval '1-2' day to minute^(^2)", ANY);
        checkExpFails("interval '1-2' day to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' hour ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' hour ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' hour ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' hour ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' hour to minute^(^2)", ANY);
        checkExpFails("interval '1-2' hour to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' minute ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' minute ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' minute ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' minute ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' minute ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' minute to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' second ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' second ^to^ second(2,6)", ANY);

        // illegal qualfiers, including precision in start and end field
        checkExpFails("interval '1' year(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' year(3) to month^(^2)", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' year(3) ^to^ second(2,6)", ANY);

        checkExpFails("interval '1-2' month(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' month(3) ^to^ second(2,6)", ANY);

        checkExpFails("interval '1-2' day(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' day(3) ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' day(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' day(3) to hour^(^2)", ANY);
        checkExpFails("interval '1-2' day(3) to minute^(^2)", ANY);
        checkExpFails("interval '1-2' day(3) to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' hour(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' hour(3) ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' hour(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' hour(3) ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' hour(3) to minute^(^2)", ANY);
        checkExpFails("interval '1-2' hour(3) to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' minute(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' minute(3) ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' minute(3) to second(2^,^6)", ANY);

        checkExpFails("interval '1-2' second(3) ^to^ year(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ month(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ day(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ hour(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ minute(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ second(2)", ANY);
        checkExpFails("interval '1-2' second(3) ^to^ second(2,6)", ANY);

        // precision of -1 (< minimum allowed)
        // FIXME should fail at "-" or "-1"
        checkExpFails("INTERVAL '0' YEAR^(^-1)", ANY);
        checkExpFails("INTERVAL '0-0' YEAR^(^-1) TO MONTH", ANY);
        checkExpFails("INTERVAL '0' MONTH^(^-1)", ANY);
        checkExpFails("INTERVAL '0' DAY^(^-1)", ANY);
        checkExpFails("INTERVAL '0 0' DAY^(^-1) TO HOUR", ANY);
        checkExpFails("INTERVAL '0 0' DAY^(^-1) TO MINUTE", ANY);
        checkExpFails("INTERVAL '0 0:0:0' DAY^(^-1) TO SECOND", ANY);
        checkExpFails("INTERVAL '0 0:0:0' DAY TO SECOND^(^-1)", ANY);
        checkExpFails("INTERVAL '0' HOUR^(^-1)", ANY);
        checkExpFails("INTERVAL '0:0' HOUR^(^-1) TO MINUTE", ANY);
        checkExpFails("INTERVAL '0:0:0' HOUR^(^-1) TO SECOND", ANY);
        checkExpFails("INTERVAL '0:0:0' HOUR TO SECOND^(^-1)", ANY);
        checkExpFails("INTERVAL '0' MINUTE^(^-1)", ANY);
        checkExpFails("INTERVAL '0:0' MINUTE^(^-1) TO SECOND", ANY);
        checkExpFails("INTERVAL '0:0' MINUTE TO SECOND^(^-1)", ANY);
        checkExpFails("INTERVAL '0' SECOND^(^-1)", ANY);
        checkExpFails("INTERVAL '0' SECOND(1^,^ -1)", ANY);

        // These may actually be legal per SQL2003, as the first field is
        // "more significant" than the last, but we do not support them
        checkExpFails("interval '1' day(3) ^to^ day", ANY);
        checkExpFails("interval '1' hour(3) ^to^ hour", ANY);
        checkExpFails("interval '1' minute(3) ^to^ minute", ANY);
        checkExpFails("interval '1' second(3) ^to^ second", ANY);
        checkExpFails("interval '1' second(3,1) ^to^ second", ANY);
        checkExpFails("interval '1' second(2,3) ^to^ second", ANY);
        checkExpFails("interval '1' second(2,2) ^to^ second(3)", ANY);
    }

    @Test public void testMiscIntervalQualifier() {
        checkExp("interval '-' day", "INTERVAL '-' DAY");

        checkExpFails(
            "interval '1 2:3:4.567' day to hour ^to^ second",
            "(?s)Encountered \"to\" at.*");
        checkExpFails(
            "interval '1:2' minute to second(2^,^ 2)",
            "(?s)Encountered \",\" at.*");
        checkExp(
            "interval '1:x' hour to minute",
            "INTERVAL '1:x' HOUR TO MINUTE");
        checkExp(
            "interval '1:x:2' hour to second",
            "INTERVAL '1:x:2' HOUR TO SECOND");
    }

    @Test public void testIntervalOperators() {
        checkExp("-interval '1' day", "(- INTERVAL '1' DAY)");
        checkExp(
            "interval '1' day + interval '1' day",
            "(INTERVAL '1' DAY + INTERVAL '1' DAY)");
        checkExp(
            "interval '1' day - interval '1:2:3' hour to second",
            "(INTERVAL '1' DAY - INTERVAL '1:2:3' HOUR TO SECOND)");

        checkExp("interval -'1' day", "INTERVAL -'1' DAY");
        checkExp("interval '-1' day", "INTERVAL '-1' DAY");
        checkExpFails(
            "interval 'wael was here^'^",
            "(?s)Encountered \"<EOF>\".*");
        checkExp(
            "interval 'wael was here' HOUR",
            "INTERVAL 'wael was here' HOUR"); // ok in parser, not in validator
    }

    @Test public void testDateMinusDate() {
        checkExp("(date1 - date2) HOUR", "((`DATE1` - `DATE2`) HOUR)");
        checkExp(
            "(date1 - date2) YEAR TO MONTH",
            "((`DATE1` - `DATE2`) YEAR TO MONTH)");
        checkExp(
            "(date1 - date2) HOUR > interval '1' HOUR",
            "(((`DATE1` - `DATE2`) HOUR) > INTERVAL '1' HOUR)");
        checkExpFails(
            "^(date1 + date2) second^",
            "(?s).*Illegal expression. Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
        checkExpFails(
            "^(date1,date2,date2) second^",
            "(?s).*Illegal expression. Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
    }

    @Test public void testExtract() {
        checkExp("extract(year from x)", "EXTRACT(YEAR FROM `X`)");
        checkExp("extract(month from x)", "EXTRACT(MONTH FROM `X`)");
        checkExp("extract(day from x)", "EXTRACT(DAY FROM `X`)");
        checkExp("extract(hour from x)", "EXTRACT(HOUR FROM `X`)");
        checkExp("extract(minute from x)", "EXTRACT(MINUTE FROM `X`)");
        checkExp("extract(second from x)", "EXTRACT(SECOND FROM `X`)");

        checkExpFails(
            "extract(day ^to^ second from x)",
            "(?s)Encountered \"to\".*");
    }

    @Test public void testIntervalArithmetics() {
        checkExp(
            "TIME '23:59:59' - interval '1' hour ",
            "(TIME '23:59:59' - INTERVAL '1' HOUR)");
        checkExp(
            "TIMESTAMP '2000-01-01 23:59:59.1' - interval '1' hour ",
            "(TIMESTAMP '2000-01-01 23:59:59.1' - INTERVAL '1' HOUR)");
        checkExp(
            "DATE '2000-01-01' - interval '1' hour ",
            "(DATE '2000-01-01' - INTERVAL '1' HOUR)");

        checkExp(
            "TIME '23:59:59' + interval '1' hour ",
            "(TIME '23:59:59' + INTERVAL '1' HOUR)");
        checkExp(
            "TIMESTAMP '2000-01-01 23:59:59.1' + interval '1' hour ",
            "(TIMESTAMP '2000-01-01 23:59:59.1' + INTERVAL '1' HOUR)");
        checkExp(
            "DATE '2000-01-01' + interval '1' hour ",
            "(DATE '2000-01-01' + INTERVAL '1' HOUR)");

        checkExp(
            "interval '1' hour + TIME '23:59:59' ",
            "(INTERVAL '1' HOUR + TIME '23:59:59')");

        checkExp("interval '1' hour * 8", "(INTERVAL '1' HOUR * 8)");
        checkExp("1 * interval '1' hour", "(1 * INTERVAL '1' HOUR)");
        checkExp("interval '1' hour / 8", "(INTERVAL '1' HOUR / 8)");
    }

    @Test public void testIntervalCompare() {
        checkExp(
            "interval '1' hour = interval '1' second",
            "(INTERVAL '1' HOUR = INTERVAL '1' SECOND)");
        checkExp(
            "interval '1' hour <> interval '1' second",
            "(INTERVAL '1' HOUR <> INTERVAL '1' SECOND)");
        checkExp(
            "interval '1' hour < interval '1' second",
            "(INTERVAL '1' HOUR < INTERVAL '1' SECOND)");
        checkExp(
            "interval '1' hour <= interval '1' second",
            "(INTERVAL '1' HOUR <= INTERVAL '1' SECOND)");
        checkExp(
            "interval '1' hour > interval '1' second",
            "(INTERVAL '1' HOUR > INTERVAL '1' SECOND)");
        checkExp(
            "interval '1' hour >= interval '1' second",
            "(INTERVAL '1' HOUR >= INTERVAL '1' SECOND)");
    }

    @Test public void testCastToInterval() {
        checkExp("cast(x as interval year)", "CAST(`X` AS INTERVAL YEAR)");
        checkExp("cast(x as interval month)", "CAST(`X` AS INTERVAL MONTH)");
        checkExp(
            "cast(x as interval year to month)",
            "CAST(`X` AS INTERVAL YEAR TO MONTH)");
        checkExp("cast(x as interval day)", "CAST(`X` AS INTERVAL DAY)");
        checkExp("cast(x as interval hour)", "CAST(`X` AS INTERVAL HOUR)");
        checkExp("cast(x as interval minute)", "CAST(`X` AS INTERVAL MINUTE)");
        checkExp("cast(x as interval second)", "CAST(`X` AS INTERVAL SECOND)");
        checkExp(
            "cast(x as interval day to hour)",
            "CAST(`X` AS INTERVAL DAY TO HOUR)");
        checkExp(
            "cast(x as interval day to minute)",
            "CAST(`X` AS INTERVAL DAY TO MINUTE)");
        checkExp(
            "cast(x as interval day to second)",
            "CAST(`X` AS INTERVAL DAY TO SECOND)");
        checkExp(
            "cast(x as interval hour to minute)",
            "CAST(`X` AS INTERVAL HOUR TO MINUTE)");
        checkExp(
            "cast(x as interval hour to second)",
            "CAST(`X` AS INTERVAL HOUR TO SECOND)");
        checkExp(
            "cast(x as interval minute to second)",
            "CAST(`X` AS INTERVAL MINUTE TO SECOND)");
    }

    @Test public void testUnnest() {
        check(
            "select*from unnest(x)",
            "SELECT *\n"
            + "FROM (UNNEST(`X`))");
        check(
            "select*from unnest(x) AS T",
            "SELECT *\n"
            + "FROM (UNNEST(`X`)) AS `T`");

        // UNNEST cannot be first word in query
        checkFails(
            "^unnest^(x)",
            "(?s)Encountered \"unnest\" at.*");
    }

    @Test public void testParensInFrom() {
        // UNNEST may not occur within parentheses.
        // FIXME should fail at "unnest"
        checkFails(
            "select *from ^(^unnest(x))",
            "(?s)Encountered \"\\( unnest\" at .*");

        // <table-name> may not occur within parentheses.
        checkFails(
            "select * from (^emp^)",
            "(?s)Non-query expression encountered in illegal context.*");

        // <table-name> may not occur within parentheses.
        checkFails(
            "select * from (^emp^ as x)",
            "(?s)Non-query expression encountered in illegal context.*");

        // <table-name> may not occur within parentheses.
        checkFails(
            "select * from (^emp^) as x",
            "(?s)Non-query expression encountered in illegal context.*");

        // Parentheses around JOINs are OK, and sometimes necessary.
        if (false) {
            // todo:
            check(
                "select * from (emp join dept using (deptno))",
                "xx");

            check(
                "select * from (emp join dept using (deptno)) join foo using (x)",
                "xx");
        }
    }

    @Test public void testProcedureCall() {
        check("call blubber(5)", "(CALL `BLUBBER`(5))");
        check("call \"blubber\"(5)", "(CALL `blubber`(5))");
        check("call whale.blubber(5)", "(CALL `WHALE`.`BLUBBER`(5))");
    }

    @Test public void testNewSpecification() {
        checkExp("new udt()", "(NEW `UDT`())");
        checkExp("new my.udt(1, 'hey')", "(NEW `MY`.`UDT`(1, 'hey'))");
        checkExp("new udt() is not null", "((NEW `UDT`()) IS NOT NULL)");
        checkExp("1 + new udt()", "(1 + (NEW `UDT`()))");
    }

    @Test public void testMultisetCast() {
        checkExp(
            "cast(multiset[1] as double multiset)",
            "CAST((MULTISET [1]) AS DOUBLE MULTISET)");
    }

    @Test public void testAddCarets() {
        assertEquals(
            "values (^foo^)",
            SqlParserUtil.addCarets("values (foo)", 1, 9, 1, 12));
        assertEquals(
            "abc^def",
            SqlParserUtil.addCarets("abcdef", 1, 4, 1, 4));
        assertEquals(
            "abcdef^",
            SqlParserUtil.addCarets("abcdef", 1, 7, 1, 7));
    }

    @Test public void testMetadata() {
        SqlAbstractParserImpl.Metadata metadata = getParserImpl().getMetadata();
        assertTrue(metadata.isReservedFunctionName("ABS"));
        assertFalse(metadata.isReservedFunctionName("FOO"));

        assertTrue(metadata.isContextVariableName("CURRENT_USER"));
        assertTrue(metadata.isContextVariableName("CURRENT_CATALOG"));
        assertTrue(metadata.isContextVariableName("CURRENT_SCHEMA"));
        assertFalse(metadata.isContextVariableName("ABS"));
        assertFalse(metadata.isContextVariableName("FOO"));

        assertTrue(metadata.isNonReservedKeyword("A"));
        assertTrue(metadata.isNonReservedKeyword("KEY"));
        assertFalse(metadata.isNonReservedKeyword("SELECT"));
        assertFalse(metadata.isNonReservedKeyword("FOO"));
        assertFalse(metadata.isNonReservedKeyword("ABS"));

        assertTrue(metadata.isKeyword("ABS"));
        assertTrue(metadata.isKeyword("CURRENT_USER"));
        assertTrue(metadata.isKeyword("CURRENT_CATALOG"));
        assertTrue(metadata.isKeyword("CURRENT_SCHEMA"));
        assertTrue(metadata.isKeyword("KEY"));
        assertTrue(metadata.isKeyword("SELECT"));
        assertTrue(metadata.isKeyword("HAVING"));
        assertTrue(metadata.isKeyword("A"));
        assertFalse(metadata.isKeyword("BAR"));

        assertTrue(metadata.isReservedWord("SELECT"));
        assertTrue(metadata.isReservedWord("CURRENT_CATALOG"));
        assertTrue(metadata.isReservedWord("CURRENT_SCHEMA"));
        assertFalse(metadata.isReservedWord("KEY"));

        String jdbcKeywords = metadata.getJdbcKeywords();
        assertTrue(jdbcKeywords.indexOf(",COLLECT,") >= 0);
        assertTrue(jdbcKeywords.indexOf(",SELECT,") < 0);
    }

    @Test public void testTabStop() {
        check(
            "SELECT *\n\tFROM mytable",
            "SELECT *\n"
            + "FROM `MYTABLE`");

        // make sure that the tab stops do not affect the placement of the
        // error tokens
        checkFails(
            "SELECT *\tFROM mytable\t\tWHERE x ^=^ = y AND b = 1",
            "(?s).*Encountered \"= =\" at line 1, column 32\\..*");
    }

    @Test public void testLongIdentifiers() {
        StringBuilder ident128Builder = new StringBuilder();
        for (int i = 0; i < 128; i++) {
            ident128Builder.append((char) ('a' + (i % 26)));
        }
        String ident128 = ident128Builder.toString();
        String ident128Upper = ident128.toUpperCase(Locale.US);
        String ident129 = "x" + ident128;
        String ident129Upper = ident129.toUpperCase(Locale.US);

        check(
            "select * from " + ident128,
            "SELECT *\n"
            + "FROM `" + ident128Upper + "`");
        checkFails(
            "select * from ^" + ident129 + "^",
            "Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");

        check(
            "select " + ident128 + " from mytable",
            "SELECT `" + ident128Upper + "`\n"
            + "FROM `MYTABLE`");
        checkFails(
            "select ^" + ident129 + "^ from mytable",
            "Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");
    }

    /**
     * Tests that you can't quote the names of builtin functions.
     *
     * @see org.eigenbase.test.SqlValidatorTest#testQuotedFunction()
     */
    @Test public void testQuotedFunction() {
        checkExpFails(
            "\"CAST\"(1 ^as^ double)",
            "(?s).*Encountered \"as\" at .*");
        checkExpFails(
            "\"POSITION\"('b' ^in^ 'alphabet')",
            "(?s).*Encountered \"in \\\\'alphabet\\\\'\" at .*");
        checkExpFails(
            "\"OVERLAY\"('a' ^PLAcing^ 'b' from 1)",
            "(?s).*Encountered \"PLAcing\" at.*");
        checkExpFails(
            "\"SUBSTRING\"('a' ^from^ 1)",
            "(?s).*Encountered \"from\" at .*");
    }

    @Test public void testUnicodeLiteral() {
        // Note that here we are constructing a SQL statement which directly
        // contains Unicode characters (not SQL Unicode escape sequences).  The
        // escaping here is Java-only, so by the time it gets to the SQL
        // parser, the literal already contains Unicode characters.
        String in1 =
            "values _UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "'";
        String out1 =
            "(VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "')))";
        check(in1, out1);

        // Without the U& prefix, escapes are left unprocessed
        String in2 =
            "values '"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
        String out2 =
            "(VALUES (ROW('"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "')))";
        check(in2, out2);

        // Likewise, even with the U& prefix, if some other escape
        // character is specified, then the backslash-escape
        // sequences are not interpreted
        String in3 =
            "values U&'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL
            + "' UESCAPE '!'";
        String out3 =
            "(VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "')))";
        check(in3, out3);
    }

    @Test public void testUnicodeEscapedLiteral() {
        // Note that here we are constructing a SQL statement which
        // contains SQL-escaped Unicode characters to be handled
        // by the SQL parser.
        String in =
            "values U&'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
        String out =
            "(VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "')))";
        check(in, out);

        // Verify that we can override with an explicit escape character
        check(in.replaceAll("\\\\", "!") + "UESCAPE '!'", out);
    }

    @Test public void testIllegalUnicodeEscape() {
        checkExpFails(
            "U&'abc' UESCAPE '!!'",
            ".*must be exactly one character.*");
        checkExpFails(
            "U&'abc' UESCAPE ''",
            ".*must be exactly one character.*");
        checkExpFails(
            "U&'abc' UESCAPE '0'",
            ".*hex digit.*");
        checkExpFails(
            "U&'abc' UESCAPE 'a'",
            ".*hex digit.*");
        checkExpFails(
            "U&'abc' UESCAPE 'F'",
            ".*hex digit.*");
        checkExpFails(
            "U&'abc' UESCAPE ' '",
            ".*whitespace.*");
        checkExpFails(
            "U&'abc' UESCAPE '+'",
            ".*plus sign.*");
        checkExpFails(
            "U&'abc' UESCAPE '\"'",
            ".*double quote.*");
        checkExpFails(
            "'abc' UESCAPE ^'!'^",
            ".*without Unicode literal introducer.*");
        checkExpFails(
            "^U&'\\0A'^",
            ".*is not exactly four hex digits.*");
        checkExpFails(
            "^U&'\\wxyz'^",
            ".*is not exactly four hex digits.*");
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Callback to control how test actions are performed.
     */
    protected interface Tester
    {
        void check(String sql, String expected);

        void checkExp(String sql, String expected);

        void checkFails(String sql, String expectedMsgPattern);

        void checkExpFails(String sql, String expectedMsgPattern);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Default implementation of {@link Tester}.
     */
    protected class TesterImpl
        implements Tester
    {
        public void check(
            String sql,
            String expected)
        {
            final SqlNode sqlNode = parseStmtAndHandleEx(sql);

            // no dialect, always parenthesize
            final String actual = sqlNode.toSqlString(null, true).getSql();
            TestUtil.assertEqualsVerbose(expected, actual);
        }

        protected SqlNode parseStmtAndHandleEx(String sql)
        {
            final SqlNode sqlNode;
            try {
                sqlNode = parseStmt(sql);
            } catch (SqlParseException e) {
                e.printStackTrace();
                String message =
                    "Received error while parsing SQL '" + sql
                    + "'; error is:\n"
                    + e.toString();
                throw new AssertionError(message);
            }
            return sqlNode;
        }

        public void checkExp(
            String sql,
            String expected)
        {
            final SqlNode sqlNode = parseExpressionAndHandleEx(sql);
            final String actual = sqlNode.toSqlString(null, true).getSql();
            TestUtil.assertEqualsVerbose(expected, actual);
        }

        protected SqlNode parseExpressionAndHandleEx(String sql)
        {
            final SqlNode sqlNode;
            try {
                sqlNode = parseExpression(sql);
            } catch (SqlParseException e) {
                String message =
                    "Received error while parsing SQL '" + sql
                    + "'; error is:\n"
                    + e.toString();
                throw new RuntimeException(message, e);
            }
            return sqlNode;
        }

        public void checkFails(
            String sql,
            String expectedMsgPattern)
        {
            SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
            Throwable thrown = null;
            try {
                final SqlNode sqlNode = parseStmt(sap.sql);
                Util.discard(sqlNode);
            } catch (Throwable ex) {
                thrown = ex;
            }

            SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
        }

        /**
         * Tests that an expression throws an exception which matches the given
         * pattern.
         */
        public void checkExpFails(
            String sql,
            String expectedMsgPattern)
        {
            SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
            Throwable thrown = null;
            try {
                final SqlNode sqlNode = parseExpression(sap.sql);
                Util.discard(sqlNode);
            } catch (Throwable ex) {
                thrown = ex;
            }

            SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
        }
    }

    /**
     * Implementation of {@link Tester} which makes sure that the results of
     * unparsing a query are consistent with the original query.
     */
    public class UnparsingTesterImpl
        extends TesterImpl
    {
        public void check(String sql, String expected)
        {
            SqlNode sqlNode = parseStmtAndHandleEx(sql);

            // Unparse with no dialect, always parenthesize.
            final String actual = sqlNode.toSqlString(null, true).getSql();
            assertEquals(expected, actual);

            // Unparse again in Eigenbase dialect (which we can parse), and
            // minimal parentheses.
            final String sql1 =
                sqlNode.toSqlString(SqlDialect.EIGENBASE, false).getSql();

            // Parse and unparse again.
            SqlNode sqlNode2 = parseStmtAndHandleEx(sql1);
            final String sql2 =
                sqlNode2.toSqlString(SqlDialect.EIGENBASE, false).getSql();

            // Should be the same as we started with.
            assertEquals(sql1, sql2);

            // Now unparse again in the null dialect.
            // If the unparser is not including sufficient parens to override
            // precedence, the problem will show up here.
            final String actual2 = sqlNode2.toSqlString(null, true).getSql();
            assertEquals(expected, actual2);
        }

        public void checkExp(String sql, String expected)
        {
            SqlNode sqlNode = parseExpressionAndHandleEx(sql);

            // Unparse with no dialect, always parenthesize.
            final String actual = sqlNode.toSqlString(null, true).getSql();
            assertEquals(expected, actual);

            // Unparse again in Eigenbase dialect (which we can parse), and
            // minimal parentheses.
            final String sql1 =
                sqlNode.toSqlString(SqlDialect.EIGENBASE, false).getSql();

            // Parse and unparse again.
            SqlNode sqlNode2 = parseExpressionAndHandleEx(sql1);
            final String sql2 =
                sqlNode2.toSqlString(SqlDialect.EIGENBASE, false).getSql();

            // Should be the same as we started with.
            assertEquals(sql1, sql2);

            // Now unparse again in the null dialect.
            // If the unparser is not including sufficient parens to override
            // precedence, the problem will show up here.
            final String actual2 = sqlNode2.toSqlString(null, true).getSql();
            assertEquals(expected, actual2);
        }

        public void checkFails(String sql, String expectedMsgPattern)
        {
            // Do nothing. We're not interested in unparsing invalid SQL
        }

        public void checkExpFails(String sql, String expectedMsgPattern)
        {
            // Do nothing. We're not interested in unparsing invalid SQL
        }
    }
}

// End SqlParserTest.java
