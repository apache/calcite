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

import java.util.logging.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing lots of unit
 * tests.
 *
 * <p>If you want to run these same tests in a different environment, create a
 * derived class whose {@link #getTester} returns a different implementation of
 * {@link org.eigenbase.test.SqlValidatorTestCase.Tester}.
 */
public class SqlValidatorTest
    extends SqlValidatorTestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * @deprecated Deprecated so that usages of this constant will show up in
     * yellow in Intellij and maybe someone will fix them.
     */
    protected static final boolean todo = false;
    public static final boolean todoTypeInference = false;
    private static final String ANY = "(?s).*";

    //~ Instance fields --------------------------------------------------------

    protected final Logger logger = Logger.getLogger(getClass().getName());

    private final String ERR_IN_VALUES_INCOMPATIBLE =
        "Values in expression list must have compatible types";

    private final String ERR_IN_OPERANDS_INCOMPATIBLE =
        "Values passed to IN operator must have compatible types";

    //~ Constructors -----------------------------------------------------------

    public SqlValidatorTest() {
      super(null);
    }

    //~ Methods ----------------------------------------------------------------

    @Test public void testMultipleSameAsPass() {
        check(
            "select 1 as again,2 as \"again\", 3 as AGAiN from (values (true))");
    }

    @Test public void testMultipleDifferentAs() {
        check("select 1 as c1,2 as c2 from (values(true))");
    }

    @Test public void testTypeOfAs() {
        checkColumnType(
            "select 1 as c1 from (values (true))",
            "INTEGER NOT NULL");
        checkColumnType(
            "select 'hej' as c1 from (values (true))",
            "CHAR(3) NOT NULL");
        checkColumnType(
            "select x'deadbeef' as c1 from (values (true))",
            "BINARY(4) NOT NULL");
        checkColumnType(
            "select cast(null as boolean) as c1 from (values (true))",
            "BOOLEAN");
    }

    @Test public void testTypesLiterals() {
        checkExpType("'abc'", "CHAR(3) NOT NULL");
        checkExpType("n'abc'", "CHAR(3) NOT NULL");
        checkExpType("_UTF16'abc'", "CHAR(3) NOT NULL");
        checkExpType("'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");
        checkExpType(
            "'ab'" + NL + "'cd'" + NL + "'ef'" + NL + "'gh'" + NL
            + "'ij'" + NL + "'kl'",
            "CHAR(12) NOT NULL");
        checkExpType("n'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");
        checkExpType("_UTF16'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");

        checkExpFails(
            "^x'abc'^",
            "Binary literal string must contain an even number of hexits");
        checkExpType("x'abcd'", "BINARY(2) NOT NULL");
        checkExpType("x'abcd'" + NL + "'ff001122aabb'", "BINARY(8) NOT NULL");
        checkExpType(
            "x'aaaa'" + NL + "'bbbb'" + NL + "'0000'" + NL + "'1111'",
            "BINARY(8) NOT NULL");

        checkExpType("1234567890", "INTEGER NOT NULL");
        checkExpType("123456.7890", "DECIMAL(10, 4) NOT NULL");
        checkExpType("123456.7890e3", "DOUBLE NOT NULL");
        checkExpType("true", "BOOLEAN NOT NULL");
        checkExpType("false", "BOOLEAN NOT NULL");
        checkExpType("unknown", "BOOLEAN");
    }

    @Test public void testBooleans() {
        check("select TRUE OR unknowN from (values(true))");
        check("select false AND unknown from (values(true))");
        check("select not UNKNOWn from (values(true))");
        check("select not true from (values(true))");
        check("select not false from (values(true))");
    }

    @Test public void testAndOrIllegalTypesFails() {
        // TODO need col+line number
        checkWholeExpFails(
            "'abc' AND FaLsE",
            "(?s).*'<CHAR.3.> AND <BOOLEAN>'.*");

        checkWholeExpFails("TRUE OR 1", ANY);

        checkWholeExpFails(
            "unknown OR 1.0",
            ANY);

        checkWholeExpFails(
            "true OR 1.0e4",
            ANY);

        if (todo) {
            checkWholeExpFails(
                "TRUE OR (TIME '12:00' AT LOCAL)",
                ANY);
        }
    }

    @Test public void testNotIllegalTypeFails() {
        assertExceptionIsThrown(
            "select ^NOT 3.141^ from (values(true))",
            "(?s).*Cannot apply 'NOT' to arguments of type 'NOT<DECIMAL.4, 3.>'.*");

        assertExceptionIsThrown(
            "select ^NOT 'abc'^ from (values(true))",
            ANY);

        assertExceptionIsThrown(
            "select ^NOT 1^ from (values(true))",
            ANY);
    }

    @Test public void testIs() {
        check("select TRUE IS FALSE FROM (values(true))");
        check("select false IS NULL FROM (values(true))");
        check("select UNKNOWN IS NULL FROM (values(true))");
        check("select FALSE IS UNKNOWN FROM (values(true))");

        check("select TRUE IS NOT FALSE FROM (values(true))");
        check("select TRUE IS NOT NULL FROM (values(true))");
        check("select false IS NOT NULL FROM (values(true))");
        check("select UNKNOWN IS NOT NULL FROM (values(true))");
        check("select FALSE IS NOT UNKNOWN FROM (values(true))");

        check("select 1 IS NULL FROM (values(true))");
        check("select 1.2 IS NULL FROM (values(true))");
        checkExpFails("^'abc' IS NOT UNKNOWN^", "(?s).*Cannot apply.*");
    }

    @Test public void testIsFails() {
        assertExceptionIsThrown(
            "select ^1 IS TRUE^ FROM (values(true))",
            "(?s).*'<INTEGER> IS TRUE'.*");

        assertExceptionIsThrown(
            "select ^1.1 IS NOT FALSE^ FROM (values(true))",
            ANY);

        assertExceptionIsThrown(
            "select ^1.1e1 IS NOT FALSE^ FROM (values(true))",
            "(?s).*Cannot apply 'IS NOT FALSE' to arguments of type '<DOUBLE> IS NOT FALSE'.*");

        assertExceptionIsThrown(
            "select ^'abc' IS NOT TRUE^ FROM (values(true))",
            ANY);
    }

    @Test public void testScalars() {
        check("select 1  + 1 from (values(true))");
        check("select 1  + 2.3 from (values(true))");
        check("select 1.2+3 from (values(true))");
        check("select 1.2+3.4 from (values(true))");

        check("select 1  - 1 from (values(true))");
        check("select 1  - 2.3 from (values(true))");
        check("select 1.2-3 from (values(true))");
        check("select 1.2-3.4 from (values(true))");

        check("select 1  * 2 from (values(true))");
        check("select 1.2* 3 from (values(true))");
        check("select 1  * 2.3 from (values(true))");
        check("select 1.2* 3.4 from (values(true))");

        check("select 1  / 2 from (values(true))");
        check("select 1  / 2.3 from (values(true))");
        check("select 1.2/ 3 from (values(true))");
        check("select 1.2/3.4 from (values(true))");
    }

    @Test public void testScalarsFails() {
        assertExceptionIsThrown(
            "select ^1+TRUE^ from (values(true))",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\):.*");
    }

    @Test public void testNumbers() {
        check("select 1+-2.*-3.e-1/-4>+5 AND true from (values(true))");
    }

    @Test public void testPrefix() {
        checkExpType("+interval '1' second", "INTERVAL SECOND NOT NULL");
        checkExpType("-interval '1' month", "INTERVAL MONTH NOT NULL");
        checkFails(
            "SELECT ^-'abc'^ from (values(true))",
            "(?s).*Cannot apply '-' to arguments of type '-<CHAR.3.>'.*");
        checkFails(
            "SELECT ^+'abc'^ from (values(true))",
            "(?s).*Cannot apply '\\+' to arguments of type '\\+<CHAR.3.>'.*");
    }

    @Test public void testEqualNotEqual() {
        checkExp("''=''");
        checkExp("'abc'=n''");
        checkExp("''=_latin1''");
        checkExp("n''=''");
        checkExp("n'abc'=n''");
        checkExp("n''=_latin1''");
        checkExp("_latin1''=''");
        checkExp("_latin1''=n''");
        checkExp("_latin1''=_latin1''");

        checkExp("''<>''");
        checkExp("'abc'<>n''");
        checkExp("''<>_latin1''");
        checkExp("n''<>''");
        checkExp("n'abc'<>n''");
        checkExp("n''<>_latin1''");
        checkExp("_latin1''<>''");
        checkExp("_latin1'abc'<>n''");
        checkExp("_latin1''<>_latin1''");

        checkExp("true=false");
        checkExp("unknown<>true");

        checkExp("1=1");
        checkExp("1=.1");
        checkExp("1=1e-1");
        checkExp("0.1=1");
        checkExp("0.1=0.1");
        checkExp("0.1=1e1");
        checkExp("1.1e1=1");
        checkExp("1.1e1=1.1");
        checkExp("1.1e-1=1e1");

        checkExp("''<>''");
        checkExp("1<>1");
        checkExp("1<>.1");
        checkExp("1<>1e-1");
        checkExp("0.1<>1");
        checkExp("0.1<>0.1");
        checkExp("0.1<>1e1");
        checkExp("1.1e1<>1");
        checkExp("1.1e1<>1.1");
        checkExp("1.1e-1<>1e1");
    }

    @Test public void testEqualNotEqualFails() {
        checkExpFails(
            "^''<>1^",
            "(?s).*Cannot apply '<>' to arguments of type '<CHAR.0.> <> <INTEGER>'.*");
        checkExpFails(
            "^'1'>=1^",
            "(?s).*Cannot apply '>=' to arguments of type '<CHAR.1.> >= <INTEGER>'.*");
        checkExpFails(
            "^1<>n'abc'^",
            "(?s).*Cannot apply '<>' to arguments of type '<INTEGER> <> <CHAR.3.>'.*");
        checkExpFails(
            "^''=.1^",
            "(?s).*Cannot apply '=' to arguments of type '<CHAR.0.> = <DECIMAL.1..1.>'.*");
        checkExpFails(
            "^true<>1e-1^",
            "(?s).*Cannot apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
        checkExpFails(
            "^false=''^",
            "(?s).*Cannot apply '=' to arguments of type '<BOOLEAN> = <CHAR.0.>'.*");
        checkExpFails(
            "^x'a4'=0.01^",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <DECIMAL.3, 2.>'.*");
        checkExpFails(
            "^x'a4'=1^",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <INTEGER>'.*");
        checkExpFails(
            "^x'13'<>0.01^",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.1.> <> <DECIMAL.3, 2.>'.*");
        checkExpFails(
            "^x'abcd'<>1^",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.2.> <> <INTEGER>'.*");
    }

    @Test public void testBinaryString() {
        check("select x'face'=X'' from (values(true))");
        check("select x'ff'=X'' from (values(true))");
    }

    @Test public void testBinaryStringFails() {
        assertExceptionIsThrown(
            "select ^x'ffee'='abc'^ from (values(true))",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.2.> = <CHAR.3.>'.*");
        assertExceptionIsThrown(
            "select ^x'ff'=88^ from (values(true))",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <INTEGER>'.*");
        assertExceptionIsThrown(
            "select ^x''<>1.1e-1^ from (values(true))",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.0.> <> <DOUBLE>'.*");
        assertExceptionIsThrown(
            "select ^x''<>1.1^ from (values(true))",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.0.> <> <DECIMAL.2, 1.>'.*");
    }

    @Test public void testStringLiteral() {
        check("select n''=_iso-8859-1'abc' from (values(true))");
        check("select N'f'<>'''' from (values(true))");
    }

    @Test public void testStringLiteralBroken() {
        check("select 'foo'" + NL + "'bar' from (values(true))");
        check("select 'foo'\r'bar' from (values(true))");
        check("select 'foo'\n\r'bar' from (values(true))");
        check("select 'foo'\r\n'bar' from (values(true))");
        check("select 'foo'\n'bar' from (values(true))");
        checkFails(
            "select 'foo' /* comment */ ^'bar'^ from (values(true))",
            "String literal continued on same line");
        check("select 'foo' -- comment\r from (values(true))");
        checkFails(
            "select 'foo' ^'bar'^ from (values(true))",
            "String literal continued on same line");
    }

    @Test public void testArithmeticOperators() {
        checkExp("power(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log10(- -.2  )");

        checkExp("mod(5.1, 3)");
        checkExp("mod(2,5.1)");
        checkExp("exp(3.67)");
    }

    @Test public void testArithmeticOperatorsFails() {
        checkExpFails(
            "^power(2,'abc')^",
            "(?s).*Cannot apply 'POWER' to arguments of type 'POWER.<INTEGER>, <CHAR.3.>.*");
        checkExpFails(
            "^power(true,1)^",
            "(?s).*Cannot apply 'POWER' to arguments of type 'POWER.<BOOLEAN>, <INTEGER>.*");
        checkExpFails(
            "^mod(x'1100',1)^",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<BINARY.2.>, <INTEGER>.*");
        checkExpFails(
            "^mod(1, x'1100')^",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<INTEGER>, <BINARY.2.>.*");
        checkExpFails(
            "^abs(x'')^",
            "(?s).*Cannot apply 'ABS' to arguments of type 'ABS.<BINARY.0.>.*");
        checkExpFails(
            "^ln(x'face12')^",
            "(?s).*Cannot apply 'LN' to arguments of type 'LN.<BINARY.3.>.*");
        checkExpFails(
            "^log10(x'fa')^",
            "(?s).*Cannot apply 'LOG10' to arguments of type 'LOG10.<BINARY.1.>.*");
        checkExpFails(
            "^exp('abc')^",
            "(?s).*Cannot apply 'EXP' to arguments of type 'EXP.<CHAR.3.>.*");
    }

    @Test public void testCaseExpression() {
        checkExp("case 1 when 1 then 'one' end");
        checkExp("case 1 when 1 then 'one' else null end");
        checkExp("case 1 when 1 then 'one' else 'more' end");
        checkExp("case 1 when 1 then 'one' when 2 then null else 'more' end");
        checkExp("case when TRUE then 'true' else 'false' end");
        check("values case when TRUE then 'true' else 'false' end");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN null END");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(null as integer) END");
        checkExp(
            "CASE 1 WHEN 1 THEN null WHEN 2 THEN cast(null as integer) END");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(cast(null as tinyint) as integer) END");
    }

    @Test public void testCaseExpressionTypes() {
        checkExpType(
            "case 1 when 1 then 'one' else 'not one' end",
            "CHAR(7) NOT NULL");
        checkExpType("case when 2<1 then 'impossible' end", "CHAR(10)");
        checkExpType(
            "case 'one' when 'two' then 2.00 when 'one' then 1.3 else 3.2 end",
            "DECIMAL(3, 2) NOT NULL");
        checkExpType(
            "case 'one' when 'two' then 2 when 'one' then 1.00 else 3 end",
            "DECIMAL(12, 2) NOT NULL");
        checkExpType(
            "case 1 when 1 then 'one' when 2 then null else 'more' end",
            "CHAR(4)");
        checkExpType(
            "case when TRUE then 'true' else 'false' end",
            "CHAR(5) NOT NULL");
        checkExpType("CASE 1 WHEN 1 THEN cast(null as integer) END", "INTEGER");
        checkExpType(
            "CASE 1 WHEN 1 THEN NULL WHEN 2 THEN cast(cast(null as tinyint) as integer) END",
            "INTEGER");
        checkExpType(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(null as integer) END",
            "INTEGER");
        checkExpType(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(cast(null as tinyint) as integer) END",
            "INTEGER");
    }

    @Test public void testCaseExpressionFails() {
        // varchar not comparable with bit string
        checkWholeExpFails(
            "case 'string' when x'01' then 'zero one' else 'something' end",
            "(?s).*Cannot apply '=' to arguments of type '<CHAR.6.> = <BINARY.1.>'.*");

        // all thens and else return null
        checkWholeExpFails(
            "case 1 when 1 then null else null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");

        // all thens and else return null
        checkWholeExpFails(
            "case 1 when 1 then null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
        checkWholeExpFails(
            "case when true and true then 1 "
            + "when false then 2 "
            + "when false then true " + "else "
            + "case when true then 3 end end",
            "Illegal mixing of types in CASE or COALESCE statement");
    }

    @Test public void testNullIf() {
        checkExp("nullif(1,2)");
        checkExpType("nullif(1,2)", "INTEGER");
        checkExpType("nullif('a','b')", "CHAR(1)");
        checkExpType("nullif(345.21, 2)", "DECIMAL(5, 2)");
        checkExpType("nullif(345.21, 2e0)", "DECIMAL(5, 2)");
        checkWholeExpFails(
            "nullif(1,2,3)",
            "Invalid number of arguments to function 'NULLIF'. Was expecting 2 arguments");
    }

    @Test public void testCoalesce() {
        checkExp("coalesce('a','b')");
        checkExpType("coalesce('a','b','c')", "CHAR(1) NOT NULL");
    }

    @Test public void testCoalesceFails() {
        checkWholeExpFails(
            "coalesce('a',1)",
            "Illegal mixing of types in CASE or COALESCE statement");
        checkWholeExpFails(
            "coalesce('a','b',1)",
            "Illegal mixing of types in CASE or COALESCE statement");
    }

    @Test public void testStringCompare() {
        checkExp("'a' = 'b'");
        checkExp("'a' <> 'b'");
        checkExp("'a' > 'b'");
        checkExp("'a' < 'b'");
        checkExp("'a' >= 'b'");
        checkExp("'a' <= 'b'");

        checkExp("cast('' as varchar(1))>cast('' as char(1))");
        checkExp("cast('' as varchar(1))<cast('' as char(1))");
        checkExp("cast('' as varchar(1))>=cast('' as char(1))");
        checkExp("cast('' as varchar(1))<=cast('' as char(1))");
        checkExp("cast('' as varchar(1))=cast('' as char(1))");
        checkExp("cast('' as varchar(1))<>cast('' as char(1))");
    }

    @Test public void testStringCompareType() {
        checkExpType("'a' = 'b'", "BOOLEAN NOT NULL");
        checkExpType("'a' <> 'b'", "BOOLEAN NOT NULL");
        checkExpType("'a' > 'b'", "BOOLEAN NOT NULL");
        checkExpType("'a' < 'b'", "BOOLEAN NOT NULL");
        checkExpType("'a' >= 'b'", "BOOLEAN NOT NULL");
        checkExpType("'a' <= 'b'", "BOOLEAN NOT NULL");
        checkExpType("CAST(NULL AS VARCHAR(33)) > 'foo'", "BOOLEAN");
    }

    @Test public void testConcat() {
        checkExp("'a'||'b'");
        checkExp("x'12'||x'34'");
        checkExpType("'a'||'b'", "CHAR(2) NOT NULL");
        checkExpType(
            "cast('a' as char(1))||cast('b' as char(2))",
            "CHAR(3) NOT NULL");
        checkExpType("cast(null as char(1))||cast('b' as char(2))", "CHAR(3)");
        checkExpType("'a'||'b'||'c'", "CHAR(3) NOT NULL");
        checkExpType("'a'||'b'||'cde'||'f'", "CHAR(6) NOT NULL");
        checkExpType(
            "'a'||'b'||cast('cde' as VARCHAR(3))|| 'f'",
            "VARCHAR(6) NOT NULL");
        checkExp("_UTF16'a'||_UTF16'b'||_UTF16'c'");
    }

    @Test public void testConcatWithCharset() {
        checkCharset(
            "_UTF16'a'||_UTF16'b'||_UTF16'c'",
            Charset.forName("UTF-16LE"));
    }

    @Test public void testConcatFails() {
        checkWholeExpFails(
            "'a'||x'ff'",
            "(?s).*Cannot apply '\\|\\|' to arguments of type '<CHAR.1.> \\|\\| <BINARY.1.>'"
            + ".*Supported form.s.: '<STRING> \\|\\| <STRING>.*'");
    }

    @Test public void testBetween() {
        checkExp("1 between 2 and 3");
        checkExp("'a' between 'b' and 'c'");
        checkWholeExpFails(
            "'' between 2 and 3",
            "(?s).*Cannot apply 'BETWEEN' to arguments of type.*");
    }

    @Test public void testCharsetMismatch() {
        checkWholeExpFails(
            "''=_UTF16''",
            "Cannot apply .* to the two different charsets ISO-8859-1 and UTF-16LE");
        checkWholeExpFails(
            "''<>_UTF16''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkWholeExpFails(
            "''>_UTF16''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkWholeExpFails(
            "''<_UTF16''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkWholeExpFails(
            "''<=_UTF16''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkWholeExpFails(
            "''>=_UTF16''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkWholeExpFails("''||_UTF16''", ANY);
        checkWholeExpFails("'a'||'b'||_UTF16'c'", ANY);
    }

    // FIXME jvs 2-Feb-2005: all collation-related tests are disabled due to
    // dtbug 280

    public void _testSimpleCollate()
    {
        checkExp("'s' collate latin1$en$1");
        checkExpType("'s' collate latin1$en$1", "CHAR(1)");
        checkCollation(
            "'s'",
            "ISO-8859-1$en_US$primary",
            SqlCollation.Coercibility.Coercible);
        checkCollation(
            "'s' collate latin1$sv$3",
            "ISO-8859-1$sv$3",
            SqlCollation.Coercibility.Explicit);
    }

    public void _testCharsetAndCollateMismatch()
    {
        // todo
        checkExpFails("_UTF16's' collate latin1$en$1", "?");
    }

    public void _testDyadicCollateCompare()
    {
        checkExp("'s' collate latin1$en$1 < 't'");
        checkExp("'t' > 's' collate latin1$en$1");
        checkExp("'s' collate latin1$en$1 <> 't' collate latin1$en$1");
    }

    public void _testDyadicCompareCollateFails()
    {
        // two different explicit collations. difference in strength
        checkExpFails(
            "'s' collate latin1$en$1 <= 't' collate latin1$en$2",
            "(?s).*Two explicit different collations.*are illegal.*");

        // two different explicit collations. difference in language
        checkExpFails(
            "'s' collate latin1$sv$1 >= 't' collate latin1$en$1",
            "(?s).*Two explicit different collations.*are illegal.*");
    }

    public void _testDyadicCollateOperator()
    {
        checkCollation(
            "'a' || 'b'",
            "ISO-8859-1$en_US$primary",
            SqlCollation.Coercibility.Coercible);
        checkCollation(
            "'a' collate latin1$sv$3 || 'b'",
            "ISO-8859-1$sv$3",
            SqlCollation.Coercibility.Explicit);
        checkCollation(
            "'a' collate latin1$sv$3 || 'b' collate latin1$sv$3",
            "ISO-8859-1$sv$3",
            SqlCollation.Coercibility.Explicit);
    }

    @Test public void testCharLength() {
        checkExp("char_length('string')");
        checkExp("char_length(_UTF16'string')");
        checkExp("character_length('string')");
        checkExpType("char_length('string')", "INTEGER NOT NULL");
        checkExpType("character_length('string')", "INTEGER NOT NULL");
    }

    @Test public void testUpperLower() {
        checkExp("upper(_UTF16'sadf')");
        checkExp("lower(n'sadf')");
        checkExpType("lower('sadf')", "CHAR(4) NOT NULL");
        checkWholeExpFails(
            "upper(123)",
            "(?s).*Cannot apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    }

    @Test public void testPosition() {
        checkExp("position('mouse' in 'house')");
        checkExp("position(x'11' in x'100110')");
        checkExp("position(x'abcd' in x'')");
        checkExpType("position('mouse' in 'house')", "INTEGER NOT NULL");
        checkWholeExpFails(
            "position(x'1234' in '110')",
            "Parameters must be of the same type");
    }

    @Test public void testTrim() {
        checkExp("trim('mustache' FROM 'beard')");
        checkExp("trim(both 'mustache' FROM 'beard')");
        checkExp("trim(leading 'mustache' FROM 'beard')");
        checkExp("trim(trailing 'mustache' FROM 'beard')");
        checkExpType("trim('mustache' FROM 'beard')", "VARCHAR(5) NOT NULL");
        checkExpType("trim('beard  ')", "VARCHAR(7) NOT NULL");
        checkExpType(
            "trim('mustache' FROM cast(null as varchar(4)))",
            "VARCHAR(4)");

        if (todo) {
            final SqlCollation.Coercibility expectedCoercibility = null;
            checkCollation(
                "trim('mustache' FROM 'beard')",
                "CHAR(5)",
                expectedCoercibility);
        }
    }

    @Test public void testTrimFails() {
        checkWholeExpFails(
            "trim(123 FROM 'beard')",
            "(?s).*Cannot apply 'TRIM' to arguments of type.*");
        checkWholeExpFails(
            "trim('a' FROM 123)",
            "(?s).*Cannot apply 'TRIM' to arguments of type.*");
        checkWholeExpFails(
            "trim('a' FROM _UTF16'b')",
            "(?s).*not comparable to each other.*");
    }

    public void _testConvertAndTranslate()
    {
        checkExp("convert('abc' using conversion)");
        checkExp("translate('abc' using translation)");
    }

    @Test public void testOverlay() {
        checkExp("overlay('ABCdef' placing 'abc' from 1)");
        checkExp("overlay('ABCdef' placing 'abc' from 1 for 3)");
        checkWholeExpFails(
            "overlay('ABCdef' placing 'abc' from '1' for 3)",
            "(?s).*OVERLAY\\(<STRING> PLACING <STRING> FROM <INTEGER>\\).*");
        checkExpType(
            "overlay('ABCdef' placing 'abc' from 1 for 3)",
            "VARCHAR(9) NOT NULL");
        checkExpType(
            "overlay('ABCdef' placing 'abc' from 6 for 3)",
            "VARCHAR(9) NOT NULL");
        checkExpType(
            "overlay('ABCdef' placing cast(null as char(5)) from 1)",
            "VARCHAR(11)");

        if (todo) {
            checkCollation(
                "overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)",
                "ISO-8859-1$sv",
                SqlCollation.Coercibility.Explicit);
        }
    }

    @Test public void testSubstring() {
        checkExp("substring('a' FROM 1)");
        checkExp("substring('a' FROM 1 FOR 3)");
        checkExp("substring('a' FROM 'reg' FOR '\\')");
        checkExp("substring(x'ff' FROM 1  FOR 2)"); // binary string

        checkExpType("substring('10' FROM 1  FOR 2)", "VARCHAR(2) NOT NULL");
        checkExpType("substring('1000' FROM 2)", "VARCHAR(4) NOT NULL");
        checkExpType(
            "substring('1000' FROM '1'  FOR 'w')",
            "VARCHAR(4) NOT NULL");
        checkExpType(
            "substring(cast(' 100 ' as CHAR(99)) FROM '1'  FOR 'w')",
            "VARCHAR(99) NOT NULL");
        checkExpType(
            "substring(x'10456b' FROM 1  FOR 2)",
            "VARBINARY(3) NOT NULL");

        checkCharset(
            "substring('10' FROM 1  FOR 2)",
            Charset.forName("latin1"));
        checkCharset(
            "substring(_UTF16'10' FROM 1  FOR 2)",
            Charset.forName("UTF-16LE"));
    }

    @Test public void testSubstringFails() {
        checkWholeExpFails(
            "substring('a' from 1 for 'b')",
            "(?s).*Cannot apply 'SUBSTRING' to arguments of type.*");
        checkWholeExpFails(
            "substring(_UTF16'10' FROM '0' FOR '\\')",
            "(?s).* not comparable to each other.*");
        checkWholeExpFails(
            "substring('10' FROM _UTF16'0' FOR '\\')",
            "(?s).* not comparable to each other.*");
        checkWholeExpFails(
            "substring('10' FROM '0' FOR _UTF16'\\')",
            "(?s).* not comparable to each other.*");
    }

    @Test public void testLikeAndSimilar() {
        checkExp("'a' like 'b'");
        checkExp("'a' like 'b'");
        checkExp("'a' similar to 'b'");
        checkExp("'a' similar to 'b' escape 'c'");
    }

    public void _testLikeAndSimilarFails()
    {
        checkExpFails(
            "'a' like _UTF16'b'  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _SHIFT_JIS.b..*");
        checkExpFails(
            "'a' similar to _UTF16'b'  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _SHIFT_JIS.b..*");

        checkExpFails(
            "'a' similar to 'b' collate UTF16$jp  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _ISO-8859-1.b. COLLATE SHIFT_JIS.jp.primary.*");
    }

    @Test public void testNull() {
        checkFails("values 1.0 + ^NULL^", "(?s).*Illegal use of .NULL.*");
        checkExpFails("1.0 + ^NULL^", "(?s).*Illegal use of .NULL.*");

        // FIXME: SQL:2003 does not allow raw NULL in IN clause
        checkExp("1 in (1, null, 2)");
        checkExp("1 in (null, 1, null, 2)");
        checkExp("1 in (cast(null as integer), null)");

        // Expression is illegal, but error message is not perfect.
        checkWholeExpFails(
            "1 in (null, null)",
            "Values passed to IN operator must have compatible types");
    }

    @Test public void testNullCast() {
        checkExpType("cast(null as tinyint)", "TINYINT");
        checkExpType("cast(null as smallint)", "SMALLINT");
        checkExpType("cast(null as integer)", "INTEGER");
        checkExpType("cast(null as bigint)", "BIGINT");
        checkExpType("cast(null as float)", "FLOAT");
        checkExpType("cast(null as real)", "REAL");
        checkExpType("cast(null as double)", "DOUBLE");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast(null as varchar(1))", "VARCHAR(1)");
        checkExpType("cast(null as char(1))", "CHAR(1)");
        checkExpType("cast(null as binary(1))", "BINARY(1)");
        checkExpType("cast(null as date)", "DATE");
        checkExpType("cast(null as time)", "TIME(0)");
        checkExpType("cast(null as timestamp)", "TIMESTAMP(0)");
        checkExpType("cast(null as decimal)", "DECIMAL(19, 0)");
        checkExpType("cast(null as varbinary(1))", "VARBINARY(1)");

        checkExp("cast(null as integer), cast(null as char(1))");
    }

    @Test public void testCastTypeToType() {
        checkExpType("cast(123 as char)", "CHAR(1) NOT NULL");
        checkExpType("cast(123 as varchar)", "VARCHAR(1) NOT NULL");
        checkExpType("cast(x'1234' as binary)", "BINARY(1) NOT NULL");
        checkExpType("cast(x'1234' as varbinary)", "VARBINARY(1) NOT NULL");
        checkExpType("cast(123 as varchar(3))", "VARCHAR(3) NOT NULL");
        checkExpType("cast(123 as char(3))", "CHAR(3) NOT NULL");
        checkExpType("cast('123' as integer)", "INTEGER NOT NULL");
        checkExpType("cast('123' as double)", "DOUBLE NOT NULL");
        checkExpType("cast('1.0' as real)", "REAL NOT NULL");
        checkExpType("cast(1.0 as tinyint)", "TINYINT NOT NULL");
        checkExpType("cast(1 as tinyint)", "TINYINT NOT NULL");
        checkExpType("cast(1.0 as smallint)", "SMALLINT NOT NULL");
        checkExpType("cast(1 as integer)", "INTEGER NOT NULL");
        checkExpType("cast(1.0 as integer)", "INTEGER NOT NULL");
        checkExpType("cast(1.0 as bigint)", "BIGINT NOT NULL");
        checkExpType("cast(1 as bigint)", "BIGINT NOT NULL");
        checkExpType("cast(1.0 as float)", "FLOAT NOT NULL");
        checkExpType("cast(1 as float)", "FLOAT NOT NULL");
        checkExpType("cast(1.0 as real)", "REAL NOT NULL");
        checkExpType("cast(1 as real)", "REAL NOT NULL");
        checkExpType("cast(1.0 as double)", "DOUBLE NOT NULL");
        checkExpType("cast(1 as double)", "DOUBLE NOT NULL");
        checkExpType("cast(123 as decimal(6,4))", "DECIMAL(6, 4) NOT NULL");
        checkExpType("cast(123 as decimal(6))", "DECIMAL(6, 0) NOT NULL");
        checkExpType("cast(123 as decimal)", "DECIMAL(19, 0) NOT NULL");
        checkExpType("cast(1.234 as decimal(2,5))", "DECIMAL(2, 5) NOT NULL");
        checkExpType("cast('4.5' as decimal(3,1))", "DECIMAL(3, 1) NOT NULL");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast('abc' as varchar(1))", "VARCHAR(1) NOT NULL");
        checkExpType("cast('abc' as char(1))", "CHAR(1) NOT NULL");
        checkExpType("cast(x'ff' as binary(1))", "BINARY(1) NOT NULL");
        checkExpType(
            "cast(multiset[1] as double multiset)",
            "DOUBLE NOT NULL MULTISET NOT NULL");
        checkExpType(
            "cast(multiset['abc'] as integer multiset)",
            "INTEGER NOT NULL MULTISET NOT NULL");
    }

    @Test public void testCastFails() {
        checkExpFails(
            "cast('foo' as ^bar^)",
            "(?s).*Unknown datatype name 'BAR'");
        checkWholeExpFails(
            "cast(multiset[1] as integer)",
            "(?s).*Cast function cannot convert value of type INTEGER MULTISET to type INTEGER");
        checkWholeExpFails(
            "cast(x'ff' as decimal(5,2))",
            "(?s).*Cast function cannot convert value of type BINARY\\(1\\) to type DECIMAL\\(5, 2\\)");

        checkWholeExpFails(
            "cast(1 as boolean)",
            "(?s).*Cast function cannot convert value of type INTEGER to type BOOLEAN.*");
        checkWholeExpFails(
            "cast(1.0e1 as boolean)",
            "(?s).*Cast function cannot convert value of type DOUBLE to type BOOLEAN.*");
        checkWholeExpFails(
            "cast(true as numeric)",
            "(?s).*Cast function cannot convert value of type BOOLEAN to type DECIMAL.*");
        checkWholeExpFails(
            "cast(DATE '1243-12-01' as TIME)",
            "(?s).*Cast function cannot convert value of type DATE to type TIME.*");
        checkWholeExpFails(
            "cast(TIME '12:34:01' as DATE)",
            "(?s).*Cast function cannot convert value of type TIME\\(0\\) to type DATE.*");

        // It's a runtime error that 'TRUE' cannot fit into CHAR(3), but at
        // validate time this expression is OK.
        checkExp("cast(true as char(3))");
    }

    @Test public void testCastBinaryLiteral() {
        checkExpFails(
            "cast(^x'0dd'^ as binary(5))",
            "Binary literal string must contain an even number of hexits");
    }

    @Test public void testDateTime() {
        // LOCAL_TIME
        checkExp("LOCALTIME(3)");
        checkExp("LOCALTIME"); //    fix sqlcontext later.
        checkWholeExpFails(
            "LOCALTIME(1+2)",
            "Argument to function 'LOCALTIME' must be a literal");
        checkWholeExpFails(
            "LOCALTIME(NULL)",
            "Argument to function 'LOCALTIME' must not be NULL");
        checkWholeExpFails(
            "LOCALTIME(CAST(NULL AS INTEGER))",
            "Argument to function 'LOCALTIME' must not be NULL");
        checkWholeExpFails(
            "LOCALTIME()",
            "No match found for function signature LOCALTIME..");
        checkExpType("LOCALTIME", "TIME(0) NOT NULL"); //  with TZ ?
        checkWholeExpFails(
            "LOCALTIME(-1)",
            "Argument to function 'LOCALTIME' must be a positive integer literal");
        checkExpFails(
            "LOCALTIME(^100000000000000^)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkWholeExpFails(
            "LOCALTIME(4)",
            "Argument to function 'LOCALTIME' must be a valid precision between '0' and '3'");
        checkWholeExpFails(
            "LOCALTIME('foo')",
            "(?s).*Cannot apply.*");

        // LOCALTIMESTAMP
        checkExp("LOCALTIMESTAMP(3)");
        checkExp("LOCALTIMESTAMP"); //    fix sqlcontext later.
        checkWholeExpFails(
            "LOCALTIMESTAMP(1+2)",
            "Argument to function 'LOCALTIMESTAMP' must be a literal");
        checkWholeExpFails(
            "LOCALTIMESTAMP()",
            "No match found for function signature LOCALTIMESTAMP..");
        checkExpType("LOCALTIMESTAMP", "TIMESTAMP(0) NOT NULL"); //  with TZ ?
        checkWholeExpFails(
            "LOCALTIMESTAMP(-1)",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal");
        checkExpFails(
            "LOCALTIMESTAMP(^100000000000000^)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkWholeExpFails(
            "LOCALTIMESTAMP(4)",
            "Argument to function 'LOCALTIMESTAMP' must be a valid precision between '0' and '3'");
        checkWholeExpFails(
            "LOCALTIMESTAMP('foo')",
            "(?s).*Cannot apply.*");

        // CURRENT_DATE
        checkWholeExpFails(
            "CURRENT_DATE(3)",
            "No match found for function signature CURRENT_DATE..NUMERIC..");
        checkExp("CURRENT_DATE"); //    fix sqlcontext later.
        checkWholeExpFails(
            "CURRENT_DATE(1+2)",
            "No match found for function signature CURRENT_DATE..NUMERIC..");
        checkWholeExpFails(
            "CURRENT_DATE()",
            "No match found for function signature CURRENT_DATE..");
        checkExpType("CURRENT_DATE", "DATE NOT NULL"); //  with TZ?
        // I guess -s1 is an expression?
        checkWholeExpFails(
            "CURRENT_DATE(-1)",
            "No match found for function signature CURRENT_DATE..NUMERIC..");
        checkWholeExpFails("CURRENT_DATE('foo')", ANY);

        // current_time
        checkExp("current_time(3)");
        checkExp("current_time"); //    fix sqlcontext later.
        checkWholeExpFails(
            "current_time(1+2)",
            "Argument to function 'CURRENT_TIME' must be a literal");
        checkWholeExpFails(
            "current_time()",
            "No match found for function signature CURRENT_TIME..");
        checkExpType("current_time", "TIME(0) NOT NULL"); //  with TZ ?
        checkWholeExpFails(
            "current_time(-1)",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");
        checkExpFails(
            "CURRENT_TIME(^100000000000000^)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkWholeExpFails(
            "CURRENT_TIME(4)",
            "Argument to function 'CURRENT_TIME' must be a valid precision between '0' and '3'");
        checkWholeExpFails(
            "current_time('foo')",
            "(?s).*Cannot apply.*");

        // current_timestamp
        checkExp("CURRENT_TIMESTAMP(3)");
        checkExp("CURRENT_TIMESTAMP"); //    fix sqlcontext later.
        check("SELECT CURRENT_TIMESTAMP AS X FROM (VALUES (1))");
        checkWholeExpFails(
            "CURRENT_TIMESTAMP(1+2)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a literal");
        checkWholeExpFails(
            "CURRENT_TIMESTAMP()",
            "No match found for function signature CURRENT_TIMESTAMP..");
        // should type be 'TIMESTAMP with TZ'?
        checkExpType("CURRENT_TIMESTAMP", "TIMESTAMP(0) NOT NULL");
        // should type be 'TIMESTAMP with TZ'?
        checkExpType("CURRENT_TIMESTAMP(2)", "TIMESTAMP(2) NOT NULL");
        checkWholeExpFails(
            "CURRENT_TIMESTAMP(-1)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a positive integer literal");
        checkExpFails(
            "CURRENT_TIMESTAMP(^100000000000000^)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkWholeExpFails(
            "CURRENT_TIMESTAMP(4)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a valid precision between '0' and '3'");
        checkWholeExpFails(
            "CURRENT_TIMESTAMP('foo')",
            "(?s).*Cannot apply.*");

        // Date literals
        checkExp("DATE '2004-12-01'");
        checkExp("TIME '12:01:01'");
        checkExp("TIME '11:59:59.99'");
        checkExp("TIME '12:01:01.001'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.001'");

        // REVIEW: Can't think of any date/time/ts literals that will parse,
        // but not validate.
    }

    /**
     * Tests casting to/from date/time types.
     */
    @Test public void testDateTimeCast() {
        checkWholeExpFails(
            "CAST(1 as DATE)",
            "Cast function cannot convert value of type INTEGER to type DATE");
        checkExp("CAST(DATE '2001-12-21' AS VARCHAR(10))");
        checkExp("CAST( '2001-12-21' AS DATE)");
        checkExp("CAST( TIMESTAMP '2001-12-21 10:12:21' AS VARCHAR(20))");
        checkExp("CAST( TIME '10:12:21' AS VARCHAR(20))");
        checkExp("CAST( '10:12:21' AS TIME)");
        checkExp("CAST( '2004-12-21 10:12:21' AS TIMESTAMP)");
    }

    @Test public void testInvalidFunction() {
        checkWholeExpFails(
            "foo()",
            "No match found for function signature FOO..");
        checkWholeExpFails(
            "mod(123)",
            "Invalid number of arguments to function 'MOD'. Was expecting 2 arguments");
    }

    @Test public void testJdbcFunctionCall() {
        checkExp("{fn log10(1)}");
        checkExp("{fn locate('','')}");
        checkExp("{fn insert('',1,2,'')}");

        // 'lower' is a valid SQL function but not valid JDBC fn; the JDBC
        // equivalent is 'lcase'
        checkWholeExpFails(
            "{fn lower('Foo' || 'Bar')}",
            "Function '\\{fn LOWER\\}' is not defined");
        checkExp("{fn lcase('Foo' || 'Bar')}");

        checkExp("{fn power(2, 3)}");
        checkWholeExpFails("{fn insert('','',1,2)}", "(?s).*.*");
        checkWholeExpFails("{fn insert('','',1)}", "(?s).*4.*");

        // TODO: this is legal JDBC syntax, but the 3 ops call is not
        // implemented
        checkWholeExpFails("{fn locate('','',1)}", ANY);
        checkWholeExpFails(
            "{fn log10('1')}",
            "(?s).*Cannot apply.*fn LOG10..<CHAR.1.>.*");
        checkWholeExpFails(
            "{fn log10(1,1)}",
            "(?s).*Encountered .fn LOG10. with 2 parameter.s.; was expecting 1 parameter.s.*");
        checkWholeExpFails(
            "{fn fn(1)}",
            "(?s).*Function '.fn FN.' is not defined.*");
        checkWholeExpFails(
            "{fn hahaha(1)}",
            "(?s).*Function '.fn HAHAHA.' is not defined.*");
    }

    @Test public void testQuotedFunction() {
        if (false) {
            // REVIEW jvs 2-Feb-2005:  I am disabling this test because I
            // removed the corresponding support from the parser.  Where in the
            // standard does it state that you're supposed to be able to quote
            // keywords for builtin functions?
            checkExp("\"CAST\"(1 as double)");
            checkExp("\"POSITION\"('b' in 'alphabet')");

            // convert and translate not yet implemented
            //        checkExp("\"CONVERT\"('b' using converstion)");
            //        checkExp("\"TRANSLATE\"('b' using translation)");
            checkExp("\"OVERLAY\"('a' PLAcing 'b' from 1)");
            checkExp("\"SUBSTRING\"('a' from 1)");
            checkExp("\"TRIM\"('b')");
        } else {
            // Very poor error message. JVS's above remarks notwithstanding,
            // the parser creates a call to TRIM with 1 rather than the
            // expected 3 args, and the remaining two args are filled in with
            // NULL literals so that we get as far as validation.
            checkExpFails(
                "^\"TRIM\"('b')^",
                "No match found for function signature TRIM\\(<CHARACTER>\\)");

            // It's OK if the function name is not quoted
            checkExpType("TRIM('b')", "VARCHAR(1) NOT NULL");
        }
    }

    @Test public void testRowtype() {
        check("values (1),(2),(1)");
        checkResultType(
            "values (1),(2),(1)",
            "RecordType(INTEGER NOT NULL EXPR$0) NOT NULL");
        check("values (1,'1'),(2,'2')");
        checkResultType(
            "values (1,'1'),(2,'2')",
            "RecordType(INTEGER NOT NULL EXPR$0, CHAR(1) NOT NULL EXPR$1) NOT NULL");
        checkResultType(
            "values true",
            "RecordType(BOOLEAN NOT NULL EXPR$0) NOT NULL");
        checkFails(
            "^values ('1'),(2)^",
            "Values passed to VALUES operator must have compatible types");
        if (todo) {
            checkColumnType("values (1),(2.0),(3)", "ROWTYPE(DOUBLE)");
        }
    }

    @Test public void testRow() {
        // double-nested rows can confuse validator namespace resolution
        checkColumnType(
            "select t.r.\"EXPR$1\".\"EXPR$2\" \n"
            + "from (select ((1,2),(3,4,5)) r from dept) t",
            "INTEGER NOT NULL");
    }

    @Test public void testMultiset() {
        checkExpType("multiset[1]", "INTEGER NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[1, CAST(null AS DOUBLE)]",
            "DOUBLE MULTISET NOT NULL");
        checkExpType(
            "multiset[1.3,2.3]",
            "DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[1,2.3, cast(4 as bigint)]",
            "DECIMAL(19, 0) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset['1','22', '333','22']",
            "CHAR(3) NOT NULL MULTISET NOT NULL");
        checkExpFails(
            "^multiset[1, '2']^",
            "Parameters must be of the same type");
        checkExpType(
            "multiset[ROW(1,2)]",
            "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[ROW(1,2),ROW(2,5)]",
            "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[ROW(1,2),ROW(3.4,5.4)]",
            "RecordType(DECIMAL(11, 1) NOT NULL EXPR$0, DECIMAL(11, 1) NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset(select*from emp)",
            "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER NOT NULL MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL MULTISET NOT NULL");
    }

    @Test public void testMultisetSetOperators() {
        checkExp("multiset[1] multiset union multiset[1,2.3]");
        checkExpType(
            "multiset[324.2] multiset union multiset[23.2,2.32]",
            "DECIMAL(5, 2) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[1] multiset union multiset[1,2.3]",
            "DECIMAL(11, 1) NOT NULL MULTISET NOT NULL");
        checkExp("multiset[1] multiset union all multiset[1,2.3]");
        checkExp("multiset[1] multiset except multiset[1,2.3]");
        checkExp("multiset[1] multiset except all multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect all multiset[1,2.3]");

        checkExpFails(
            "^multiset[1, '2']^ multiset union multiset[1]",
            "Parameters must be of the same type");
        checkExp("multiset[ROW(1,2)] multiset intersect multiset[row(3,4)]");
        if (todo) {
            checkWholeExpFails(
                "multiset[ROW(1,'2')] multiset union multiset[ROW(1,2)]",
                "Parameters must be of the same type");
        }
    }

    @Test public void testSubMultisetOf() {
        checkExpType(
            "multiset[1] submultiset of multiset[1,2.3]",
            "BOOLEAN NOT NULL");
        checkExpType(
            "multiset[1] submultiset of multiset[1]",
            "BOOLEAN NOT NULL");

        checkExpFails(
            "^multiset[1, '2']^ submultiset of multiset[1]",
            "Parameters must be of the same type");
        checkExp("multiset[ROW(1,2)] submultiset of multiset[row(3,4)]");
    }

    @Test public void testElement() {
        checkExpType("element(multiset[1])", "INTEGER NOT NULL");
        checkExpType("1.0+element(multiset[1])", "DECIMAL(12, 1) NOT NULL");
        checkExpType("element(multiset['1'])", "CHAR(1) NOT NULL");
        checkExpType("element(multiset[1e-2])", "DOUBLE NOT NULL");
        checkExpType(
            "element(multiset[multiset[cast(null as tinyint)]])",
            "TINYINT MULTISET NOT NULL");
    }

    @Test public void testMemberOf() {
        checkExpType("1 member of multiset[1]", "BOOLEAN NOT NULL");
        checkWholeExpFails(
            "1 member of multiset['1']",
            "Cannot compare values of types 'INTEGER', 'CHAR\\(1\\)'");
    }

    @Test public void testIsASet() {
        checkExp("multiset[1] is a set");
        checkExp("multiset['1'] is a set");
        checkWholeExpFails("'a' is a set", ".*Cannot apply 'IS A SET' to.*");
    }

    @Test public void testCardinality() {
        checkExpType("cardinality(multiset[1])", "INTEGER NOT NULL");
        checkExpType("cardinality(multiset['1'])", "INTEGER NOT NULL");
        checkWholeExpFails(
            "cardinality('a')",
            "Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY\\(<CHAR\\(1\\)>\\)'\\. Supported form\\(s\\): 'CARDINALITY\\(<MULTISET>\\)'\n"
            + "'CARDINALITY\\(<ARRAY>\\)'\n"
            + "'CARDINALITY\\(<MAP>\\)'");
    }

    @Test public void testIntervalTimeUnitEnumeration() {
        // Since there is validation code relaying on the fact that the
        // enumerated time unit ordinals in SqlIntervalQualifier starts with 0
        // and ends with 5, this test is here to make sure that if someone
        // changes how the time untis are setup, an early feedback will be
        // generated by this test.
        assertEquals(
            0,
            SqlIntervalQualifier.TimeUnit.YEAR.ordinal());
        assertEquals(
            1,
            SqlIntervalQualifier.TimeUnit.MONTH.ordinal());
        assertEquals(
            2,
            SqlIntervalQualifier.TimeUnit.DAY.ordinal());
        assertEquals(
            3,
            SqlIntervalQualifier.TimeUnit.HOUR.ordinal());
        assertEquals(
            4,
            SqlIntervalQualifier.TimeUnit.MINUTE.ordinal());
        assertEquals(
            5,
            SqlIntervalQualifier.TimeUnit.SECOND.ordinal());
        boolean b =
            (SqlIntervalQualifier.TimeUnit.YEAR.ordinal()
                < SqlIntervalQualifier.TimeUnit.MONTH.ordinal())
            && (SqlIntervalQualifier.TimeUnit.MONTH.ordinal()
                < SqlIntervalQualifier.TimeUnit.DAY.ordinal())
            && (SqlIntervalQualifier.TimeUnit.DAY.ordinal()
                < SqlIntervalQualifier.TimeUnit.HOUR.ordinal())
            && (SqlIntervalQualifier.TimeUnit.HOUR.ordinal()
                < SqlIntervalQualifier.TimeUnit.MINUTE.ordinal())
            && (SqlIntervalQualifier.TimeUnit.MINUTE.ordinal()
                < SqlIntervalQualifier.TimeUnit.SECOND.ordinal());
        assertTrue(b);
    }

    @Test public void testIntervalMonthsConversion() {
        checkIntervalConv("INTERVAL '1' YEAR", "12");
        checkIntervalConv("INTERVAL '5' MONTH", "5");
        checkIntervalConv("INTERVAL '3-2' YEAR TO MONTH", "38");
        checkIntervalConv("INTERVAL '-5-4' YEAR TO MONTH", "-64");
    }

    @Test public void testIntervalMillisConversion() {
        checkIntervalConv("INTERVAL '1' DAY", "86400000");
        checkIntervalConv("INTERVAL '1' HOUR", "3600000");
        checkIntervalConv("INTERVAL '1' MINUTE", "60000");
        checkIntervalConv("INTERVAL '1' SECOND", "1000");
        checkIntervalConv("INTERVAL '1:05' HOUR TO MINUTE", "3900000");
        checkIntervalConv("INTERVAL '1:05' MINUTE TO SECOND", "65000");
        checkIntervalConv("INTERVAL '1 1' DAY TO HOUR", "90000000");
        checkIntervalConv("INTERVAL '1 1:05' DAY TO MINUTE", "90300000");
        checkIntervalConv("INTERVAL '1 1:05:03' DAY TO SECOND", "90303000");
        checkIntervalConv(
            "INTERVAL '1 1:05:03.12345' DAY TO SECOND",
            "90303123");
        checkIntervalConv("INTERVAL '1.12345' SECOND", "1123");
        checkIntervalConv("INTERVAL '1:05.12345' MINUTE TO SECOND", "65123");
        checkIntervalConv("INTERVAL '1:05:03' HOUR TO SECOND", "3903000");
        checkIntervalConv("INTERVAL '1:05:03.12345' HOUR TO SECOND", "3903123");
    }

    /**
     * Runs tests for INTERVAL... YEAR that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalYearPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL '99' YEAR",
            "INTERVAL YEAR NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' YEAR(2)",
            "INTERVAL YEAR(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' YEAR(2)",
            "INTERVAL YEAR(2) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' YEAR(10)",
            "INTERVAL YEAR(10) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' YEAR(1)",
            "INTERVAL YEAR(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' YEAR(4)",
            "INTERVAL YEAR(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL '-1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL +'1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL +'+1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL +'-1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL -'1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL -'+1' YEAR",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "INTERVAL -'-1' YEAR",
            "INTERVAL YEAR NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... YEAR TO MONTH that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalYearToMonthPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL '99-11' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL '99-0' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1-2' YEAR(2) TO MONTH",
            "INTERVAL YEAR(2) TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL '99-11' YEAR(2) TO MONTH",
            "INTERVAL YEAR(2) TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL '99-0' YEAR(2) TO MONTH",
            "INTERVAL YEAR(2) TO MONTH NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647-11' YEAR(10) TO MONTH",
            "INTERVAL YEAR(10) TO MONTH NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0-0' YEAR(1) TO MONTH",
            "INTERVAL YEAR(1) TO MONTH NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2006-2' YEAR(4) TO MONTH",
            "INTERVAL YEAR(4) TO MONTH NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL '+1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'-1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'+1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'-1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'+1-2' YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... MONTH that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMonthPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL '99' MONTH",
            "INTERVAL MONTH NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' MONTH(2)",
            "INTERVAL MONTH(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' MONTH(2)",
            "INTERVAL MONTH(2) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' MONTH(10)",
            "INTERVAL MONTH(10) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' MONTH(1)",
            "INTERVAL MONTH(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' MONTH(4)",
            "INTERVAL MONTH(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL '-1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'+1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL +'-1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'+1' MONTH",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "INTERVAL -'-1' MONTH",
            "INTERVAL MONTH NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... DAY that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL '99' DAY",
            "INTERVAL DAY NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' DAY(2)",
            "INTERVAL DAY(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' DAY(2)",
            "INTERVAL DAY(2) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' DAY(10)",
            "INTERVAL DAY(10) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' DAY(1)",
            "INTERVAL DAY(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' DAY(4)",
            "INTERVAL DAY(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL '-1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL +'1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL +'+1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL +'-1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL -'1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL -'+1' DAY",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "INTERVAL -'-1' DAY",
            "INTERVAL DAY NOT NULL");
    }

    public void subTestIntervalDayToHourPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL '99 23' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL '99 0' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1 2' DAY(2) TO HOUR",
            "INTERVAL DAY(2) TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL '99 23' DAY(2) TO HOUR",
            "INTERVAL DAY(2) TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL '99 0' DAY(2) TO HOUR",
            "INTERVAL DAY(2) TO HOUR NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647 23' DAY(10) TO HOUR",
            "INTERVAL DAY(10) TO HOUR NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0 0' DAY(1) TO HOUR",
            "INTERVAL DAY(1) TO HOUR NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345 2' DAY(4) TO HOUR",
            "INTERVAL DAY(4) TO HOUR NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL '+1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'-1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'+1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'-1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'+1 2' DAY TO HOUR",
            "INTERVAL DAY TO HOUR NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... DAY TO MINUTE that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayToMinutePositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1 2:3' DAY(2) TO MINUTE",
            "INTERVAL DAY(2) TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59' DAY(2) TO MINUTE",
            "INTERVAL DAY(2) TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0' DAY(2) TO MINUTE",
            "INTERVAL DAY(2) TO MINUTE NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647 23:59' DAY(10) TO MINUTE",
            "INTERVAL DAY(10) TO MINUTE NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0 0:0' DAY(1) TO MINUTE",
            "INTERVAL DAY(1) TO MINUTE NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345 6:7' DAY(4) TO MINUTE",
            "INTERVAL DAY(4) TO MINUTE NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '+1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'-1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'+1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'-1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'+1 2:3' DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... DAY TO SECOND that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalDayToSecondPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59:59' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0:0' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59:59.999999' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0:0.0' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1 2:3:4' DAY(2) TO SECOND",
            "INTERVAL DAY(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59:59' DAY(2) TO SECOND",
            "INTERVAL DAY(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0:0' DAY(2) TO SECOND",
            "INTERVAL DAY(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)",
            "INTERVAL DAY TO SECOND(6) NOT NULL");
        checkExpType(
            "INTERVAL '99 0:0:0.0' DAY TO SECOND(6)",
            "INTERVAL DAY TO SECOND(6) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND",
            "INTERVAL DAY(10) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)",
            "INTERVAL DAY(10) TO SECOND(9) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0 0:0:0' DAY(1) TO SECOND",
            "INTERVAL DAY(1) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)",
            "INTERVAL DAY(1) TO SECOND(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345 6:7:8' DAY(4) TO SECOND",
            "INTERVAL DAY(4) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)",
            "INTERVAL DAY(4) TO SECOND(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '+1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'-1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'+1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'-1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'+1 2:3:4' DAY TO SECOND",
            "INTERVAL DAY TO SECOND NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... HOUR that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL '99' HOUR",
            "INTERVAL HOUR NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' HOUR(2)",
            "INTERVAL HOUR(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' HOUR(2)",
            "INTERVAL HOUR(2) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' HOUR(10)",
            "INTERVAL HOUR(10) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' HOUR(1)",
            "INTERVAL HOUR(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' HOUR(4)",
            "INTERVAL HOUR(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL '-1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'+1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL +'-1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'+1' HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "INTERVAL -'-1' HOUR",
            "INTERVAL HOUR NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO MINUTE that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourToMinutePositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '23:59' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99:0' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '2:3' HOUR(2) TO MINUTE",
            "INTERVAL HOUR(2) TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '23:59' HOUR(2) TO MINUTE",
            "INTERVAL HOUR(2) TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99:0' HOUR(2) TO MINUTE",
            "INTERVAL HOUR(2) TO MINUTE NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647:59' HOUR(10) TO MINUTE",
            "INTERVAL HOUR(10) TO MINUTE NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0:0' HOUR(1) TO MINUTE",
            "INTERVAL HOUR(1) TO MINUTE NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345:7' HOUR(4) TO MINUTE",
            "INTERVAL HOUR(4) TO MINUTE NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-1:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '+1:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'-2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'+2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'-2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'+2:3' HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO SECOND that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalHourToSecondPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '23:59:59' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0:0' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '23:59:59.999999' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0:0.0' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '2:3:4' HOUR(2) TO SECOND",
            "INTERVAL HOUR(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:59:59' HOUR(2) TO SECOND",
            "INTERVAL HOUR(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0:0' HOUR(2) TO SECOND",
            "INTERVAL HOUR(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:59:59.999999' HOUR TO SECOND(6)",
            "INTERVAL HOUR TO SECOND(6) NOT NULL");
        checkExpType(
            "INTERVAL '99:0:0.0' HOUR TO SECOND(6)",
            "INTERVAL HOUR TO SECOND(6) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647:59:59' HOUR(10) TO SECOND",
            "INTERVAL HOUR(10) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)",
            "INTERVAL HOUR(10) TO SECOND(9) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0:0:0' HOUR(1) TO SECOND",
            "INTERVAL HOUR(1) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)",
            "INTERVAL HOUR(1) TO SECOND(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345:7:8' HOUR(4) TO SECOND",
            "INTERVAL HOUR(4) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)",
            "INTERVAL HOUR(4) TO SECOND(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '+2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'-2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'+2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'-2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'+2:3:4' HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... MINUTE that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMinutePositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '99' MINUTE",
            "INTERVAL MINUTE NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' MINUTE(2)",
            "INTERVAL MINUTE(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' MINUTE(2)",
            "INTERVAL MINUTE(2) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' MINUTE(10)",
            "INTERVAL MINUTE(10) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' MINUTE(1)",
            "INTERVAL MINUTE(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' MINUTE(4)",
            "INTERVAL MINUTE(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL '-1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'+1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL +'-1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'+1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
        checkExpType(
            "INTERVAL -'-1' MINUTE",
            "INTERVAL MINUTE NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... MINUTE TO SECOND that should pass both parser
     * and validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalMinuteToSecondPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '2:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '59:59' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '59:59.999999' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0.0' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '2:4' MINUTE(2) TO SECOND",
            "INTERVAL MINUTE(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:59' MINUTE(2) TO SECOND",
            "INTERVAL MINUTE(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:0' MINUTE(2) TO SECOND",
            "INTERVAL MINUTE(2) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99:59.999999' MINUTE TO SECOND(6)",
            "INTERVAL MINUTE TO SECOND(6) NOT NULL");
        checkExpType(
            "INTERVAL '99:0.0' MINUTE TO SECOND(6)",
            "INTERVAL MINUTE TO SECOND(6) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647:59' MINUTE(10) TO SECOND",
            "INTERVAL MINUTE(10) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)",
            "INTERVAL MINUTE(10) TO SECOND(9) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0:0' MINUTE(1) TO SECOND",
            "INTERVAL MINUTE(1) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)",
            "INTERVAL MINUTE(1) TO SECOND(1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '2345:8' MINUTE(4) TO SECOND",
            "INTERVAL MINUTE(4) TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)",
            "INTERVAL MINUTE(4) TO SECOND(4) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '-3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL '+3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'-3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'+3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'-3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'+3:4' MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... SECOND that should pass both parser and
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXPositive() tests.
     */
    public void subTestIntervalSecondPositive()
    {
        // default precision
        checkExpType(
            "INTERVAL '1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL '99' SECOND",
            "INTERVAL SECOND NOT NULL");

        // explicit precision equal to default
        checkExpType(
            "INTERVAL '1' SECOND(2)",
            "INTERVAL SECOND(2) NOT NULL");
        checkExpType(
            "INTERVAL '99' SECOND(2)",
            "INTERVAL SECOND(2) NOT NULL");
        checkExpType(
            "INTERVAL '1' SECOND(2, 6)",
            "INTERVAL SECOND(2, 6) NOT NULL");
        checkExpType(
            "INTERVAL '99' SECOND(2, 6)",
            "INTERVAL SECOND(2, 6) NOT NULL");

        // max precision
        checkExpType(
            "INTERVAL '2147483647' SECOND(10)",
            "INTERVAL SECOND(10) NOT NULL");
        checkExpType(
            "INTERVAL '2147483647.999999999' SECOND(10, 9)",
            "INTERVAL SECOND(10, 9) NOT NULL");

        // min precision
        checkExpType(
            "INTERVAL '0' SECOND(1)",
            "INTERVAL SECOND(1) NOT NULL");
        checkExpType(
            "INTERVAL '0.0' SECOND(1, 1)",
            "INTERVAL SECOND(1, 1) NOT NULL");

        // alternate precision
        checkExpType(
            "INTERVAL '1234' SECOND(4)",
            "INTERVAL SECOND(4) NOT NULL");
        checkExpType(
            "INTERVAL '1234.56789' SECOND(4, 5)",
            "INTERVAL SECOND(4, 5) NOT NULL");

        // sign
        checkExpType(
            "INTERVAL '+1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL '-1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'+1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL +'-1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'+1' SECOND",
            "INTERVAL SECOND NOT NULL");
        checkExpType(
            "INTERVAL -'-1' SECOND",
            "INTERVAL SECOND NOT NULL");
    }

    /**
     * Runs tests for INTERVAL... YEAR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalYearNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' YEAR",
            "Illegal interval literal format '-' for INTERVAL YEAR.*");
        checkWholeExpFails(
            "INTERVAL '1-2' YEAR",
            "Illegal interval literal format '1-2' for INTERVAL YEAR.*");
        checkWholeExpFails(
            "INTERVAL '1.2' YEAR",
            "Illegal interval literal format '1.2' for INTERVAL YEAR.*");
        checkWholeExpFails(
            "INTERVAL '1 2' YEAR",
            "Illegal interval literal format '1 2' for INTERVAL YEAR.*");
        checkWholeExpFails(
            "INTERVAL '1-2' YEAR(2)",
            "Illegal interval literal format '1-2' for INTERVAL YEAR\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' YEAR",
            "Illegal interval literal format 'bogus text' for INTERVAL YEAR.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' YEAR",
            "Illegal interval literal format '--1' for INTERVAL YEAR.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' YEAR",
            "Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' YEAR(2)",
            "Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' YEAR(3)",
            "Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' YEAR(3)",
            "Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' YEAR(10)",
            "Interval field value 2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' YEAR(10)",
            "Interval field value -2,147,483,648 exceeds precision of YEAR\\(10\\) field");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' YEAR(11^)^",
            "Interval leading field precision '11' out of range for INTERVAL YEAR\\(11\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' YEAR(0^)^",
            "Interval leading field precision '0' out of range for INTERVAL YEAR\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... YEAR TO MONTH that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalYearToMonthNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' YEAR TO MONTH",
            "Illegal interval literal format '-' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1' YEAR TO MONTH",
            "Illegal interval literal format '1' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1:2' YEAR TO MONTH",
            "Illegal interval literal format '1:2' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1.2' YEAR TO MONTH",
            "Illegal interval literal format '1.2' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1 2' YEAR TO MONTH",
            "Illegal interval literal format '1 2' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1:2' YEAR(2) TO MONTH",
            "Illegal interval literal format '1:2' for INTERVAL YEAR\\(2\\) TO MONTH");
        checkWholeExpFails(
            "INTERVAL 'bogus text' YEAR TO MONTH",
            "Illegal interval literal format 'bogus text' for INTERVAL YEAR TO MONTH");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1-2' YEAR TO MONTH",
            "Illegal interval literal format '--1-2' for INTERVAL YEAR TO MONTH");
        checkWholeExpFails(
            "INTERVAL '1--2' YEAR TO MONTH",
            "Illegal interval literal format '1--2' for INTERVAL YEAR TO MONTH");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100-0' YEAR TO MONTH",
            "Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100-0' YEAR(2) TO MONTH",
            "Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000-0' YEAR(3) TO MONTH",
            "Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000-0' YEAR(3) TO MONTH",
            "Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648-0' YEAR(10) TO MONTH",
            "Interval field value 2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648-0' YEAR(10) TO MONTH",
            "Interval field value -2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1-12' YEAR TO MONTH",
            "Illegal interval literal format '1-12' for INTERVAL YEAR TO MONTH.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1-1' YEAR(11) TO ^MONTH^",
            "Interval leading field precision '11' out of range for INTERVAL YEAR\\(11\\) TO MONTH");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0-0' YEAR(0) TO ^MONTH^",
            "Interval leading field precision '0' out of range for INTERVAL YEAR\\(0\\) TO MONTH");
    }

    /**
     * Runs tests for INTERVAL... MONTH that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalMonthNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' MONTH",
            "Illegal interval literal format '-' for INTERVAL MONTH.*");
        checkWholeExpFails(
            "INTERVAL '1-2' MONTH",
            "Illegal interval literal format '1-2' for INTERVAL MONTH.*");
        checkWholeExpFails(
            "INTERVAL '1.2' MONTH",
            "Illegal interval literal format '1.2' for INTERVAL MONTH.*");
        checkWholeExpFails(
            "INTERVAL '1 2' MONTH",
            "Illegal interval literal format '1 2' for INTERVAL MONTH.*");
        checkWholeExpFails(
            "INTERVAL '1-2' MONTH(2)",
            "Illegal interval literal format '1-2' for INTERVAL MONTH\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' MONTH",
            "Illegal interval literal format 'bogus text' for INTERVAL MONTH.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' MONTH",
            "Illegal interval literal format '--1' for INTERVAL MONTH.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' MONTH",
            "Interval field value 100 exceeds precision of MONTH\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' MONTH(2)",
            "Interval field value 100 exceeds precision of MONTH\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' MONTH(3)",
            "Interval field value 1,000 exceeds precision of MONTH\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' MONTH(3)",
            "Interval field value -1,000 exceeds precision of MONTH\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' MONTH(10)",
            "Interval field value 2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' MONTH(10)",
            "Interval field value -2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' MONTH(11^)^",
            "Interval leading field precision '11' out of range for INTERVAL MONTH\\(11\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' MONTH(0^)^",
            "Interval leading field precision '0' out of range for INTERVAL MONTH\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... DAY that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalDayNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' DAY",
            "Illegal interval literal format '-' for INTERVAL DAY.*");
        checkWholeExpFails(
            "INTERVAL '1-2' DAY",
            "Illegal interval literal format '1-2' for INTERVAL DAY.*");
        checkWholeExpFails(
            "INTERVAL '1.2' DAY",
            "Illegal interval literal format '1.2' for INTERVAL DAY.*");
        checkWholeExpFails(
            "INTERVAL '1 2' DAY",
            "Illegal interval literal format '1 2' for INTERVAL DAY.*");
        checkWholeExpFails(
            "INTERVAL '1:2' DAY",
            "Illegal interval literal format '1:2' for INTERVAL DAY.*");
        checkWholeExpFails(
            "INTERVAL '1-2' DAY(2)",
            "Illegal interval literal format '1-2' for INTERVAL DAY\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' DAY",
            "Illegal interval literal format 'bogus text' for INTERVAL DAY.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' DAY",
            "Illegal interval literal format '--1' for INTERVAL DAY.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' DAY",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' DAY(2)",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' DAY(3)",
            "Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' DAY(3)",
            "Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' DAY(10)",
            "Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' DAY(10)",
            "Interval field value -2,147,483,648 exceeds precision of DAY\\(10\\) field.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' DAY(11^)^",
            "Interval leading field precision '11' out of range for INTERVAL DAY\\(11\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' DAY(0^)^",
            "Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... DAY TO HOUR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalDayToHourNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' DAY TO HOUR",
            "Illegal interval literal format '-' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1' DAY TO HOUR",
            "Illegal interval literal format '1' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1:2' DAY TO HOUR",
            "Illegal interval literal format '1:2' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1.2' DAY TO HOUR",
            "Illegal interval literal format '1.2' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1 x' DAY TO HOUR",
            "Illegal interval literal format '1 x' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL ' ' DAY TO HOUR",
            "Illegal interval literal format ' ' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1:2' DAY(2) TO HOUR",
            "Illegal interval literal format '1:2' for INTERVAL DAY\\(2\\) TO HOUR");
        checkWholeExpFails(
            "INTERVAL 'bogus text' DAY TO HOUR",
            "Illegal interval literal format 'bogus text' for INTERVAL DAY TO HOUR");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1 1' DAY TO HOUR",
            "Illegal interval literal format '--1 1' for INTERVAL DAY TO HOUR");
        checkWholeExpFails(
            "INTERVAL '1 -1' DAY TO HOUR",
            "Illegal interval literal format '1 -1' for INTERVAL DAY TO HOUR");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100 0' DAY TO HOUR",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100 0' DAY(2) TO HOUR",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000 0' DAY(3) TO HOUR",
            "Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000 0' DAY(3) TO HOUR",
            "Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648 0' DAY(10) TO HOUR",
            "Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648 0' DAY(10) TO HOUR",
            "Interval field value -2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1 24' DAY TO HOUR",
            "Illegal interval literal format '1 24' for INTERVAL DAY TO HOUR.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1 1' DAY(11) TO ^HOUR^",
            "Interval leading field precision '11' out of range for INTERVAL DAY\\(11\\) TO HOUR");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0 0' DAY(0) TO ^HOUR^",
            "Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\) TO HOUR");
    }

    /**
     * Runs tests for INTERVAL... DAY TO MINUTE that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalDayToMinuteNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL ' :' DAY TO MINUTE",
            "Illegal interval literal format ' :' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1' DAY TO MINUTE",
            "Illegal interval literal format '1' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 2' DAY TO MINUTE",
            "Illegal interval literal format '1 2' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1:2' DAY TO MINUTE",
            "Illegal interval literal format '1:2' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1.2' DAY TO MINUTE",
            "Illegal interval literal format '1.2' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL 'x 1:1' DAY TO MINUTE",
            "Illegal interval literal format 'x 1:1' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 x:1' DAY TO MINUTE",
            "Illegal interval literal format '1 x:1' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1:x' DAY TO MINUTE",
            "Illegal interval literal format '1 1:x' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1:2:3' DAY TO MINUTE",
            "Illegal interval literal format '1 1:2:3' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1:1:1.2' DAY TO MINUTE",
            "Illegal interval literal format '1 1:1:1.2' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1:2:3' DAY(2) TO MINUTE",
            "Illegal interval literal format '1 1:2:3' for INTERVAL DAY\\(2\\) TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1' DAY(2) TO MINUTE",
            "Illegal interval literal format '1 1' for INTERVAL DAY\\(2\\) TO MINUTE");
        checkWholeExpFails(
            "INTERVAL 'bogus text' DAY TO MINUTE",
            "Illegal interval literal format 'bogus text' for INTERVAL DAY TO MINUTE");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1 1:1' DAY TO MINUTE",
            "Illegal interval literal format '--1 1:1' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 -1:1' DAY TO MINUTE",
            "Illegal interval literal format '1 -1:1' for INTERVAL DAY TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 1:-1' DAY TO MINUTE",
            "Illegal interval literal format '1 1:-1' for INTERVAL DAY TO MINUTE");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100 0:0' DAY TO MINUTE",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100 0:0' DAY(2) TO MINUTE",
            "Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000 0:0' DAY(3) TO MINUTE",
            "Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000 0:0' DAY(3) TO MINUTE",
            "Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648 0:0' DAY(10) TO MINUTE",
            "Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648 0:0' DAY(10) TO MINUTE",
            "Interval field value -2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1 24:1' DAY TO MINUTE",
            "Illegal interval literal format '1 24:1' for INTERVAL DAY TO MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1 1:60' DAY TO MINUTE",
            "Illegal interval literal format '1 1:60' for INTERVAL DAY TO MINUTE.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1 1:1' DAY(11) TO ^MINUTE^",
            "Interval leading field precision '11' out of range for INTERVAL DAY\\(11\\) TO MINUTE");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0 0' DAY(0) TO ^MINUTE^",
            "Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\) TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... DAY TO SECOND that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalDayToSecondNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL ' ::' DAY TO SECOND",
            "Illegal interval literal format ' ::' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL ' ::.' DAY TO SECOND",
            "Illegal interval literal format ' ::\\.' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1' DAY TO SECOND",
            "Illegal interval literal format '1' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 2' DAY TO SECOND",
            "Illegal interval literal format '1 2' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:2' DAY TO SECOND",
            "Illegal interval literal format '1:2' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1.2' DAY TO SECOND",
            "Illegal interval literal format '1\\.2' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' DAY TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2:x' DAY TO SECOND",
            "Illegal interval literal format '1 1:2:x' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:2:3' DAY TO SECOND",
            "Illegal interval literal format '1:2:3' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1:1.2' DAY TO SECOND",
            "Illegal interval literal format '1:1:1\\.2' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' DAY(2) TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL DAY\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1' DAY(2) TO SECOND",
            "Illegal interval literal format '1 1' for INTERVAL DAY\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL 'bogus text' DAY TO SECOND",
            "Illegal interval literal format 'bogus text' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '2345 6:7:8901' DAY TO SECOND(4)",
            "Illegal interval literal format '2345 6:7:8901' for INTERVAL DAY TO SECOND\\(4\\)");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1 1:1:1' DAY TO SECOND",
            "Illegal interval literal format '--1 1:1:1' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 -1:1:1' DAY TO SECOND",
            "Illegal interval literal format '1 -1:1:1' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:-1:1' DAY TO SECOND",
            "Illegal interval literal format '1 1:-1:1' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:1:-1' DAY TO SECOND",
            "Illegal interval literal format '1 1:1:-1' for INTERVAL DAY TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:1:1.-1' DAY TO SECOND",
            "Illegal interval literal format '1 1:1:1.-1' for INTERVAL DAY TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100 0' DAY TO SECOND",
            "Illegal interval literal format '100 0' for INTERVAL DAY TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '100 0' DAY(2) TO SECOND",
            "Illegal interval literal format '100 0' for INTERVAL DAY\\(2\\) TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1000 0' DAY(3) TO SECOND",
            "Illegal interval literal format '1000 0' for INTERVAL DAY\\(3\\) TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '-1000 0' DAY(3) TO SECOND",
            "Illegal interval literal format '-1000 0' for INTERVAL DAY\\(3\\) TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '2147483648 1:1:0' DAY(10) TO SECOND",
            "Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648 1:1:0' DAY(10) TO SECOND",
            "Interval field value -2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648 0' DAY(10) TO SECOND",
            "Illegal interval literal format '2147483648 0' for INTERVAL DAY\\(10\\) TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648 0' DAY(10) TO SECOND",
            "Illegal interval literal format '-2147483648 0' for INTERVAL DAY\\(10\\) TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 24:1:1' DAY TO SECOND",
            "Illegal interval literal format '1 24:1:1' for INTERVAL DAY TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 1:60:1' DAY TO SECOND",
            "Illegal interval literal format '1 1:60:1' for INTERVAL DAY TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 1:1:60' DAY TO SECOND",
            "Illegal interval literal format '1 1:1:60' for INTERVAL DAY TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 1:1:1.0000001' DAY TO SECOND",
            "Illegal interval literal format '1 1:1:1\\.0000001' for INTERVAL DAY TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)",
            "Illegal interval literal format '1 1:1:1\\.0001' for INTERVAL DAY TO SECOND\\(3\\).*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1 1' DAY(11) TO ^SECOND^",
            "Interval leading field precision '11' out of range for INTERVAL DAY\\(11\\) TO SECOND");
        checkExpFails(
            "INTERVAL '1 1' DAY TO SECOND(10^)^",
            "Interval fractional second precision '10' out of range for INTERVAL DAY TO SECOND\\(10\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0 0:0:0' DAY(0) TO ^SECOND^",
            "Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\) TO SECOND");
        checkExpFails(
            "INTERVAL '0 0:0:0' DAY TO SECOND(0^)^",
            "Interval fractional second precision '0' out of range for INTERVAL DAY TO SECOND\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... HOUR that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalHourNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' HOUR",
            "Illegal interval literal format '-' for INTERVAL HOUR.*");
        checkWholeExpFails(
            "INTERVAL '1-2' HOUR",
            "Illegal interval literal format '1-2' for INTERVAL HOUR.*");
        checkWholeExpFails(
            "INTERVAL '1.2' HOUR",
            "Illegal interval literal format '1.2' for INTERVAL HOUR.*");
        checkWholeExpFails(
            "INTERVAL '1 2' HOUR",
            "Illegal interval literal format '1 2' for INTERVAL HOUR.*");
        checkWholeExpFails(
            "INTERVAL '1:2' HOUR",
            "Illegal interval literal format '1:2' for INTERVAL HOUR.*");
        checkWholeExpFails(
            "INTERVAL '1-2' HOUR(2)",
            "Illegal interval literal format '1-2' for INTERVAL HOUR\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' HOUR",
            "Illegal interval literal format 'bogus text' for INTERVAL HOUR.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' HOUR",
            "Illegal interval literal format '--1' for INTERVAL HOUR.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' HOUR",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' HOUR(2)",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' HOUR(3)",
            "Interval field value 1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' HOUR(3)",
            "Interval field value -1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' HOUR(10)",
            "Interval field value 2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' HOUR(10)",
            "Interval field value -2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' HOUR(11^)^",
            "Interval leading field precision '11' out of range for INTERVAL HOUR\\(11\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' HOUR(0^)^",
            "Interval leading field precision '0' out of range for INTERVAL HOUR\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO MINUTE that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalHourToMinuteNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL ':' HOUR TO MINUTE",
            "Illegal interval literal format ':' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1' HOUR TO MINUTE",
            "Illegal interval literal format '1' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1:x' HOUR TO MINUTE",
            "Illegal interval literal format '1:x' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1.2' HOUR TO MINUTE",
            "Illegal interval literal format '1.2' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 2' HOUR TO MINUTE",
            "Illegal interval literal format '1 2' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1:2:3' HOUR TO MINUTE",
            "Illegal interval literal format '1:2:3' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1 2' HOUR(2) TO MINUTE",
            "Illegal interval literal format '1 2' for INTERVAL HOUR\\(2\\) TO MINUTE");
        checkWholeExpFails(
            "INTERVAL 'bogus text' HOUR TO MINUTE",
            "Illegal interval literal format 'bogus text' for INTERVAL HOUR TO MINUTE");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1:1' HOUR TO MINUTE",
            "Illegal interval literal format '--1:1' for INTERVAL HOUR TO MINUTE");
        checkWholeExpFails(
            "INTERVAL '1:-1' HOUR TO MINUTE",
            "Illegal interval literal format '1:-1' for INTERVAL HOUR TO MINUTE");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100:0' HOUR TO MINUTE",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100:0' HOUR(2) TO MINUTE",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000:0' HOUR(3) TO MINUTE",
            "Interval field value 1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000:0' HOUR(3) TO MINUTE",
            "Interval field value -1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648:0' HOUR(10) TO MINUTE",
            "Interval field value 2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648:0' HOUR(10) TO MINUTE",
            "Interval field value -2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1:60' HOUR TO MINUTE",
            "Illegal interval literal format '1:60' for INTERVAL HOUR TO MINUTE.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1:1' HOUR(11) TO ^MINUTE^",
            "Interval leading field precision '11' out of range for INTERVAL HOUR\\(11\\) TO MINUTE");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0:0' HOUR(0) TO ^MINUTE^",
            "Interval leading field precision '0' out of range for INTERVAL HOUR\\(0\\) TO MINUTE");
    }

    /**
     * Runs tests for INTERVAL... HOUR TO SECOND that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalHourToSecondNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '::' HOUR TO SECOND",
            "Illegal interval literal format '::' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '::.' HOUR TO SECOND",
            "Illegal interval literal format '::\\.' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1' HOUR TO SECOND",
            "Illegal interval literal format '1' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 2' HOUR TO SECOND",
            "Illegal interval literal format '1 2' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:2' HOUR TO SECOND",
            "Illegal interval literal format '1:2' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1.2' HOUR TO SECOND",
            "Illegal interval literal format '1\\.2' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' HOUR TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:2:x' HOUR TO SECOND",
            "Illegal interval literal format '1:2:x' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:x:3' HOUR TO SECOND",
            "Illegal interval literal format '1:x:3' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1:1.x' HOUR TO SECOND",
            "Illegal interval literal format '1:1:1\\.x' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' HOUR(2) TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL HOUR\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1' HOUR(2) TO SECOND",
            "Illegal interval literal format '1 1' for INTERVAL HOUR\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL 'bogus text' HOUR TO SECOND",
            "Illegal interval literal format 'bogus text' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '6:7:8901' HOUR TO SECOND(4)",
            "Illegal interval literal format '6:7:8901' for INTERVAL HOUR TO SECOND\\(4\\)");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1:1:1' HOUR TO SECOND",
            "Illegal interval literal format '--1:1:1' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:-1:1' HOUR TO SECOND",
            "Illegal interval literal format '1:-1:1' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1:-1' HOUR TO SECOND",
            "Illegal interval literal format '1:1:-1' for INTERVAL HOUR TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1:1.-1' HOUR TO SECOND",
            "Illegal interval literal format '1:1:1\\.-1' for INTERVAL HOUR TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100:0:0' HOUR TO SECOND",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100:0:0' HOUR(2) TO SECOND",
            "Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000:0:0' HOUR(3) TO SECOND",
            "Interval field value 1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000:0:0' HOUR(3) TO SECOND",
            "Interval field value -1,000 exceeds precision of HOUR\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648:0:0' HOUR(10) TO SECOND",
            "Interval field value 2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND",
            "Interval field value -2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1:60:1' HOUR TO SECOND",
            "Illegal interval literal format '1:60:1' for INTERVAL HOUR TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:1:60' HOUR TO SECOND",
            "Illegal interval literal format '1:1:60' for INTERVAL HOUR TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:1:1.0000001' HOUR TO SECOND",
            "Illegal interval literal format '1:1:1\\.0000001' for INTERVAL HOUR TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:1:1.0001' HOUR TO SECOND(3)",
            "Illegal interval literal format '1:1:1\\.0001' for INTERVAL HOUR TO SECOND\\(3\\).*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1:1:1' HOUR(11) TO ^SECOND^",
            "Interval leading field precision '11' out of range for INTERVAL HOUR\\(11\\) TO SECOND");
        checkExpFails(
            "INTERVAL '1:1:1' HOUR TO SECOND(10^)^",
            "Interval fractional second precision '10' out of range for INTERVAL HOUR TO SECOND\\(10\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0:0:0' HOUR(0) TO ^SECOND^",
            "Interval leading field precision '0' out of range for INTERVAL HOUR\\(0\\) TO SECOND");
        checkExpFails(
            "INTERVAL '0:0:0' HOUR TO SECOND(0^)^",
            "Interval fractional second precision '0' out of range for INTERVAL HOUR TO SECOND\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... MINUTE that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalMinuteNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL '-' MINUTE",
            "Illegal interval literal format '-' for INTERVAL MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1-2' MINUTE",
            "Illegal interval literal format '1-2' for INTERVAL MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1.2' MINUTE",
            "Illegal interval literal format '1.2' for INTERVAL MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1 2' MINUTE",
            "Illegal interval literal format '1 2' for INTERVAL MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1:2' MINUTE",
            "Illegal interval literal format '1:2' for INTERVAL MINUTE.*");
        checkWholeExpFails(
            "INTERVAL '1-2' MINUTE(2)",
            "Illegal interval literal format '1-2' for INTERVAL MINUTE\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' MINUTE",
            "Illegal interval literal format 'bogus text' for INTERVAL MINUTE.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' MINUTE",
            "Illegal interval literal format '--1' for INTERVAL MINUTE.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' MINUTE",
            "Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' MINUTE(2)",
            "Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' MINUTE(3)",
            "Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' MINUTE(3)",
            "Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' MINUTE(10)",
            "Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' MINUTE(10)",
            "Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' MINUTE(11^)^",
            "Interval leading field precision '11' out of range for INTERVAL MINUTE\\(11\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' MINUTE(0^)^",
            "Interval leading field precision '0' out of range for INTERVAL MINUTE\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... MINUTE TO SECOND that should pass parser but
     * fail validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalMinuteToSecondNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL ':' MINUTE TO SECOND",
            "Illegal interval literal format ':' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL ':.' MINUTE TO SECOND",
            "Illegal interval literal format ':\\.' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1' MINUTE TO SECOND",
            "Illegal interval literal format '1' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 2' MINUTE TO SECOND",
            "Illegal interval literal format '1 2' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1.2' MINUTE TO SECOND",
            "Illegal interval literal format '1\\.2' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' MINUTE TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:x' MINUTE TO SECOND",
            "Illegal interval literal format '1:x' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL 'x:3' MINUTE TO SECOND",
            "Illegal interval literal format 'x:3' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1.x' MINUTE TO SECOND",
            "Illegal interval literal format '1:1\\.x' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1:2' MINUTE(2) TO SECOND",
            "Illegal interval literal format '1 1:2' for INTERVAL MINUTE\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1 1' MINUTE(2) TO SECOND",
            "Illegal interval literal format '1 1' for INTERVAL MINUTE\\(2\\) TO SECOND");
        checkWholeExpFails(
            "INTERVAL 'bogus text' MINUTE TO SECOND",
            "Illegal interval literal format 'bogus text' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '7:8901' MINUTE TO SECOND(4)",
            "Illegal interval literal format '7:8901' for INTERVAL MINUTE TO SECOND\\(4\\)");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1:1' MINUTE TO SECOND",
            "Illegal interval literal format '--1:1' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:-1' MINUTE TO SECOND",
            "Illegal interval literal format '1:-1' for INTERVAL MINUTE TO SECOND");
        checkWholeExpFails(
            "INTERVAL '1:1.-1' MINUTE TO SECOND",
            "Illegal interval literal format '1:1.-1' for INTERVAL MINUTE TO SECOND");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        //  plus >max value for mid/end fields
        checkWholeExpFails(
            "INTERVAL '100:0' MINUTE TO SECOND",
            "Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100:0' MINUTE(2) TO SECOND",
            "Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000:0' MINUTE(3) TO SECOND",
            "Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000:0' MINUTE(3) TO SECOND",
            "Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648:0' MINUTE(10) TO SECOND",
            "Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648:0' MINUTE(10) TO SECOND",
            "Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1:60' MINUTE TO SECOND",
            "Illegal interval literal format '1:60' for"
            + " INTERVAL MINUTE TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:1.0000001' MINUTE TO SECOND",
            "Illegal interval literal format '1:1\\.0000001' for"
            + " INTERVAL MINUTE TO SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)",
            "Illegal interval literal format '1:1:1\\.0001' for"
            + " INTERVAL MINUTE TO SECOND\\(3\\).*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1:1' MINUTE(11) TO ^SECOND^",
            "Interval leading field precision '11' out of range for"
            + " INTERVAL MINUTE\\(11\\) TO SECOND");
        checkExpFails(
            "INTERVAL '1:1' MINUTE TO SECOND(10^)^",
            "Interval fractional second precision '10' out of range for"
            + " INTERVAL MINUTE TO SECOND\\(10\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0:0' MINUTE(0) TO ^SECOND^",
            "Interval leading field precision '0' out of range for"
            + " INTERVAL MINUTE\\(0\\) TO SECOND");
        checkExpFails(
            "INTERVAL '0:0' MINUTE TO SECOND(0^)^",
            "Interval fractional second precision '0' out of range for"
            + " INTERVAL MINUTE TO SECOND\\(0\\)");
    }

    /**
     * Runs tests for INTERVAL... SECOND that should pass parser but fail
     * validator. A substantially identical set of tests exists in
     * SqlParserTest, and any changes here should be synchronized there.
     * Similarly, any changes to tests here should be echoed appropriately to
     * each of the other 12 subTestIntervalXXXNegative() tests.
     */
    public void subTestIntervalSecondNegative()
    {
        // Qualifier - field mismatches
        checkWholeExpFails(
            "INTERVAL ':' SECOND",
            "Illegal interval literal format ':' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '.' SECOND",
            "Illegal interval literal format '\\.' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1-2' SECOND",
            "Illegal interval literal format '1-2' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1.x' SECOND",
            "Illegal interval literal format '1\\.x' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL 'x.1' SECOND",
            "Illegal interval literal format 'x\\.1' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1 2' SECOND",
            "Illegal interval literal format '1 2' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1:2' SECOND",
            "Illegal interval literal format '1:2' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1-2' SECOND(2)",
            "Illegal interval literal format '1-2' for INTERVAL SECOND\\(2\\)");
        checkWholeExpFails(
            "INTERVAL 'bogus text' SECOND",
            "Illegal interval literal format 'bogus text' for INTERVAL SECOND.*");

        // negative field values
        checkWholeExpFails(
            "INTERVAL '--1' SECOND",
            "Illegal interval literal format '--1' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1.-1' SECOND",
            "Illegal interval literal format '1.-1' for INTERVAL SECOND.*");

        // Field value out of range
        //  (default, explicit default, alt, neg alt, max, neg max)
        checkWholeExpFails(
            "INTERVAL '100' SECOND",
            "Interval field value 100 exceeds precision of SECOND\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '100' SECOND(2)",
            "Interval field value 100 exceeds precision of SECOND\\(2\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1000' SECOND(3)",
            "Interval field value 1,000 exceeds precision of SECOND\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-1000' SECOND(3)",
            "Interval field value -1,000 exceeds precision of SECOND\\(3\\) field.*");
        checkWholeExpFails(
            "INTERVAL '2147483648' SECOND(10)",
            "Interval field value 2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '-2147483648' SECOND(10)",
            "Interval field value -2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
        checkWholeExpFails(
            "INTERVAL '1.0000001' SECOND",
            "Illegal interval literal format '1\\.0000001' for INTERVAL SECOND.*");
        checkWholeExpFails(
            "INTERVAL '1.0000001' SECOND(2)",
            "Illegal interval literal format '1\\.0000001' for INTERVAL SECOND\\(2\\).*");
        checkWholeExpFails(
            "INTERVAL '1.0001' SECOND(2, 3)",
            "Illegal interval literal format '1\\.0001' for INTERVAL SECOND\\(2, 3\\).*");
        checkWholeExpFails(
            "INTERVAL '1.0000000001' SECOND(2, 9)",
            "Illegal interval literal format '1\\.0000000001' for"
            + " INTERVAL SECOND\\(2, 9\\).*");

        // precision > maximum
        checkExpFails(
            "INTERVAL '1' SECOND(11^)^",
            "Interval leading field precision '11' out of range for"
            + " INTERVAL SECOND\\(11\\)");
        checkExpFails(
            "INTERVAL '1.1' SECOND(1, 10^)^",
            "Interval fractional second precision '10' out of range for"
            + " INTERVAL SECOND\\(1, 10\\)");

        // precision < minimum allowed)
        // note: parser will catch negative values, here we
        // just need to check for 0
        checkExpFails(
            "INTERVAL '0' SECOND(0^)^",
            "Interval leading field precision '0' out of range for"
            + " INTERVAL SECOND\\(0\\)");
        checkExpFails(
            "INTERVAL '0' SECOND(1, 0^)^",
            "Interval fractional second precision '0' out of range for"
            + " INTERVAL SECOND\\(1, 0\\)");
    }

    @Test public void testIntervalLiterals() {
        // First check that min, max, and defaults are what we expect
        // (values used in subtests depend on these being true to
        // accurately test bounds)
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getMinPrecision() == 1);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getMinPrecision() == 1);
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getMaxPrecision() == 10);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getMaxPrecision() == 10);
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultPrecision() == 2);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getDefaultPrecision() == 2);
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getMinScale() == 1);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getMinScale() == 1);
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getMaxScale() == 9);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getMaxScale() == 9);
        assertTrue(
            SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultScale() == 6);
        assertTrue(
            SqlTypeName.INTERVAL_DAY_TIME.getDefaultScale() == 6);

        // Tests that should pass both parser and validator
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

        // Tests that should pass parser but fail validator
        subTestIntervalYearNegative();
        subTestIntervalYearToMonthNegative();
        subTestIntervalMonthNegative();
        subTestIntervalDayNegative();
        subTestIntervalDayToHourNegative();
        subTestIntervalDayToMinuteNegative();
        subTestIntervalDayToSecondNegative();
        subTestIntervalHourNegative();
        subTestIntervalHourToMinuteNegative();
        subTestIntervalHourToSecondNegative();
        subTestIntervalMinuteNegative();
        subTestIntervalMinuteToSecondNegative();
        subTestIntervalSecondNegative();

        // Miscellaneous
        // fractional value is not OK, even if it is 0
        checkWholeExpFails(
            "INTERVAL '1.0' HOUR",
            "Illegal interval literal format '1.0' for INTERVAL HOUR");
        // only seconds are allowed to have a fractional part
        checkExpType(
            "INTERVAL '1.0' SECOND",
            "INTERVAL SECOND NOT NULL");
        // leading zeroes do not cause precision to be exceeded
        checkExpType(
            "INTERVAL '0999' MONTH(3)",
            "INTERVAL MONTH(3) NOT NULL");
    }

    @Test public void testIntervalOperators() {
        checkExpType("interval '1' hour + TIME '8:8:8'", "TIME(0) NOT NULL");
        checkExpType("TIME '8:8:8' - interval '1' hour", "TIME(0) NOT NULL");
        checkExpType("TIME '8:8:8' + interval '1' hour", "TIME(0) NOT NULL");

        checkExpType(
            "interval '1' day + interval '1' DAY(4)",
            "INTERVAL DAY(4) NOT NULL");
        checkExpType(
            "interval '1' day(5) + interval '1' DAY",
            "INTERVAL DAY(5) NOT NULL");
        checkExpType(
            "interval '1' day + interval '1' HOUR(10)",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "interval '1' day + interval '1' MINUTE",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "interval '1' day + interval '1' second",
            "INTERVAL DAY TO SECOND NOT NULL");

        checkExpType(
            "interval '1:2' hour to minute + interval '1' second",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType(
            "interval '1:3' hour to minute + interval '1 1:2:3.4' day to second",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType(
            "interval '1:2' hour to minute + interval '1 1' day to hour",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "interval '1:2' hour to minute + interval '1 1' day to hour",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType(
            "interval '1 2' day to hour + interval '1:1' minute to second",
            "INTERVAL DAY TO SECOND NOT NULL");

        checkExpType(
            "interval '1' year + interval '1' month",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "interval '1' day - interval '1' hour",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType(
            "interval '1' year - interval '1' month",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType(
            "interval '1' month - interval '1' year",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkWholeExpFails(
            "interval '1' year + interval '1' day",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTERVAL YEAR> \\+ <INTERVAL DAY>'.*");
        checkWholeExpFails(
            "interval '1' month + interval '1' second",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTERVAL MONTH> \\+ <INTERVAL SECOND>'.*");
        checkWholeExpFails(
            "interval '1' year - interval '1' day",
            "(?s).*Cannot apply '-' to arguments of type '<INTERVAL YEAR> - <INTERVAL DAY>'.*");
        checkWholeExpFails(
            "interval '1' month - interval '1' second",
            "(?s).*Cannot apply '-' to arguments of type '<INTERVAL MONTH> - <INTERVAL SECOND>'.*");

        // mixing between datetime and interval todo        checkExpType("date
        // '1234-12-12' + INTERVAL '1' month + interval '1' day","DATE"); todo
        //      checkExpFails("date '1234-12-12' + (INTERVAL '1' month +
        // interval '1' day)","?");

        // multiply operator
        checkExpType("interval '1' year * 2", "INTERVAL YEAR NOT NULL");
        checkExpType(
            "1.234*interval '1 1:2:3' day to second ",
            "INTERVAL DAY TO SECOND NOT NULL");

        // division operator
        checkExpType("interval '1' month / 0.1", "INTERVAL MONTH NOT NULL");
        checkExpType(
            "interval '1-2' year TO month / 0.1e-9",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkWholeExpFails(
            "1.234/interval '1 1:2:3' day to second",
            "(?s).*Cannot apply '/' to arguments of type '<DECIMAL.4, 3.> / <INTERVAL DAY TO SECOND>'.*");
    }

    @Test public void testNumericOperators() {
        // unary operator
        checkExpType("- cast(1 as TINYINT)", "TINYINT NOT NULL");
        checkExpType("+ cast(1 as INT)", "INTEGER NOT NULL");
        checkExpType("- cast(1 as FLOAT)", "FLOAT NOT NULL");
        checkExpType("+ cast(1 as DOUBLE)", "DOUBLE NOT NULL");
        checkExpType("-1.643", "DECIMAL(4, 3) NOT NULL");
        checkExpType("+1.643", "DECIMAL(4, 3) NOT NULL");

        // addition operator
        checkExpType(
            "cast(1 as TINYINT) + cast(5 as INTEGER)",
            "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) + cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) + cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) + cast(5 as DOUBLE)", "DOUBLE");
        checkExpType("cast(null as REAL) + cast(5 as REAL)", "REAL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as REAL)",
            "DOUBLE NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as DOUBLE)",
            "DOUBLE NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(5, 2)) + cast(1 as DOUBLE)",
            "DOUBLE");

        checkExpType("1.543 + 2.34", "DECIMAL(5, 3) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as BIGINT)",
            "DECIMAL(19, 2) NOT NULL");
        checkExpType(
            "cast(1 as NUMERIC(5, 2)) + cast(1 as INTEGER)",
            "DECIMAL(13, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(null as SMALLINT)",
            "DECIMAL(8, 2)");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as TINYINT)",
            "DECIMAL(6, 2) NOT NULL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(5, 2))",
            "DECIMAL(6, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(6, 2))",
            "DECIMAL(7, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))",
            "DECIMAL(7, 4) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))",
            "DECIMAL(7, 4)");
        checkExpType(
            "cast(1 as DECIMAL(19, 2)) + cast(1 as DECIMAL(19, 2))",
            "DECIMAL(19, 2) NOT NULL");

        // substraction operator
        checkExpType(
            "cast(1 as TINYINT) - cast(5 as BIGINT)",
            "BIGINT NOT NULL");
        checkExpType("cast(null as INTEGER) - cast(5 as SMALLINT)", "INTEGER");
        checkExpType("cast(1 as INTEGER) - cast(5 as REAL)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) - cast(5 as DOUBLE)", "DOUBLE");
        checkExpType("cast(null as REAL) - cast(5 as REAL)", "REAL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(1 as DOUBLE)",
            "DOUBLE NOT NULL");
        checkExpType("cast(null as DOUBLE) - cast(1 as DECIMAL)", "DOUBLE");

        checkExpType("1.543 - 24", "DECIMAL(14, 3) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5)) - cast(1 as BIGINT)",
            "DECIMAL(19, 0) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(1 as INTEGER)",
            "DECIMAL(13, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(null as SMALLINT)",
            "DECIMAL(8, 2)");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(1 as TINYINT)",
            "DECIMAL(6, 2) NOT NULL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(7))",
            "DECIMAL(10, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(6, 2))",
            "DECIMAL(7, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(4, 2)) - cast(1 as DECIMAL(6, 4))",
            "DECIMAL(7, 4) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL) - cast(1 as DECIMAL(6, 4))",
            "DECIMAL(19, 4)");
        checkExpType(
            "cast(1 as DECIMAL(19, 2)) - cast(1 as DECIMAL(19, 2))",
            "DECIMAL(19, 2) NOT NULL");

        // multiply operator
        checkExpType(
            "cast(1 as TINYINT) * cast(5 as INTEGER)",
            "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) * cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) * cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) * cast(5 as DOUBLE)", "DOUBLE");

        checkExpType(
            "cast(1 as DECIMAL(7, 3)) * 1.654",
            "DECIMAL(11, 6) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(7, 3)) * cast (1.654 as DOUBLE)",
            "DOUBLE");

        checkExpType(
            "cast(null as DECIMAL(5, 2)) * cast(1 as BIGINT)",
            "DECIMAL(19, 2)");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) * cast(1 as INTEGER)",
            "DECIMAL(15, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) * cast(1 as SMALLINT)",
            "DECIMAL(10, 2) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) * cast(1 as TINYINT)",
            "DECIMAL(8, 2) NOT NULL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(5, 2))",
            "DECIMAL(10, 4) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(6, 2))",
            "DECIMAL(11, 4) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))",
            "DECIMAL(10, 6) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))",
            "DECIMAL(10, 6)");
        checkExpType(
            "cast(1 as DECIMAL(4, 10)) * cast(null as DECIMAL(6, 10))",
            "DECIMAL(10, 19)");
        checkExpType(
            "cast(1 as DECIMAL(19, 2)) * cast(1 as DECIMAL(19, 2))",
            "DECIMAL(19, 4) NOT NULL");

        // divide operator
        checkExpType(
            "cast(1 as TINYINT) / cast(5 as INTEGER)",
            "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) / cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) / cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) / cast(5 as DOUBLE)", "DOUBLE");
        checkExpType(
            "cast(1 as DECIMAL(7, 3)) / 1.654",
            "DECIMAL(15, 8) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(7, 3)) / cast (1.654 as DOUBLE)",
            "DOUBLE");

        checkExpType(
            "cast(null as DECIMAL(5, 2)) / cast(1 as BIGINT)",
            "DECIMAL(19, 16)");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) / cast(1 as INTEGER)",
            "DECIMAL(16, 13) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) / cast(1 as SMALLINT)",
            "DECIMAL(11, 8) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) / cast(1 as TINYINT)",
            "DECIMAL(9, 6) NOT NULL");

        checkExpType(
            "cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(5, 2))",
            "DECIMAL(13, 8) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(6, 2))",
            "DECIMAL(14, 9) NOT NULL");
        checkExpType(
            "cast(1 as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))",
            "DECIMAL(15, 9) NOT NULL");
        checkExpType(
            "cast(null as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))",
            "DECIMAL(15, 9)");
        checkExpType(
            "cast(1 as DECIMAL(4, 10)) / cast(null as DECIMAL(6, 19))",
            "DECIMAL(19, 6)");
        checkExpType(
            "cast(1 as DECIMAL(19, 2)) / cast(1 as DECIMAL(19, 2))",
            "DECIMAL(19, 0) NOT NULL");
    }

    @Test public void testFloorCeil() {
        checkExpType("floor(cast(null as tinyint))", "TINYINT");
        checkExpType("floor(1.2)", "DECIMAL(2, 0) NOT NULL");
        checkExpType("floor(1)", "INTEGER NOT NULL");
        checkExpType("floor(1.2e-2)", "DOUBLE NOT NULL");
        checkExpType("floor(interval '2' day)", "INTERVAL DAY NOT NULL");

        checkExpType("ceil(cast(null as bigint))", "BIGINT");
        checkExpType("ceil(1.2)", "DECIMAL(2, 0) NOT NULL");
        checkExpType("ceil(1)", "INTEGER NOT NULL");
        checkExpType("ceil(1.2e-2)", "DOUBLE NOT NULL");
        checkExpType("ceil(interval '2' second)", "INTERVAL SECOND NOT NULL");
    }

    protected void checkWin(String sql, String expectedMsgPattern)
    {
        logger.info(sql);
        checkFails(sql, expectedMsgPattern);
    }

    public void checkWinClauseExp(String sql, String expectedMsgPattern)
    {
        sql = "select * from emp " + sql;
        checkWin(sql, expectedMsgPattern);
    }

    public void checkWinFuncExpWithWinClause(
        String sql,
        String expectedMsgPattern)
    {
        sql = "select " + sql + " from emp window w as (order by deptno)";
        checkWin(sql, expectedMsgPattern);
    }

    public void checkWinFuncExp(String sql, String expectedMsgPattern)
    {
        sql = "select " + sql + " from emp";
        checkWin(sql, expectedMsgPattern);
    }

    // test window partition clause. See SQL 2003 specification for detail
    public void _testWinPartClause()
    {
        checkWinClauseExp(
            "window w as (w2 order by deptno), w2 as (^rang^e 100 preceding)",
            "Referenced window cannot have framing declarations");
        // Test specified collation, window clause syntax rule 4,5.
    }

    @Test public void testWindowFunctions() {
        // SQL 03 Section 6.10

        // Window functions may only appear in the <select list> of a
        // <query specification> or <select statement: single row>,
        // or the <order by clause> of a simple table query.
        // See 4.15.3 for detail
        checkWin(
            "select *\n"
            + " from emp\n"
            + " where ^sum(sal) over (partition by deptno\n"
            + "    order by empno\n"
            + "    rows 3 preceding)^ > 10",
            "Windowed aggregate expression is illegal in WHERE clause");

        checkWin(
            "select *\n"
            + " from emp\n"
            + " group by ename, ^sum(sal) over (partition by deptno\n"
            + "    order by empno\n"
            + "    rows 3 preceding)^ + 10\n"
            + "order by deptno",
            "Windowed aggregate expression is illegal in GROUP BY clause");

        checkWin(
            "select *\n"
            + " from emp\n"
            + " join dept on emp.deptno = dept.deptno\n"
            + " and ^sum(sal) over (partition by deptno\n"
            + "    order by empno\n"
            + "    rows 3 preceding)^ = dept.deptno + 40\n"
            + "order by deptno",
            "Windowed aggregate expression is illegal in ON clause");

        // rule 3, a)
        checkWin(
            "select sal from emp order by sum(sal) over (partition by deptno order by deptno)",
            null);

        // scope reference

        // rule 4,
        // valid window functions
        checkWinFuncExpWithWinClause("sum(sal)", null);
    }

    @Ignore
    @Test public void testWindowFunctions2() {
        if (Bug.Dt1446Fixed) {
            // row_number function
            checkWinFuncExpWithWinClause(
                "row_number() over (order by deptno)",
                null);

            // rank function type
            checkWinFuncExpWithWinClause("dense_rank()", null);
            checkWinFuncExpWithWinClause("rank() over (order by empno)", null);
            checkWinFuncExpWithWinClause(
                "percent_rank() over (order by empno)",
                null);
            checkWinFuncExpWithWinClause(
                "cume_dist() over (order by empno)",
                null);

            // rule 6a
            // ORDER BY required with RANK & DENSE_RANK
            checkWin(
                "select rank() over ^(partition by deptno)^ from emp",
                "RANK or DENSE_RANK functions require ORDER BY clause in window specification");
            checkWin(
                "select dense_rank() over ^(partition by deptno)^ from emp ",
                "RANK or DENSE_RANK functions require ORDER BY clause in window specification");
            // The following fail but it is reported as window needing OBC due
            // to
            // test sequence, so
            // not really failing due to 6a.
/*
             checkWin(
                "select rank() over w from emp "
                + "window w as ^(partition by deptno)^",
                "RANK or DENSE_RANK functions require ORDER BY clause in "
                + "window specification");
            checkWin(
                "select dense_rank() over w from emp "
                + "window w as ^(partition by deptno)^",
                "RANK or DENSE_RANK functions require ORDER BY clause in "
                + "window specification");
*/

            // rule 6b
            // Framing not allowed with RANK & DENSE_RANK functions
            // window framing defined in window clause
            checkWin(
                "select rank() over w from emp window w as (order by empno ^rows^ 2 preceding )",
                "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
            checkWin(
                "select dense_rank() over w from emp window w as (order by empno ^rows^ 2 preceding)",
                "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
            checkWin(
                "select percent_rank() over w from emp window w as (rows 2 preceding )",
                null);
            checkWin(
                "select cume_dist() over w from emp window w as (rows 2 preceding)",
                null);

            // window framing defined in in-line window
            checkWin(
                "select rank() over (order by empno ^range^ 2 preceding ) from emp ",
                "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
            checkWin(
                "select dense_rank() over (order by empno ^rows^ 2 preceding ) from emp ",
                "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
            checkWin(
                "select percent_rank() over (rows 2 preceding ) from emp",
                null);
            checkWin(
                "select cume_dist() over (rows 2 preceding ) from emp ",
                null);
        } else {
            // Check for Rank function failure.
            checkWinFuncExpWithWinClause(
                "^dense_rank()^",
                "Function 'DENSE_RANK\\(\\)' is not defined");
            checkWinFuncExpWithWinClause(
                "^percent_rank()^",
                "Function 'PERCENT_RANK\\(\\)' is not defined");
            checkWinFuncExpWithWinClause(
                "^rank()^",
                "Function 'RANK\\(\\)' is not defined");
            checkWinFuncExpWithWinClause(
                "^cume_dist()^",
                "Function 'CUME_DIST\\(\\)' is not defined");
            checkWinFuncExpWithWinClause(
                "^row_number()^",
                "Function 'ROW_NUMBER\\(\\)' is not defined");
        }

        // invalid column reference
        checkWinFuncExpWithWinClause(
            "sum(^invalidColumn^)",
            "Column 'INVALIDCOLUMN' not found in any table");

        // invalid window functions
        checkWinFuncExpWithWinClause(
            "^invalidFun(sal)^",
            "No match found for function signature INVALIDFUN\\(<NUMERIC>\\)");

        // 6.10 rule 10. no distinct allowed aggreagate function
        // Fails in parser.
        // checkWinFuncExpWithWinClause(" sum(distinct sal) over w ", null);

        // 7.11 rule 10c
        checkWin(
            "select sum(sal) over (w partition by ^deptno^)" + NL
            + " from emp window w as (order by empno rows 2 preceding )",
            "PARTITION BY not allowed with existing window reference");

        // 7.11 rule 10d
        checkWin(
            "select sum(sal) over (w order by ^empno^)" + NL
            + " from emp window w as (order by empno rows 2 preceding )",
            "ORDER BY not allowed in both base and referenced windows");

        // 7.11 rule 10e
        checkWin(
            "select sum(sal) over (w) " + NL
            + " from emp window w as (order by empno ^rows^ 2 preceding )",
            "Referenced window cannot have framing declarations");
    }

    @Test public void testInlineWinDef() {
        // the <window specification> used by windowed agg functions is
        // fully defined in SQL 03 Std. section 7.1 <window clause>
        check(
            "select sum(sal) over (partition by deptno order by empno) from emp order by empno");
        checkWinFuncExp(
            "sum(sal) OVER ("
            + "partition by deptno "
            + "order by empno "
            + "rows 2 preceding )",
            null);
        checkWinFuncExp(
            "sum(sal) OVER ("
            + "order by 1 "
            + "rows 2 preceding )",
            null);
        checkWinFuncExp(
            "sum(sal) OVER ("
            + "order by 'b' "
            + "rows 2 preceding )",
            null);
        checkWinFuncExp(
            "sum(sal) over ("
            + "partition by deptno "
            + "order by 1+1 rows 26 preceding)",
            null);
        checkWinFuncExp(
            "sum(sal) over (order by deptno rows unbounded preceding)",
            null);
        checkWinFuncExp(
            "sum(sal) over (order by deptno rows current row)",
            null);
        checkWinFuncExp(
            "sum(sal) over ^("
            + "order by deptno "
            + "rows between unbounded preceding and unbounded following)^",
            "UNBOUNDED FOLLOWING window not supported");
        checkWinFuncExp(
            "sum(sal) over ^("
            + "order by deptno "
            + "rows between CURRENT ROW and unbounded following)^",
            "UNBOUNDED FOLLOWING window not supported");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between unbounded preceding and CURRENT ROW)",
            null);

        // logical current row/current row
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between CURRENT ROW and CURRENT ROW)",
            null);

        // physical current row/current row
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "range between CURRENT ROW and CURRENT ROW)",
            null);

        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between 2 preceding and CURRENT ROW)",
            null);
        checkWinFuncExpWithWinClause(
            "sum(sal) OVER (w "
            + "rows 2 preceding )",
            null);
        checkWinFuncExp(
            "sum(sal) over (order by deptno range 2.0 preceding)",
            null);

        // Failure mode tests
        checkWinFuncExp(
            "sum(sal) over (order by deptno "
            + "rows between ^UNBOUNDED FOLLOWING^ and unbounded preceding)",
            "UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between 2 preceding and ^UNBOUNDED PRECEDING^)",
            "UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between CURRENT ROW and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between 2 following and ^CURRENT ROW^)",
            "Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "rows between 2 following and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by deptno "
            + "RANGE BETWEEN ^INTERVAL '1' SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)",
            "Data Type mismatch between ORDER BY and RANGE clause");
        checkWinFuncExp(
            "sum(sal) over ("
            + "order by empno "
            + "RANGE BETWEEN ^INTERVAL '1' SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)",
            "Data Type mismatch between ORDER BY and RANGE clause");
        checkWinFuncExp(
            "sum(sal) over (order by deptno, empno ^range^ 2 preceding)",
            "RANGE clause cannot be used with compound ORDER BY clause");
        checkWinFuncExp(
            "sum(sal) over ^(partition by deptno range 5 preceding)^",
            "Window specification must contain an ORDER BY clause");
        checkWinFuncExp(
            "sum(sal) over ^w1^",
            "Window 'W1' not found");
        checkWinFuncExp(
            "sum(sal) OVER (^w1^ "
            + "partition by deptno "
            + "order by empno "
            + "rows 2 preceding )",
            "Window 'W1' not found");
    }

    @Test public void testPartitionByExpr() {
        checkWinFuncExp(
            "sum(sal) over (partition by empno + deptno order by empno range 5 preceding)",
            null);

        checkWinFuncExp(
            "sum(sal) over (partition by ^empno + ename^ order by empno range 5 preceding)",
            "(?s)Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <VARCHAR\\(20\\)>'.*");
    }

    @Test public void testWindowClause() {
        // -----------------------------------
        // --   positive testings           --
        // -----------------------------------
        // correct syntax:
        checkWinFuncExpWithWinClause("sum(sal) as sumsal", null);
        checkWinClauseExp(
            "window w as (partition by sal order by deptno rows 2 preceding)",
            null);

        // define window on an existing window
        checkWinClauseExp("window w as (order by sal), w1 as (w)", null);

        // -----------------------------------
        // --   negative testings           --
        // -----------------------------------
        // Test fails in parser
        // checkWinClauseExp("window foo.w as (range 100 preceding) "+
        //    "Window name must be a simple identifier\");

        // rule 11
        // a)
        // missing window order clause.
        checkWinClauseExp(
            "window w as ^(range 100 preceding)^",
            "Window specification must contain an ORDER BY clause");

        // order by number
        checkWinClauseExp(
            "window w as (order by sal range 100 preceding)",
            null);

        // order by date
        checkWinClauseExp(
            "window w as (order by hiredate range ^100^ preceding)",
            "Data Type mismatch between ORDER BY and RANGE clause");

        // order by string, should fail
        checkWinClauseExp(
            "window w as (order by ename range ^100^ preceding)",
            "Data type of ORDER BY prohibits use of RANGE clause");
        // todo: interval test ???

        // b)
        // valid
        checkWinClauseExp("window w as (rows 2 preceding)", null);

        // invalid tests exact numeric for the unsigned value specification The
        // followoing two test fail as they should but in the parser: JR not
        // anymore now the validator kicks out
        checkWinClauseExp(
            "window w as (rows ^-2.5^ preceding)",
            "ROWS value must be a non-negative integral constant");
        checkWinClauseExp(
            "window w as (rows ^-2^ preceding)",
            "ROWS value must be a non-negative integral constant");

        // This test should fail as per 03 Std. but we pass it and plan
        // to apply the FLOOR function before window processing
        checkWinClauseExp(
            "window w as (rows ^2.5^ preceding)",
            "ROWS value must be a non-negative integral constant");

        // -----------------------------------
        // --   negative testings           --
        // -----------------------------------
        // reference undefined xyz column
        checkWinClauseExp(
            "window w as (partition by ^xyz^)",
            "Column 'XYZ' not found in any table");

        // window defintion is empty when applied to unsorted table
        checkWinClauseExp(
            "window w as ^( /* boo! */  )^",
            "Window specification must contain an ORDER BY clause");

        // duplidate window name
        checkWinClauseExp(
            "window w as (order by empno), ^w^ as (order by empno)",
            "Duplicate window names not allowed");
        checkWinClauseExp(
            "window win1 as (order by empno), ^win1^ as (order by empno)",
            "Duplicate window names not allowed");

        // syntax rule 6
        checkFails(
            "select min(sal) over (order by deptno) from emp group by deptno,sal",
            null);
        checkFails(
            "select min(sal) over (order by ^deptno^) from emp group by sal",
            "Expression 'DEPTNO' is not being grouped");
        checkFails(
            "select min(sal) over " + NL
            + "(partition by comm order by deptno) from emp group by deptno,sal,comm",
            null);
        checkFails(
            "select min(sal) over " + NL
            + "(partition by ^comm^ order by deptno) from emp group by deptno,sal",
            "Expression 'COMM' is not being grouped");

        // syntax rule 7
        checkWinClauseExp(
            "window w as (order by rank() over (order by sal))",
            null);

        // ------------------------------------
        // ---- window frame between tests ----
        // ------------------------------------
        // bound 1 shall not specify UNBOUNDED FOLLOWING
        checkWinClauseExp(
            "window w as (rows between ^unbounded following^ and 5 following)",
            "UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");

        // bound 2 shall not specify UNBOUNDED PRECEDING
        checkWinClauseExp(
            "window w as ("
            + "order by deptno "
            + "rows between 2 preceding and ^UNBOUNDED PRECEDING^)",
            "UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
        checkWinClauseExp(
            "window w as ("
            + "order by deptno "
            + "rows between 2 following and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
        checkWinClauseExp(
            "window w as ("
            + "order by deptno "
            + "rows between CURRENT ROW and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
        checkWinClauseExp(
            "window w as ("
            + "order by deptno "
            + "rows between 2 following and ^CURRENT ROW^)",
            "Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");

        // Sql '03 rule 10 c) assertExceptionIsThrown("select deptno as d, sal
        // as s from emp window w as (partition by deptno order by sal), w2 as
        // (w partition by deptno)", null); checkWinClauseExp("window w as
        // (partition by sal order by deptno), w2 as (w partition by sal)",
        // null); d) valid because existing window does not have an ORDER BY
        // clause
        checkWinClauseExp(
            "window w as (w2 range 2 preceding ), w2 as (order by sal)",
            null);
        checkWinClauseExp(
            "window w as ^(partition by sal)^, w2 as (w order by deptno)",
            "Window specification must contain an ORDER BY clause");
        checkWinClauseExp(
            "window w as (w2 partition by ^sal^), w2 as (order by deptno)",
            "PARTITION BY not allowed with existing window reference");
        checkWinClauseExp(
            "window w as (partition by sal order by deptno), w2 as (w order by ^deptno^)",
            "ORDER BY not allowed in both base and referenced windows");

        // e)
        checkWinClauseExp(
            "window w as (w2 order by deptno), w2 as (^range^ 100 preceding)",
            "Referenced window cannot have framing declarations");

        // rule 12, todo: test scope of window assertExceptionIsThrown("select
        // deptno as d from emp window d as (partition by deptno)", null);

        // rule 13
        checkWinClauseExp("window w as (order by sal)", null);
        checkWinClauseExp(
            "window w as (order by ^non_exist_col^)",
            "Column 'NON_EXIST_COL' not found in any table");
        checkWinClauseExp(
            "window w as (partition by ^non_exist_col^ order by sal)",
            "Column 'NON_EXIST_COL' not found in any table");
    }

    @Test public void testWindowClause2() {
        // 7.10 syntax rule 2 <new window name> NWN1 shall not be contained in
        // the scope of another <new window name> NWN2 such that NWN1 and NWN2
        // are equivalent.
        checkWinClauseExp(
            "window\n"
            + "w  as (partition by deptno order by empno rows 2 preceding),\n"
            + "w2 as ^(partition by deptno order by empno rows 2 preceding)^\n",
            "Duplicate window specification not allowed in the same window clause");
    }

    @Test public void testWindowClauseWithSubquery() {
        check(
            "select * from \n"
            + "( select sum(empno) over w, sum(deptno) over w from emp \n"
            + "window w as (order by hiredate range interval '1' minute preceding))");

        check(
            "select * from \n"
            + "( select sum(empno) over w, sum(deptno) over w, hiredate from emp) \n"
            + "window w as (order by hiredate range interval '1' minute preceding)");

        checkFails(
            "select * from \n"
            + "( select sum(empno) over w, sum(deptno) over w from emp) \n"
            + "window w as (order by ^hiredate^ range interval '1' minute preceding)",
            "Column 'HIREDATE' not found in any table");
    }

    @Test public void testWindowNegative() {
        final String negSize = "Window has negative size";
        checkNegWindow("rows between 2 preceding and 4 preceding", negSize);
        checkNegWindow("rows between 2 preceding and 3 preceding", negSize);
        checkNegWindow("rows between 2 preceding and 2 preceding", null);
        checkNegWindow(
            "rows between unbounded preceding and current row",
            null);
        final String unboundedFollowing =
            "UNBOUNDED FOLLOWING window not supported";
        checkNegWindow(
            "rows between unbounded preceding and unbounded following",
            unboundedFollowing);
        checkNegWindow(
            "rows between current row and unbounded following",
            unboundedFollowing);
        checkNegWindow("rows between current row and 2 following", null);
        checkNegWindow("range between 2 preceding and 2 following", null);
        checkNegWindow("range between 2 preceding and -2 preceding", null);
        checkNegWindow("range between 4 following and 3 following", negSize);
        checkNegWindow("range between 4 following and 5 following", null);
        checkNegWindow("rows between 1 following and 0 following", negSize);
        checkNegWindow("rows between 0 following and 0 following", null);
    }

    private void checkNegWindow(String s, String msg)
    {
        String sql =
            "select sum(deptno) over ^(order by empno "
            + s
            + ")^ from emp";
        checkFails(
            sql,
            msg);
    }

    @Test public void testWindowPartial() {
        check(
            "select sum(deptno) over (\n"
            + "order by deptno, empno rows 2 preceding disallow partial)\n"
            + "from emp");

        // cannot do partial over logical window
        checkFails(
            "select sum(deptno) over (\n"
            + "  partition by deptno\n"
            + "  order by empno\n"
            + "  range between 2 preceding and 3 following\n"
            + "  ^disallow partial^)\n"
            + "from emp",
            "Cannot use DISALLOW PARTIAL with window based on RANGE");
    }

    @Test public void testOneWinFunc() {
        checkWinClauseExp(
            "window w as (partition by sal order by deptno rows 2 preceding)",
            null);
    }

    @Test public void testNameResolutionInValuesClause() {
        final String emps =
            "(select 1 as empno, 'x' as name, 10 as deptno, 'M' as gender, 'San Francisco' as city, 30 as empid, 25 as age from (values (1)))";
        final String depts =
            "(select 10 as deptno, 'Sales' as name from (values (1)))";

        checkFails(
            "select * from " + emps + " join " + depts + NL
            + " on ^emps^.deptno = deptno",
            "Table 'EMPS' not found");

        // this is ok
        check(
            "select * from " + emps + " as e" + NL
            + " join " + depts + " as d" + NL
            + " on e.deptno = d.deptno");

        // fail: ambiguous column in WHERE
        checkFails(
            "select * from " + emps + " as emps," + NL
            + " " + depts + NL
            + "where ^deptno^ > 5",
            "Column 'DEPTNO' is ambiguous");

        // fail: ambiguous column reference in ON clause
        checkFails(
            "select * from " + emps + " as e" + NL
            + " join " + depts + " as d" + NL
            + " on e.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");

        // ok: column 'age' is unambiguous
        check(
            "select * from " + emps + " as e" + NL
            + " join " + depts + " as d" + NL
            + " on e.deptno = age");

        // ok: reference to derived column
        check(
            "select * from " + depts + NL
            + " join (select mod(age, 30) as agemod from " + emps + ") " + NL
            + "on deptno = agemod");

        // fail: deptno is ambiguous
        checkFails(
            "select name from " + depts + " " + NL
            + "join (select mod(age, 30) as agemod, deptno from " + emps + ") "
            + NL
            + "on ^deptno^ = agemod",
            "Column 'DEPTNO' is ambiguous");

        // fail: lateral reference
        checkFails(
            "select * from " + emps + " as e," + NL
            + " (select 1, ^e^.deptno from (values(true))) as d",
            "Table 'E' not found");
    }

    @Test public void testNestedFrom() {
        checkColumnType("values (true)", "BOOLEAN NOT NULL");
        checkColumnType("select * from (values(true))", "BOOLEAN NOT NULL");
        checkColumnType(
            "select * from (select * from (values(true)))",
            "BOOLEAN NOT NULL");
        checkColumnType(
            "select * from (select * from (select * from (values(true))))",
            "BOOLEAN NOT NULL");
        checkColumnType(
            "select * from ("
            + "  select * from ("
            + "    select * from (values(true))"
            + "    union"
            + "    select * from (values (false)))"
            + "  except"
            + "  select * from (values(true)))",
            "BOOLEAN NOT NULL");
    }

    @Test public void testAmbiguousColumn() {
        checkFails(
            "select * from emp join dept" + NL
            + " on emp.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");

        // this is ok
        check(
            "select * from emp as e" + NL
            + " join dept as d" + NL
            + " on e.deptno = d.deptno");

        // fail: ambiguous column in WHERE
        checkFails(
            "select * from emp as emps, dept" + NL
            + "where ^deptno^ > 5",
            "Column 'DEPTNO' is ambiguous");

        // fail: alias 'd' obscures original table name 'dept'
        checkFails(
            "select * from emp as emps, dept as d" + NL
            + "where ^dept^.deptno > 5",
            "Table 'DEPT' not found");

        // fail: ambiguous column reference in ON clause
        checkFails(
            "select * from emp as e" + NL
            + " join dept as d" + NL
            + " on e.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");

        // ok: column 'comm' is unambiguous
        check(
            "select * from emp as e" + NL
            + " join dept as d" + NL
            + " on e.deptno = comm");

        // ok: reference to derived column
        check(
            "select * from dept" + NL
            + " join (select mod(comm, 30) as commmod from emp) " + NL
            + "on deptno = commmod");

        // fail: deptno is ambiguous
        checkFails(
            "select name from dept " + NL
            + "join (select mod(comm, 30) as commmod, deptno from emp) " + NL
            + "on ^deptno^ = commmod",
            "Column 'DEPTNO' is ambiguous");

        // fail: lateral reference
        checkFails(
            "select * from emp as e," + NL
            + " (select 1, ^e^.deptno from (values(true))) as d",
            "Table 'E' not found");
    }

    @Test public void testExpandStar() {
        // dtbug 282 -- "select r.* from sales.depts" gives NPE.
        // dtbug 318 -- error location should be ^r^ not ^r.*^.
        checkFails(
            "select ^r^.* from dept",
            "Unknown identifier 'R'");

        check("select e.* from emp as e");
        check("select emp.* from emp");

        // Error message could be better (EMPNO does exist, but it's a column).
        checkFails(
            "select ^empno^ .  * from emp",
            "Unknown identifier 'EMPNO'");
    }

    @Test public void testAsColumnList() {
        check("select d.a, b from dept as d(a, b)");
        checkFails(
            "select d.^deptno^ from dept as d(a, b)",
            "(?s).*Column 'DEPTNO' not found in table 'D'.*");
        checkFails(
            "select 1 from dept as d(^a^, b, c)",
            "(?s).*List of column aliases must have same degree as table; "
            + "table has 2 columns \\('DEPTNO', 'NAME'\\), "
            + "whereas alias list has 3 columns.*");
        checkResultType(
            "select * from dept as d(a, b)",
            "RecordType(INTEGER NOT NULL A, VARCHAR(10) NOT NULL B) NOT NULL");
        checkResultType(
            "select * from (values ('a', 1), ('bc', 2)) t (a, b)",
            "RecordType(CHAR(2) NOT NULL A, INTEGER NOT NULL B) NOT NULL");
    }

    // todo: implement IN
    public void _testAmbiguousColumnInIn()
    {
        // ok: cyclic reference
        check(
            "select * from emp as e" + NL
            + "where e.deptno in (" + NL
            + "  select 1 from (values(true)) where e.empno > 10)");

        // ok: cyclic reference
        check(
            "select * from emp as e" + NL
            + "where e.deptno in (" + NL
            + "  select e.deptno from (values(true)))");
    }

    @Test public void testInList() {
        check("select * from emp where empno in (10,20)");

        // "select * from emp where empno in ()" is invalid -- see parser test
        check(
            "select * from emp where empno in (10 + deptno, cast(null as integer))");
        checkFails(
            "select * from emp where empno in ^(10, '20')^",
            ERR_IN_VALUES_INCOMPATIBLE);

        checkExpType("1 in (2, 3, 4)", "BOOLEAN NOT NULL");
        checkExpType("cast(null as integer) in (2, 3, 4)", "BOOLEAN");
        checkExpType("1 in (2, cast(null as integer) , 4)", "BOOLEAN");
        checkExpType("1 in (2.5, 3.14)", "BOOLEAN NOT NULL");
        checkExpType("true in (false, unknown)", "BOOLEAN");
        checkExpType("true in (false, false or unknown)", "BOOLEAN");
        checkExpType("true in (false, true)", "BOOLEAN NOT NULL");
        checkExpType("(1,2) in ((1,2), (3,4))", "BOOLEAN NOT NULL");
        checkExpType(
            "'medium' in (cast(null as varchar(10)), 'bc')",
            "BOOLEAN");

        // nullability depends on nullability of both sides
        checkColumnType("select empno in (1, 2) from emp", "BOOLEAN NOT NULL");
        checkColumnType(
            "select nullif(empno,empno) in (1, 2) from emp",
            "BOOLEAN");
        checkColumnType(
            "select empno in (1, nullif(empno,empno), 2) from emp",
            "BOOLEAN");

        checkExpFails(
            "1 in ^(2, 'c')^",
            ERR_IN_VALUES_INCOMPATIBLE);
        checkExpFails(
            "1 in ^((2), (3,4))^",
            ERR_IN_VALUES_INCOMPATIBLE);
        checkExpFails(
            "false and ^1 in ('b', 'c')^",
            ERR_IN_OPERANDS_INCOMPATIBLE);
        checkExpFails(
            "1 > 5 ^or (1, 2) in (3, 4)^",
            ERR_IN_OPERANDS_INCOMPATIBLE);
    }

    @Test public void testInSubquery() {
        check("select * from emp where deptno in (select deptno from dept)");
        check(
            "select * from emp where (empno,deptno)"
            + " in (select deptno,deptno from dept)");

        // NOTE: jhyde: The closing caret should be one character to the right
        // ("dept)^"), but it's difficult to achieve, because parentheses are
        // discarded during the parsing process.
        checkFails(
            "select * from emp where ^deptno in "
            + "(select deptno,deptno from dept^)",
            "Values passed to IN operator must have compatible types");
    }

    @Test public void testDoubleNoAlias() {
        check("select * from emp join dept on true");
        check("select * from emp, dept");
        check("select * from emp cross join dept");
    }

    @Test public void testDuplicateColumnAliasIsOK() {
        // duplicate column aliases are daft, but SQL:2003 allows them
        check("select 1 as a, 2 as b, 3 as a from emp");
    }

    @Test public void testDuplicateTableAliasFails() {
        // implicit alias clashes with implicit alias
        checkFails(
            "select 1 from emp, ^emp^",
            "Duplicate relation name 'EMP' in FROM clause");

        // implicit alias clashes with implicit alias, using join syntax
        checkFails(
            "select 1 from emp join ^emp^ on emp.empno = emp.mgrno",
            "Duplicate relation name 'EMP' in FROM clause");

        // explicit alias clashes with implicit alias
        checkFails(
            "select 1 from emp join ^dept as emp^ on emp.empno = emp.deptno",
            "Duplicate relation name 'EMP' in FROM clause");

        // implicit alias does not clash with overridden alias
        check("select 1 from emp as e join emp on emp.empno = e.deptno");

        // explicit alias does not clash with overridden alias
        check(
            "select 1 from emp as e join dept as emp on e.empno = emp.deptno");

        // more than 2 in from clause
        checkFails(
            "select 1 from emp, dept, emp as e, ^dept as emp^, emp",
            "Duplicate relation name 'EMP' in FROM clause");

        // alias applied to subquery
        checkFails(
            "select 1 from emp, (^select 1 as x from (values (true))) as emp^",
            "Duplicate relation name 'EMP' in FROM clause");
        checkFails(
            "select 1 from emp, (^values (true,false)) as emp (b, c)^, dept as emp",
            "Duplicate relation name 'EMP' in FROM clause");

        // alias applied to table function. doesn't matter that table fn
        // doesn't exist - should find the alias problem first
        checkFails(
            "select 1 from emp, ^table(foo()) as emp^",
            "Duplicate relation name 'EMP' in FROM clause");

        // explicit table
        checkFails(
            "select 1 from emp, ^(table foo.bar.emp) as emp^",
            "Duplicate relation name 'EMP' in FROM clause");

        // alias does not clash with alias inherited from enclosing context
        check(
            "select 1 from emp, dept where exists (\n"
            + "  select 1 from emp where emp.empno = emp.deptno)");
    }

    @Test public void testInvalidGroupBy() {
        checkFails(
            "select ^empno^, deptno from emp group by deptno",
            "Expression 'EMPNO' is not being grouped");
    }

    @Test public void testSingleNoAlias() {
        check("select * from emp");
    }

    @Test public void testObscuredAliasFails() {
        // It is an error to refer to a table which has been given another
        // alias.
        checkFails(
            "select * from emp as e where exists (" + NL
            + "  select 1 from dept where dept.deptno = ^emp^.deptno)",
            "Table 'EMP' not found");
    }

    @Test public void testFromReferenceFails() {
        // You cannot refer to a table ('e2') in the parent scope of a query in
        // the from clause.
        checkFails(
            "select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = ^e2^.deptno))",
            "Table 'E2' not found");
    }

    @Test public void testWhereReference() {
        // You can refer to a table ('e1') in the parent scope of a query in
        // the from clause.
        //
        // Note: Oracle10g does not allow this query.
        check(
            "select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = e1.deptno))");
    }

    @Test public void testUnionNameResolution() {
        checkFails(
            "select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "  (select deptno from dept as d" + NL
            + "   union" + NL
            + "   select deptno from emp as e3 where deptno = ^e2^.deptno))",
            "Table 'E2' not found");

        checkFails(
            "select * from emp" + NL
            + "union" + NL
            + "select * from dept where ^empno^ < 10",
            "Column 'EMPNO' not found in any table");
    }

    @Test public void testUnionCountMismatchFails() {
        checkFails(
            "select 1,2 from emp" + NL
            + "union" + NL
            + "select ^3^ from dept",
            "Column count mismatch in UNION");
    }

    @Test public void testUnionCountMismatcWithValuesFails() {
        checkFails(
            "select * from ( values (1))" + NL
            + "union" + NL
            + "select ^*^ from ( values (1,2))",
            "Column count mismatch in UNION");

        checkFails(
            "select * from ( values (1))" + NL
            + "union" + NL
            + "select ^*^ from emp",
            "Column count mismatch in UNION");

        checkFails(
            "select * from emp" + NL
            + "union" + NL
            + "select ^*^ from ( values (1))",
            "Column count mismatch in UNION");
    }

    @Test public void testUnionTypeMismatchFails() {
        checkFails(
            "select 1, ^2^ from emp union select deptno, name from dept",
            "Type mismatch in column 2 of UNION");

        checkFails(
            "select ^slacker^ from emp union select name from dept",
            "Type mismatch in column 1 of UNION");
    }

    @Test public void testUnionTypeMismatchWithStarFails() {
        checkFails(
            "select ^*^ from dept union select 1, 2 from emp",
            "Type mismatch in column 2 of UNION");

        checkFails(
            "select ^dept.*^ from dept union select 1, 2 from emp",
            "Type mismatch in column 2 of UNION");
    }

    @Test public void testUnionTypeMismatchWithValuesFails() {
        checkFails(
            "values (1, ^2^, 3), (3, 4, 5), (6, 7, 8) union " + NL
            + "select deptno, name, deptno from dept",
            "Type mismatch in column 2 of UNION");

        checkFails(
            "select 1 from (values (^'x'^)) union " + NL
            + "select 'a' from (values ('y'))",
            "Type mismatch in column 1 of UNION");

        checkFails(
            "select 1 from (values (^'x'^)) union " + NL
            + "(values ('a'))",
            "Type mismatch in column 1 of UNION");
    }

    @Test public void testValuesTypeMismatchFails() {
        checkFails(
            "^values (1), ('a')^",
            "Values passed to VALUES operator must have compatible types");
    }

    @Test public void testNaturalCrossJoinFails() {
        checkFails(
            "select * from emp natural cross ^join^ dept",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    @Test public void testCrossJoinUsingFails() {
        checkFails(
            "select * from emp cross join dept ^using (deptno)^",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    @Test public void testJoinUsing() {
        check("select * from emp join dept using (deptno)");

        // fail: comm exists on one side not the other
        // todo: The error message could be improved.
        checkFails(
            "select * from emp join dept using (deptno, ^comm^)",
            "Column 'COMM' not found in any table");

        // ok to repeat (ok in Oracle10g too)
        check("select * from emp join dept using (deptno, deptno)");

        // inherited column, not found in either side of the join, in the
        // USING clause
        checkFails(
            "select * from dept where exists (" + NL
            + "select 1 from emp join bonus using (^dname^))",
            "Column 'DNAME' not found in any table");

        // inherited column, found in only one side of the join, in the
        // USING clause
        checkFails(
            "select * from dept where exists (" + NL
            + "select 1 from emp join bonus using (^deptno^))",
            "Column 'DEPTNO' not found in any table");
    }

    @Test public void testCrossJoinOnFails() {
        checkFails(
            "select * from emp cross join dept" + NL
            + " ^on emp.deptno = dept.deptno^",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    @Test public void testInnerJoinWithoutUsingOrOnFails() {
        checkFails(
            "select * from emp inner ^join^ dept " + NL
            + "where emp.deptno = dept.deptno",
            "INNER, LEFT, RIGHT or FULL join requires a condition \\(NATURAL keyword or ON or USING clause\\)");
    }

    @Test public void testNaturalJoinWithOnFails() {
        checkFails(
            "select * from emp natural join dept on ^emp.deptno = dept.deptno^",
            "Cannot specify NATURAL keyword with ON or USING clause");
    }

    @Test public void testNaturalJoinWithUsing() {
        checkFails(
            "select * from emp natural join dept ^using (deptno)^",
            "Cannot specify NATURAL keyword with ON or USING clause");
    }

    @Test public void testNaturalJoinIncompatibleDatatype() {
        checkFails(
            "select * from emp natural ^join^\n"
            + "(select deptno, name as sal from dept)",
            "Column 'SAL' matched using NATURAL keyword or USING clause has incompatible types: cannot compare 'INTEGER' to 'VARCHAR\\(10\\)'");

        // make sal occur more than once on rhs, it is ignored and therefore
        // there is no error about incompatible types
        check(
            "select * from emp natural join\n"
            + " (select deptno, name as sal, 'foo' as sal from dept)");
    }

    @Test public void testJoinUsingIncompatibleDatatype() {
        checkFails(
            "select * from emp join (select deptno, name as sal from dept) using (deptno, ^sal^)",
            "Column 'SAL' matched using NATURAL keyword or USING clause has incompatible types: cannot compare 'INTEGER' to 'VARCHAR\\(10\\)'");
    }

    @Test public void testJoinUsingInvalidColsFails() {
        // todo: Improve error msg
        checkFails(
            "select * from emp left join dept using (^gender^)",
            "Column 'GENDER' not found in any table");
    }

    @Test public void testJoinUsingDupColsFails() {
        checkFails(
            "select * from emp left join (select deptno, name as deptno from dept) using (^deptno^)",
            "Column name 'DEPTNO' in USING clause is not unique on one side of join");
    }

    @Test public void testJoinRowType() {
        checkResultType(
            "select * from emp left join dept on emp.deptno = dept.deptno",
            "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER NOT NULL MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER,"
            + " INTEGER DEPTNO0,"
            + " VARCHAR(10) NAME) NOT NULL");

        checkResultType(
            "select * from emp right join dept on emp.deptno = dept.deptno",
            "RecordType(INTEGER EMPNO,"
            + " VARCHAR(20) ENAME,"
            + " VARCHAR(10) JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) HIREDATE,"
            + " INTEGER SAL,"
            + " INTEGER COMM,"
            + " INTEGER DEPTNO,"
            + " BOOLEAN SLACKER,"
            + " INTEGER NOT NULL DEPTNO0,"
            + " VARCHAR(10) NOT NULL NAME) NOT NULL");

        checkResultType(
            "select * from emp full join dept on emp.deptno = dept.deptno",
            "RecordType(INTEGER EMPNO,"
            + " VARCHAR(20) ENAME,"
            + " VARCHAR(10) JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) HIREDATE,"
            + " INTEGER SAL,"
            + " INTEGER COMM,"
            + " INTEGER DEPTNO,"
            + " BOOLEAN SLACKER,"
            + " INTEGER DEPTNO0,"
            + " VARCHAR(10) NAME) NOT NULL");
    }

    // todo: Cannot handle '(a join b)' yet -- we see the '(' and expect to
    // see 'select'.
    public void _testJoinUsing()
    {
        check(
            "select * from (emp join bonus using (job))" + NL
            + "join dept using (deptno)");

        // cannot alias a JOIN (actually this is a parser error, but who's
        // counting?)
        checkFails(
            "select * from (emp join bonus using (job)) as x" + NL
            + "join dept using (deptno)",
            "as wrong here");
        checkFails(
            "select * from (emp join bonus using (job))" + NL
            + "join dept using (^dname^)",
            "dname not found in lhs");

        // Needs real Error Message and error marks in query
        checkFails(
            "select * from (emp join bonus using (job))" + NL
            + "join (select 1 as job from (true)) using (job)",
            "ambig");
    }

    @Test public void testWhere() {
        checkFails(
            "select * from emp where ^sal^",
            "WHERE clause must be a condition");
    }

    @Test public void testOn() {
        checkFails(
            "select * from emp e1 left outer join emp e2 on ^e1.sal^",
            "ON clause must be a condition");
    }

    @Test public void testHaving() {
        checkFails(
            "select * from emp having ^sum(sal)^",
            "HAVING clause must be a condition");
        checkFails(
            "select ^*^ from emp having sum(sal) > 10",
            "Expression 'EMP\\.EMPNO' is not being grouped");

        // agg in select and having, no group by
        check("select sum(sal + sal) from emp having sum(sal) > 10");
        checkFails(
            "SELECT deptno FROM emp GROUP BY deptno HAVING ^sal^ > 10",
            "Expression 'SAL' is not being grouped");
    }

    @Test public void testHavingBetween() {
        // FRG-115: having clause with between not working
        check(
            "select deptno from emp group by deptno having deptno between 10 and 12");

        // this worked even before FRG-115 was fixed
        check("select deptno from emp group by deptno having deptno + 5 > 10");
    }

    /**
     * Tests a large scalar expression, which will expose any O(n^2) algorithms
     * lurking in the validation process.
     */
    @Test public void testLarge() {
        int x = 1000;
        if (System.getProperty("os.name").startsWith("Windows")) {
            // NOTE jvs 1-Nov-2006:  Default thread stack size
            // on Windows is too small, so avoid stack overflow
            x /= 3;
        }

        // E.g. large = "deptno * 1 + deptno * 2 + deptno * 3".
        String large = list(" + ", "deptno * ", x);
        check("select " + large + "from emp");
        check("select distinct " + large + "from emp");
        check("select " + large + " from emp " + "group by deptno");
        check("select * from emp where " + large + " > 5");
        check("select * from emp order by " + large + " desc");
        check("select " + large + " from emp order by 1");
        check("select distinct " + large + " from emp order by " + large);

        // E.g. "in (0, 1, 2, ...)"
        check("select * from emp where deptno in (" + list(", ", "", x) + ")");

        // E.g. "where x = 1 or x = 2 or x = 3 ..."
        check("select * from emp where " + list(" or ", "deptno = ", x));

        // E.g. "select x1, x2 ... from (
        // select 'a' as x1, 'a' as x2, ... from emp union
        // select 'bb' as x1, 'bb' as x2, ... from dept)"
        check(
            "select " + list(", ", "x", x)
            + " from (select " + list(", ", "'a' as x", x) + " from emp "
            + "union all select " + list(", ", "'bb' as x", x) + " from dept)");
    }

    private String list(String sep, String before, int count)
    {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                buf.append(sep);
            }
            buf.append(before).append(i);
        }
        return buf.toString();
    }

    @Test public void testOrder() {
        final SqlConformance conformance = tester.getConformance();
        check("select empno as x from emp order by empno");

        // invalid use of 'asc'
        checkFails(
            "select empno, sal from emp order by ^asc^",
            "Column 'ASC' not found in any table");

        // In sql92, empno is obscured by the alias.
        // Otherwise valid.
        // Checked Oracle10G -- is it valid.
        checkFails(
            "select empno as x from emp order by empno",

            // in sql92, empno is obscured by the alias
            conformance.isSortByAliasObscures()
            ? "unknown column empno"
            // otherwise valid
            : null);

        checkFails(
            "select empno as x from emp order by ^x^",

            // valid in oracle and pre-99 sql
            conformance.isSortByAlias()
            ? null
            // invalid in sql:2003
            : "Column 'X' not found in any table");

        checkFails(
            "select empno as x from emp order by ^10^",

            // invalid in oracle and pre-99
            conformance.isSortByOrdinal()
            ? "Ordinal out of range"
            // valid from sql:99 onwards (but sorting by constant achieves
            // nothing!)
            : null);

        // Has different meanings in different dialects (which makes it very
        // confusing!) but is always valid.
        check("select empno + 1 as empno from emp order by empno");

        // Always fails
        checkFails(
            "select empno as x from emp, dept order by ^deptno^",
            "Column 'DEPTNO' is ambiguous");

        check("select empno + 1 from emp order by deptno asc, empno + 1 desc");

        checkFails(
            "select empno as deptno from emp, dept order by deptno",

            // Alias 'deptno' is closer in scope than 'emp.deptno'
            // and 'dept.deptno', and is therefore not ambiguous.
            // Checked Oracle10G -- it is valid.
            conformance.isSortByAlias()
            ? null
            // Ambiguous in SQL:2003
            : "col ambig");

        check(
            "select deptno from dept" + NL
            + "union" + NL
            + "select empno from emp" + NL
            + "order by deptno");

        checkFails(
            "select deptno from dept" + NL
            + "union" + NL
            + "select empno from emp" + NL
            + "order by ^empno^",
            "Column 'EMPNO' not found in any table");

        checkFails(
            "select deptno from dept" + NL
            + "union" + NL
            + "select empno from emp" + NL
            + "order by ^10^",

            // invalid in oracle and pre-99
            conformance.isSortByOrdinal() ? "Ordinal out of range" : null);

        // Sort by scalar subquery
        check(
            "select * from emp " + NL
            + "order by (select name from dept where deptno = emp.deptno)");
        checkFails(
            "select * from emp " + NL
            + "order by (select name from dept where deptno = emp.^foo^)",
            "Column 'FOO' not found in table 'EMP'");

        // REVIEW jvs 10-Apr-2008:  I disabled this because I don't
        // understand what it means; see
        // testAggregateInOrderByFails for the discrimination I added
        // (SELECT should be aggregating for this to make sense).
/*
        // Sort by aggregate. Oracle allows this.
        check("select 1 from emp order by sum(sal)");
*/

        // ORDER BY and SELECT *
        check("select * from emp order by empno");
        checkFails(
            "select * from emp order by ^nonExistent^, deptno",
            "Column 'NONEXISTENT' not found in any table");

        // Overriding expression has different type.
        checkFails(
            "select 'foo' as empno from emp order by ^empno + 5^",
            "(?s)Cannot apply '\\+' to arguments of type '<CHAR\\(3\\)> \\+ <INTEGER>'\\..*");
    }

    @Test public void testOrderUnion() {
        check(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by empno");

        checkFails(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by ^asc^",
            "Column 'ASC' not found in any table");

        // name belongs to emp but is not projected so cannot sort on it
        checkFails(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by ^ename^ desc",
            "Column 'ENAME' not found in any table");

        // empno is not an alias in the first select in the union
        checkFails(
            "select deptno, deptno from dept "
            + "union all "
            + "select empno, sal from emp "
            + "order by deptno asc, ^empno^",
            "Column 'EMPNO' not found in any table");

        // ordinals ok
        check(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by 2");

        // ordinal out of range -- if 'order by <ordinal>' means something in
        // this dialect
        if (tester.getConformance().isSortByOrdinal()) {
            checkFails(
                "select empno, sal from emp "
                + "union all "
                + "select deptno, deptno from dept "
                + "order by ^3^",
                "Ordinal out of range");
        }

        // Expressions made up of aliases are OK.
        // (This is illegal in Oracle 10G.)
        check(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by empno * sal + 2");

        check(
            "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by 'foobar'");
    }

    /**
     * Tests validation of the ORDER BY clause when GROUP BY is present.
     */
    @Test public void testOrderGroup() {
        // Group by
        checkFails(
            "select 1 from emp group by deptno order by ^empno^",
            "Expression 'EMP\\.EMPNO' is not being grouped");

        // order by can contain aggregate expressions
        check(
            "select empno from emp "
            + "group by empno, deptno "
            + "order by deptno * sum(sal + 2)");

        // Having

        checkFails(
            "select sum(sal) from emp having count(*) > 3 order by ^empno^",
            "Expression 'EMP\\.EMPNO' is not being grouped");

        check(
            "select sum(sal) from emp having count(*) > 3 order by sum(deptno)");

        // Select distinct

        checkFails(
            "select distinct deptno from emp group by deptno order by ^empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");

        checkFails(
            "select distinct deptno from emp group by deptno order by deptno, ^empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");

        check(
            "select distinct deptno from emp group by deptno order by deptno");

        // UNION of SELECT DISTINCT and GROUP BY behaves just like a UNION.
        check(
            "select distinct deptno from dept "
            + "union all "
            + "select empno from emp group by deptno, empno "
            + "order by deptno");

        // order by can contain a mixture of aliases and aggregate expressions
        check(
            "select empno as x "
            + "from emp "
            + "group by empno, deptno "
            + "order by x * sum(sal + 2)");

        checkFails(
            "select empno as x "
            + "from emp "
            + "group by empno, deptno "
            + "order by empno * sum(sal + 2)",
            tester.getConformance().isSortByAliasObscures() ? "xxxx" : null);

        // Distinct on expressions with attempts to order on a column in
        // the underlying table
        checkFails(
            "select distinct cast(empno as bigint) "
            + "from emp order by ^empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");
        checkFails(
            "select distinct cast(empno as bigint) "
            + "from emp order by ^emp.empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");
        checkFails(
            "select distinct cast(empno as bigint) as empno "
            + "from emp order by ^emp.empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");
        checkFails(
            "select distinct cast(empno as bigint) as empno "
            + "from emp as e order by ^e.empno^",
            "Expression 'E\\.EMPNO' is not in the select clause");

        // These tests are primarily intended to test cases where sorting by
        // an alias is allowed.  But for instances that don't support sorting
        // by alias, the tests also verify that a proper exception is thrown.
        checkFails(
            "select distinct cast(empno as bigint) as empno "
            + "from emp order by ^empno^",
            tester.getConformance().isSortByAlias() ? null
            : "Expression 'EMPNO' is not in the select clause");
        checkFails(
            "select distinct cast(empno as bigint) as eno "
            + "from emp order by ^eno^",
            tester.getConformance().isSortByAlias() ? null
            : "Column 'ENO' not found in any table");
        checkFails(
            "select distinct cast(empno as bigint) as empno "
            + "from emp e order by ^empno^",
            tester.getConformance().isSortByAlias() ? null
            : "Expression 'EMPNO' is not in the select clause");

        // Distinct on expressions, sorting using ordinals.
        if (tester.getConformance().isSortByOrdinal()) {
            check(
                "select distinct cast(empno as bigint) from emp order by 1");
            check(
                "select distinct cast(empno as bigint) as empno "
                + "from emp order by 1");
            check(
                "select distinct cast(empno as bigint) as empno "
                + "from emp as e order by 1");
        }

        // Distinct on expressions with ordering on expressions as well
        check(
            "select distinct cast(empno as varchar(10)) from emp "
            + "order by cast(empno as varchar(10))");
        checkFails(
            "select distinct cast(empno as varchar(10)) as eno from emp "
            + " order by upper(^eno^)",
            tester.getConformance().isSortByAlias() ? null
            : "Column 'ENO' not found in any table");
    }

    @Test public void testGroup() {
        checkFails(
            "select empno from emp where ^sum(sal)^ > 50",
            "Aggregate expression is illegal in WHERE clause");

        checkFails(
            "select ^empno^ from emp group by deptno",
            "Expression 'EMPNO' is not being grouped");

        checkFails(
            "select ^*^ from emp group by deptno",
            "Expression 'EMP\\.EMPNO' is not being grouped");

        // If we're grouping on ALL columns, 'select *' is ok.
        // Checked on Oracle10G.
        check(
            "select * from (select empno,deptno from emp) group by deptno,empno");

        // This query tries to reference an agg expression from within a
        // subquery as a correlating expression, but the SQL syntax rules say
        // that the agg function SUM always applies to the current scope.
        // As it happens, the query is valid.
        check(
            "select deptno " + NL
            + "from emp " + NL
            + "group by deptno " + NL
            + "having exists (select sum(emp.sal) > 10 from (values(true)))");

        // if you reference a column from a subquery, it must be a group col
        check(
            "select deptno "
            + "from emp "
            + "group by deptno "
            + "having exists (select 1 from (values(true)) where emp.deptno = 10)");

        // Needs proper error message text and error markers in query
        if (todo) {
            checkFails(
                "select deptno "
                + "from emp "
                + "group by deptno "
                + "having exists (select 1 from (values(true)) where emp.empno = 10)",
                "xx");
        }

        // constant expressions
        check("select cast(1 as integer) + 2 from emp group by deptno");
        check("select localtime, deptno + 3 from emp group by deptno");
    }

    @Test public void testGroupByCorrelatedColumnFails() {
        // -- this is not sql 2003 standard
        // -- see sql2003 part2,  7.9
        checkFails(
            "select count(*)\n"
            + "from emp\n"
            + "where exists (select count(*) from dept group by ^emp^.empno)",
            "Table 'EMP' not found");
    }

    @Test public void testGroupExpressionEquivalence() {
        // operator equivalence
        check("select empno + 1 from emp group by empno + 1");
        checkFails(
            "select 1 + ^empno^ from emp group by empno + 1",
            "Expression 'EMPNO' is not being grouped");

        // datatype equivalence
        check(
            "select cast(empno as VARCHAR(10)) from emp group by cast(empno as VARCHAR(10))");
        checkFails(
            "select cast(^empno^ as VARCHAR(11)) from emp group by cast(empno as VARCHAR(10))",
            "Expression 'EMPNO' is not being grouped");
    }

    @Test public void testGroupExpressionEquivalenceId() {
        // identifier equivalence
        check(
            "select case empno when 10 then deptno else null end from emp "
            + "group by case empno when 10 then deptno else null end");

        // matches even when one column is qualified (checked on Oracle10.1)
        check(
            "select case empno when 10 then deptno else null end from emp "
            + "group by case empno when 10 then emp.deptno else null end");
        check(
            "select case empno when 10 then deptno else null end from emp "
            + "group by case emp.empno when 10 then emp.deptno else null end");
        check(
            "select case emp.empno when 10 then deptno else null end from emp "
            + "group by case empno when 10 then emp.deptno else null end");

        // emp.deptno is different to dept.deptno (even though there is an '='
        // between them)
        checkFails(
            "select case ^emp.empno^ when 10 then emp.deptno else null end "
            + "from emp join dept on emp.deptno = dept.deptno "
            + "group by case emp.empno when 10 then dept.deptno else null end",
            "Expression 'EMP\\.EMPNO' is not being grouped");
    }

    // todo: enable when correlating variables work
    public void _testGroupExpressionEquivalenceCorrelated()
    {
        // dname comes from dept, so it is constant within the subquery, and
        // is so is a valid expr in a group-by query
        check(
            "select * from dept where exists ("
            + "select dname from emp group by empno)");
        check(
            "select * from dept where exists ("
            + "select dname + empno + 1 from emp group by empno, dept.deptno)");
    }

    // todo: enable when params are implemented
    public void _testGroupExpressionEquivalenceParams()
    {
        check("select cast(? as integer) from emp group by cast(? as integer)");
    }

    @Test public void testGroupExpressionEquivalenceLiteral() {
        // The purpose of this test is to see whether the validator
        // regards a pair of constants as equivalent. If we just used the raw
        // constants the validator wouldn't care ('SELECT 1 FROM emp GROUP BY
        // 2' is legal), so we combine a column and a constant into the same
        // CASE expression.

        // literal equivalence
        check(
            "select case empno when 10 then date '1969-04-29' else null end from emp "
            + "group by case empno when 10 then date '1969-04-29' else null end");

        // this query succeeds in oracle 10.1 because 1 and 1.0 have the same
        // type
        checkFails(
            "select case ^empno^ when 10 then 1 else null end from emp "
            + "group by case empno when 10 then 1.0 else null end",
            "Expression 'EMPNO' is not being grouped");

        // 3.1415 and 3.14150 are different literals (I don't care either way)
        checkFails(
            "select case ^empno^ when 10 then 3.1415 else null end from emp "
            + "group by case empno when 10 then 3.14150 else null end",
            "Expression 'EMPNO' is not being grouped");

        // 3 and 03 are the same literal (I don't care either way)
        check(
            "select case empno when 10 then 03 else null end from emp "
            + "group by case empno when 10 then 3 else null end");
        checkFails(
            "select case ^empno^ when 10 then 1 else null end from emp "
            + "group by case empno when 10 then 2 else null end",
            "Expression 'EMPNO' is not being grouped");
        check(
            "select case empno when 10 then timestamp '1969-04-29 12:34:56.0' else null end from emp "
            + "group by case empno when 10 then timestamp '1969-04-29 12:34:56' else null end");
    }

    @Test public void testGroupExpressionEquivalenceStringLiteral() {
        check(
            "select case empno when 10 then 'foo bar' else null end from emp "
            + "group by case empno when 10 then 'foo bar' else null end");

        if (Bug.Frg78Fixed) {
            check(
                "select case empno when 10 then _iso-8859-1'foo bar' collate latin1$en$1 else null end from emp "
                + "group by case empno when 10 then _iso-8859-1'foo bar' collate latin1$en$1 else null end");
        }

        checkFails(
            "select case ^empno^ when 10 then _iso-8859-1'foo bar' else null end from emp "
            + "group by case empno when 10 then _UTF16'foo bar' else null end",
            "Expression 'EMPNO' is not being grouped");

        if (Bug.Frg78Fixed) {
            checkFails(
                "select case ^empno^ when 10 then 'foo bar' collate latin1$en$1 else null end from emp "
                + "group by case empno when 10 then 'foo bar' collate latin1$fr$1 else null end",
                "Expression 'EMPNO' is not being grouped");
        }
    }

    @Test public void testGroupAgg() {
        // alias in GROUP BY query has been known to cause problems
        check("select deptno as d, count(*) as c from emp group by deptno");
    }

    @Test public void testNestedAggFails() {
        String ERR_NESTED_AGG = "Aggregate expressions cannot be nested";

        // simple case
        checkFails(
            "select ^sum(max(empno))^ from emp",
            ERR_NESTED_AGG);

        // should still fail with intermediate expression
        checkFails(
            "select ^sum(2*max(empno))^ from emp",
            ERR_NESTED_AGG);

        // make sure it fails with GROUP BY too
        checkFails(
            "select ^sum(max(empno))^ from emp group by deptno",
            ERR_NESTED_AGG);

        // make sure it fails in HAVING too
        checkFails(
            "select count(*) from emp group by deptno "
            + "having ^sum(max(empno))^=3",
            ERR_NESTED_AGG);

        // double-nesting should fail too; bottom-up validation currently
        // causes us to flag the intermediate level
        checkFails(
            "select sum(^max(min(empno))^) from emp",
            ERR_NESTED_AGG);

        // in OVER clause
        checkFails(
            "select ^sum(max(empno)) OVER^ (order by deptno ROWS 2 PRECEDING) from emp",
            ERR_NESTED_AGG);

        // OVER in clause
        checkFails(
            "select ^sum(max(empno) OVER (order by deptno ROWS 2 PRECEDING))^ from emp",
            ERR_NESTED_AGG);
    }

    @Test public void testAggregateInGroupByFails() {
        String ERR_AGG_IN_GROUP_BY =
            "Aggregate expression is illegal in GROUP BY clause";

        checkFails(
            "select count(*) from emp group by ^sum(empno)^",
            ERR_AGG_IN_GROUP_BY);
    }

    @Test public void testAggregateInOrderByFails() {
        String ERR_AGG_IN_ORDER_BY =
            "Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT";

        checkFails(
            "select empno from emp order by ^sum(empno)^",
            ERR_AGG_IN_ORDER_BY);

        // but this should be OK
        check("select sum(empno) from emp group by deptno order by sum(empno)");

        // this should also be OK
        check("select sum(empno) from emp order by sum(empno)");
    }

    @Test public void testCorrelatingVariables() {
        // reference to unqualified correlating column
        check(
            "select * from emp where exists (" + NL
            + "select * from dept where deptno = sal)");

        // reference to qualified correlating column
        check(
            "select * from emp where exists (" + NL
            + "select * from dept where deptno = emp.sal)");
    }

    @Test public void testIntervalCompare() {
        checkExpType(
            "interval '1' hour = interval '1' day",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' hour <> interval '1' hour",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' hour < interval '1' second",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' hour <= interval '1' minute",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' minute > interval '1' second",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' second >= interval '1' day",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' year >= interval '1' year",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' month = interval '1' year",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' month <> interval '1' month",
            "BOOLEAN NOT NULL");
        checkExpType(
            "interval '1' year >= interval '1' month",
            "BOOLEAN NOT NULL");

        checkWholeExpFails(
            "interval '1' second >= interval '1' year",
            "(?s).*Cannot apply '>=' to arguments of type '<INTERVAL SECOND> >= <INTERVAL YEAR>'.*");
        checkWholeExpFails(
            "interval '1' month = interval '1' day",
            "(?s).*Cannot apply '=' to arguments of type '<INTERVAL MONTH> = <INTERVAL DAY>'.*");
    }

    @Test public void testOverlaps() {
        checkExpType(
            "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', date '1-2-3')",
            "BOOLEAN NOT NULL");
        checkExp(
            "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
        checkExp(
            "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')");
        checkExp(
            "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");

        checkWholeExpFails(
            "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIMESTAMP>, <TIMESTAMP>. OVERLAPS .<TIME.0.>, <INTERVAL DAY TO SECOND>.*");
        checkWholeExpFails(
            "(time '4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIME.0.>, <TIMESTAMP>. OVERLAPS .<TIME.0.>, <INTERVAL DAY TO SECOND>.'.*");
        checkWholeExpFails(
            "(time '4:5:6', time '4:5:6' ) overlaps (time '4:5:6', date '1-2-3')",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIME.0.>, <TIME.0.>. OVERLAPS .<TIME.0.>, <DATE>.'.*");
    }

    @Test public void testExtract() {
        // TODO: Need to have extract return decimal type for seconds
        // so we can have seconds fractions
        checkExpType(
            "extract(year from interval '1-2' year to month)",
            "BIGINT NOT NULL");
        checkExp("extract(minute from interval '1.1' second)");

        checkWholeExpFails(
            "extract(minute from interval '11' month)",
            "(?s).*Cannot apply.*");
        checkWholeExpFails(
            "extract(year from interval '11' second)",
            "(?s).*Cannot apply.*");
    }

    @Test public void testCastToInterval() {
        checkExpType(
            "cast(interval '1' hour as varchar(20))",
            "VARCHAR(20) NOT NULL");
        checkExpType("cast(interval '1' hour as bigint)", "BIGINT NOT NULL");
        checkExpType("cast(1000 as interval hour)", "INTERVAL HOUR NOT NULL");

        checkExpType(
            "cast(interval '1' month as interval year)",
            "INTERVAL YEAR NOT NULL");
        checkExpType(
            "cast(interval '1-1' year to month as interval month)",
            "INTERVAL MONTH NOT NULL");
        checkExpType(
            "cast(interval '1:1' hour to minute as interval day)",
            "INTERVAL DAY NOT NULL");
        checkExpType(
            "cast(interval '1:1' hour to minute as interval minute to second)",
            "INTERVAL MINUTE TO SECOND NOT NULL");

        checkWholeExpFails(
            "cast(interval '1:1' hour to minute as interval month)",
            "Cast function cannot convert value of type INTERVAL HOUR TO MINUTE to type INTERVAL MONTH");
        checkWholeExpFails(
            "cast(interval '1-1' year to month as interval second)",
            "Cast function cannot convert value of type INTERVAL YEAR TO MONTH to type INTERVAL SECOND");
    }

    @Test public void testMinusDateOperator() {
        checkExpType(
            "(CURRENT_DATE - CURRENT_DATE) HOUR",
            "INTERVAL HOUR NOT NULL");
        checkExpType(
            "(CURRENT_DATE - CURRENT_DATE) YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkWholeExpFails(
            "(CURRENT_DATE - LOCALTIME) YEAR TO MONTH",
            "(?s).*Parameters must be of the same type.*");
    }

    @Test public void testBind() {
        check("select * from emp where deptno = ?");
        check("select * from emp where deptno = ? and sal < 100000");
        if (todoTypeInference) {
            check("select case when deptno = ? then 1 else 2 end from emp");
        }
        if (todoTypeInference) {
            check(
                "select deptno from emp group by substring(name from ? for ?)");
        }
        if (todoTypeInference) {
            check(
                "select deptno from emp group by case when deptno = ? then 1 else 2 end");
        }
        check("select 1 from emp having sum(sal) < ?");
    }

    @Test public void testUnnest() {
        checkColumnType("select*from unnest(multiset[1])", "INTEGER NOT NULL");
        checkColumnType(
            "select*from unnest(multiset[1, 2])",
            "INTEGER NOT NULL");
        checkColumnType(
            "select*from unnest(multiset[321.3, 2.33])",
            "DECIMAL(5, 2) NOT NULL");
        checkColumnType(
            "select*from unnest(multiset[321.3, 4.23e0])",
            "DOUBLE NOT NULL");
        checkColumnType(
            "select*from unnest(multiset[43.2e1, cast(null as decimal(4,2))])",
            "DOUBLE");
        checkColumnType(
            "select*from unnest(multiset[1, 2.3, 1])",
            "DECIMAL(11, 1) NOT NULL");
        checkColumnType(
            "select*from unnest(multiset['1','22','333'])",
            "CHAR(3) NOT NULL");
        checkColumnType(
            "select*from unnest(multiset['1','22','333','22'])",
            "CHAR(3) NOT NULL");
        checkFails(
            "select*from ^unnest(1)^",
            "(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
        check("select*from unnest(multiset(select*from dept))");
    }

    @Test public void testCorrelationJoin() {
        check(
            "select *,"
            + "         multiset(select * from emp where deptno=dept.deptno) "
            + "               as empset"
            + "      from dept");
        check("select*from unnest(select multiset[8] from dept)");
        check("select*from unnest(select multiset[deptno] from dept)");
    }

    @Test public void testStructuredTypes() {
        checkColumnType(
            "values new address()",
            "ObjectSqlType(ADDRESS) NOT NULL");
        checkColumnType(
            "select home_address from emp_address",
            "ObjectSqlType(ADDRESS) NOT NULL");
        checkColumnType(
            "select ea.home_address.zip from emp_address ea",
            "INTEGER NOT NULL");
        checkColumnType(
            "select ea.mailing_address.city from emp_address ea",
            "VARCHAR(20) NOT NULL");
    }

    @Test public void testLateral() {
        checkFails(
            "select * from emp, (select * from dept where ^emp^.deptno=dept.deptno)",
            "Table 'EMP' not found");

        check(
            "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)");
        check(
            "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno) as ldt");
        check(
            "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno) ldt");
    }

    @Test public void testCollect() {
        check("select collect(deptno) from emp");
        check("select collect(multiset[3]) from emp");
        // todo. COLLECT is an aggregate function. test that validator only can
        // take set operators in its select list once aggregation support is
        // complete
    }

    @Test public void testFusion() {
        checkFails(
            "select ^fusion(deptno)^ from emp",
            "(?s).*Cannot apply 'FUSION' to arguments of type 'FUSION.<INTEGER>.'.*");
        check("select fusion(multiset[3]) from emp");
        // todo. FUSION is an aggregate function. test that validator only can
        // take set operators in its select list once aggregation support is
        // complete
    }

    @Test public void testCountFunction() {
        check("select count(*) from emp");
        check("select count(ename) from emp");
        check("select count(sal) from emp");
        check("select count(1) from emp");
        checkFails(
            "select ^count(sal,ename)^ from emp",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
    }

    @Test public void testLastFunction() {
        check("select LAST_VALUE(sal) over (order by empno) from emp");
        check("select LAST_VALUE(ename) over (order by empno) from emp");

        check("select FIRST_VALUE(sal) over (order by empno) from emp");
        check("select FIRST_VALUE(ename) over (order by empno) from emp");
    }

    @Test public void testMinMaxFunctions() {
        check("SELECT MIN(true) from emp");
        check("SELECT MAX(false) from emp");

        check("SELECT MIN(sal+deptno) FROM emp");
        check("SELECT MAX(ename) FROM emp");
        check("SELECT MIN(5.5) FROM emp");
        check("SELECT MAX(5) FROM emp");
    }

    @Test public void testFunctionalDistinct() {
        check("select count(distinct sal) from emp");
        checkFails(
            "select COALESCE(^distinct^ sal) from emp",
            "DISTINCT/ALL not allowed with COALESCE function");
    }

    @Test public void testSelectDistinct() {
        check("SELECT DISTINCT deptno FROM emp");
        check("SELECT DISTINCT deptno, sal FROM emp");
        check("SELECT DISTINCT deptno FROM emp GROUP BY deptno");
        checkFails(
            "SELECT DISTINCT ^deptno^ FROM emp GROUP BY sal",
            "Expression 'DEPTNO' is not being grouped");
        check("SELECT DISTINCT avg(sal) from emp");
        checkFails(
            "SELECT DISTINCT ^deptno^, avg(sal) from emp",
            "Expression 'DEPTNO' is not being grouped");
        check("SELECT DISTINCT deptno, sal from emp GROUP BY sal, deptno");
        check("SELECT deptno FROM emp GROUP BY deptno HAVING deptno > 55");
        check(
            "SELECT DISTINCT deptno, 33 FROM emp GROUP BY deptno HAVING deptno > 55");
        checkFails(
            "SELECT DISTINCT deptno, 33 FROM emp HAVING ^deptno^ > 55",
            "Expression 'DEPTNO' is not being grouped");
        check("SELECT DISTINCT * from emp");
        checkFails(
            "SELECT DISTINCT ^*^ from emp GROUP BY deptno",
            "Expression 'EMP\\.EMPNO' is not being grouped");

        // similar validation for SELECT DISTINCT and GROUP BY
        checkFails(
            "SELECT deptno FROM emp GROUP BY deptno ORDER BY deptno, ^empno^",
            "Expression 'EMP\\.EMPNO' is not being grouped");
        checkFails(
            "SELECT DISTINCT deptno from emp ORDER BY deptno, ^empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");
        check("SELECT DISTINCT deptno from emp ORDER BY deptno + 2");
        if (false) { // Hersker 2008917: Julian will fix immediately after
                     // integration
            checkFails(
                "SELECT DISTINCT deptno from emp ORDER BY deptno, ^sum(empno)^",
                "Expression 'SUM\\(`EMP`\\.`EMPNO`\\)' is not in the select clause");
        }

        // The ORDER BY clause works on what is projected by DISTINCT - even if
        // GROUP BY is present.
        checkFails(
            "SELECT DISTINCT deptno FROM emp GROUP BY deptno, empno ORDER BY deptno, ^empno^",
            "Expression 'EMP\\.EMPNO' is not in the select clause");

        // redundant distinct; same query is in unitsql/optimizer/distinct.sql
        check(
            "select distinct * from (select distinct deptno from emp) order by 1");

        check("SELECT DISTINCT 5, 10+5, 'string' from emp");
    }

    @Test public void testExplicitTable() {
        final String empRecordType =
            "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER NOT NULL MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL";
        checkResultType("select * from (table emp)", empRecordType);
        checkResultType("table emp", empRecordType);
        checkFails(
            "table ^nonexistent^",
            "Table 'NONEXISTENT' not found");
    }

    @Test public void testCollectionTable() {
        checkResultType(
            "select * from table(ramp(3))",
            "RecordType(INTEGER NOT NULL I) NOT NULL");

        checkFails(
            "select * from table(^ramp('3')^)",
            "Cannot apply 'RAMP' to arguments of type 'RAMP\\(<CHAR\\(1\\)>\\)'\\. Supported form\\(s\\): 'RAMP\\(<NUMERIC>\\)'");

        checkFails(
            "select * from table(^nonExistentRamp('3')^)",
            "No match found for function signature NONEXISTENTRAMP\\(<CHARACTER>\\)");
    }

    @Test public void testCollectionTableWithCursorParam() {
        checkResultType(
            "select * from table(dedup(cursor(select * from emp),'ename'))",
            "RecordType(VARCHAR(1024) NOT NULL NAME) NOT NULL");
        checkFails(
            "select * from table(dedup(cursor(select * from ^bloop^),'ename'))",
            "Table 'BLOOP' not found");
    }

    @Test public void testScalarSubQuery() {
        check("SELECT  ename,(select name from dept where deptno=1) FROM emp");
        checkFails(
            "SELECT ename,(^select losal, hisal from salgrade where grade=1^) FROM emp",
            "Cannot apply '\\$SCALAR_QUERY' to arguments of type '\\$SCALAR_QUERY\\(<RECORDTYPE\\(INTEGER LOSAL, INTEGER HISAL\\)>\\)'\\. Supported form\\(s\\): '\\$SCALAR_QUERY\\(<RECORDTYPE\\(SINGLE FIELD\\)>\\)'");

        // Note that X is a field (not a record) and is nullable even though
        // EMP.NAME is NOT NULL.
        checkResultType(
            "SELECT  ename,(select name from dept where deptno=1) FROM emp",
            "RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) EXPR$1) NOT NULL");

        // scalar subqery inside AS operator
        checkResultType(
            "SELECT  ename,(select name from dept where deptno=1) as X FROM emp",
            "RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) X) NOT NULL");

        // scalar subqery inside + operator
        checkResultType(
            "SELECT  ename, 1 + (select deptno from dept where deptno=1) as X FROM emp",
            "RecordType(VARCHAR(20) NOT NULL ENAME, INTEGER X) NOT NULL");

        // scalar subquery inside WHERE
        check("select * from emp where (select true from dept)");
    }

    public void _testSubqueryInOnClause()
    {
        // Currently not supported. Should give validator error, but gives
        // internal error.
        check(
            "select * from emp as emps left outer join dept as depts\n"
            + "on emps.deptno = depts.deptno and emps.deptno = (\n"
            + "select min(deptno) from dept as depts2)");
    }

    @Test public void testRecordType() {
        // Have to qualify columns with table name.
        checkFails(
            "SELECT ^coord^.x, coord.y FROM customer.contact",
            "Table 'COORD' not found");

        checkResultType(
            "SELECT contact.coord.x, contact.coord.y FROM customer.contact",
            "RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

        // Qualifying with schema is OK.
        if (Bug.Frg140Fixed) {
            checkResultType(
                "SELECT customer.contact.coord.x, customer.contact.email, contact.coord.y FROM customer.contact",
                "RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");
        }
    }

    @Test public void testSample() {
        // applied to table
        check("SELECT * FROM emp TABLESAMPLE SUBSTITUTE('foo')");
        check("SELECT * FROM emp TABLESAMPLE BERNOULLI(50)");
        check("SELECT * FROM emp TABLESAMPLE SYSTEM(50)");

        // applied to query
        check(
            "SELECT * FROM ("
            + "SELECT deptno FROM emp "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
            + "WHERE x.deptno < 100");

        checkFails(
            "SELECT x.^empno^ FROM ("
            + "SELECT deptno FROM emp TABLESAMPLE SUBSTITUTE('bar') "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
            + "ORDER BY 1",
            "Column 'EMPNO' not found in table 'X'");

        check(
            "select * from (\n"
            + "    select * from emp\n"
            + "    join dept on emp.deptno = dept.deptno\n"
            + ") tablesample substitute('SMALL')");

        check(
            "SELECT * FROM ("
            + "SELECT deptno FROM emp "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(50) "
            + "WHERE x.deptno < 100");

        checkFails(
            "SELECT x.^empno^ FROM ("
            + "SELECT deptno FROM emp TABLESAMPLE BERNOULLI(50) "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(10) "
            + "ORDER BY 1",
            "Column 'EMPNO' not found in table 'X'");

        check(
            "select * from (\n"
            + "    select * from emp\n"
            + "    join dept on emp.deptno = dept.deptno\n"
            + ") tablesample bernoulli(10)");

        check(
            "SELECT * FROM ("
            + "SELECT deptno FROM emp "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(50) "
            + "WHERE x.deptno < 100");

        checkFails(
            "SELECT x.^empno^ FROM ("
            + "SELECT deptno FROM emp TABLESAMPLE SYSTEM(50) "
            + "UNION ALL "
            + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(10) "
            + "ORDER BY 1",
            "Column 'EMPNO' not found in table 'X'");

        check(
            "select * from (\n"
            + "    select * from emp\n"
            + "    join dept on emp.deptno = dept.deptno\n"
            + ") tablesample system(10)");
    }

    @Test public void testRewriteWithoutIdentifierExpansion() {
        SqlValidator validator = tester.getValidator();
        validator.setIdentifierExpansion(false);
        tester.checkRewrite(
            validator,
            "select * from dept",
            "SELECT *" + NL + "FROM `DEPT`");
    }

    @Test public void testRewriteWithIdentifierExpansion() {
        SqlValidator validator = tester.getValidator();
        validator.setIdentifierExpansion(true);
        tester.checkRewrite(
            validator,
            "select * from dept",
            "SELECT `DEPT`.`DEPTNO`, `DEPT`.`NAME`"
            + NL
            + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`");
    }

    @Test public void testRewriteWithColumnReferenceExpansion() {
        // NOTE jvs 9-Apr-2007:  This tests illustrates that
        // ORDER BY is still a special case.  Update expected
        // output if that gets fixed in the future.

        SqlValidator validator = tester.getValidator();
        validator.setIdentifierExpansion(true);
        validator.setColumnReferenceExpansion(true);
        tester.checkRewrite(
            validator,
            "select name from dept where name = 'Moonracer' group by name"
            + " having sum(deptno) > 3 order by name",
            TestUtil.fold(
                "SELECT `DEPT`.`NAME`\n"
                + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`\n"
                + "WHERE `DEPT`.`NAME` = 'Moonracer'\n"
                + "GROUP BY `DEPT`.`NAME`\n"
                + "HAVING SUM(`DEPT`.`DEPTNO`) > 3\n"
                + "ORDER BY `NAME`"));
    }

    @Test public void testRewriteWithColumnReferenceExpansionAndFromAlias() {
        // NOTE jvs 9-Apr-2007:  This tests illustrates that
        // ORDER BY is still a special case.  Update expected
        // output if that gets fixed in the future.

        SqlValidator validator = tester.getValidator();
        validator.setIdentifierExpansion(true);
        validator.setColumnReferenceExpansion(true);
        tester.checkRewrite(
            validator,
            "select name from (select * from dept)"
            + " where name = 'Moonracer' group by name"
            + " having sum(deptno) > 3 order by name",
            TestUtil.fold(
                "SELECT `EXPR$0`.`NAME`\n"
                + "FROM (SELECT `DEPT`.`DEPTNO`, `DEPT`.`NAME`\n"
                + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`) AS `EXPR$0`\n"
                + "WHERE `EXPR$0`.`NAME` = 'Moonracer'\n"
                + "GROUP BY `EXPR$0`.`NAME`\n"
                + "HAVING SUM(`EXPR$0`.`DEPTNO`) > 3\n"
                + "ORDER BY `NAME`"));
    }

    @Test public void testCoalesceWithoutRewrite() {
        SqlValidator validator = tester.getValidator();
        validator.setCallRewrite(false);
        if (validator.shouldExpandIdentifiers()) {
            tester.checkRewrite(
                validator,
                "select coalesce(deptno, empno) from emp",
                TestUtil.fold(
                    "SELECT COALESCE(`EMP`.`DEPTNO`, `EMP`.`EMPNO`)\n"
                    + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`"));
        } else {
            tester.checkRewrite(
                validator,
                "select coalesce(deptno, empno) from emp",
                "SELECT COALESCE(`DEPTNO`, `EMPNO`)"
                + NL
                + "FROM `EMP`");
        }
    }

    @Test public void testCoalesceWithRewrite() {
        SqlValidator validator = tester.getValidator();
        validator.setCallRewrite(true);
        if (validator.shouldExpandIdentifiers()) {
            tester.checkRewrite(
                validator,
                "select coalesce(deptno, empno) from emp",
                TestUtil.fold(
                    "SELECT CASE WHEN `EMP`.`DEPTNO` IS NOT NULL THEN `EMP`.`DEPTNO` ELSE `EMP`.`EMPNO` END\n"
                    + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`"));
        } else {
            tester.checkRewrite(
                validator,
                "select coalesce(deptno, empno) from emp",
                "SELECT CASE WHEN `DEPTNO` IS NOT NULL THEN `DEPTNO` "
                + "ELSE `EMPNO` END"
                + NL
                + "FROM `EMP`");
        }
    }

    public void _testValuesWithAggFuncs()
    {
        checkFails(
            "values(^count(1)^)",
            "Call to xxx is invalid\\. Direct calls to aggregate functions not allowed in ROW definitions\\.");
    }

    @Test public void testFieldOrigin() {
        tester.checkFieldOrigin(
            "select * from emp join dept on true",
            "{CATALOG.SALES.EMP.EMPNO,"
            + " CATALOG.SALES.EMP.ENAME,"
            + " CATALOG.SALES.EMP.JOB,"
            + " CATALOG.SALES.EMP.MGR,"
            + " CATALOG.SALES.EMP.HIREDATE,"
            + " CATALOG.SALES.EMP.SAL,"
            + " CATALOG.SALES.EMP.COMM,"
            + " CATALOG.SALES.EMP.DEPTNO,"
            + " CATALOG.SALES.EMP.SLACKER,"
            + " CATALOG.SALES.DEPT.DEPTNO,"
            + " CATALOG.SALES.DEPT.NAME}");

        tester.checkFieldOrigin(
            "select distinct emp.empno, hiredate, 1 as one,\n"
            + " emp.empno * 2 as twiceEmpno\n"
            + "from emp join dept on true",
            "{CATALOG.SALES.EMP.EMPNO,"
            + " CATALOG.SALES.EMP.HIREDATE,"
            + " null,"
            + " null}");
    }

    @Test public void testNew() {
        // (To debug invidual statements, paste them into this method.)
        //            1         2         3         4         5         6
        //   12345678901234567890123456789012345678901234567890123456789012345
        //        check("SELECT count(0) FROM emp GROUP BY ()");
    }
}

// End SqlValidatorTest.java
