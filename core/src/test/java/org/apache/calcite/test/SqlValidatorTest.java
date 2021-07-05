/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlAbstractConformance;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.catalog.CountingFactory;
import org.apache.calcite.testlib.annotations.LocaleEnUs;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.util.Arrays.asList;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing lots of unit
 * tests.
 *
 * <p>If you want to run these same tests in a different environment, create a
 * derived class whose {@link #getTester} returns a different implementation of
 * {@link org.apache.calcite.sql.test.SqlTester}.
 */
@LocaleEnUs
public class SqlValidatorTest extends SqlValidatorTestCase {
  //~ Static fields/initializers ---------------------------------------------

  // CHECKSTYLE: IGNORE 1
  /**
   * @deprecated Deprecated so that usages of this constant will show up in
   * yellow in Intellij and maybe someone will fix them.
   */
  protected static final boolean TODO = false;
  private static final String ANY = "(?s).*";

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(SqlValidatorTest.class);

  private static final String ERR_IN_VALUES_INCOMPATIBLE =
      "Values in expression list must have compatible types";

  private static final String ERR_IN_OPERANDS_INCOMPATIBLE =
      "Values passed to IN operator must have compatible types";

  private static final String ERR_AGG_IN_GROUP_BY =
      "Aggregate expression is illegal in GROUP BY clause";

  private static final String ERR_AGG_IN_ORDER_BY =
      "Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT";

  private static final String ERR_NESTED_AGG =
      "Aggregate expressions cannot be nested";

  private static final String EMP_RECORD_TYPE =
      "RecordType(INTEGER NOT NULL EMPNO,"
          + " VARCHAR(20) NOT NULL ENAME,"
          + " VARCHAR(10) NOT NULL JOB,"
          + " INTEGER MGR,"
          + " TIMESTAMP(0) NOT NULL HIREDATE,"
          + " INTEGER NOT NULL SAL,"
          + " INTEGER NOT NULL COMM,"
          + " INTEGER NOT NULL DEPTNO,"
          + " BOOLEAN NOT NULL SLACKER) NOT NULL";

  private static final String STR_AGG_REQUIRES_MONO = "Streaming aggregation "
      + "requires at least one monotonic expression in GROUP BY clause";

  private static final String STR_ORDER_REQUIRES_MONO =
      "Streaming ORDER BY must start with monotonic expression";

  private static final String STR_SET_OP_INCONSISTENT =
      "Set operator cannot combine streaming and non-streaming inputs";

  private static final String ROW_RANGE_NOT_ALLOWED_WITH_RANK =
      "ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions";

  private static final String RANK_REQUIRES_ORDER_BY = "RANK or DENSE_RANK "
      + "functions require ORDER BY clause in window specification";

  private static String cannotConvertToStream(String name) {
    return "Cannot convert table '" + name + "' to stream";
  }

  private static String cannotConvertToRelation(String table) {
    return "Cannot convert stream '" + table + "' to relation";
  }

  private static String cannotStreamResultsForNonStreamingInputs(String inputs) {
    return "Cannot stream results of a query with no streaming inputs: '"
        + inputs
        + "'. At least one input should be convertible to a stream";
  }

  static SqlOperatorTable operatorTableFor(SqlLibrary library) {
    return SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
        SqlLibrary.STANDARD, library);
  }

  @Test void testMultipleSameAsPass() {
    sql("select 1 as again,2 as \"again\", 3 as AGAiN from (values (true))")
        .ok();
  }

  @Test void testMultipleDifferentAs() {
    sql("select 1 as c1,2 as c2 from (values(true))").ok();
  }

  @Test void testTypeOfAs() {
    sql("select 1 as c1 from (values (true))")
        .columnType("INTEGER NOT NULL");
    sql("select 'hej' as c1 from (values (true))")
        .columnType("CHAR(3) NOT NULL");
    sql("select x'deadbeef' as c1 from (values (true))")
        .columnType("BINARY(4) NOT NULL");
    sql("select cast(null as boolean) as c1 from (values (true))")
        .columnType("BOOLEAN");
  }

  @Test void testTypesLiterals() {
    expr("'abc'")
        .columnType("CHAR(3) NOT NULL");
    expr("n'abc'")
        .columnType("CHAR(3) NOT NULL");
    expr("_UTF16'abc'")
        .columnType("CHAR(3) NOT NULL");
    expr("'ab '\n"
        + "' cd'")
        .columnType("CHAR(6) NOT NULL");
    expr("'ab'\n"
        + "'cd'\n"
        + "'ef'\n"
        + "'gh'\n"
        + "'ij'\n"
        + "'kl'")
        .columnType("CHAR(12) NOT NULL");
    expr("n'ab '\n"
        + "' cd'")
        .columnType("CHAR(6) NOT NULL");
    expr("_UTF16'ab '\n"
        + "' cd'")
        .columnType("CHAR(6) NOT NULL");

    expr("^x'abc'^")
        .fails("Binary literal string must contain an even number of hexits");
    expr("x'abcd'")
        .columnType("BINARY(2) NOT NULL");
    expr("x'abcd'\n"
        + "'ff001122aabb'")
        .columnType("BINARY(8) NOT NULL");
    expr("x'aaaa'\n"
        + "'bbbb'\n"
        + "'0000'\n"
        + "'1111'")
        .columnType("BINARY(8) NOT NULL");

    expr("1234567890")
        .columnType("INTEGER NOT NULL");
    expr("123456.7890")
        .columnType("DECIMAL(10, 4) NOT NULL");
    expr("123456.7890e3")
        .columnType("DOUBLE NOT NULL");
    expr("true")
        .columnType("BOOLEAN NOT NULL");
    expr("false")
        .columnType("BOOLEAN NOT NULL");
    expr("unknown")
        .columnType("BOOLEAN");
  }

  @Test void testBooleans() {
    sql("select TRUE OR unknowN from (values(true))").ok();
    sql("select false AND unknown from (values(true))").ok();
    sql("select not UNKNOWn from (values(true))").ok();
    sql("select not true from (values(true))").ok();
    sql("select not false from (values(true))").ok();
  }

  @Test void testAndOrIllegalTypesFails() {
    // TODO need col+line number
    wholeExpr("'abc' AND FaLsE")
        .fails("(?s).*'<CHAR.3.> AND <BOOLEAN>'.*");

    wholeExpr("TRUE OR 1")
        .fails(ANY);

    wholeExpr("unknown OR 1.0")
        .fails(ANY);

    wholeExpr("true OR 1.0e4")
        .fails(ANY);

    if (TODO) {
      wholeExpr("TRUE OR (TIME '12:00' AT LOCAL)")
          .fails(ANY);
    }
  }

  @Test void testNotIllegalTypeFails() {
    sql("select ^NOT 3.141^ from (values(true))")
        .fails("(?s).*Cannot apply 'NOT' to arguments of type "
            + "'NOT<DECIMAL.4, 3.>'.*");

    sql("select ^NOT 'abc'^ from (values(true))")
        .fails(ANY);

    sql("select ^NOT 1^ from (values(true))")
        .fails(ANY);
  }

  @Test void testIs() {
    sql("select TRUE IS FALSE FROM (values(true))").ok();
    sql("select false IS NULL FROM (values(true))").ok();
    sql("select UNKNOWN IS NULL FROM (values(true))").ok();
    sql("select FALSE IS UNKNOWN FROM (values(true))").ok();

    sql("select TRUE IS NOT FALSE FROM (values(true))").ok();
    sql("select TRUE IS NOT NULL FROM (values(true))").ok();
    sql("select false IS NOT NULL FROM (values(true))").ok();
    sql("select UNKNOWN IS NOT NULL FROM (values(true))").ok();
    sql("select FALSE IS NOT UNKNOWN FROM (values(true))").ok();

    sql("select 1 IS NULL FROM (values(true))").ok();
    sql("select 1.2 IS NULL FROM (values(true))").ok();
    expr("^'abc' IS NOT UNKNOWN^")
        .fails("(?s).*Cannot apply.*");
  }

  @Test void testIsFails() {
    sql("select ^1 IS TRUE^ FROM (values(true))")
        .fails("(?s).*'<INTEGER> IS TRUE'.*");

    sql("select ^1.1 IS NOT FALSE^ FROM (values(true))")
        .fails(ANY);

    sql("select ^1.1e1 IS NOT FALSE^ FROM (values(true))")
        .fails("(?s).*Cannot apply 'IS NOT FALSE' to arguments of type "
            + "'<DOUBLE> IS NOT FALSE'.*");

    sql("select ^'abc' IS NOT TRUE^ FROM (values(true))")
        .fails(ANY);
  }

  @Test void testScalars() {
    sql("select 1  + 1 from (values(true))").ok();
    sql("select 1  + 2.3 from (values(true))").ok();
    sql("select 1.2+3 from (values(true))").ok();
    sql("select 1.2+3.4 from (values(true))").ok();

    sql("select 1  - 1 from (values(true))").ok();
    sql("select 1  - 2.3 from (values(true))").ok();
    sql("select 1.2-3 from (values(true))").ok();
    sql("select 1.2-3.4 from (values(true))").ok();

    sql("select 1  * 2 from (values(true))").ok();
    sql("select 1.2* 3 from (values(true))").ok();
    sql("select 1  * 2.3 from (values(true))").ok();
    sql("select 1.2* 3.4 from (values(true))").ok();

    sql("select 1  / 2 from (values(true))").ok();
    sql("select 1  / 2.3 from (values(true))").ok();
    sql("select 1.2/ 3 from (values(true))").ok();
    sql("select 1.2/3.4 from (values(true))").ok();
  }

  @Test void testScalarsFails() {
    sql("select ^1+TRUE^ from (values(true))")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\):.*");
  }

  @Test void testNumbers() {
    sql("select 1+-2.*-3.e-1/-4>+5 AND true from (values(true))").ok();
  }

  @Test void testPrefix() {
    expr("+interval '1' second")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("-interval '1' month")
        .columnType("INTERVAL MONTH NOT NULL");
    sql("SELECT ^-'abc'^ from (values(true))")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply '-' to arguments of type '-<CHAR.3.>'.*");
    sql("SELECT -'abc' from (values(true))").ok();
    sql("SELECT ^+'abc'^ from (values(true))")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply '\\+' to arguments of type '\\+<CHAR.3.>'.*");
    sql("SELECT +'abc' from (values(true))").ok();
  }

  @Test void testNiladicForBigQuery() {
    sql("select current_time, current_time(), current_date, "
        + "current_date(), current_timestamp, current_timestamp()")
        .withConformance(SqlConformanceEnum.BIG_QUERY).ok();
  }

  @Test void testEqualNotEqual() {
    expr("''=''").ok();
    expr("'abc'=n''").ok();
    expr("''=_latin1''").ok();
    expr("n''=''").ok();
    expr("n'abc'=n''").ok();
    expr("n''=_latin1''").ok();
    expr("_latin1''=''").ok();
    expr("_latin1''=n''").ok();
    expr("_latin1''=_latin1''").ok();

    expr("''<>''").ok();
    expr("'abc'<>n''").ok();
    expr("''<>_latin1''").ok();
    expr("n''<>''").ok();
    expr("n'abc'<>n''").ok();
    expr("n''<>_latin1''").ok();
    expr("_latin1''<>''").ok();
    expr("_latin1'abc'<>n''").ok();
    expr("_latin1''<>_latin1''").ok();

    expr("true=false").ok();
    expr("unknown<>true").ok();

    expr("1=1").ok();
    expr("1=.1").ok();
    expr("1=1e-1").ok();
    expr("0.1=1").ok();
    expr("0.1=0.1").ok();
    expr("0.1=1e1").ok();
    expr("1.1e1=1").ok();
    expr("1.1e1=1.1").ok();
    expr("1.1e-1=1e1").ok();

    expr("''<>''").ok();
    expr("1<>1").ok();
    expr("1<>.1").ok();
    expr("1<>1e-1").ok();
    expr("0.1<>1").ok();
    expr("0.1<>0.1").ok();
    expr("0.1<>1e1").ok();
    expr("1.1e1<>1").ok();
    expr("1.1e1<>1.1").ok();
    expr("1.1e-1<>1e1").ok();
  }

  @Test void testEqualNotEqualFails() {
    // compare CHAR, INTEGER ok; implicitly convert CHAR
    expr("''<>1").ok();
    expr("'1'>=1").ok();
    // compare INTEGER, NCHAR ok
    expr("1<>n'abc'").ok();
    // compare CHAR, DECIMAL ok
    expr("''=.1").ok();
    expr("true<>1e-1").ok();
    expr("^true<>1e-1^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
    // compare BOOLEAN, CHAR ok
    expr("false=''").ok();
    expr("^x'a4'=0.01^")
        .fails("(?s).*Cannot apply '=' to arguments of type "
            + "'<BINARY.1.> = <DECIMAL.3, 2.>'.*");
    expr("^x'a4'=1^")
        .fails("(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <INTEGER>'.*");
    expr("^x'13'<>0.01^")
        .fails("(?s).*Cannot apply '<>' to arguments of type "
            + "'<BINARY.1.> <> <DECIMAL.3, 2.>'.*");
    expr("^x'abcd'<>1^")
        .fails("(?s).*Cannot apply '<>' to arguments of type "
            + "'<BINARY.2.> <> <INTEGER>'.*");
  }

  @Test void testBinaryString() {
    sql("select x'face'=X'' from (values(true))").ok();
    sql("select x'ff'=X'' from (values(true))").ok();
  }

  @Test void testBinaryStringFails() {
    expr("select x'ffee'='abc' from (values(true))")
        .columnType("BOOLEAN");
    sql("select ^x'ffee'='abc'^ from (values(true))")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply '=' to arguments of type "
            + "'<BINARY.2.> = <CHAR.3.>'.*");
    sql("select ^x'ff'=88^ from (values(true))")
        .fails("(?s).*Cannot apply '=' to arguments of type "
            + "'<BINARY.1.> = <INTEGER>'.*");
    sql("select ^x''<>1.1e-1^ from (values(true))")
        .fails("(?s).*Cannot apply '<>' to arguments of type "
            + "'<BINARY.0.> <> <DOUBLE>'.*");
    sql("select ^x''<>1.1^ from (values(true))")
        .fails("(?s).*Cannot apply '<>' to arguments of type "
            + "'<BINARY.0.> <> <DECIMAL.2, 1.>'.*");
  }

  @Test void testStringLiteral() {
    sql("select n''=_iso-8859-1'abc' from (values(true))").ok();
    sql("select N'f'<>'''' from (values(true))").ok();
  }

  @Test void testStringLiteralBroken() {
    sql("select 'foo'\n"
        + "'bar' from (values(true))").ok();
    sql("select 'foo'\r'bar' from (values(true))").ok();
    sql("select 'foo'\n\r'bar' from (values(true))").ok();
    sql("select 'foo'\r\n'bar' from (values(true))").ok();
    sql("select 'foo'\n'bar' from (values(true))").ok();
    sql("select 'foo' /* comment */ ^'bar'^ from (values(true))")
        .fails("String literal continued on same line");
    sql("select 'foo' -- comment\r from (values(true))").ok();
    sql("select 'foo' ^'bar'^ from (values(true))")
        .fails("String literal continued on same line");
  }

  @Test void testArithmeticOperators() {
    expr("power(2,3)").ok();
    expr("aBs(-2.3e-2)").ok();
    expr("MOD(5             ,\t\f\r\n2)").ok();
    expr("ln(5.43  )").ok();
    expr("log10(- -.2  )").ok();

    expr("mod(5.1, 3)").ok();
    expr("mod(2,5.1)").ok();
    expr("exp(3.67)").ok();
  }

  @Test void testArithmeticOperatorsFails() {
    expr("^power(2,'abc')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER.<INTEGER>, <CHAR.3.>.*");
    expr("power(2,'abc')")
        .columnType("DOUBLE NOT NULL");
    expr("^power(true,1)^")
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER.<BOOLEAN>, <INTEGER>.*");
    expr("^mod(x'1100',1)^")
        .fails("(?s).*Cannot apply 'MOD' to arguments of type "
            + "'MOD.<BINARY.2.>, <INTEGER>.*");
    expr("^mod(1, x'1100')^")
        .fails("(?s).*Cannot apply 'MOD' to arguments of type "
            + "'MOD.<INTEGER>, <BINARY.2.>.*");
    expr("^abs(x'')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'ABS' to arguments of type 'ABS.<BINARY.0.>.*");
    expr("^ln(x'face12')^")
        .fails("(?s).*Cannot apply 'LN' to arguments of type 'LN.<BINARY.3.>.*");
    expr("^log10(x'fa')^")
        .fails("(?s).*Cannot apply 'LOG10' to arguments of type 'LOG10.<BINARY.1.>.*");
    expr("^exp('abc')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'EXP' to arguments of type 'EXP.<CHAR.3.>.*");
    expr("exp('abc')")
        .columnType("DOUBLE NOT NULL");
  }

  @Test void testCaseExpression() {
    expr("case 1 when 1 then 'one' end").ok();
    expr("case 1 when 1 then 'one' else null end").ok();
    expr("case 1 when 1 then 'one' else 'more' end").ok();
    expr("case 1 when 1 then 'one' when 2 then null else 'more' end").ok();
    expr("case when TRUE then 'true' else 'false' end").ok();
    sql("values case when TRUE then 'true' else 'false' end").ok();
    expr("CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN null END").ok();
    expr("CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(null as "
        + "integer) END").ok();
    expr("CASE 1 WHEN 1 THEN null WHEN 2 THEN cast(null as integer) END").ok();
    expr("CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(cast(null"
        + " as tinyint) as integer) END").ok();
  }

  @Test void testCaseExpressionTypes() {
    expr("case 1 when 1 then 'one' else 'not one' end")
        .columnType("CHAR(7) NOT NULL");
    expr("case when 2<1 then 'impossible' end")
        .columnType("CHAR(10)");
    expr("case 'one' when 'two' then 2.00 when 'one' then 1.3 else 3.2 end")
        .columnType("DECIMAL(3, 2) NOT NULL");
    expr("case 'one' when 'two' then 2 when 'one' then 1.00 else 3 end")
        .columnType("DECIMAL(12, 2) NOT NULL");
    expr("case 1 when 1 then 'one' when 2 then null else 'more' end")
        .columnType("CHAR(4)");
    expr("case when TRUE then 'true' else 'false' end")
        .columnType("CHAR(5) NOT NULL");
    expr("CASE 1 WHEN 1 THEN cast(null as integer) END")
        .columnType("INTEGER");
    expr("CASE 1\n"
        + "WHEN 1 THEN NULL\n"
        + "WHEN 2 THEN cast(cast(null as tinyint) as integer) END")
        .columnType("INTEGER");
    expr("CASE 1\n"
        + "WHEN 1 THEN cast(null as integer)\n"
        + "WHEN 2 THEN cast(null as integer) END")
        .columnType("INTEGER");
    expr("CASE 1\n"
        + "WHEN 1 THEN cast(null as integer)\n"
        + "WHEN 2 THEN cast(cast(null as tinyint) as integer)\n"
        + "END")
        .columnType("INTEGER");
    expr("CASE 1\n"
        + "WHEN 1 THEN INTERVAL '12 3:4:5.6' DAY TO SECOND(6)\n"
        + "WHEN 2 THEN INTERVAL '12 3:4:5.6' DAY TO SECOND(9)\n"
        + "END")
        .columnType("INTERVAL DAY TO SECOND(9)");

    sql("select\n"
        + "CASE WHEN job is not null THEN mgr\n"
        + "ELSE 5 end as mgr\n"
        + "from EMP")
        .columnType("INTEGER");
  }

  @Test void testCaseExpressionFails() {
    // varchar not comparable with bit string
    expr("case 'string' when x'01' then 'zero one' else 'something' end")
        .columnType("CHAR(9) NOT NULL");
    wholeExpr("case 'string' when x'01' then 'zero one' else 'something' end")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply '=' to arguments of type '<CHAR.6.> = <BINARY.1.>'.*");

    // all thens and else return null
    wholeExpr("case 1 when 1 then null else null end")
        .withTypeCoercion(false)
        .fails("(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
    expr("case 1 when 1 then null else null end")
        .columnType("NULL");

    // all thens and else return null
    wholeExpr("case 1 when 1 then null end")
        .withTypeCoercion(false)
        .fails("(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
    expr("case 1 when 1 then null end")
        .columnType("NULL");

    wholeExpr("case when true and true then 1 "
        + "when false then 2 "
        + "when false then true " + "else "
        + "case when true then 3 end end")
        .fails("Illegal mixing of types in CASE or COALESCE statement");
  }

  @Test void testNullIf() {
    expr("nullif(1,2)").ok();
    expr("nullif(1,2)")
        .columnType("INTEGER");
    expr("nullif('a','b')")
        .columnType("CHAR(1)");
    expr("nullif(345.21, 2)")
        .columnType("DECIMAL(5, 2)");
    expr("nullif(345.21, 2e0)")
        .columnType("DECIMAL(5, 2)");
    wholeExpr("nullif(1,2,3)")
        .fails("Invalid number of arguments to function 'NULLIF'. Was "
            + "expecting 2 arguments");
  }

  @Test void testCoalesce() {
    expr("coalesce('a','b')").ok();
    expr("coalesce('a','b','c')")
        .columnType("CHAR(1) NOT NULL");

    sql("select COALESCE(mgr, 12) as m from EMP")
        .columnType("INTEGER NOT NULL");
  }

  @Test void testCoalesceFails() {
    wholeExpr("coalesce('a',1)")
        .withTypeCoercion(false)
        .fails("Illegal mixing of types in CASE or COALESCE statement");
    expr("coalesce('a',1)")
        .columnType("VARCHAR NOT NULL");
    wholeExpr("coalesce('a','b',1)")
        .withTypeCoercion(false)
        .fails("Illegal mixing of types in CASE or COALESCE statement");
    expr("coalesce('a','b',1)")
        .columnType("VARCHAR NOT NULL");
  }

  @Test void testStringCompare() {
    expr("'a' = 'b'").ok();
    expr("'a' <> 'b'").ok();
    expr("'a' > 'b'").ok();
    expr("'a' < 'b'").ok();
    expr("'a' >= 'b'").ok();
    expr("'a' <= 'b'").ok();

    expr("cast('' as varchar(1))>cast('' as char(1))").ok();
    expr("cast('' as varchar(1))<cast('' as char(1))").ok();
    expr("cast('' as varchar(1))>=cast('' as char(1))").ok();
    expr("cast('' as varchar(1))<=cast('' as char(1))").ok();
    expr("cast('' as varchar(1))=cast('' as char(1))").ok();
    expr("cast('' as varchar(1))<>cast('' as char(1))").ok();
  }

  @Test void testStringCompareType() {
    expr("'a' = 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("'a' <> 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("'a' > 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("'a' < 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("'a' >= 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("'a' <= 'b'")
        .columnType("BOOLEAN NOT NULL");
    expr("CAST(NULL AS VARCHAR(33)) > 'foo'")
        .columnType("BOOLEAN");
  }

  @Test void testConcat() {
    expr("'a'||'b'").ok();
    expr("x'12'||x'34'").ok();
    expr("'a'||'b'")
        .columnType("CHAR(2) NOT NULL");
    expr("cast('a' as char(1))||cast('b' as char(2))")
        .columnType("CHAR(3) NOT NULL");
    expr("cast(null as char(1))||cast('b' as char(2))")
        .columnType("CHAR(3)");
    expr("'a'||'b'||'c'")
        .columnType("CHAR(3) NOT NULL");
    expr("'a'||'b'||'cde'||'f'")
        .columnType("CHAR(6) NOT NULL");
    expr("'a'||'b'||cast('cde' as VARCHAR(3))|| 'f'")
        .columnType("VARCHAR(6) NOT NULL");
    expr("_UTF16'a'||_UTF16'b'||_UTF16'c'").ok();
  }

  @Test void testConcatWithCharset() {
    sql("_UTF16'a'||_UTF16'b'||_UTF16'c'")
        .charset(Charset.forName("UTF-16LE"));
  }

  @Test void testConcatFails() {
    wholeExpr("'a'||x'ff'")
        .fails("(?s).*Cannot apply '\\|\\|' to arguments of type "
            + "'<CHAR.1.> \\|\\| <BINARY.1.>'.*Supported form.s.: "
            + "'<STRING> \\|\\| <STRING>.*'");
  }

  /** Tests the CONCAT function, which unlike the concat operator ('||') is not
   * standard but only in the ORACLE and POSTGRESQL libraries. */
  @Test void testConcatFunction() {
    // CONCAT is not in the library operator table
    final Sql s = sql("?")
        .withOperatorTable(operatorTableFor(SqlLibrary.POSTGRESQL));
    s.expr("concat('a', 'b')").ok();
    s.expr("concat(x'12', x'34')").ok();
    s.expr("concat(_UTF16'a', _UTF16'b', _UTF16'c')").ok();
    s.expr("concat('aabbcc', 'ab', '+-')")
        .columnType("VARCHAR(10) NOT NULL");
    s.expr("concat('aabbcc', CAST(NULL AS VARCHAR(20)), '+-')")
        .columnType("VARCHAR(28)");
    s.expr("concat('aabbcc', 2)")
        .withWhole(true)
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'CONCAT' to arguments of type "
            + "'CONCAT\\(<CHAR\\(6\\)>, <INTEGER>\\)'\\. .*");
    s.expr("concat('aabbcc', 2)").ok();
    s.expr("concat('abc', 'ab', 123)")
        .withWhole(true)
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'CONCAT' to arguments of type "
            + "'CONCAT\\(<CHAR\\(3\\)>, <CHAR\\(2\\)>, <INTEGER>\\)'\\. .*");
    s.expr("concat('abc', 'ab', 123)").ok();
    s.expr("concat(true, false)")
        .withWhole(true)
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'CONCAT' to arguments of type "
            + "'CONCAT\\(<BOOLEAN>, <BOOLEAN>\\)'\\. .*");
    s.expr("concat(true, false)").ok();
    s.expr("concat(DATE '2020-04-17', TIMESTAMP '2020-04-17 14:17:51')")
        .withWhole(true)
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'CONCAT' to arguments of type "
            + "'CONCAT\\(<DATE>, <TIMESTAMP\\(0\\)>\\)'\\. .*");
    s.expr("concat(DATE '2020-04-17', TIMESTAMP '2020-04-17 14:17:51')").ok();
  }

  @Test void testBetween() {
    expr("1 between 2 and 3").ok();
    expr("'a' between 'b' and 'c'").ok();
    // can implicitly convert CHAR to INTEGER
    expr("'' between 2 and 3").ok();
    wholeExpr("date '2012-02-03' between 2 and 3")
        .fails("(?s).*Cannot apply 'BETWEEN ASYMMETRIC' to arguments of type.*");
  }

  @Test void testCharsetMismatch() {
    wholeExpr("''=_UTF16''")
        .fails("Cannot apply .* to the two different charsets ISO-8859-1 and "
            + "UTF-16LE");
    wholeExpr("''<>_UTF16''")
        .fails("(?s).*Cannot apply .* to the two different charsets.*");
    wholeExpr("''>_UTF16''")
        .fails("(?s).*Cannot apply .* to the two different charsets.*");
    wholeExpr("''<_UTF16''")
        .fails("(?s).*Cannot apply .* to the two different charsets.*");
    wholeExpr("''<=_UTF16''")
        .fails("(?s).*Cannot apply .* to the two different charsets.*");
    wholeExpr("''>=_UTF16''")
        .fails("(?s).*Cannot apply .* to the two different charsets.*");
    wholeExpr("''||_UTF16''")
        .fails(ANY);
    wholeExpr("'a'||'b'||_UTF16'c'")
        .fails(ANY);
  }

  // FIXME jvs 2-Feb-2005: all collation-related tests are disabled due to
  // dtbug 280

  public void _testSimpleCollate() {
    expr("'s' collate latin1$en$1").ok();
    expr("'s' collate latin1$en$1")
        .columnType("CHAR(1)");
    sql("'s'")
        .collation("ISO-8859-1$en_US$primary", SqlCollation.Coercibility.COERCIBLE);
    sql("'s' collate latin1$sv$3")
        .collation("ISO-8859-1$sv$3", SqlCollation.Coercibility.EXPLICIT);
  }

  public void _testCharsetAndCollateMismatch() {
    // todo
    expr("_UTF16's' collate latin1$en$1")
        .fails("?");
  }

  public void _testDyadicCollateCompare() {
    expr("'s' collate latin1$en$1 < 't'").ok();
    expr("'t' > 's' collate latin1$en$1").ok();
    expr("'s' collate latin1$en$1 <> 't' collate latin1$en$1").ok();
  }

  public void _testDyadicCompareCollateFails() {
    // two different explicit collations. difference in strength
    expr("'s' collate latin1$en$1 <= 't' collate latin1$en$2")
        .fails("(?s).*Two explicit different collations.*are illegal.*");

    // two different explicit collations. difference in language
    expr("'s' collate latin1$sv$1 >= 't' collate latin1$en$1")
        .fails("(?s).*Two explicit different collations.*are illegal.*");
  }

  public void _testDyadicCollateOperator() {
    sql("'a' || 'b'")
        .collation("ISO-8859-1$en_US$primary", SqlCollation.Coercibility.COERCIBLE);
    sql("'a' collate latin1$sv$3 || 'b'")
        .collation("ISO-8859-1$sv$3", SqlCollation.Coercibility.EXPLICIT);
    sql("'a' collate latin1$sv$3 || 'b' collate latin1$sv$3")
        .collation("ISO-8859-1$sv$3", SqlCollation.Coercibility.EXPLICIT);
  }

  @Test void testCharLength() {
    expr("char_length('string')").ok();
    expr("char_length(_UTF16'string')").ok();
    expr("character_length('string')").ok();
    expr("char_length('string')")
        .columnType("INTEGER NOT NULL");
    expr("character_length('string')")
        .columnType("INTEGER NOT NULL");
  }

  @Test void testUpperLower() {
    expr("upper(_UTF16'sadf')").ok();
    expr("lower(n'sadf')").ok();
    expr("lower('sadf')")
        .columnType("CHAR(4) NOT NULL");
    wholeExpr("upper(123)")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    expr("upper(123)")
        .columnType("VARCHAR NOT NULL");
  }

  @Test void testPosition() {
    expr("position('mouse' in 'house')").ok();
    expr("position(x'11' in x'100110')").ok();
    expr("position(x'11' in x'100110' FROM 10)").ok();
    expr("position(x'abcd' in x'')").ok();
    expr("position('mouse' in 'house')")
        .columnType("INTEGER NOT NULL");
    wholeExpr("position(x'1234' in '110')")
        .fails("Parameters must be of the same type");
    wholeExpr("position(x'1234' in '110' from 3)")
        .fails("Parameters must be of the same type");
  }

  @Test void testTrim() {
    expr("trim('mustache' FROM 'beard')").ok();
    expr("trim(both 'mustache' FROM 'beard')").ok();
    expr("trim(leading 'mustache' FROM 'beard')").ok();
    expr("trim(trailing 'mustache' FROM 'beard')").ok();
    expr("trim('mustache' FROM 'beard')")
        .columnType("VARCHAR(5) NOT NULL");
    expr("trim('beard  ')")
        .columnType("VARCHAR(7) NOT NULL");
    expr("trim('mustache' FROM cast(null as varchar(4)))")
        .columnType("VARCHAR(4)");

    if (TODO) {
      final SqlCollation.Coercibility expectedCoercibility = null;
      sql("trim('mustache' FROM 'beard')")
          .collation("CHAR(5)", expectedCoercibility);
    }
  }

  @Test void testTrimFails() {
    wholeExpr("trim(123 FROM 'beard')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'TRIM' to arguments of type.*");
    expr("trim(123 FROM 'beard')")
        .columnType("VARCHAR(5) NOT NULL");
    wholeExpr("trim('a' FROM 123)")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'TRIM' to arguments of type.*");
    expr("trim('a' FROM 123)")
        .columnType("VARCHAR NOT NULL");
    wholeExpr("trim('a' FROM _UTF16'b')")
        .fails("(?s).*not comparable to each other.*");
  }

  public void _testConvertAndTranslate() {
    expr("convert('abc' using conversion)").ok();
    expr("translate('abc' using translation)").ok();
  }

  @Test void testTranslate3() {
    // TRANSLATE3 is not in the standard operator table
    wholeExpr("translate('aabbcc', 'ab', '+-')")
        .fails("No match found for function signature "
            + "TRANSLATE3\\(<CHARACTER>, <CHARACTER>, <CHARACTER>\\)");

    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.ORACLE);

    expr("translate('aabbcc', 'ab', '+-')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR(6) NOT NULL");
    wholeExpr("translate('abc', 'ab')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TRANSLATE3'. "
            + "Was expecting 3 arguments");
    wholeExpr("translate('abc', 'ab', 123)")
        .withOperatorTable(opTable)
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'TRANSLATE3' to arguments of type "
            + "'TRANSLATE3\\(<CHAR\\(3\\)>, <CHAR\\(2\\)>, <INTEGER>\\)'\\. .*");
    expr("translate('abc', 'ab', 123)")
        .withOperatorTable(opTable)
        .columnType("VARCHAR(3) NOT NULL");
    wholeExpr("translate('abc', 'ab', '+-', 'four')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TRANSLATE3'. "
            + "Was expecting 3 arguments");
  }

  @Test void testOverlay() {
    expr("overlay('ABCdef' placing 'abc' from 1)").ok();
    expr("overlay('ABCdef' placing 'abc' from 1 for 3)").ok();
    wholeExpr("overlay('ABCdef' placing 'abc' from '1' for 3)")
        .withTypeCoercion(false)
        .fails("(?s).*OVERLAY\\(<STRING> PLACING <STRING> FROM <INTEGER>\\).*");
    expr("overlay('ABCdef' placing 'abc' from '1' for 3)")
        .columnType("VARCHAR(9) NOT NULL");
    expr("overlay('ABCdef' placing 'abc' from 1 for 3)")
        .columnType("VARCHAR(9) NOT NULL");
    expr("overlay('ABCdef' placing 'abc' from 6 for 3)")
        .columnType("VARCHAR(9) NOT NULL");
    expr("overlay('ABCdef' placing cast(null as char(5)) from 1)")
        .columnType("VARCHAR(11)");

    if (TODO) {
      sql("overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)")
          .collation("ISO-8859-1$sv", SqlCollation.Coercibility.EXPLICIT);
    }
  }

  @Test void testSubstring() {
    expr("substring('a' FROM 1)").ok();
    expr("substring('a' FROM 1 FOR 3)").ok();
    expr("substring('a' FROM 'reg' FOR '\\')").ok();
    // binary string
    expr("substring(x'ff' FROM 1  FOR 2)").ok();

    expr("substring('10' FROM 1  FOR 2)")
        .columnType("VARCHAR(2) NOT NULL");
    expr("substring('1000' FROM 2)")
        .columnType("VARCHAR(4) NOT NULL");
    expr("substring('1000' FROM '1'  FOR 'w')")
        .columnType("VARCHAR(4) NOT NULL");
    expr("substring(cast(' 100 ' as CHAR(99)) FROM '1'  FOR 'w')")
        .columnType("VARCHAR(99) NOT NULL");
    expr("substring(x'10456b' FROM 1  FOR 2)")
        .columnType("VARBINARY(3) NOT NULL");

    sql("substring('10' FROM 1  FOR 2)")
        .charset(Charset.forName("latin1"));
    sql("substring(_UTF16'10' FROM 1  FOR 2)")
        .charset(Charset.forName("UTF-16LE"));
    expr("substring('a', 1)").ok();
    expr("substring('a', 1, 3)").ok();
    // Implicit type coercion.
    expr("substring(12345, '1')")
        .columnType("VARCHAR NOT NULL");
    expr("substring('a', '1')")
        .columnType("VARCHAR(1) NOT NULL");
    expr("substring('a', 1, '3')")
        .columnType("VARCHAR(1) NOT NULL");
  }

  @Test void testSubstringFails() {
    wholeExpr("substring('a' from 1 for 'b')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'SUBSTRING' to arguments of type.*");
    expr("substring('a' from 1 for 'b')")
        .columnType("VARCHAR(1) NOT NULL");
    wholeExpr("substring(_UTF16'10' FROM '0' FOR '\\')")
        .fails("(?s).* not comparable to each other.*");
    wholeExpr("substring('10' FROM _UTF16'0' FOR '\\')")
        .fails("(?s).* not comparable to each other.*");
    wholeExpr("substring('10' FROM '0' FOR _UTF16'\\')")
        .fails("(?s).* not comparable to each other.*");
  }

  @Test void testLikeAndSimilar() {
    expr("'a' like 'b'").ok();
    expr("'a' like 'b'").ok();
    expr("'a' similar to 'b'").ok();
    expr("'a' similar to 'b' escape 'c'").ok();
  }

  @Test void testIlike() {
    final Sql s = sql("?")
        .withOperatorTable(operatorTableFor(SqlLibrary.POSTGRESQL));
    s.expr("'a' ilike 'b'").columnType("BOOLEAN NOT NULL");
    s.expr("'a' ilike cast(null as varchar(99))").columnType("BOOLEAN");
    s.expr("cast(null as varchar(99)) not ilike 'b'").columnType("BOOLEAN");
    s.expr("'a' not ilike 'b' || 'c'").columnType("BOOLEAN NOT NULL");

    // ILIKE is only available in the PostgreSQL function library
    expr("^'a' ilike 'b'^")
        .fails("No match found for function signature ILIKE");
  }

  @Test void testRlike() {
    // RLIKE is supported for SPARK
    final Sql s = sql("?")
        .withOperatorTable(operatorTableFor(SqlLibrary.SPARK));
    s.expr("'first_name' rlike '%Ted%'").columnType("BOOLEAN NOT NULL");
    s.expr("'first_name' rlike '^M+'").columnType("BOOLEAN NOT NULL");

    // RLIKE is only supported for Spark and Hive
    String noMatch = "(?s).*No match found for function signature RLIKE";
    expr("^'b' rlike '.+@.+\\\\..+'^")
        .fails(noMatch)
        .withOperatorTable(operatorTableFor(SqlLibrary.POSTGRESQL))
        .fails(noMatch)
        .withOperatorTable(operatorTableFor(SqlLibrary.SPARK))
        .columnType("BOOLEAN NOT NULL")
        .withOperatorTable(operatorTableFor(SqlLibrary.HIVE))
        .columnType("BOOLEAN NOT NULL");
  }

  public void _testLikeAndSimilarFails() {
    expr("'a' like _UTF16'b'  escape 'c'")
        .fails("(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary,"
            + " _SHIFT_JIS.b..*");
    expr("'a' similar to _UTF16'b'  escape 'c'")
        .fails("(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary,"
            + " _SHIFT_JIS.b..*");

    expr("'a' similar to 'b' collate UTF16$jp  escape 'c'")
        .fails("(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary,"
            + " _ISO-8859-1.b. COLLATE SHIFT_JIS.jp.primary.*");
  }

  @Test void testNull() {
    expr("nullif(null, 1)").ok();
    expr("values 1.0 + ^NULL^").ok();
    expr("1.0 + ^NULL^").ok();
    expr("case when 1 > 0 then null else 0 end").ok();
    expr("1 > 0 and null").ok();
    expr("position(null in 'abc' from 1)").ok();
    expr("substring(null from 1)").ok();
    expr("trim(null from 'ab')").ok();
    expr("trim(null from null)").ok();
    expr("null || 'a'").ok();
    expr("not(null)").ok();
    expr("+null").ok();
    expr("-null").ok();
    expr("upper(null)").ok();
    expr("lower(null)").ok();
    expr("initcap(null)").ok();
    expr("mod(null, 2) + 1").ok();
    expr("abs(null)").ok();
    expr("round(null,1)").ok();
    expr("sign(null) + 1").ok();
    expr("truncate(null,1) + 1").ok();

    sql("select null as a from emp").ok();
    sql("select avg(null) from emp").ok();
    sql("select bit_and(null) from emp").ok();
    sql("select bit_or(null) from emp").ok();

    expr("substring(null from 1) + 1").ok();
    expr("substring(^NULL^ from 1)")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");

    expr("values 1.0 + ^NULL^")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
    expr("values 1.0 + NULL")
        .columnType("DECIMAL(2, 1)");
    expr("1.0 + ^NULL^")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
    expr("1.0 + NULL")
        .columnType("DECIMAL(2, 1)");

    // FIXME: SQL:2003 does not allow raw NULL in IN clause
    expr("1 in (1, null, 2)").ok();
    expr("1 in (null, 1, null, 2)").ok();
    expr("1 in (cast(null as integer), null)").ok();
    expr("1 in (null, null)").ok();
  }

  @Test void testNullCast() {
    expr("cast(null as tinyint)")
        .columnType("TINYINT");
    expr("cast(null as smallint)")
        .columnType("SMALLINT");
    expr("cast(null as integer)")
        .columnType("INTEGER");
    expr("cast(null as bigint)")
        .columnType("BIGINT");
    expr("cast(null as float)")
        .columnType("FLOAT");
    expr("cast(null as real)")
        .columnType("REAL");
    expr("cast(null as double)")
        .columnType("DOUBLE");
    expr("cast(null as boolean)")
        .columnType("BOOLEAN");
    expr("cast(null as varchar(1))")
        .columnType("VARCHAR(1)");
    expr("cast(null as char(1))")
        .columnType("CHAR(1)");
    expr("cast(null as binary(1))")
        .columnType("BINARY(1)");
    expr("cast(null as date)")
        .columnType("DATE");
    expr("cast(null as time)")
        .columnType("TIME(0)");
    expr("cast(null as timestamp)")
        .columnType("TIMESTAMP(0)");
    expr("cast(null as decimal)")
        .columnType("DECIMAL(19, 0)");
    expr("cast(null as varbinary(1))")
        .columnType("VARBINARY(1)");

    expr("cast(null as integer), cast(null as char(1))").ok();
  }

  @Test void testCastTypeToType() {
    expr("cast(123 as char)")
        .columnType("CHAR(1) NOT NULL");
    expr("cast(123 as varchar)")
        .columnType("VARCHAR NOT NULL");
    expr("cast(x'1234' as binary)")
        .columnType("BINARY(1) NOT NULL");
    expr("cast(x'1234' as varbinary)")
        .columnType("VARBINARY NOT NULL");
    expr("cast(123 as varchar(3))")
        .columnType("VARCHAR(3) NOT NULL");
    expr("cast(123 as char(3))")
        .columnType("CHAR(3) NOT NULL");
    expr("cast('123' as integer)")
        .columnType("INTEGER NOT NULL");
    expr("cast('123' as double)")
        .columnType("DOUBLE NOT NULL");
    expr("cast('1.0' as real)")
        .columnType("REAL NOT NULL");
    expr("cast(1.0 as tinyint)")
        .columnType("TINYINT NOT NULL");
    expr("cast(1 as tinyint)")
        .columnType("TINYINT NOT NULL");
    expr("cast(1.0 as smallint)")
        .columnType("SMALLINT NOT NULL");
    expr("cast(1 as integer)")
        .columnType("INTEGER NOT NULL");
    expr("cast(1.0 as integer)")
        .columnType("INTEGER NOT NULL");
    expr("cast(1.0 as bigint)")
        .columnType("BIGINT NOT NULL");
    expr("cast(1 as bigint)")
        .columnType("BIGINT NOT NULL");
    expr("cast(1.0 as float)")
        .columnType("FLOAT NOT NULL");
    expr("cast(1 as float)")
        .columnType("FLOAT NOT NULL");
    expr("cast(1.0 as real)")
        .columnType("REAL NOT NULL");
    expr("cast(1 as real)")
        .columnType("REAL NOT NULL");
    expr("cast(1.0 as double)")
        .columnType("DOUBLE NOT NULL");
    expr("cast(1 as double)")
        .columnType("DOUBLE NOT NULL");
    expr("cast(123 as decimal(6,4))")
        .columnType("DECIMAL(6, 4) NOT NULL");
    expr("cast(123 as decimal(6))")
        .columnType("DECIMAL(6, 0) NOT NULL");
    expr("cast(123 as decimal)")
        .columnType("DECIMAL(19, 0) NOT NULL");
    expr("cast(1.234 as decimal(2,5))")
        .columnType("DECIMAL(2, 5) NOT NULL");
    expr("cast('4.5' as decimal(3,1))")
        .columnType("DECIMAL(3, 1) NOT NULL");
    expr("cast(null as boolean)")
        .columnType("BOOLEAN");
    expr("cast('abc' as varchar(1))")
        .columnType("VARCHAR(1) NOT NULL");
    expr("cast('abc' as char(1))")
        .columnType("CHAR(1) NOT NULL");
    expr("cast(x'ff' as binary(1))")
        .columnType("BINARY(1) NOT NULL");
    expr("cast(multiset[1] as double multiset)")
        .columnType("DOUBLE NOT NULL MULTISET NOT NULL");
    expr("cast(multiset['abc'] as integer multiset)")
        .columnType("INTEGER NOT NULL MULTISET NOT NULL");
    expr("cast(1 as boolean)")
        .columnType("BOOLEAN NOT NULL");
    expr("cast(1.0e1 as boolean)")
        .columnType("BOOLEAN NOT NULL");
    expr("cast(true as numeric)")
        .columnType("DECIMAL(19, 0) NOT NULL");
    // It's a runtime error that 'TRUE' cannot fit into CHAR(3), but at
    // validate time this expression is OK.
    expr("cast(true as char(3))")
        .columnType("CHAR(3) NOT NULL");
    // test cast to time type.
    expr("cast('abc' as time)")
        .columnType("TIME(0) NOT NULL");
    expr("cast('abc' as time without time zone)")
        .columnType("TIME(0) NOT NULL");
    expr("cast('abc' as time with local time zone)")
        .columnType("TIME_WITH_LOCAL_TIME_ZONE(0) NOT NULL");
    expr("cast('abc' as time(3))")
        .columnType("TIME(3) NOT NULL");
    expr("cast('abc' as time(3) without time zone)")
        .columnType("TIME(3) NOT NULL");
    expr("cast('abc' as time(3) with local time zone)")
        .columnType("TIME_WITH_LOCAL_TIME_ZONE(3) NOT NULL");
    // test cast to timestamp type.
    expr("cast('abc' as timestamp)")
        .columnType("TIMESTAMP(0) NOT NULL");
    expr("cast('abc' as timestamp without time zone)")
        .columnType("TIMESTAMP(0) NOT NULL");
    expr("cast('abc' as timestamp with local time zone)")
        .columnType("TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL");
    expr("cast('abc' as timestamp(3))")
        .columnType("TIMESTAMP(3) NOT NULL");
    expr("cast('abc' as timestamp(3) without time zone)")
        .columnType("TIMESTAMP(3) NOT NULL");
    expr("cast('abc' as timestamp(3) with local time zone)")
        .columnType("TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL");
  }

  @Test void testCastRegisteredType() {
    expr("cast(123 as ^customBigInt^)")
        .fails("Unknown identifier 'CUSTOMBIGINT'");
    expr("cast(123 as sales.customBigInt)")
        .columnType("BIGINT NOT NULL");
    expr("cast(123 as catalog.sales.customBigInt)")
        .columnType("BIGINT NOT NULL");
  }

  @Test void testCastFails() {
    expr("cast('foo' as ^bar^)")
        .fails("Unknown identifier 'BAR'");
    wholeExpr("cast(multiset[1] as integer)")
        .fails("(?s).*Cast function cannot convert value of type "
            + "INTEGER MULTISET to type INTEGER");
    wholeExpr("cast(x'ff' as decimal(5,2))")
        .fails("(?s).*Cast function cannot convert value of type "
            + "BINARY\\(1\\) to type DECIMAL\\(5, 2\\)");
    wholeExpr("cast(DATE '1243-12-01' as TIME)")
        .fails("(?s).*Cast function cannot convert value of type "
            + "DATE to type TIME.*");
    wholeExpr("cast(TIME '12:34:01' as DATE)")
        .fails("(?s).*Cast function cannot convert value of type "
            + "TIME\\(0\\) to type DATE.*");
  }

  @Test void testCastBinaryLiteral() {
    expr("cast(^x'0dd'^ as binary(5))")
        .fails("Binary literal string must contain an even number of hexits");
  }

  /**
   * Tests whether the GEOMETRY data type is allowed.
   *
   * @see SqlConformance#allowGeometry()
   */
  @Test void testGeometry() {
    final String err =
        "Geo-spatial extensions and the GEOMETRY data type are not enabled";
    sql("select cast(null as ^geometry^) as g from emp")
        .withConformance(SqlConformanceEnum.STRICT_2003).fails(err)
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testDateTime() {
    // LOCAL_TIME
    expr("LOCALTIME(3)").ok();
    expr("LOCALTIME").ok(); // fix sqlcontext later.
    wholeExpr("LOCALTIME(1+2)")
        .fails("Argument to function 'LOCALTIME' must be a literal");
    wholeExpr("LOCALTIME(NULL)")
        .withTypeCoercion(false)
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME(NULL)")
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME(CAST(NULL AS INTEGER))")
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME()")
        .fails("No match found for function signature LOCALTIME..");
    //  with TZ?
    expr("LOCALTIME")
        .columnType("TIME(0) NOT NULL");
    wholeExpr("LOCALTIME(-1)")
        .fails("Argument to function 'LOCALTIME' must be a positive integer literal");
    expr("^LOCALTIME(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("LOCALTIME(4)")
        .fails("Argument to function 'LOCALTIME' must be a valid precision "
            + "between '0' and '3'");
    wholeExpr("LOCALTIME('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("LOCALTIME('foo')")
        .fails("Argument to function 'LOCALTIME' must be a literal");

    // LOCALTIMESTAMP
    expr("LOCALTIMESTAMP(3)").ok();
    //    fix sqlcontext later.
    expr("LOCALTIMESTAMP").ok();
    wholeExpr("LOCALTIMESTAMP(1+2)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a literal");
    wholeExpr("LOCALTIMESTAMP()")
        .fails("No match found for function signature LOCALTIMESTAMP..");
    // with TZ?
    expr("LOCALTIMESTAMP")
        .columnType("TIMESTAMP(0) NOT NULL");
    wholeExpr("LOCALTIMESTAMP(-1)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a positive "
            + "integer literal");
    expr("^LOCALTIMESTAMP(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("LOCALTIMESTAMP(4)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a valid "
            + "precision between '0' and '3'");
    wholeExpr("LOCALTIMESTAMP('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("LOCALTIMESTAMP('foo')")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a literal");

    // CURRENT_DATE
    wholeExpr("CURRENT_DATE(3)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    //    fix sqlcontext later.
    expr("CURRENT_DATE").ok();
    wholeExpr("CURRENT_DATE(1+2)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    wholeExpr("CURRENT_DATE()")
        .fails("No match found for function signature CURRENT_DATE..");
    //  with TZ?
    expr("CURRENT_DATE")
        .columnType("DATE NOT NULL");
    // I guess -s1 is an expression?
    wholeExpr("CURRENT_DATE(-1)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    wholeExpr("CURRENT_DATE('foo')")
        .fails(ANY);

    // current_time
    expr("current_time(3)").ok();
    //    fix sqlcontext later.
    expr("current_time").ok();
    wholeExpr("current_time(1+2)")
        .fails("Argument to function 'CURRENT_TIME' must be a literal");
    wholeExpr("current_time()")
        .fails("No match found for function signature CURRENT_TIME..");
    // with TZ?
    expr("current_time")
        .columnType("TIME(0) NOT NULL");
    wholeExpr("current_time(-1)")
        .fails("Argument to function 'CURRENT_TIME' must be a positive integer literal");
    expr("^CURRENT_TIME(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("CURRENT_TIME(4)")
        .fails("Argument to function 'CURRENT_TIME' must be a valid precision "
            + "between '0' and '3'");
    wholeExpr("current_time('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("current_time('foo')")
        .fails("Argument to function 'CURRENT_TIME' must be a literal");

    // current_timestamp
    expr("CURRENT_TIMESTAMP(3)").ok();
    //    fix sqlcontext later.
    expr("CURRENT_TIMESTAMP").ok();
    sql("SELECT CURRENT_TIMESTAMP AS X FROM (VALUES (1))").ok();
    wholeExpr("CURRENT_TIMESTAMP(1+2)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a literal");
    wholeExpr("CURRENT_TIMESTAMP()")
        .fails("No match found for function signature CURRENT_TIMESTAMP..");
    // should type be 'TIMESTAMP with TZ'?
    expr("CURRENT_TIMESTAMP")
        .columnType("TIMESTAMP(0) NOT NULL");
    // should type be 'TIMESTAMP with TZ'?
    expr("CURRENT_TIMESTAMP(2)")
        .columnType("TIMESTAMP(2) NOT NULL");
    wholeExpr("CURRENT_TIMESTAMP(-1)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a positive "
            + "integer literal");
    expr("^CURRENT_TIMESTAMP(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("CURRENT_TIMESTAMP(4)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a valid "
            + "precision between '0' and '3'");
    wholeExpr("CURRENT_TIMESTAMP('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("CURRENT_TIMESTAMP('foo')")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a literal");

    // Date literals
    expr("DATE '2004-12-01'").ok();
    expr("TIME '12:01:01'").ok();
    expr("TIME '11:59:59.99'").ok();
    expr("TIME '12:01:01.001'").ok();
    expr("TIMESTAMP '2004-12-01 12:01:01'").ok();
    expr("TIMESTAMP '2004-12-01 12:01:01.001'").ok();

    // REVIEW: Can't think of any date/time/ts literals that will parse,
    // but not validate.
  }

  /**
   * Tests casting to/from date/time types.
   */
  @Test void testDateTimeCast() {
    wholeExpr("CAST(1 as DATE)")
        .fails("Cast function cannot convert value of type INTEGER to type DATE");
    expr("CAST(DATE '2001-12-21' AS VARCHAR(10))").ok();
    expr("CAST( '2001-12-21' AS DATE)").ok();
    expr("CAST( TIMESTAMP '2001-12-21 10:12:21' AS VARCHAR(20))").ok();
    expr("CAST( TIME '10:12:21' AS VARCHAR(20))").ok();
    expr("CAST( '10:12:21' AS TIME)").ok();
    expr("CAST( '2004-12-21 10:12:21' AS TIMESTAMP)").ok();
  }

  @Test void testConvertTimezoneFunction() {
    wholeExpr("CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',"
        + " CAST('2000-01-01' AS TIMESTAMP))")
        .fails("No match found for function signature "
            + "CONVERT_TIMEZONE\\(<CHARACTER>, <CHARACTER>, <TIMESTAMP>\\)");

    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.POSTGRESQL);
    expr("CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',\n"
        + "  CAST('2000-01-01' AS TIMESTAMP))")
        .withOperatorTable(opTable)
        .columnType("DATE NOT NULL");
    wholeExpr("CONVERT_TIMEZONE('UTC', 'America/Los_Angeles')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'CONVERT_TIMEZONE'. "
            + "Was expecting 3 arguments");
    wholeExpr("CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', '2000-01-01')")
        .withOperatorTable(opTable)
        .fails("Cannot apply 'CONVERT_TIMEZONE' to arguments of type "
            + "'CONVERT_TIMEZONE\\(<CHAR\\(3\\)>, <CHAR\\(19\\)>, "
            + "<CHAR\\(10\\)>\\)'\\. Supported form\\(s\\): "
            + "'CONVERT_TIMEZONE\\(<CHARACTER>, <CHARACTER>, <DATETIME>\\)'");
    wholeExpr("CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', "
        + "'UTC', CAST('2000-01-01' AS TIMESTAMP))")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'CONVERT_TIMEZONE'. "
            + "Was expecting 3 arguments");
  }

  @Test void testToDateFunction() {
    wholeExpr("TO_DATE('2000-01-01', 'YYYY-MM-DD')")
        .fails("No match found for function signature "
            + "TO_DATE\\(<CHARACTER>, <CHARACTER>\\)");

    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.POSTGRESQL);
    expr("TO_DATE('2000-01-01', 'YYYY-MM-DD')")
        .withOperatorTable(opTable)
        .columnType("DATE NOT NULL");
    wholeExpr("TO_DATE('2000-01-01')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TO_DATE'. "
            + "Was expecting 2 arguments");
    expr("TO_DATE(2000, 'YYYY')")
        .withOperatorTable(opTable)
        .columnType("DATE NOT NULL");
    wholeExpr("TO_DATE(2000, 'YYYY')")
        .withOperatorTable(opTable)
        .withTypeCoercion(false)
        .fails("Cannot apply 'TO_DATE' to arguments of type "
            + "'TO_DATE\\(<INTEGER>, <CHAR\\(4\\)>\\)'\\. "
            + "Supported form\\(s\\): 'TO_DATE\\(<STRING>, <STRING>\\)'");
    wholeExpr("TO_DATE('2000-01-01', 'YYYY-MM-DD', 'YYYY-MM-DD')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TO_DATE'. "
            + "Was expecting 2 arguments");
  }

  @Test void testToTimestampFunction() {
    wholeExpr("TO_TIMESTAMP('2000-01-01 01:00:00', 'YYYY-MM-DD HH:MM:SS')")
        .fails("No match found for function signature "
            + "TO_TIMESTAMP\\(<CHARACTER>, <CHARACTER>\\)");

    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.POSTGRESQL);
    expr("TO_TIMESTAMP('2000-01-01 01:00:00', 'YYYY-MM-DD HH:MM:SS')")
        .withOperatorTable(opTable)
        .columnType("DATE NOT NULL");
    wholeExpr("TO_TIMESTAMP('2000-01-01 01:00:00')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TO_TIMESTAMP'. "
            + "Was expecting 2 arguments");
    expr("TO_TIMESTAMP(2000, 'YYYY')")
        .withOperatorTable(opTable)
        .columnType("DATE NOT NULL");
    wholeExpr("TO_TIMESTAMP(2000, 'YYYY')")
        .withOperatorTable(opTable)
        .withTypeCoercion(false)
        .fails("Cannot apply 'TO_TIMESTAMP' to arguments of type "
            + "'TO_TIMESTAMP\\(<INTEGER>, <CHAR\\(4\\)>\\)'\\. "
            + "Supported form\\(s\\): 'TO_TIMESTAMP\\(<STRING>, <STRING>\\)'");
    wholeExpr("TO_TIMESTAMP('2000-01-01 01:00:00', 'YYYY-MM-DD HH:MM:SS',"
        + " 'YYYY-MM-DD')")
        .withOperatorTable(opTable)
        .fails("Invalid number of arguments to function 'TO_TIMESTAMP'. "
            + "Was expecting 2 arguments");
  }

  @Test void testCurrentDatetime() throws SqlParseException, ValidationException {
    final String currentDateTimeExpr = "select ^current_datetime^";
    Sql shouldFail = sql(currentDateTimeExpr)
        .withConformance(SqlConformanceEnum.BIG_QUERY);
    final String expectedError = "query [select CURRENT_DATETIME]; exception "
        + "[Column 'CURRENT_DATETIME' not found in any table]; class "
        + "[class org.apache.calcite.sql.validate.SqlValidatorException]; pos [line 1 col 8 thru line 1 col 8]";
    shouldFail.fails("Column 'CURRENT_DATETIME' not found in any table");

    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.BIG_QUERY);
    sql("select current_datetime()")
        .withConformance(SqlConformanceEnum.BIG_QUERY)
        .withOperatorTable(opTable).ok();
    sql("select CURRENT_DATETIME('America/Los_Angeles')")
        .withConformance(SqlConformanceEnum.BIG_QUERY)
        .withOperatorTable(opTable).ok();
    sql("select CURRENT_DATETIME(CAST(NULL AS VARCHAR(20)))")
        .withConformance(SqlConformanceEnum.BIG_QUERY)
        .withOperatorTable(opTable).ok();
  }

  @Test void testInvalidFunction() {
    wholeExpr("foo()")
        .fails("No match found for function signature FOO..");
    wholeExpr("mod(123)")
        .fails("Invalid number of arguments to function 'MOD'. "
            + "Was expecting 2 arguments");
    assumeTrue(false,
        "test case for [CALCITE-3326], disabled til it is fixed");
    sql("select foo()")
        .withTypeCoercion(false)
        .fails("No match found for function signature FOO..");
  }

  @Test void testUnknownFunctionHandling() {
    final Sql s = sql("?").withTester(t -> t.withLenientOperatorLookup(true));
    s.expr("concat('a', 2)").ok();
    s.expr("foo('2001-12-21')").ok();
    s.expr("\"foo\"('b')").ok();
    s.expr("foo()").ok();
    s.expr("'a' || foo(bar('2001-12-21'))").ok();
    s.expr("cast(foo(5, 2) as DECIMAL)").ok();
    s.expr("select ascii('xyz')").ok();
    s.expr("select get_bit(CAST('FFFF' as BINARY), 1)").ok();
    s.expr("select now()").ok();
    s.expr("^TIMESTAMP_CMP_TIMESTAMPTZ^").fails("(?s).*");
    s.expr("atan(0)").ok();
    s.expr("select row_number() over () from emp").ok();
    s.expr("select coalesce(1, 2, 3)").ok();
    s.sql("select count() from emp").ok(); // too few args
    s.sql("select sum(1, 2) from emp").ok(); // too many args
  }

  @Test void testJdbcFunctionCall() {
    expr("{fn log10(1)}").ok();
    expr("{fn locate('','')}").ok();
    expr("{fn insert('',1,2,'')}").ok();

    // 'lower' is a valid SQL function but not valid JDBC fn; the JDBC
    // equivalent is 'lcase'
    wholeExpr("{fn lower('Foo' || 'Bar')}")
        .fails("Function '\\{fn LOWER\\}' is not defined");
    expr("{fn lcase('Foo' || 'Bar')}").ok();

    expr("{fn power(2, 3)}").ok();
    wholeExpr("{fn insert('','',1,2)}")
        .withTypeCoercion(false)
        .fails("(?s).*.*");
    expr("{fn insert('','',1,2)}").ok();
    wholeExpr("{fn insert('','',1)}")
        .fails("(?s).*4.*");

    expr("{fn locate('','',1)}").ok();
    wholeExpr("{fn log10('1')}")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*fn LOG10..<CHAR.1.>.*");
    expr("{fn log10('1')}").ok();
    final String expected = "Cannot apply '\\{fn LOG10\\}' to arguments of"
        + " type '\\{fn LOG10\\}\\(<INTEGER>, <INTEGER>\\)'\\. "
        + "Supported form\\(s\\): '\\{fn LOG10\\}\\(<NUMERIC>\\)'";
    wholeExpr("{fn log10(1,1)}")
        .fails(expected);
    wholeExpr("{fn fn(1)}")
        .fails("(?s).*Function '.fn FN.' is not defined.*");
    wholeExpr("{fn hahaha(1)}")
        .fails("(?s).*Function '.fn HAHAHA.' is not defined.*");
  }

  @Test public void testQuotedFunction() {
    if (false) {
      // REVIEW jvs 2-Feb-2005:  I am disabling this test because I
      // removed the corresponding support from the parser.  Where in the
      // standard does it state that you're supposed to be able to quote
      // keywords for builtin functions?
      expr("\"CAST\"(1 as double)").ok();
      expr("\"POSITION\"('b' in 'alphabet')").ok();

      // convert and translate not yet implemented
      //        checkExp("\"CONVERT\"('b' using conversion)");
      //        checkExp("\"TRANSLATE\"('b' using translation)");
      expr("\"OVERLAY\"('a' PLAcing 'b' from 1)").ok();
      expr("\"SUBSTRING\"('a' from 1)").ok();
      expr("\"TRIM\"('b')").ok();
    } else {
      expr("\"TRIM\"('b' ^FROM^ 'a')")
          .fails("(?s).*Encountered \"FROM\" at .*");

      // Without the "FROM" noise word, TRIM is parsed as a regular
      // function without quoting and built-in function with quoting.
      expr("\"TRIM\"('b', 'FROM', 'a')")
          .columnType("VARCHAR(1) NOT NULL");
      expr("TRIM('b')")
          .columnType("VARCHAR(1) NOT NULL");
    }
  }

  /**
   * Not able to parse member function yet.
   */
  @Test void testInvalidMemberFunction() {
    expr("myCol.^func()^")
        .fails("(?s).*No match found for function signature FUNC().*");
    expr("customer.mySubschema.^memberFunc()^")
        .fails("(?s).*No match found for function signature MEMBERFUNC().*");
  }

  @Test void testRowtype() {
    sql("values (1),(2),(1)").ok();
    sql("values (1),(2),(1)")
        .type("RecordType(INTEGER NOT NULL EXPR$0) NOT NULL");
    sql("values (1,'1'),(2,'2')").ok();
    sql("values (1,'1'),(2,'2')")
        .type("RecordType(INTEGER NOT NULL EXPR$0, CHAR(1) NOT NULL EXPR$1) NOT NULL");
    sql("values true")
        .type("RecordType(BOOLEAN NOT NULL EXPR$0) NOT NULL");
    sql("^values ('1'),(2)^")
        .fails("Values passed to VALUES operator must have compatible types");
    if (TODO) {
      sql("values (1),(2.0),(3)")
          .columnType("ROWTYPE(DOUBLE)");
    }
  }

  @Test void testRow() {
    // double-nested rows can confuse validator namespace resolution
    sql("select t.r.\"EXPR$1\".\"EXPR$2\"\n"
        + "from (select ((1,2),(3,4,5)) r from dept) t")
        .columnType("INTEGER NOT NULL");
    sql("select row(emp.empno, emp.ename) from emp")
        .columnType("RecordType(INTEGER NOT NULL EXPR$0, VARCHAR(20) NOT NULL EXPR$1) NOT NULL");
    sql("select row(emp.empno + 1, emp.ename) from emp")
        .columnType("RecordType(INTEGER NOT NULL EXPR$0, VARCHAR(20) NOT NULL EXPR$1) NOT NULL");
    sql("select row((select deptno from dept where dept.deptno = emp.deptno), emp.ename)\n"
        + "from emp")
        .columnType("RecordType(INTEGER EXPR$0, VARCHAR(20) NOT NULL EXPR$1) NOT NULL");
  }

  @Test void testRowWithValidDot() {
    sql("select ((1,2),(3,4,5)).\"EXPR$1\".\"EXPR$2\"\n from dept")
        .columnType("INTEGER NOT NULL");
    sql("select row(1,2).\"EXPR$1\" from dept")
        .columnType("INTEGER NOT NULL");
    sql("select t.a.\"EXPR$1\" from (select row(1,2) as a from (values (1))) as t")
        .columnType("INTEGER NOT NULL");
  }

  @Test void testRowWithInvalidDotOperation() {
    final String sql = "select t.^s.\"EXPR$1\"^ from (\n"
        + "  select 1 AS s from (values (1))) as t";
    expr(sql)
        .fails("(?s).*Column 'S\\.EXPR\\$1' not found in table 'T'.*");
    expr("select ^array[1, 2, 3]^.\"EXPR$1\" from dept")
        .fails("(?s).*Incompatible types.*");
    expr("select ^'mystr'^.\"EXPR$1\" from dept")
        .fails("(?s).*Incompatible types.*");
  }

  @Test void testMultiset() {
    expr("multiset[1]")
        .columnType("INTEGER NOT NULL MULTISET NOT NULL");
    expr("multiset[1, CAST(null AS DOUBLE)]")
        .columnType("DOUBLE MULTISET NOT NULL");
    expr("multiset[1.3,2.3]")
        .columnType("DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
    expr("multiset[1,2.3, cast(4 as bigint)]")
        .columnType("DECIMAL(19, 0) NOT NULL MULTISET NOT NULL");
    expr("multiset['1','22', '333','22']")
        .columnType("CHAR(3) NOT NULL MULTISET NOT NULL");
    expr("^multiset[1, '2']^")
        .fails("Parameters must be of the same type");
    expr("multiset[ROW(1,2)]")
        .columnType("RecordType(INTEGER NOT NULL EXPR$0,"
            + " INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
    expr("multiset[ROW(1,2),ROW(2,5)]")
        .columnType("RecordType(INTEGER NOT NULL EXPR$0,"
            + " INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
    expr("multiset[ROW(1,2),ROW(3.4,5.4)]")
        .columnType("RecordType(DECIMAL(11, 1) NOT NULL EXPR$0,"
            + " DECIMAL(11, 1) NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
    expr("multiset(select*from emp)")
        .columnType("RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL MULTISET NOT NULL");
  }

  @Test void testMultisetSetOperators() {
    expr("multiset[1] multiset union multiset[1,2.3]").ok();
    expr("multiset[324.2] multiset union multiset[23.2,2.32]")
        .columnType("DECIMAL(5, 2) NOT NULL MULTISET NOT NULL");
    expr("multiset[1] multiset union multiset[1,2.3]")
        .columnType("DECIMAL(11, 1) NOT NULL MULTISET NOT NULL");
    expr("multiset[1] multiset union all multiset[1,2.3]").ok();
    expr("multiset[1] multiset except multiset[1,2.3]").ok();
    expr("multiset[1] multiset except all multiset[1,2.3]").ok();
    expr("multiset[1] multiset intersect multiset[1,2.3]").ok();
    expr("multiset[1] multiset intersect all multiset[1,2.3]").ok();

    expr("^multiset[1, '2']^ multiset union multiset[1]")
        .fails("Parameters must be of the same type");
    expr("multiset[ROW(1,2)] multiset intersect multiset[row(3,4)]").ok();
    if (TODO) {
      wholeExpr("multiset[ROW(1,'2')] multiset union multiset[ROW(1,2)]")
          .fails("Parameters must be of the same type");
    }
  }

  @Test void testSubMultisetOf() {
    expr("multiset[1] submultiset of multiset[1,2.3]")
        .columnType("BOOLEAN NOT NULL");
    expr("multiset[1] submultiset of multiset[1]")
        .columnType("BOOLEAN NOT NULL");

    expr("^multiset[1, '2']^ submultiset of multiset[1]")
        .fails("Parameters must be of the same type");
    expr("multiset[ROW(1,2)] submultiset of multiset[row(3,4)]").ok();
  }

  @Test void testElement() {
    expr("element(multiset[1])")
        .columnType("INTEGER NOT NULL");
    expr("1.0+element(multiset[1])")
        .columnType("DECIMAL(12, 1) NOT NULL");
    expr("element(multiset['1'])")
        .columnType("CHAR(1) NOT NULL");
    expr("element(multiset[1e-2])")
        .columnType("DOUBLE NOT NULL");
    expr("element(multiset[multiset[cast(null as tinyint)]])")
        .columnType("TINYINT MULTISET NOT NULL");
  }

  @Test void testMemberOf() {
    expr("1 member of multiset[1]")
        .columnType("BOOLEAN NOT NULL");
    wholeExpr("1 member of multiset['1']")
        .fails("Cannot compare values of types 'INTEGER', 'CHAR\\(1\\)'");
  }

  @Test void testIsASet() {
    expr("multiset[1] is a set").ok();
    expr("multiset['1'] is a set").ok();
    wholeExpr("'a' is a set")
        .fails(".*Cannot apply 'IS A SET' to.*");
  }

  @Test void testCardinality() {
    expr("cardinality(multiset[1])")
        .columnType("INTEGER NOT NULL");
    expr("cardinality(multiset['1'])")
        .columnType("INTEGER NOT NULL");
    wholeExpr("cardinality('a')")
        .fails("Cannot apply 'CARDINALITY' to arguments of type "
            + "'CARDINALITY\\(<CHAR\\(1\\)>\\)'\\. Supported form\\(s\\): "
            + "'CARDINALITY\\(<MULTISET>\\)'\n"
            + "'CARDINALITY\\(<ARRAY>\\)'\n"
            + "'CARDINALITY\\(<MAP>\\)'");
  }

  @Test void testPivot() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS ss FOR job in ('CLERK' AS c, 'MANAGER' AS m))";
    sql(sql).type("RecordType(INTEGER NOT NULL EMPNO,"
        + " VARCHAR(20) NOT NULL ENAME, INTEGER MGR,"
        + " TIMESTAMP(0) NOT NULL HIREDATE, INTEGER NOT NULL COMM,"
        + " INTEGER NOT NULL DEPTNO, BOOLEAN NOT NULL SLACKER,"
        + " INTEGER C_SS, INTEGER M_SS) NOT NULL");
  }

  @Test void testPivot2() {
    final String sql = "SELECT *\n"
        + "FROM   (SELECT deptno, job, sal\n"
        + "        FROM   emp)\n"
        + "PIVOT  (SUM(sal) AS sum_sal, COUNT(*) AS \"COUNT\"\n"
        + "        FOR (job) IN ('CLERK', 'MANAGER' mgr, 'ANALYST' AS \"a\"))\n"
        + "ORDER BY deptno";
    final String type = "RecordType(INTEGER NOT NULL DEPTNO, "
        + "INTEGER 'CLERK'_SUM_SAL, BIGINT NOT NULL 'CLERK'_COUNT, "
        + "INTEGER MGR_SUM_SAL, BIGINT NOT NULL MGR_COUNT, INTEGER a_SUM_SAL, "
        + "BIGINT NOT NULL a_COUNT) NOT NULL";
    sql(sql).type(type);
  }

  @Test void testPivotAliases() {
    final String sql = "SELECT *\n"
        + "FROM (\n"
        + "    SELECT deptno, job, sal FROM   emp)\n"
        + "PIVOT (SUM(sal) AS ss\n"
        + "    FOR (job, deptno)\n"
        + "    IN (('A B'/*C*/||' D', 10),\n"
        + "        ('MANAGER', null) mgr,\n"
        + "        ('ANALYST', 30) AS \"a\"))";
    // Oracle uses parse tree without spaces around '||',
    //   'A B'||' D'_10_SUM_SAL
    // but close enough.
    final String type = "RecordType(INTEGER 'A B' || ' D'_10_SS, "
        + "INTEGER MGR_SS, INTEGER a_SS) NOT NULL";
    sql(sql).type(type);
  }

  @Test void testPivotAggAliases() {
    final String sql = "SELECT *\n"
        + "FROM (SELECT deptno, job, sal FROM emp)\n"
        + "PIVOT (SUM(sal) AS ss, MIN(job)\n"
        + "    FOR deptno IN (10 AS ten, 20))";
    final String type = "RecordType(INTEGER TEN_SS, VARCHAR(10) TEN, "
        + "INTEGER 20_SS, VARCHAR(10) 20) NOT NULL";
    sql(sql).type(type);
  }

  @Test void testPivotNoValues() {
    final String sql = "SELECT *\n"
        + "FROM (SELECT deptno, sal, job FROM emp)\n"
        + "PIVOT (sum(sal) AS sum_sal FOR job in ())";
    sql(sql).type("RecordType(INTEGER NOT NULL DEPTNO) NOT NULL");
  }

  /** Output only includes columns not referenced in an aggregate or axis. */
  @Test void testPivotRemoveColumns() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sum_sal, count(comm) AS count_comm,\n"
        + "      min(hiredate) AS min_hiredate, max(hiredate) AS max_hiredate\n"
        + "  FOR (job, deptno, slacker, mgr, ename)\n"
        + "  IN (('CLERK', 10, false, null, ename) AS c10))";
    sql(sql).type("RecordType(INTEGER NOT NULL EMPNO,"
        + " INTEGER C10_SUM_SAL, BIGINT NOT NULL C10_COUNT_COMM,"
        + " TIMESTAMP(0) C10_MIN_HIREDATE,"
        + " TIMESTAMP(0) C10_MAX_HIREDATE) NOT NULL");
  }

  @Test void testPivotInvalidCol() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(^invalid^) AS sal FOR job in ('CLERK' AS c, 'MANAGER'))";
    sql(sql).fails("Column 'INVALID' not found in any table");
  }

  @Test void testPivotInvalidCol2() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR (job, ^invalid^) in (('CLERK', 'x') AS c))";
    sql(sql).fails("Column 'INVALID' not found in any table");
  }

  @Test void testPivotMeasureMustBeAgg() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sal ^+^ 1 AS sal1 FOR job in ('CLERK' AS c, 'MANAGER'))";
    sql(sql).fails("(?s).*Encountered \"\\+\" at .*");

    final String sql2 = "SELECT * FROM emp\n"
        + "PIVOT (^log10(sal)^ AS logSal FOR job in ('CLERK' AS c, 'MANAGER'))";
    sql(sql2).fails("Measure expression in PIVOT must use aggregate function");

    final String sql3 = "SELECT * FROM emp\n"
        + "PIVOT (^123^ AS logSal FOR job in ('CLERK' AS c, 'MANAGER'))";
    sql(sql3).fails("(?s).*Encountered \"123\" at .*");
  }

  /** Tests an expression as argument to an aggregate function in a PIVOT.
   * Both of the columns referenced ({@code sum} and {@code deptno}) are removed
   * from the implicit GROUP BY. */
  @Test void testPivotAggExpression() {
    final String sql = "SELECT * FROM (SELECT sal, deptno, job, mgr FROM Emp)\n"
        + "PIVOT (sum(sal + deptno + 1)\n"
        + "   FOR job in ('CLERK' AS c, 'ANALYST' AS a))";
    sql(sql).type("RecordType(INTEGER MGR, INTEGER C, INTEGER A) NOT NULL");
  }

  @Test void testPivotValueMismatch() {
    final String sql = "SELECT * FROM Emp\n"
        + "PIVOT (SUM(sal) FOR job IN (^('A', 'B')^, ('C', 'D')))";
    sql(sql).fails("Value count in PIVOT \\(2\\) must match number of "
        + "FOR columns \\(1\\)");

    final String sql2 = "SELECT * FROM Emp\n"
        + "PIVOT (SUM(sal) FOR job IN (^('A', 'B')^ AS x, ('C', 'D')))";
    sql(sql2).fails("Value count in PIVOT \\(2\\) must match number of "
        + "FOR columns \\(1\\)");

    final String sql3 = "SELECT * FROM Emp\n"
        + "PIVOT (SUM(sal) FOR (job) IN (^('A', 'B')^))";
    sql(sql3).fails("Value count in PIVOT \\(2\\) must match number of "
        + "FOR columns \\(1\\)");

    final String sql4 = "SELECT * FROM Emp\n"
        + "PIVOT (SUM(sal) FOR (job, deptno) IN (^'CLERK'^, 10))";
    sql(sql4).fails("Value count in PIVOT \\(1\\) must match number of "
        + "FOR columns \\(2\\)");
  }

  @Test void testUnpivot() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR remuneration_type IN (comm AS 'commission',\n"
        + "                            sal as 'salary'))";
    sql(sql).type("RecordType(INTEGER NOT NULL EMPNO,"
        + " VARCHAR(20) NOT NULL ENAME, VARCHAR(10) NOT NULL JOB, INTEGER MGR,"
        + " TIMESTAMP(0) NOT NULL HIREDATE, INTEGER NOT NULL DEPTNO,"
        + " BOOLEAN NOT NULL SLACKER, CHAR(10) NOT NULL REMUNERATION_TYPE,"
        + " INTEGER NOT NULL REMUNERATION) NOT NULL");
  }

  @Test void testUnpivotInvalidColumn() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR remuneration_type IN (comm AS 'commission',\n"
        + "                            ^unknownCol^ as 'salary'))";
    sql(sql).fails("Column 'UNKNOWNCOL' not found in any table");
  }

  @Test void testUnpivotCannotDeriveMeasureType() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR remuneration_type IN (^comm^ AS 'commission',\n"
        + "                            ename as 'salary'))";
    sql(sql).fails("In UNPIVOT, cannot derive type for measure 'REMUNERATION'"
        + " because source columns have different data types");
  }

  @Test void testUnpivotValueMismatch() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR remuneration_type IN (comm AS 'commission',\n"
        + "                            sal AS ^('salary', 1)^))";
    String expected = "Value count in UNPIVOT \\(2\\) must match "
        + "number of FOR columns \\(1\\)";
    sql(sql).fails(expected);
  }

  @Test void testUnpivotDuplicateName() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT ((remuneration, ^remuneration^)\n"
        + "  FOR remuneration_type\n"
        + "  IN ((comm, comm) AS 'commission',\n"
        + "      (sal, sal) AS 'salary'))";
    sql(sql).fails("Duplicate column name 'REMUNERATION' in UNPIVOT");
  }

  @Test void testUnpivotDuplicateName2() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR ^remuneration^ IN (comm AS 'commission',\n"
        + "                         sal AS 'salary'))";
    sql(sql).fails("Duplicate column name 'REMUNERATION' in UNPIVOT");
  }

  @Test void testUnpivotDuplicateName3() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT (remuneration\n"
        + "  FOR ^deptno^ IN (comm AS 'commission',\n"
        + "                         sal AS 'salary'))";
    sql(sql).fails("Duplicate column name 'DEPTNO' in UNPIVOT");
  }

  @Test void testUnpivotMissingAs() {
    final String sql = "SELECT *\n"
        + "FROM (\n"
        + "  SELECT *\n"
        + "  FROM (VALUES (0, 1, 2, 3, 4),\n"
        + "               (10, 11, 12, 13, 14))\n"
        + "      AS t (c0, c1, c2, c3, c4))\n"
        + "UNPIVOT ((m0, m1, m2)\n"
        + "    FOR (a0, a1)\n"
        + "     IN ((c1, c2, c3) AS ('col1','col2'),\n"
        + "         (c2, c3, c4)))";
    sql(sql).type("RecordType(INTEGER NOT NULL C0, VARCHAR(8) NOT NULL A0,"
        + " VARCHAR(8) NOT NULL A1, INTEGER M0, INTEGER M1,"
        + " INTEGER M2) NOT NULL");
  }

  @Test void testUnpivotMissingAs2() {
    final String sql = "SELECT *\n"
        + "FROM (\n"
        + "  SELECT *\n"
        + "  FROM (VALUES (0, 1, 2, 3, 4),\n"
        + "               (10, 11, 12, 13, 14))\n"
        + "      AS t (c0, c1, c2, c3, c4))\n"
        + "UNPIVOT ((m0, m1, m2)\n"
        + "    FOR (^a0^, a1)\n"
        + "     IN ((c1, c2, c3) AS (6, true),\n"
        + "         (c2, c3, c4)))";
    sql(sql).fails("In UNPIVOT, cannot derive type for axis 'A0'");
  }

  @Test void testMatchRecognizeWithDistinctAggregation() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "MATCH_RECOGNIZE (\n"
        + "  ORDER BY ename\n"
        + "  MEASURES\n"
        + "    ^COUNT(DISTINCT A.deptno)^ AS deptno\n"
        + "  PATTERN (A B)\n"
        + "  DEFINE\n"
        + "    A AS A.empno = 123\n"
        + ") AS T";
    sql(sql).fails("DISTINCT/ALL not allowed with "
        + "COUNT\\(DISTINCT `A`\\.`DEPTNO`\\) function");
  }

  @Test void testIntervalTimeUnitEnumeration() {
    // Since there is validation code relaying on the fact that the
    // enumerated time unit ordinals in SqlIntervalQualifier starts with 0
    // and ends with 5, this test is here to make sure that if someone
    // changes how the time untis are setup, an early feedback will be
    // generated by this test.
    assertEquals(
        0,
        TimeUnit.YEAR.ordinal());
    assertEquals(
        1,
        TimeUnit.MONTH.ordinal());
    assertEquals(
        2,
        TimeUnit.DAY.ordinal());
    assertEquals(
        3,
        TimeUnit.HOUR.ordinal());
    assertEquals(
        4,
        TimeUnit.MINUTE.ordinal());
    assertEquals(
        5,
        TimeUnit.SECOND.ordinal());
    boolean b =
        (TimeUnit.YEAR.ordinal()
            < TimeUnit.MONTH.ordinal())
            && (TimeUnit.MONTH.ordinal()
            < TimeUnit.DAY.ordinal())
            && (TimeUnit.DAY.ordinal()
            < TimeUnit.HOUR.ordinal())
            && (TimeUnit.HOUR.ordinal()
            < TimeUnit.MINUTE.ordinal())
            && (TimeUnit.MINUTE.ordinal()
            < TimeUnit.SECOND.ordinal());
    assertTrue(b);
  }

  @Test void testIntervalMonthsConversion() {
    expr("INTERVAL '1' YEAR").intervalConv("12");
    expr("INTERVAL '5' MONTH").intervalConv("5");
    expr("INTERVAL '3-2' YEAR TO MONTH").intervalConv("38");
    expr("INTERVAL '-5-4' YEAR TO MONTH").intervalConv("-64");
  }

  @Test void testIntervalMillisConversion() {
    expr("INTERVAL '1' DAY").intervalConv("86400000");
    expr("INTERVAL '1' HOUR").intervalConv("3600000");
    expr("INTERVAL '1' MINUTE").intervalConv("60000");
    expr("INTERVAL '1' SECOND").intervalConv("1000");
    expr("INTERVAL '1:05' HOUR TO MINUTE").intervalConv("3900000");
    expr("INTERVAL '1:05' MINUTE TO SECOND").intervalConv("65000");
    expr("INTERVAL '1 1' DAY TO HOUR").intervalConv("90000000");
    expr("INTERVAL '1 1:05' DAY TO MINUTE").intervalConv("90300000");
    expr("INTERVAL '1 1:05:03' DAY TO SECOND").intervalConv("90303000");
    expr("INTERVAL '1 1:05:03.12345' DAY TO SECOND").intervalConv("90303123");
    expr("INTERVAL '1.12345' SECOND").intervalConv("1123");
    expr("INTERVAL '1:05.12345' MINUTE TO SECOND").intervalConv("65123");
    expr("INTERVAL '1:05:03' HOUR TO SECOND").intervalConv("3903000");
    expr("INTERVAL '1:05:03.12345' HOUR TO SECOND").intervalConv("3903123");
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalYearPositive() {
    // default precision
    expr("INTERVAL '1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL '99' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' YEAR(2)")
        .columnType("INTERVAL YEAR(2) NOT NULL");
    expr("INTERVAL '99' YEAR(2)")
        .columnType("INTERVAL YEAR(2) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' YEAR(10)")
        .columnType("INTERVAL YEAR(10) NOT NULL");

    // min precision
    expr("INTERVAL '0' YEAR(1)")
        .columnType("INTERVAL YEAR(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' YEAR(4)")
        .columnType("INTERVAL YEAR(4) NOT NULL");

    // sign
    expr("INTERVAL '+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL '-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL +'1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL +'+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL +'-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL -'1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL -'+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("INTERVAL -'-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalYearToMonthPositive() {
    // default precision
    expr("INTERVAL '1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL '99-11' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL '99-0' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1-2' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");
    expr("INTERVAL '99-11' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");
    expr("INTERVAL '99-0' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");

    // max precision
    expr("INTERVAL '2147483647-11' YEAR(10) TO MONTH")
        .columnType("INTERVAL YEAR(10) TO MONTH NOT NULL");

    // min precision
    expr("INTERVAL '0-0' YEAR(1) TO MONTH")
        .columnType("INTERVAL YEAR(1) TO MONTH NOT NULL");

    // alternate precision
    expr("INTERVAL '2006-2' YEAR(4) TO MONTH")
        .columnType("INTERVAL YEAR(4) TO MONTH NOT NULL");

    // sign
    expr("INTERVAL '-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL '+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL +'1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL +'-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL +'+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL -'1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL -'-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("INTERVAL -'+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalMonthPositive() {
    // default precision
    expr("INTERVAL '1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL '99' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' MONTH(2)")
        .columnType("INTERVAL MONTH(2) NOT NULL");
    expr("INTERVAL '99' MONTH(2)")
        .columnType("INTERVAL MONTH(2) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' MONTH(10)")
        .columnType("INTERVAL MONTH(10) NOT NULL");

    // min precision
    expr("INTERVAL '0' MONTH(1)")
        .columnType("INTERVAL MONTH(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' MONTH(4)")
        .columnType("INTERVAL MONTH(4) NOT NULL");

    // sign
    expr("INTERVAL '+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL '-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL +'1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL +'+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL +'-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL -'1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL -'+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("INTERVAL -'-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalDayPositive() {
    // default precision
    expr("INTERVAL '1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL '99' DAY")
        .columnType("INTERVAL DAY NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' DAY(2)")
        .columnType("INTERVAL DAY(2) NOT NULL");
    expr("INTERVAL '99' DAY(2)")
        .columnType("INTERVAL DAY(2) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' DAY(10)")
        .columnType("INTERVAL DAY(10) NOT NULL");

    // min precision
    expr("INTERVAL '0' DAY(1)")
        .columnType("INTERVAL DAY(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' DAY(4)")
        .columnType("INTERVAL DAY(4) NOT NULL");

    // sign
    expr("INTERVAL '+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL '-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL +'1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL +'+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL +'-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL -'1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL -'+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    expr("INTERVAL -'-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
  }

  void subTestIntervalDayToHourPositive() {
    // default precision
    expr("INTERVAL '1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL '99 23' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL '99 0' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1 2' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");
    expr("INTERVAL '99 23' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");
    expr("INTERVAL '99 0' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");

    // max precision
    expr("INTERVAL '2147483647 23' DAY(10) TO HOUR")
        .columnType("INTERVAL DAY(10) TO HOUR NOT NULL");

    // min precision
    expr("INTERVAL '0 0' DAY(1) TO HOUR")
        .columnType("INTERVAL DAY(1) TO HOUR NOT NULL");

    // alternate precision
    expr("INTERVAL '2345 2' DAY(4) TO HOUR")
        .columnType("INTERVAL DAY(4) TO HOUR NOT NULL");

    // sign
    expr("INTERVAL '-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL '+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL +'1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL +'-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL +'+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL -'1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL -'-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("INTERVAL -'+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalDayToMinutePositive() {
    // default precision
    expr("INTERVAL '1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL '99 23:59' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL '99 0:0' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1 2:3' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");
    expr("INTERVAL '99 23:59' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");
    expr("INTERVAL '99 0:0' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");

    // max precision
    expr("INTERVAL '2147483647 23:59' DAY(10) TO MINUTE")
        .columnType("INTERVAL DAY(10) TO MINUTE NOT NULL");

    // min precision
    expr("INTERVAL '0 0:0' DAY(1) TO MINUTE")
        .columnType("INTERVAL DAY(1) TO MINUTE NOT NULL");

    // alternate precision
    expr("INTERVAL '2345 6:7' DAY(4) TO MINUTE")
        .columnType("INTERVAL DAY(4) TO MINUTE NOT NULL");

    // sign
    expr("INTERVAL '-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL '+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL +'1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL +'-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL +'+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL -'1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL -'-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("INTERVAL -'+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalDayToSecondPositive() {
    // default precision
    expr("INTERVAL '1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL '99 23:59:59' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL '99 0:0:0' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL '99 23:59:59.999999' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL '99 0:0:0.0' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1 2:3:4' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    expr("INTERVAL '99 23:59:59' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    expr("INTERVAL '99 0:0:0' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    expr("INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)")
        .columnType("INTERVAL DAY TO SECOND(6) NOT NULL");
    expr("INTERVAL '99 0:0:0.0' DAY TO SECOND(6)")
        .columnType("INTERVAL DAY TO SECOND(6) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND")
        .columnType("INTERVAL DAY(10) TO SECOND NOT NULL");
    expr("INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)")
        .columnType("INTERVAL DAY(10) TO SECOND(9) NOT NULL");

    // min precision
    expr("INTERVAL '0 0:0:0' DAY(1) TO SECOND")
        .columnType("INTERVAL DAY(1) TO SECOND NOT NULL");
    expr("INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)")
        .columnType("INTERVAL DAY(1) TO SECOND(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '2345 6:7:8' DAY(4) TO SECOND")
        .columnType("INTERVAL DAY(4) TO SECOND NOT NULL");
    expr("INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)")
        .columnType("INTERVAL DAY(4) TO SECOND(4) NOT NULL");

    // sign
    expr("INTERVAL '-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL '+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL +'1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL +'-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL +'+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL -'1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL -'-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("INTERVAL -'+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalHourPositive() {
    // default precision
    expr("INTERVAL '1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL '99' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' HOUR(2)")
        .columnType("INTERVAL HOUR(2) NOT NULL");
    expr("INTERVAL '99' HOUR(2)")
        .columnType("INTERVAL HOUR(2) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' HOUR(10)")
        .columnType("INTERVAL HOUR(10) NOT NULL");

    // min precision
    expr("INTERVAL '0' HOUR(1)")
        .columnType("INTERVAL HOUR(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' HOUR(4)")
        .columnType("INTERVAL HOUR(4) NOT NULL");

    // sign
    expr("INTERVAL '+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL '-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL +'1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL +'+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL +'-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL -'1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL -'+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("INTERVAL -'-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalHourToMinutePositive() {
    // default precision
    expr("INTERVAL '2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL '23:59' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL '99:0' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '2:3' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");
    expr("INTERVAL '23:59' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");
    expr("INTERVAL '99:0' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");

    // max precision
    expr("INTERVAL '2147483647:59' HOUR(10) TO MINUTE")
        .columnType("INTERVAL HOUR(10) TO MINUTE NOT NULL");

    // min precision
    expr("INTERVAL '0:0' HOUR(1) TO MINUTE")
        .columnType("INTERVAL HOUR(1) TO MINUTE NOT NULL");

    // alternate precision
    expr("INTERVAL '2345:7' HOUR(4) TO MINUTE")
        .columnType("INTERVAL HOUR(4) TO MINUTE NOT NULL");

    // sign
    expr("INTERVAL '-1:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL '+1:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL +'2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL +'-2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL +'+2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL -'2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL -'-2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    expr("INTERVAL -'+2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalHourToSecondPositive() {
    // default precision
    expr("INTERVAL '2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL '23:59:59' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL '99:0:0' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL '23:59:59.999999' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL '99:0:0.0' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '2:3:4' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:59:59' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:0:0' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:59:59.999999' HOUR TO SECOND(6)")
        .columnType("INTERVAL HOUR TO SECOND(6) NOT NULL");
    expr("INTERVAL '99:0:0.0' HOUR TO SECOND(6)")
        .columnType("INTERVAL HOUR TO SECOND(6) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647:59:59' HOUR(10) TO SECOND")
        .columnType("INTERVAL HOUR(10) TO SECOND NOT NULL");
    expr("INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)")
        .columnType("INTERVAL HOUR(10) TO SECOND(9) NOT NULL");

    // min precision
    expr("INTERVAL '0:0:0' HOUR(1) TO SECOND")
        .columnType("INTERVAL HOUR(1) TO SECOND NOT NULL");
    expr("INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)")
        .columnType("INTERVAL HOUR(1) TO SECOND(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '2345:7:8' HOUR(4) TO SECOND")
        .columnType("INTERVAL HOUR(4) TO SECOND NOT NULL");
    expr("INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)")
        .columnType("INTERVAL HOUR(4) TO SECOND(4) NOT NULL");

    // sign
    expr("INTERVAL '-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL '+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL +'2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL +'-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL +'+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL -'2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL -'-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("INTERVAL -'+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalMinutePositive() {
    // default precision
    expr("INTERVAL '1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL '99' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' MINUTE(2)")
        .columnType("INTERVAL MINUTE(2) NOT NULL");
    expr("INTERVAL '99' MINUTE(2)")
        .columnType("INTERVAL MINUTE(2) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' MINUTE(10)")
        .columnType("INTERVAL MINUTE(10) NOT NULL");

    // min precision
    expr("INTERVAL '0' MINUTE(1)")
        .columnType("INTERVAL MINUTE(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' MINUTE(4)")
        .columnType("INTERVAL MINUTE(4) NOT NULL");

    // sign
    expr("INTERVAL '+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL '-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL +'1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL +'+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL +'-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL -'1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL -'+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    expr("INTERVAL -'-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalMinuteToSecondPositive() {
    // default precision
    expr("INTERVAL '2:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL '59:59' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL '99:0' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL '59:59.999999' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL '99:0.0' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '2:4' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:59' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:0' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    expr("INTERVAL '99:59.999999' MINUTE TO SECOND(6)")
        .columnType("INTERVAL MINUTE TO SECOND(6) NOT NULL");
    expr("INTERVAL '99:0.0' MINUTE TO SECOND(6)")
        .columnType("INTERVAL MINUTE TO SECOND(6) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647:59' MINUTE(10) TO SECOND")
        .columnType("INTERVAL MINUTE(10) TO SECOND NOT NULL");
    expr("INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)")
        .columnType("INTERVAL MINUTE(10) TO SECOND(9) NOT NULL");

    // min precision
    expr("INTERVAL '0:0' MINUTE(1) TO SECOND")
        .columnType("INTERVAL MINUTE(1) TO SECOND NOT NULL");
    expr("INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)")
        .columnType("INTERVAL MINUTE(1) TO SECOND(1) NOT NULL");

    // alternate precision
    expr("INTERVAL '2345:8' MINUTE(4) TO SECOND")
        .columnType("INTERVAL MINUTE(4) TO SECOND NOT NULL");
    expr("INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)")
        .columnType("INTERVAL MINUTE(4) TO SECOND(4) NOT NULL");

    // sign
    expr("INTERVAL '-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL '+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL +'3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL +'-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL +'+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL -'3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL -'-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    expr("INTERVAL -'+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  void subTestIntervalSecondPositive() {
    // default precision
    expr("INTERVAL '1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL '99' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");

    // explicit precision equal to default
    expr("INTERVAL '1' SECOND(2)")
        .columnType("INTERVAL SECOND(2) NOT NULL");
    expr("INTERVAL '99' SECOND(2)")
        .columnType("INTERVAL SECOND(2) NOT NULL");
    expr("INTERVAL '1' SECOND(2, 6)")
        .columnType("INTERVAL SECOND(2, 6) NOT NULL");
    expr("INTERVAL '99' SECOND(2, 6)")
        .columnType("INTERVAL SECOND(2, 6) NOT NULL");

    // max precision
    expr("INTERVAL '2147483647' SECOND(10)")
        .columnType("INTERVAL SECOND(10) NOT NULL");
    expr("INTERVAL '2147483647.999999999' SECOND(10, 9)")
        .columnType("INTERVAL SECOND(10, 9) NOT NULL");

    // min precision
    expr("INTERVAL '0' SECOND(1)")
        .columnType("INTERVAL SECOND(1) NOT NULL");
    expr("INTERVAL '0.0' SECOND(1, 1)")
        .columnType("INTERVAL SECOND(1, 1) NOT NULL");

    // alternate precision
    expr("INTERVAL '1234' SECOND(4)")
        .columnType("INTERVAL SECOND(4) NOT NULL");
    expr("INTERVAL '1234.56789' SECOND(4, 5)")
        .columnType("INTERVAL SECOND(4, 5) NOT NULL");

    // sign
    expr("INTERVAL '+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL '-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL +'1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL +'+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL +'-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL -'1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL -'+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    expr("INTERVAL -'-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalYearNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' YEAR")
        .fails("Illegal interval literal format '-' for INTERVAL YEAR.*");
    wholeExpr("INTERVAL '1-2' YEAR")
        .fails("Illegal interval literal format '1-2' for INTERVAL YEAR.*");
    wholeExpr("INTERVAL '1.2' YEAR")
        .fails("Illegal interval literal format '1.2' for INTERVAL YEAR.*");
    wholeExpr("INTERVAL '1 2' YEAR")
        .fails("Illegal interval literal format '1 2' for INTERVAL YEAR.*");
    wholeExpr("INTERVAL '1-2' YEAR(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL YEAR\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' YEAR")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL YEAR.*");

    // negative field values
    wholeExpr("INTERVAL '--1' YEAR")
        .fails("Illegal interval literal format '--1' for INTERVAL YEAR.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' YEAR")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    wholeExpr("INTERVAL '100' YEAR(2)")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' YEAR(3)")
        .fails("Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' YEAR(3)")
        .fails("Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' YEAR(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "YEAR\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' YEAR(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "YEAR\\(10\\) field");

    // precision > maximum
    expr("INTERVAL '1' ^YEAR(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL YEAR\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^YEAR(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL YEAR\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalYearToMonthNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' YEAR TO MONTH")
        .fails("Illegal interval literal format '-' for INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1' YEAR TO MONTH")
        .fails("Illegal interval literal format '1' for INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1:2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1:2' for INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1.2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1.2' for INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1 2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1 2' for INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1:2' YEAR(2) TO MONTH")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL YEAR\\(2\\) TO MONTH");
    wholeExpr("INTERVAL 'bogus text' YEAR TO MONTH")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL YEAR TO MONTH");

    // negative field values
    wholeExpr("INTERVAL '--1-2' YEAR TO MONTH")
        .fails("Illegal interval literal format '--1-2' for "
            + "INTERVAL YEAR TO MONTH");
    wholeExpr("INTERVAL '1--2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1--2' for "
            + "INTERVAL YEAR TO MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100-0' YEAR TO MONTH")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    wholeExpr("INTERVAL '100-0' YEAR(2) TO MONTH")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    wholeExpr("INTERVAL '1000-0' YEAR(3) TO MONTH")
        .fails("Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000-0' YEAR(3) TO MONTH")
        .fails("Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648-0' YEAR(10) TO MONTH")
        .fails("Interval field value 2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648-0' YEAR(10) TO MONTH")
        .fails("Interval field value -2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
    wholeExpr("INTERVAL '1-12' YEAR TO MONTH")
        .fails("Illegal interval literal format '1-12' for INTERVAL YEAR TO MONTH.*");

    // precision > maximum
    expr("INTERVAL '1-1' ^YEAR(11) TO MONTH^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL YEAR\\(11\\) TO MONTH");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0-0' ^YEAR(0) TO MONTH^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL YEAR\\(0\\) TO MONTH");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalMonthNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' MONTH")
        .fails("Illegal interval literal format '-' for INTERVAL MONTH.*");
    wholeExpr("INTERVAL '1-2' MONTH")
        .fails("Illegal interval literal format '1-2' for INTERVAL MONTH.*");
    wholeExpr("INTERVAL '1.2' MONTH")
        .fails("Illegal interval literal format '1.2' for INTERVAL MONTH.*");
    wholeExpr("INTERVAL '1 2' MONTH")
        .fails("Illegal interval literal format '1 2' for INTERVAL MONTH.*");
    wholeExpr("INTERVAL '1-2' MONTH(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL MONTH\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' MONTH")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MONTH.*");

    // negative field values
    wholeExpr("INTERVAL '--1' MONTH")
        .fails("Illegal interval literal format '--1' for INTERVAL MONTH.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' MONTH")
        .fails("Interval field value 100 exceeds precision of MONTH\\(2\\) field.*");
    wholeExpr("INTERVAL '100' MONTH(2)")
        .fails("Interval field value 100 exceeds precision of MONTH\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' MONTH(3)")
        .fails("Interval field value 1,000 exceeds precision of MONTH\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' MONTH(3)")
        .fails("Interval field value -1,000 exceeds precision of MONTH\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' MONTH(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' MONTH(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");

    // precision > maximum
    expr("INTERVAL '1' ^MONTH(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL MONTH\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^MONTH(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL MONTH\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalDayNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' DAY")
        .fails("Illegal interval literal format '-' for INTERVAL DAY.*");
    wholeExpr("INTERVAL '1-2' DAY")
        .fails("Illegal interval literal format '1-2' for INTERVAL DAY.*");
    wholeExpr("INTERVAL '1.2' DAY")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY.*");
    wholeExpr("INTERVAL '1 2' DAY")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY.*");
    wholeExpr("INTERVAL '1:2' DAY")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY.*");
    wholeExpr("INTERVAL '1-2' DAY(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL DAY\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' DAY")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL DAY.*");

    // negative field values
    wholeExpr("INTERVAL '--1' DAY")
        .fails("Illegal interval literal format '--1' for INTERVAL DAY.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' DAY")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '100' DAY(2)")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' DAY(3)")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' DAY(3)")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' DAY(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' DAY(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");

    // precision > maximum
    expr("INTERVAL '1' ^DAY(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^DAY(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... DAY TO HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalDayToHourNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' DAY TO HOUR")
        .fails("Illegal interval literal format '-' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1' DAY TO HOUR")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1:2' DAY TO HOUR")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1.2' DAY TO HOUR")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1 x' DAY TO HOUR")
        .fails("Illegal interval literal format '1 x' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL ' ' DAY TO HOUR")
        .fails("Illegal interval literal format ' ' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1:2' DAY(2) TO HOUR")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL DAY\\(2\\) TO HOUR");
    wholeExpr("INTERVAL 'bogus text' DAY TO HOUR")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO HOUR");

    // negative field values
    wholeExpr("INTERVAL '--1 1' DAY TO HOUR")
        .fails("Illegal interval literal format '--1 1' for INTERVAL DAY TO HOUR");
    wholeExpr("INTERVAL '1 -1' DAY TO HOUR")
        .fails("Illegal interval literal format '1 -1' for INTERVAL DAY TO HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100 0' DAY TO HOUR")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '100 0' DAY(2) TO HOUR")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '1000 0' DAY(3) TO HOUR")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000 0' DAY(3) TO HOUR")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648 0' DAY(10) TO HOUR")
        .fails("Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648 0' DAY(10) TO HOUR")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '1 24' DAY TO HOUR")
        .fails("Illegal interval literal format '1 24' for INTERVAL DAY TO HOUR.*");

    // precision > maximum
    expr("INTERVAL '1 1' ^DAY(11) TO HOUR^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO HOUR");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0' ^DAY(0) TO HOUR^")
        .fails("Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\) TO HOUR");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalDayToMinuteNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL ' :' DAY TO MINUTE")
        .fails("Illegal interval literal format ' :' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1:2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1.2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL 'x 1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format 'x 1:1' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 x:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 x:1' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 1:x' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:x' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 1:2:3' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:2:3' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 1:1:1.2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:1:1.2' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 1:2:3' DAY(2) TO MINUTE")
        .fails("Illegal interval literal format '1 1:2:3' for "
            + "INTERVAL DAY\\(2\\) TO MINUTE");
    wholeExpr("INTERVAL '1 1' DAY(2) TO MINUTE")
        .fails("Illegal interval literal format '1 1' for "
            + "INTERVAL DAY\\(2\\) TO MINUTE");
    wholeExpr("INTERVAL 'bogus text' DAY TO MINUTE")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO MINUTE");

    // negative field values
    wholeExpr("INTERVAL '--1 1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '--1 1:1' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 -1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 -1:1' for INTERVAL DAY TO MINUTE");
    wholeExpr("INTERVAL '1 1:-1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:-1' for INTERVAL DAY TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100 0:0' DAY TO MINUTE")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '100 0:0' DAY(2) TO MINUTE")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    wholeExpr("INTERVAL '1000 0:0' DAY(3) TO MINUTE")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000 0:0' DAY(3) TO MINUTE")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648 0:0' DAY(10) TO MINUTE")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648 0:0' DAY(10) TO MINUTE")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '1 24:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 24:1' for "
            + "INTERVAL DAY TO MINUTE.*");
    wholeExpr("INTERVAL '1 1:60' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:60' for INTERVAL DAY TO MINUTE.*");

    // precision > maximum
    expr("INTERVAL '1 1:1' ^DAY(11) TO MINUTE^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0' ^DAY(0) TO MINUTE^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalDayToSecondNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL ' ::' DAY TO SECOND")
        .fails("Illegal interval literal format ' ::' for INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL ' ::.' DAY TO SECOND")
        .fails("Illegal interval literal format ' ::\\.' for INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1' DAY TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 2' DAY TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1:2' DAY TO SECOND")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1.2' DAY TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:2' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:2:x' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:2:x' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1:2:3' DAY TO SECOND")
        .fails("Illegal interval literal format '1:2:3' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1:1:1.2' DAY TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.2' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:2' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for "
            + "INTERVAL DAY\\(2\\) TO SECOND");
    wholeExpr("INTERVAL '1 1' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for "
            + "INTERVAL DAY\\(2\\) TO SECOND");
    wholeExpr("INTERVAL 'bogus text' DAY TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '2345 6:7:8901' DAY TO SECOND(4)")
        .fails("Illegal interval literal format '2345 6:7:8901' for "
            + "INTERVAL DAY TO SECOND\\(4\\)");

    // negative field values
    wholeExpr("INTERVAL '--1 1:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '--1 1:1:1' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 -1:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 -1:1:1' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:-1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:-1:1' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:1:-1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:-1' for "
            + "INTERVAL DAY TO SECOND");
    wholeExpr("INTERVAL '1 1:1:1.-1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:1.-1' for "
            + "INTERVAL DAY TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100 0' DAY TO SECOND")
        .fails("Illegal interval literal format '100 0' for "
            + "INTERVAL DAY TO SECOND.*");
    wholeExpr("INTERVAL '100 0' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '100 0' for "
            + "INTERVAL DAY\\(2\\) TO SECOND.*");
    wholeExpr("INTERVAL '1000 0' DAY(3) TO SECOND")
        .fails("Illegal interval literal format '1000 0' for "
            + "INTERVAL DAY\\(3\\) TO SECOND.*");
    wholeExpr("INTERVAL '-1000 0' DAY(3) TO SECOND")
        .fails("Illegal interval literal format '-1000 0' for "
            + "INTERVAL DAY\\(3\\) TO SECOND.*");
    wholeExpr("INTERVAL '2147483648 1:1:0' DAY(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648 1:1:0' DAY(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    wholeExpr("INTERVAL '2147483648 0' DAY(10) TO SECOND")
        .fails("Illegal interval literal format '2147483648 0' for "
            + "INTERVAL DAY\\(10\\) TO SECOND.*");
    wholeExpr("INTERVAL '-2147483648 0' DAY(10) TO SECOND")
        .fails("Illegal interval literal format '-2147483648 0' for "
            + "INTERVAL DAY\\(10\\) TO SECOND.*");
    wholeExpr("INTERVAL '1 24:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 24:1:1' for "
            + "INTERVAL DAY TO SECOND.*");
    wholeExpr("INTERVAL '1 1:60:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:60:1' for "
            + "INTERVAL DAY TO SECOND.*");
    wholeExpr("INTERVAL '1 1:1:60' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:60' for "
            + "INTERVAL DAY TO SECOND.*");
    wholeExpr("INTERVAL '1 1:1:1.0000001' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:1\\.0000001' for "
            + "INTERVAL DAY TO SECOND.*");
    wholeExpr("INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)")
        .fails("Illegal interval literal format '1 1:1:1\\.0001' for "
            + "INTERVAL DAY TO SECOND\\(3\\).*");

    // precision > maximum
    expr("INTERVAL '1 1' ^DAY(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO SECOND");
    expr("INTERVAL '1 1' ^DAY TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for "
            + "INTERVAL DAY TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0:0:0' ^DAY(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\) TO SECOND");
    expr("INTERVAL '0 0:0:0' ^DAY TO SECOND(0)^")
        .fails("Interval fractional second precision '0' out of range for "
            + "INTERVAL DAY TO SECOND\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalHourNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' HOUR")
        .fails("Illegal interval literal format '-' for INTERVAL HOUR.*");
    wholeExpr("INTERVAL '1-2' HOUR")
        .fails("Illegal interval literal format '1-2' for INTERVAL HOUR.*");
    wholeExpr("INTERVAL '1.2' HOUR")
        .fails("Illegal interval literal format '1.2' for INTERVAL HOUR.*");
    wholeExpr("INTERVAL '1 2' HOUR")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR.*");
    wholeExpr("INTERVAL '1:2' HOUR")
        .fails("Illegal interval literal format '1:2' for INTERVAL HOUR.*");
    wholeExpr("INTERVAL '1-2' HOUR(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL HOUR\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' HOUR")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL HOUR.*");

    // negative field values
    wholeExpr("INTERVAL '--1' HOUR")
        .fails("Illegal interval literal format '--1' for INTERVAL HOUR.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' HOUR")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '100' HOUR(2)")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' HOUR(3)")
        .fails("Interval field value 1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' HOUR(3)")
        .fails("Interval field value -1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' HOUR(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' HOUR(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");

    // precision > maximum
    expr("INTERVAL '1' ^HOUR(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^HOUR(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalHourToMinuteNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL ':' HOUR TO MINUTE")
        .fails("Illegal interval literal format ':' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1:x' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:x' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1.2' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1 2' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1:2:3' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:2:3' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1 2' HOUR(2) TO MINUTE")
        .fails("Illegal interval literal format '1 2' for "
            + "INTERVAL HOUR\\(2\\) TO MINUTE");
    wholeExpr("INTERVAL 'bogus text' HOUR TO MINUTE")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL HOUR TO MINUTE");

    // negative field values
    wholeExpr("INTERVAL '--1:1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '--1:1' for INTERVAL HOUR TO MINUTE");
    wholeExpr("INTERVAL '1:-1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:-1' for INTERVAL HOUR TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100:0' HOUR TO MINUTE")
        .fails("Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '100:0' HOUR(2) TO MINUTE")
        .fails("Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '1000:0' HOUR(3) TO MINUTE")
        .fails("Interval field value 1,000 exceeds precision of HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000:0' HOUR(3) TO MINUTE")
        .fails("Interval field value -1,000 exceeds precision of HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648:0' HOUR(10) TO MINUTE")
        .fails("Interval field value 2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648:0' HOUR(10) TO MINUTE")
        .fails("Interval field value -2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
    wholeExpr("INTERVAL '1:60' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:60' for INTERVAL HOUR TO MINUTE.*");

    // precision > maximum
    expr("INTERVAL '1:1' ^HOUR(11) TO MINUTE^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0' ^HOUR(0) TO MINUTE^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalHourToSecondNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '::' HOUR TO SECOND")
        .fails("Illegal interval literal format '::' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '::.' HOUR TO SECOND")
        .fails("Illegal interval literal format '::\\.' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1 2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:2' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1.2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1 1:2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:2:x' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:2:x' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:x:3' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:x:3' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:1:1.x' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.x' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1 1:2' HOUR(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL HOUR\\(2\\) TO SECOND");
    wholeExpr("INTERVAL '1 1' HOUR(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for INTERVAL HOUR\\(2\\) TO SECOND");
    wholeExpr("INTERVAL 'bogus text' HOUR TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '6:7:8901' HOUR TO SECOND(4)")
        .fails("Illegal interval literal format '6:7:8901' for INTERVAL HOUR TO SECOND\\(4\\)");

    // negative field values
    wholeExpr("INTERVAL '--1:1:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '--1:1:1' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:-1:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:-1:1' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:1:-1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:-1' for INTERVAL HOUR TO SECOND");
    wholeExpr("INTERVAL '1:1:1.-1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.-1' for INTERVAL HOUR TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100:0:0' HOUR TO SECOND")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '100:0:0' HOUR(2) TO SECOND")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    wholeExpr("INTERVAL '1000:0:0' HOUR(3) TO SECOND")
        .fails("Interval field value 1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000:0:0' HOUR(3) TO SECOND")
        .fails("Interval field value -1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648:0:0' HOUR(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    wholeExpr("INTERVAL '1:60:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:60:1' for "
            + "INTERVAL HOUR TO SECOND.*");
    wholeExpr("INTERVAL '1:1:60' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:60' for "
            + "INTERVAL HOUR TO SECOND.*");
    wholeExpr("INTERVAL '1:1:1.0000001' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.0000001' for "
            + "INTERVAL HOUR TO SECOND.*");
    wholeExpr("INTERVAL '1:1:1.0001' HOUR TO SECOND(3)")
        .fails("Illegal interval literal format '1:1:1\\.0001' for "
            + "INTERVAL HOUR TO SECOND\\(3\\).*");

    // precision > maximum
    expr("INTERVAL '1:1:1' ^HOUR(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\) TO SECOND");
    expr("INTERVAL '1:1:1' ^HOUR TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for "
            + "INTERVAL HOUR TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0:0' ^HOUR(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\) TO SECOND");
    expr("INTERVAL '0:0:0' ^HOUR TO SECOND(0)^")
        .fails("Interval fractional second precision '0' out of range for "
            + "INTERVAL HOUR TO SECOND\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalMinuteNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL '-' MINUTE")
        .fails("Illegal interval literal format '-' for INTERVAL MINUTE.*");
    wholeExpr("INTERVAL '1-2' MINUTE")
        .fails("Illegal interval literal format '1-2' for INTERVAL MINUTE.*");
    wholeExpr("INTERVAL '1.2' MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL MINUTE.*");
    wholeExpr("INTERVAL '1 2' MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL MINUTE.*");
    wholeExpr("INTERVAL '1:2' MINUTE")
        .fails("Illegal interval literal format '1:2' for INTERVAL MINUTE.*");
    wholeExpr("INTERVAL '1-2' MINUTE(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL MINUTE\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' MINUTE")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MINUTE.*");

    // negative field values
    wholeExpr("INTERVAL '--1' MINUTE")
        .fails("Illegal interval literal format '--1' for INTERVAL MINUTE.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' MINUTE")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    wholeExpr("INTERVAL '100' MINUTE(2)")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' MINUTE(3)")
        .fails("Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' MINUTE(3)")
        .fails("Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' MINUTE(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' MINUTE(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");

    // precision > maximum
    expr("INTERVAL '1' ^MINUTE(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL MINUTE\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^MINUTE(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL MINUTE\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalMinuteToSecondNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL ':' MINUTE TO SECOND")
        .fails("Illegal interval literal format ':' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL ':.' MINUTE TO SECOND")
        .fails("Illegal interval literal format ':\\.' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1 2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1.2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1 1:2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1:x' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:x' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL 'x:3' MINUTE TO SECOND")
        .fails("Illegal interval literal format 'x:3' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1:1.x' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1\\.x' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1 1:2' MINUTE(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL MINUTE\\(2\\) TO SECOND");
    wholeExpr("INTERVAL '1 1' MINUTE(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for INTERVAL MINUTE\\(2\\) TO SECOND");
    wholeExpr("INTERVAL 'bogus text' MINUTE TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '7:8901' MINUTE TO SECOND(4)")
        .fails("Illegal interval literal format '7:8901' for INTERVAL MINUTE TO SECOND\\(4\\)");

    // negative field values
    wholeExpr("INTERVAL '--1:1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '--1:1' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1:-1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:-1' for INTERVAL MINUTE TO SECOND");
    wholeExpr("INTERVAL '1:1.-1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1.-1' for INTERVAL MINUTE TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    wholeExpr("INTERVAL '100:0' MINUTE TO SECOND")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    wholeExpr("INTERVAL '100:0' MINUTE(2) TO SECOND")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    wholeExpr("INTERVAL '1000:0' MINUTE(3) TO SECOND")
        .fails("Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000:0' MINUTE(3) TO SECOND")
        .fails("Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648:0' MINUTE(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648:0' MINUTE(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    wholeExpr("INTERVAL '1:60' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:60' for"
            + " INTERVAL MINUTE TO SECOND.*");
    wholeExpr("INTERVAL '1:1.0000001' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1\\.0000001' for"
            + " INTERVAL MINUTE TO SECOND.*");
    wholeExpr("INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)")
        .fails("Illegal interval literal format '1:1:1\\.0001' for"
            + " INTERVAL MINUTE TO SECOND\\(3\\).*");

    // precision > maximum
    expr("INTERVAL '1:1' ^MINUTE(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for"
            + " INTERVAL MINUTE\\(11\\) TO SECOND");
    expr("INTERVAL '1:1' ^MINUTE TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for"
            + " INTERVAL MINUTE TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0' ^MINUTE(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for"
            + " INTERVAL MINUTE\\(0\\) TO SECOND");
    expr("INTERVAL '0:0' ^MINUTE TO SECOND(0)^")
        .fails("Interval fractional second precision '0' out of range for"
            + " INTERVAL MINUTE TO SECOND\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  void subTestIntervalSecondNegative() {
    // Qualifier - field mismatches
    wholeExpr("INTERVAL ':' SECOND")
        .fails("Illegal interval literal format ':' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '.' SECOND")
        .fails("Illegal interval literal format '\\.' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1-2' SECOND")
        .fails("Illegal interval literal format '1-2' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1.x' SECOND")
        .fails("Illegal interval literal format '1\\.x' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL 'x.1' SECOND")
        .fails("Illegal interval literal format 'x\\.1' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1 2' SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1:2' SECOND")
        .fails("Illegal interval literal format '1:2' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1-2' SECOND(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL SECOND\\(2\\)");
    wholeExpr("INTERVAL 'bogus text' SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL SECOND.*");

    // negative field values
    wholeExpr("INTERVAL '--1' SECOND")
        .fails("Illegal interval literal format '--1' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1.-1' SECOND")
        .fails("Illegal interval literal format '1.-1' for INTERVAL SECOND.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    wholeExpr("INTERVAL '100' SECOND")
        .fails("Interval field value 100 exceeds precision of SECOND\\(2\\) field.*");
    wholeExpr("INTERVAL '100' SECOND(2)")
        .fails("Interval field value 100 exceeds precision of SECOND\\(2\\) field.*");
    wholeExpr("INTERVAL '1000' SECOND(3)")
        .fails("Interval field value 1,000 exceeds precision of SECOND\\(3\\) field.*");
    wholeExpr("INTERVAL '-1000' SECOND(3)")
        .fails("Interval field value -1,000 exceeds precision of SECOND\\(3\\) field.*");
    wholeExpr("INTERVAL '2147483648' SECOND(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
    wholeExpr("INTERVAL '-2147483648' SECOND(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
    wholeExpr("INTERVAL '1.0000001' SECOND")
        .fails("Illegal interval literal format '1\\.0000001' for INTERVAL SECOND.*");
    wholeExpr("INTERVAL '1.0000001' SECOND(2)")
        .fails("Illegal interval literal format '1\\.0000001' for INTERVAL SECOND\\(2\\).*");
    wholeExpr("INTERVAL '1.0001' SECOND(2, 3)")
        .fails("Illegal interval literal format '1\\.0001' for INTERVAL SECOND\\(2, 3\\).*");
    wholeExpr("INTERVAL '1.0000000001' SECOND(2, 9)")
        .fails("Illegal interval literal format '1\\.0000000001' for"
            + " INTERVAL SECOND\\(2, 9\\).*");

    // precision > maximum
    expr("INTERVAL '1' ^SECOND(11)^")
        .fails("Interval leading field precision '11' out of range for"
            + " INTERVAL SECOND\\(11\\)");
    expr("INTERVAL '1.1' ^SECOND(1, 10)^")
        .fails("Interval fractional second precision '10' out of range for"
            + " INTERVAL SECOND\\(1, 10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' ^SECOND(0)^")
        .fails("Interval leading field precision '0' out of range for"
            + " INTERVAL SECOND\\(0\\)");
    expr("INTERVAL '0' ^SECOND(1, 0)^")
        .fails("Interval fractional second precision '0' out of range for"
            + " INTERVAL SECOND\\(1, 0\\)");
  }

  @Test void testDatetimePlusNullInterval() {
    expr("TIME '8:8:8' + cast(NULL AS interval hour)").columnType("TIME(0)");
    expr("TIME '8:8:8' + cast(NULL AS interval YEAR)").columnType("TIME(0)");
    expr("TIMESTAMP '1990-12-12 12:12:12' + cast(NULL AS interval hour)")
        .columnType("TIMESTAMP(0)");
    expr("TIMESTAMP '1990-12-12 12:12:12' + cast(NULL AS interval YEAR)")
        .columnType("TIMESTAMP(0)");

    expr("cast(NULL AS interval hour) + TIME '8:8:8'").columnType("TIME(0)");
    expr("cast(NULL AS interval YEAR) + TIME '8:8:8'").columnType("TIME(0)");
    expr("cast(NULL AS interval hour) + TIMESTAMP '1990-12-12 12:12:12'")
        .columnType("TIMESTAMP(0)");
    expr("cast(NULL AS interval YEAR) + TIMESTAMP '1990-12-12 12:12:12'")
        .columnType("TIMESTAMP(0)");
  }

  @Test void testIntervalLiterals() {
    // First check that min, max, and defaults are what we expect
    // (values used in subtests depend on these being true to
    // accurately test bounds)
    final RelDataTypeSystem typeSystem =
        getTester().getValidator().getTypeFactory().getTypeSystem();
    final RelDataTypeSystem defTypeSystem = RelDataTypeSystem.DEFAULT;
    for (SqlTypeName typeName : SqlTypeName.INTERVAL_TYPES) {
      assertThat(typeName.getMinPrecision(), is(1));
      assertThat(typeSystem.getMaxPrecision(typeName), is(10));
      assertThat(typeSystem.getDefaultPrecision(typeName), is(2));
      assertThat(typeName.getMinScale(), is(1));
      assertThat(typeSystem.getMaxScale(typeName), is(9));
      assertThat(typeName.getDefaultScale(), is(6));
    }

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
    wholeExpr("INTERVAL '1.0' HOUR")
        .fails("Illegal interval literal format '1.0' for INTERVAL HOUR");
    // only seconds are allowed to have a fractional part
    expr("INTERVAL '1.0' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    // leading zeros do not cause precision to be exceeded
    expr("INTERVAL '0999' MONTH(3)")
        .columnType("INTERVAL MONTH(3) NOT NULL");
  }

  @Test void testIntervalExpression() {
    expr("interval 1 hour").columnType("INTERVAL HOUR NOT NULL");
    expr("interval (2 + 3) month").columnType("INTERVAL MONTH NOT NULL");
    expr("interval (cast(null as integer)) year").columnType("INTERVAL YEAR");
    expr("interval (cast(null as integer)) year(2)")
        .columnType("INTERVAL YEAR(2)");
    expr("interval (date '1970-01-01') hour").withWhole(true)
        .fails("Cannot apply 'INTERVAL' to arguments of type "
            + "'INTERVAL <DATE> <INTERVAL HOUR>'\\. Supported form\\(s\\): "
            + "'INTERVAL <NUMERIC> <DATETIME_INTERVAL>'");
    expr("interval (nullif(true, true)) hour").withWhole(true)
        .fails("Cannot apply 'INTERVAL' to arguments of type "
            + "'INTERVAL <BOOLEAN> <INTERVAL HOUR>'\\. Supported form\\(s\\): "
            + "'INTERVAL <NUMERIC> <DATETIME_INTERVAL>'");
    expr("interval (interval '1' day) hour").withWhole(true)
        .fails("Cannot apply 'INTERVAL' to arguments of type "
            + "'INTERVAL <INTERVAL DAY> <INTERVAL HOUR>'\\. "
            + "Supported form\\(s\\): "
            + "'INTERVAL <NUMERIC> <DATETIME_INTERVAL>'");
    sql("select interval empno hour as h from emp")
        .columnType("INTERVAL HOUR NOT NULL");
    sql("select interval emp.mgr hour as h from emp")
        .columnType("INTERVAL HOUR");
  }

  @Test void testIntervalOperators() {
    expr("interval '1' hour + TIME '8:8:8'")
        .columnType("TIME(0) NOT NULL");
    expr("TIME '8:8:8' - interval '1' hour")
        .columnType("TIME(0) NOT NULL");
    expr("TIME '8:8:8' + interval '1' hour")
        .columnType("TIME(0) NOT NULL");

    expr("interval '1' day + interval '1' DAY(4)")
        .columnType("INTERVAL DAY(4) NOT NULL");
    expr("interval '1' day(5) + interval '1' DAY")
        .columnType("INTERVAL DAY(5) NOT NULL");
    expr("interval '1' day + interval '1' HOUR(10)")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("interval '1' day + interval '1' MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("interval '1' day + interval '1' second")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");

    expr("interval '1:2' hour to minute + interval '1' second")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    expr("interval '1:3' hour to minute + interval '1 1:2:3.4' day to second")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    expr("interval '1:2' hour to minute + interval '1 1' day to hour")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("interval '1:2' hour to minute + interval '1 1' day to hour")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    expr("interval '1 2' day to hour + interval '1:1' minute to second")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");

    expr("interval '1' year + interval '1' month")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("interval '1' day - interval '1' hour")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    expr("interval '1' year - interval '1' month")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    expr("interval '1' month - interval '1' year")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    wholeExpr("interval '1' year + interval '1' day")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<INTERVAL YEAR> \\+ <INTERVAL DAY>'.*");
    wholeExpr("interval '1' month + interval '1' second")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<INTERVAL MONTH> \\+ <INTERVAL SECOND>'.*");
    wholeExpr("interval '1' year - interval '1' day")
        .fails("(?s).*Cannot apply '-' to arguments of type "
            + "'<INTERVAL YEAR> - <INTERVAL DAY>'.*");
    wholeExpr("interval '1' month - interval '1' second")
        .fails("(?s).*Cannot apply '-' to arguments of type "
            + "'<INTERVAL MONTH> - <INTERVAL SECOND>'.*");

    // mixing between datetime and interval todo        checkExpType("date
    // '1234-12-12' + INTERVAL '1' month + interval '1' day","DATE"); todo
    //      checkExpFails("date '1234-12-12' + (INTERVAL '1' month +
    // interval '1' day)","?");

    // multiply operator
    expr("interval '1' year * 2")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("1.234*interval '1 1:2:3' day to second ")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");

    // division operator
    expr("interval '1' month / 0.1")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("interval '1-2' year TO month / 0.1e-9")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    wholeExpr("1.234/interval '1 1:2:3' day to second")
        .fails("(?s).*Cannot apply '/' to arguments of type "
            + "'<DECIMAL.4, 3.> / <INTERVAL DAY TO SECOND>'.*");
  }

  @Test void testTimestampAddAndDiff() {
    List<String> tsi = ImmutableList.<String>builder()
        .add("FRAC_SECOND")
        .add("MICROSECOND")
        .add("MINUTE")
        .add("HOUR")
        .add("DAY")
        .add("WEEK")
        .add("MONTH")
        .add("QUARTER")
        .add("YEAR")
        .add("SQL_TSI_FRAC_SECOND")
        .add("SQL_TSI_MICROSECOND")
        .add("SQL_TSI_MINUTE")
        .add("SQL_TSI_HOUR")
        .add("SQL_TSI_DAY")
        .add("SQL_TSI_WEEK")
        .add("SQL_TSI_MONTH")
        .add("SQL_TSI_QUARTER")
        .add("SQL_TSI_YEAR")
        .build();

    List<String> functions = ImmutableList.<String>builder()
        .add("timestampadd(%s, 12, current_timestamp)")
        .add("timestampdiff(%s, current_timestamp, current_timestamp)")
        .build();

    for (String interval : tsi) {
      for (String function : functions) {
        expr(String.format(Locale.ROOT, function, interval)).ok();
      }
    }

    expr("timestampadd(SQL_TSI_WEEK, 2, current_timestamp)")
        .columnType("TIMESTAMP(0) NOT NULL");
    expr("timestampadd(SQL_TSI_WEEK, 2, cast(null as timestamp))")
        .columnType("TIMESTAMP(0)");
    expr("timestampdiff(SQL_TSI_WEEK, current_timestamp, current_timestamp)")
        .columnType("INTEGER NOT NULL");
    expr("timestampdiff(SQL_TSI_WEEK, cast(null as timestamp), current_timestamp)")
        .columnType("INTEGER");

    expr("timestampadd(^incorrect^, 1, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
    expr("timestampdiff(^incorrect^, current_timestamp, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
  }

  @Test void testTimestampAddNullInterval() {
    expr("timestampadd(SQL_TSI_SECOND, cast(NULL AS INTEGER),"
        + " current_timestamp)")
        .columnType("TIMESTAMP(0)");
    expr("timestampadd(SQL_TSI_DAY, cast(NULL AS INTEGER),"
        + " current_timestamp)")
        .columnType("TIMESTAMP(0)");
  }

  @Test void testNumericOperators() {
    // unary operator
    expr("- cast(1 as TINYINT)")
        .columnType("TINYINT NOT NULL");
    expr("+ cast(1 as INT)")
        .columnType("INTEGER NOT NULL");
    expr("- cast(1 as FLOAT)")
        .columnType("FLOAT NOT NULL");
    expr("+ cast(1 as DOUBLE)")
        .columnType("DOUBLE NOT NULL");
    expr("-1.643")
        .columnType("DECIMAL(4, 3) NOT NULL");
    expr("+1.643")
        .columnType("DECIMAL(4, 3) NOT NULL");

    // addition operator
    expr("cast(1 as TINYINT) + cast(5 as INTEGER)")
        .columnType("INTEGER NOT NULL");
    expr("cast(null as SMALLINT) + cast(5 as BIGINT)")
        .columnType("BIGINT");
    expr("cast(1 as REAL) + cast(5 as INTEGER)")
        .columnType("REAL NOT NULL");
    expr("cast(null as REAL) + cast(5 as DOUBLE)")
        .columnType("DOUBLE");
    expr("cast(null as REAL) + cast(5 as REAL)")
        .columnType("REAL");

    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as REAL)")
        .columnType("DOUBLE NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as DOUBLE)")
        .columnType("DOUBLE NOT NULL");
    expr("cast(null as DECIMAL(5, 2)) + cast(1 as DOUBLE)")
        .columnType("DOUBLE");

    expr("1.543 + 2.34")
        .columnType("DECIMAL(5, 3) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as BIGINT)")
        .columnType("DECIMAL(19, 2) NOT NULL");
    expr("cast(1 as NUMERIC(5, 2)) + cast(1 as INTEGER)")
        .columnType("DECIMAL(13, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) + cast(null as SMALLINT)")
        .columnType("DECIMAL(8, 2)");
    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as TINYINT)")
        .columnType("DECIMAL(6, 2) NOT NULL");

    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(5, 2))")
        .columnType("DECIMAL(6, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(6, 2))")
        .columnType("DECIMAL(7, 2) NOT NULL");
    expr("cast(1 as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(7, 4) NOT NULL");
    expr("cast(null as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(7, 4)");
    expr("cast(1 as DECIMAL(19, 2)) + cast(1 as DECIMAL(19, 2))")
        .columnType("DECIMAL(19, 2) NOT NULL");

    // subtraction operator
    expr("cast(1 as TINYINT) - cast(5 as BIGINT)")
        .columnType("BIGINT NOT NULL");
    expr("cast(null as INTEGER) - cast(5 as SMALLINT)")
        .columnType("INTEGER");
    expr("cast(1 as INTEGER) - cast(5 as REAL)")
        .columnType("REAL NOT NULL");
    expr("cast(null as REAL) - cast(5 as DOUBLE)")
        .columnType("DOUBLE");
    expr("cast(null as REAL) - cast(5 as REAL)")
        .columnType("REAL");

    expr("cast(1 as DECIMAL(5, 2)) - cast(1 as DOUBLE)")
        .columnType("DOUBLE NOT NULL");
    expr("cast(null as DOUBLE) - cast(1 as DECIMAL)")
        .columnType("DOUBLE");

    expr("1.543 - 24")
        .columnType("DECIMAL(14, 3) NOT NULL");
    expr("cast(1 as DECIMAL(5)) - cast(1 as BIGINT)")
        .columnType("DECIMAL(19, 0) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) - cast(1 as INTEGER)")
        .columnType("DECIMAL(13, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) - cast(null as SMALLINT)")
        .columnType("DECIMAL(8, 2)");
    expr("cast(1 as DECIMAL(5, 2)) - cast(1 as TINYINT)")
        .columnType("DECIMAL(6, 2) NOT NULL");

    expr("cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(7))")
        .columnType("DECIMAL(10, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(6, 2))")
        .columnType("DECIMAL(7, 2) NOT NULL");
    expr("cast(1 as DECIMAL(4, 2)) - cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(7, 4) NOT NULL");
    expr("cast(null as DECIMAL) - cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(19, 4)");
    expr("cast(1 as DECIMAL(19, 2)) - cast(1 as DECIMAL(19, 2))")
        .columnType("DECIMAL(19, 2) NOT NULL");

    // multiply operator
    expr("cast(1 as TINYINT) * cast(5 as INTEGER)")
        .columnType("INTEGER NOT NULL");
    expr("cast(null as SMALLINT) * cast(5 as BIGINT)")
        .columnType("BIGINT");
    expr("cast(1 as REAL) * cast(5 as INTEGER)")
        .columnType("REAL NOT NULL");
    expr("cast(null as REAL) * cast(5 as DOUBLE)")
        .columnType("DOUBLE");

    expr("cast(1 as DECIMAL(7, 3)) * 1.654")
        .columnType("DECIMAL(11, 6) NOT NULL");
    expr("cast(null as DECIMAL(7, 3)) * cast (1.654 as DOUBLE)")
        .columnType("DOUBLE");

    expr("cast(null as DECIMAL(5, 2)) * cast(1 as BIGINT)")
        .columnType("DECIMAL(19, 2)");
    expr("cast(1 as DECIMAL(5, 2)) * cast(1 as INTEGER)")
        .columnType("DECIMAL(15, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) * cast(1 as SMALLINT)")
        .columnType("DECIMAL(10, 2) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) * cast(1 as TINYINT)")
        .columnType("DECIMAL(8, 2) NOT NULL");

    expr("cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(5, 2))")
        .columnType("DECIMAL(10, 4) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(6, 2))")
        .columnType("DECIMAL(11, 4) NOT NULL");
    expr("cast(1 as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(10, 6) NOT NULL");
    expr("cast(null as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(10, 6)");
    expr("cast(1 as DECIMAL(4, 10)) * cast(null as DECIMAL(6, 10))")
        .columnType("DECIMAL(10, 19)");
    expr("cast(1 as DECIMAL(19, 2)) * cast(1 as DECIMAL(19, 2))")
        .columnType("DECIMAL(19, 4) NOT NULL");

    // divide operator
    expr("cast(1 as TINYINT) / cast(5 as INTEGER)")
        .columnType("INTEGER NOT NULL");
    expr("cast(null as SMALLINT) / cast(5 as BIGINT)")
        .columnType("BIGINT");
    expr("cast(1 as REAL) / cast(5 as INTEGER)")
        .columnType("REAL NOT NULL");
    expr("cast(null as REAL) / cast(5 as DOUBLE)")
        .columnType("DOUBLE");
    expr("cast(1 as DECIMAL(7, 3)) / 1.654")
        .columnType("DECIMAL(15, 8) NOT NULL");
    expr("cast(null as DECIMAL(7, 3)) / cast (1.654 as DOUBLE)")
        .columnType("DOUBLE");

    expr("cast(null as DECIMAL(5, 2)) / cast(1 as BIGINT)")
        .columnType("DECIMAL(19, 16)");
    expr("cast(1 as DECIMAL(5, 2)) / cast(1 as INTEGER)")
        .columnType("DECIMAL(16, 13) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) / cast(1 as SMALLINT)")
        .columnType("DECIMAL(11, 8) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) / cast(1 as TINYINT)")
        .columnType("DECIMAL(9, 6) NOT NULL");

    expr("cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(5, 2))")
        .columnType("DECIMAL(13, 8) NOT NULL");
    expr("cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(6, 2))")
        .columnType("DECIMAL(14, 9) NOT NULL");
    expr("cast(1 as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(15, 9) NOT NULL");
    expr("cast(null as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))")
        .columnType("DECIMAL(15, 9)");
    expr("cast(1 as DECIMAL(4, 10)) / cast(null as DECIMAL(6, 19))")
        .columnType("DECIMAL(19, 6)");
    expr("cast(1 as DECIMAL(19, 2)) / cast(1 as DECIMAL(19, 2))")
        .columnType("DECIMAL(19, 0) NOT NULL");
    expr("4/3")
        .columnType("INTEGER NOT NULL");
    expr("-4.0/3")
        .columnType("DECIMAL(13, 12) NOT NULL");
    expr("4/3.0")
        .columnType("DECIMAL(17, 6) NOT NULL");
    expr("cast(2.3 as float)/3")
        .columnType("FLOAT NOT NULL");
    // null
    expr("cast(2.3 as float)/null")
        .columnType("FLOAT");
  }

  @Test void testFloorCeil() {
    expr("floor(cast(null as tinyint))")
        .columnType("TINYINT");
    expr("floor(1.2)")
        .columnType("DECIMAL(2, 0) NOT NULL");
    expr("floor(1)")
        .columnType("INTEGER NOT NULL");
    expr("floor(1.2e-2)")
        .columnType("DOUBLE NOT NULL");
    expr("floor(interval '2' day)")
        .columnType("INTERVAL DAY NOT NULL");

    expr("ceil(cast(null as bigint))")
        .columnType("BIGINT");
    expr("ceil(1.2)")
        .columnType("DECIMAL(2, 0) NOT NULL");
    expr("ceil(1)")
        .columnType("INTEGER NOT NULL");
    expr("ceil(1.2e-2)")
        .columnType("DOUBLE NOT NULL");
    expr("ceil(interval '2' second)")
        .columnType("INTERVAL SECOND NOT NULL");
  }

  public void checkWinFuncExpWithWinClause(
      String sql,
      String expectedMsgPattern) {
    winExp(sql).fails(expectedMsgPattern);
  }

  // test window partition clause. See SQL 2003 specification for detail
  public void _testWinPartClause() {
    win("window w as (w2 order by deptno), w2 as (^rang^e 100 preceding)")
        .fails("Referenced window cannot have framing declarations");
    // Test specified collation, window clause syntax rule 4,5.
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-820">[CALCITE-820]
   * Validate that window functions have OVER clause</a>, and
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1340">[CALCITE-1340]
   * Window aggregates give invalid errors</a>. */
  @Test void testWindowFunctionsWithoutOver() {
    winSql("select sum(empno)\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by ^row_number()^")
        .fails("OVER clause is necessary for window functions");

    winSql("select ^rank()^\n"
        + "from emp")
        .fails("OVER clause is necessary for window functions");

    // With [CALCITE-1340], the validator would see RANK without OVER,
    // mistakenly think this is an aggregating query, and wrongly complain
    // about the PARTITION BY: "Expression 'DEPTNO' is not being grouped"
    winSql("select cume_dist() over w , ^rank()^\n"
        + "from emp\n"
        + "window w as (partition by deptno order by deptno)")
        .fails("OVER clause is necessary for window functions");

    winSql("select ^nth_value(sal, 2)^\n"
        + "from emp")
        .fails("OVER clause is necessary for window functions");
  }

  @Test void testOverInPartitionBy() {
    winSql("select sum(deptno) over ^(partition by sum(deptno)\n"
        + "over(order by deptno))^ from emp")
        .fails("PARTITION BY expression should not contain OVER clause");

    winSql("select sum(deptno) over w\n"
        + "from emp\n"
        + "window w as ^(partition by sum(deptno) over(order by deptno))^")
        .fails("PARTITION BY expression should not contain OVER clause");
  }

  @Test void testOverInOrderBy() {
    winSql("select sum(deptno) over ^(order by sum(deptno)\n"
        + "over(order by deptno))^ from emp")
        .fails("ORDER BY expression should not contain OVER clause");

    winSql("select sum(deptno) over w\n"
        + "from emp\n"
        + "window w as ^(order by sum(deptno) over(order by deptno))^")
        .fails("ORDER BY expression should not contain OVER clause");
  }

  @Test void testAggregateFunctionInOver() {
    final String sql = "select sum(deptno) over (order by count(empno))\n"
        + "from emp\n"
        + "group by deptno";
    winSql(sql).ok();
    final String sql2 = "select sum(^empno^) over (order by count(empno))\n"
        + "from emp\n"
        + "group by deptno";
    winSql(sql2).fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testAggregateInsideOverClause() {
    final String sql = "select ^empno^,\n"
        + "  sum(empno) over (partition by min(sal)) empno_sum\n"
        + "from emp";
    sql(sql).fails("Expression 'EMPNO' is not being grouped");

    final String sql2 = "select ^empno^,\n"
        + "  sum(empno) over (partition by min(sal)) empno_sum\n"
        + "from emp\n"
        + "group by empno";
    sql(sql2).ok();
  }

  @Test void testAggregateInsideOverClause2() {
    final String sql = "select ^empno^,\n"
        + "  sum(empno) over ()\n"
        + "  + sum(empno) over (partition by min(sal)) empno_sum\n"
        + "from emp";
    sql(sql).fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testWindowFunctions() {
    // SQL 03 Section 6.10

    // Window functions may only appear in the <select list> of a
    // <query specification> or <select statement: single row>,
    // or the <order by clause> of a simple table query.
    // See 4.15.3 for detail
    winSql("select *\n"
        + " from emp\n"
        + " where ^sum(sal) over (partition by deptno\n"
        + "    order by empno\n"
        + "    rows 3 preceding)^ > 10")
        .fails("Windowed aggregate expression is illegal in WHERE clause");

    winSql("select *\n"
        + " from emp\n"
        + " group by ename, ^sum(sal) over (partition by deptno\n"
        + "    order by empno\n"
        + "    rows 3 preceding)^ + 10\n"
        + "order by deptno")
        .fails("Windowed aggregate expression is illegal in GROUP BY clause");

    winSql("select *\n"
        + " from emp\n"
        + " join dept on emp.deptno = dept.deptno\n"
        + " and ^sum(sal) over (partition by emp.deptno\n"
        + "    order by empno\n"
        + "    rows 3 preceding)^ = dept.deptno + 40\n"
        + "order by deptno")
        .fails("Windowed aggregate expression is illegal in ON clause");

    // rule 3, a)
    winSql("select sal from emp\n"
        + "order by sum(sal) over (partition by deptno order by deptno)")
        .ok();

    // scope reference

    // rule 4,
    // valid window functions
    winExp("sum(sal)").ok();
  }

  @Test void testWindowFunctions2() {
    List<String> defined =
        asList("CUME_DIST", "DENSE_RANK", "PERCENT_RANK", "RANK",
            "ROW_NUMBER");
    if (Bug.TODO_FIXED) {
      sql("select rank() over (order by deptno) from emp")
          .columnType("INTEGER NOT NULL");
    }
    winSql("select rank() over w from emp\n"
        + "window w as ^(partition by sal)^, w2 as (w order by deptno)")
        .fails(RANK_REQUIRES_ORDER_BY);
    winSql("select rank() over w2 from emp\n"
        + "window w as (partition by sal), w2 as (w order by deptno)").ok();

    // row_number function
    winExp("row_number() over (order by deptno)").ok();
    winExp("row_number() over (partition by deptno)").ok();
    winExp("row_number() over ()").ok();
    winExp("row_number() over (order by deptno ^rows^ 2 preceding)")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
    winExp("row_number() over (order by deptno ^range^ 2 preceding)")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);

    // rank function type
    if (defined.contains("DENSE_RANK")) {
      winExp("^dense_rank()^")
          .fails("OVER clause is necessary for window functions");
    } else {
      checkWinFuncExpWithWinClause("^dense_rank()^",
          "Function 'DENSE_RANK\\(\\)' is not defined");
    }
    winExp("rank() over (order by empno)").ok();
    winExp("percent_rank() over (order by empno)").ok();
    winExp("cume_dist() over (order by empno)").ok();
    winExp("nth_value(sal, 2) over (order by empno)").ok();

    // rule 6a
    // ORDER BY required with RANK & DENSE_RANK
    winSql("select rank() over ^(partition by deptno)^ from emp")
        .fails(RANK_REQUIRES_ORDER_BY);
    winSql("select dense_rank() over ^(partition by deptno)^ from emp ")
        .fails(RANK_REQUIRES_ORDER_BY);
    winSql("select rank() over w from emp window w as ^(partition by deptno)^")
        .fails(RANK_REQUIRES_ORDER_BY);
    winSql("select dense_rank() over w from emp\n"
        + "window w as ^(partition by deptno)^")
        .fails(RANK_REQUIRES_ORDER_BY);

    // rule 6b
    // Framing not allowed with RANK & DENSE_RANK functions
    // window framing defined in window clause
    winSql("select rank() over w from emp\n"
        + "window w as (order by empno ^rows^ 2 preceding )")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
    winSql("select dense_rank() over w from emp\n"
        + "window w as (order by empno ^rows^ 2 preceding)")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
    if (defined.contains("PERCENT_RANK")) {
      winSql("select percent_rank() over w from emp\n"
          + "window w as (order by empno)")
          .ok();
      winSql("select percent_rank() over w from emp\n"
          + "window w as (order by empno ^rows^ 2 preceding)")
          .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
      winSql("select percent_rank() over w from emp\n"
          + "window w as ^(partition by empno)^")
          .fails(RANK_REQUIRES_ORDER_BY);
    } else {
      checkWinFuncExpWithWinClause("^percent_rank()^",
          "Function 'PERCENT_RANK\\(\\)' is not defined");
    }
    if (defined.contains("CUME_DIST")) {
      winSql("select cume_dist() over w from emp\n"
          + "window w as ^(rows 2 preceding)^")
          .fails(RANK_REQUIRES_ORDER_BY);
      winSql("select cume_dist() over w from emp\n"
          + "window w as (order by empno ^rows^ 2 preceding)")
          .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
      winSql("select cume_dist() over w from emp window w as (order by empno)")
          .ok();
      winSql("select cume_dist() over (order by empno) from emp ").ok();
    } else {
      checkWinFuncExpWithWinClause("^cume_dist()^",
          "Function 'CUME_DIST\\(\\)' is not defined");
    }
    // window framing defined in in-line window
    winSql("select rank() over (order by empno ^range^ 2 preceding ) from emp ")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
    winSql("select dense_rank() over (order by empno ^rows^ 2 preceding ) from emp ")
        .fails(ROW_RANGE_NOT_ALLOWED_WITH_RANK);
    if (defined.contains("PERCENT_RANK")) {
      winSql("select percent_rank() over (order by empno) from emp").ok();
    }

    // invalid column reference
    checkWinFuncExpWithWinClause("sum(^invalidColumn^)",
        "Column 'INVALIDCOLUMN' not found in any table");

    // invalid window functions
    checkWinFuncExpWithWinClause("^invalidFun(sal)^",
        "No match found for function signature INVALIDFUN\\(<NUMERIC>\\)");

    // 6.10 rule 10. no distinct allowed aggregate function
    // Fails in parser.
    // checkWinFuncExpWithWinClause(" sum(distinct sal) over w ", null);

    // 7.11 rule 10c
    winSql("select sum(sal) over (w partition by ^deptno^)\n"
        + " from emp window w as (order by empno rows 2 preceding )")
        .fails("PARTITION BY not allowed with existing window reference");

    // 7.11 rule 10d
    winSql("select sum(sal) over (w order by ^empno^)\n"
        + " from emp window w as (order by empno rows 2 preceding )")
        .fails("ORDER BY not allowed in both base and referenced windows");

    // 7.11 rule 10e
    winSql("select sum(sal) over (w)\n"
        + " from emp window w as (order by empno ^rows^ 2 preceding )")
        .fails("Referenced window cannot have framing declarations");

    // Empty window is OK for functions that don't require ordering.
    winSql("select sum(sal) over () from emp").ok();
    winSql("select sum(sal) over w from emp window w as ()").ok();
    winSql("select count(*) over () from emp").ok();
    winSql("select count(*) over w from emp window w as ()").ok();
    winSql("select rank() over ^()^ from emp")
        .fails(RANK_REQUIRES_ORDER_BY);
    winSql("select rank() over w from emp window w as ^()^")
        .fails(RANK_REQUIRES_ORDER_BY);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-883">[CALCITE-883]
   * Give error if the aggregate function don't support null treatment</a>. */
  @Test void testWindowFunctionsIgnoreNulls() {
    winSql("select lead(sal, 4) over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select lead(sal, 4) IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select lag(sal, 4) over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select lag(sal, 4) IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select first_value(sal) over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select first_value(sal) IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select last_value(sal) over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select last_value(sal) IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)").ok();

    winSql("select ^sum(sal)^ IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'SUM'");

    winSql("select ^count(sal)^ IGNORE NULLS over (w)\n"
        + " from emp window w as (order by empno)")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'COUNT'");

    winSql("select ^avg(sal)^ IGNORE NULLS\n"
        + " from emp")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'AVG'");

    winSql("select ^abs(sal)^ IGNORE NULLS\n"
        + " from emp")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'ABS'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-883">[CALCITE-883]
   * Give error if the aggregate function don't support null treatment</a>. */
  @Test void testWindowFunctionsRespectNulls() {
    winSql("select lead(sal, 4) over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select lead(sal, 4) RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select lag(sal, 4) over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select lag(sal, 4) RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select first_value(sal) over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select first_value(sal) RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select last_value(sal) over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select last_value(sal) RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)").ok();

    winSql("select ^sum(sal)^ RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'SUM'");

    winSql("select ^count(sal)^ RESPECT NULLS over (w)\n"
        + "from emp window w as (order by empno)")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'COUNT'");

    winSql("select ^avg(sal)^ RESPECT NULLS\n"
        + "from emp")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'AVG'");

    winSql("select ^abs(sal)^ RESPECT NULLS\n"
        + "from emp")
        .fails("Cannot specify IGNORE NULLS or RESPECT NULLS following 'ABS'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1954">[CALCITE-1954]
   * Column from outer join should be null, whether or not it is aliased</a>. */
  @Test void testLeftOuterJoinWithAlias() {
    final String query = "select *\n"
        + "from (select row_number() over (order by sal) from emp) as emp1(r1)\n"
        + "left outer join\n"
        + "(select  dense_rank() over(order by sal) from emp) as emp2(r2)\n"
        + "on (emp1.r1 = emp2.r2)";
    // In this case, R2 is nullable in the join since we have a left outer join.
    final String type = "RecordType(BIGINT NOT NULL R1, BIGINT R2) NOT NULL";
    sql(query).type(type);

    // Similar query, without "AS t(c)"
    final String query2 = "select *\n"
        + "from (select row_number() over (order by sal) as r1 from emp) as emp1\n"
        + "left outer join\n"
        + "(select dense_rank() over(order by sal) as r2 from emp) as emp2\n"
        + "on (emp1.r1 = emp2.r2)";
    sql(query2).type(type);

    // Similar query, without "AS t"
    final String query3 = "select *\n"
        + "from (select row_number() over (order by sal) as r1 from emp)\n"
        + "left outer join\n"
        + "(select dense_rank() over(order by sal) as r2 from emp)\n"
        + "on r1 = r2";
    sql(query3).type(type);
  }

  @Test void testInvalidWindowFunctionWithGroupBy() {
    sql("select max(^empno^) over () from emp\n"
        + "group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");

    sql("select max(deptno) over (partition by ^empno^) from emp\n"
        + "group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");

    sql("select rank() over (order by ^empno^) from emp\n"
        + "group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testInlineWinDef() {
    // the <window specification> used by windowed agg functions is
    // fully defined in SQL 03 Std. section 7.1 <window clause>
    sql("select sum(sal) over (partition by deptno order by empno)\n"
        + "from emp order by empno").ok();
    winExp2("sum(sal) OVER ("
        + "partition by deptno "
        + "order by empno "
        + "rows 2 preceding )").ok();
    winExp2("sum(sal) OVER ("
        + "order by 1 "
        + "rows 2 preceding )").ok();
    winExp2("sum(sal) OVER ("
        + "order by 'b' "
        + "rows 2 preceding )").ok();
    winExp2("sum(sal) over ("
        + "partition by deptno "
        + "order by 1+1 rows 26 preceding)").ok();
    winExp2("sum(sal) over (order by deptno rows unbounded preceding)").ok();
    winExp2("sum(sal) over (order by deptno rows current row)").ok();
    winExp2("sum(sal) over ^("
        + "order by deptno "
        + "rows between unbounded preceding and unbounded following)^").ok();
    winExp2("sum(sal) over ^("
        + "order by deptno "
        + "rows between CURRENT ROW and unbounded following)^").ok();
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between unbounded preceding and CURRENT ROW)").ok();

    // logical current row/current row
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between CURRENT ROW and CURRENT ROW)").ok();

    // physical current row/current row
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "range between CURRENT ROW and CURRENT ROW)").ok();

    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between 2 preceding and CURRENT ROW)").ok();
    winExp("sum(sal) OVER (w "
        + "rows 2 preceding )").ok();
    winExp2("sum(sal) over (order by deptno range 2.0 preceding)").ok();

    // Failure mode tests
    winExp2("sum(sal) over (order by deptno "
        + "rows between ^UNBOUNDED FOLLOWING^ and unbounded preceding)")
        .fails("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between 2 preceding and ^UNBOUNDED PRECEDING^)")
        .fails("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between CURRENT ROW and ^2 preceding^)")
        .fails("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between 2 following and ^CURRENT ROW^)")
        .fails("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "rows between 2 following and ^2 preceding^)")
        .fails("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
    winExp2("sum(sal) over ("
        + "order by deptno "
        + "RANGE BETWEEN ^INTERVAL '1' SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)")
        .fails("Data Type mismatch between ORDER BY and RANGE clause");
    winExp2("sum(sal) over ("
        + "order by empno "
        + "RANGE BETWEEN ^INTERVAL '1' SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)")
        .fails("Data Type mismatch between ORDER BY and RANGE clause");
    winExp2("sum(sal) over (order by deptno, empno ^range^ 2 preceding)")
        .fails("RANGE clause cannot be used with compound ORDER BY clause");
    winExp2("sum(sal) over ^(partition by deptno range 5 preceding)^")
        .fails("Window specification must contain an ORDER BY clause");
    winExp2("sum(sal) over ^w1^")
        .fails("Window 'W1' not found");
    winExp2("sum(sal) OVER (^w1^ "
        + "partition by deptno "
        + "order by empno "
        + "rows 2 preceding )")
        .fails("Window 'W1' not found");
  }

  @Test void testPartitionByExpr() {
    winExp2("sum(sal) over (partition by empno + deptno order by empno range 5 preceding)")
        .ok();

    winSql("select "
        + "sum(sal) over (partition by ^empno + ename^ order by empno range 5 preceding)"
        + " from emp")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <VARCHAR\\(20\\)>'.*");

    winExp2("sum(sal) over (partition by empno + ename order by empno range 5 preceding)");
  }

  @Test void testWindowClause() {
    // -----------------------------------
    // --   positive testings           --
    // -----------------------------------
    // correct syntax:
    winExp("sum(sal) as sumsal").ok();
    win("window w as (partition by sal order by deptno rows 2 preceding)")
        .ok();

    // define window on an existing window
    win("window w as (order by sal), w1 as (w)").ok();

    // -----------------------------------
    // --   negative testings           --
    // -----------------------------------
    // Test fails in parser
    // checkWinClauseExp("window foo.w as (range 100 preceding) "+
    //    "Window name must be a simple identifier\");

    // rule 11
    // a)
    // missing window order clause.
    win("window w as ^(range 100 preceding)^")
        .fails("Window specification must contain an ORDER BY clause");

    // order by number
    win("window w as (order by sal range 100 preceding)").ok();

    // order by date
    win("window w as (order by hiredate range ^100^ preceding)")
        .fails("Data Type mismatch between ORDER BY and RANGE clause");

    // order by string, should fail
    win("window w as (order by ename range ^100^ preceding)")
        .fails("Data type of ORDER BY prohibits use of RANGE clause");
    // todo: interval test ???

    // b)
    // valid
    win("window w as (rows 2 preceding)").ok();

    // invalid tests exact numeric for the unsigned value specification The
    // following two test fail as they should but in the parser: JR not
    // anymore now the validator kicks out
    win("window w as (rows ^-2.5^ preceding)")
        .fails("ROWS value must be a non-negative integral constant");
    win("window w as (rows ^-2^ preceding)")
        .fails("ROWS value must be a non-negative integral constant");

    // This test should fail as per 03 Std. but we pass it and plan
    // to apply the FLOOR function before window processing
    win("window w as (rows ^2.5^ preceding)")
        .fails("ROWS value must be a non-negative integral constant");

    // -----------------------------------
    // --   negative testings           --
    // -----------------------------------
    // reference undefined xyz column
    win("window w as (partition by ^xyz^)")
        .fails("Column 'XYZ' not found in any table");

    // window definition is empty when applied to unsorted table
    win("window w as ^( /* boo! */  )^").ok();

    // duplicate window name
    win("window w as (order by empno), ^w^ as (order by empno)")
        .fails("Duplicate window names not allowed");
    win("window win1 as (order by empno), ^win1^ as (order by empno)")
        .fails("Duplicate window names not allowed");

    // syntax rule 6
    sql("select min(sal) over (order by deptno) from emp group by deptno,sal")
        .ok();
    sql("select min(sal) over (order by ^deptno^) from emp group by sal")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select min(sal) over\n"
        + "(partition by comm order by deptno) from emp group by deptno,sal,comm")
        .ok();
    sql("select min(sal) over\n"
        + "(partition by ^comm^ order by deptno) from emp group by deptno,sal")
        .fails("Expression 'COMM' is not being grouped");

    // syntax rule 7
    win("window w as ^(order by rank() over (order by sal))^")
        .fails("ORDER BY expression should not contain OVER clause");

    // ------------------------------------
    // ---- window frame between tests ----
    // ------------------------------------
    // bound 1 shall not specify UNBOUNDED FOLLOWING
    win("window w as (rows between ^unbounded following^ and 5 following)")
        .fails("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");

    // bound 2 shall not specify UNBOUNDED PRECEDING
    win("window w as ("
        + "order by deptno "
        + "rows between 2 preceding and ^UNBOUNDED PRECEDING^)")
        .fails("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
    win("window w as ("
        + "order by deptno "
        + "rows between 2 following and ^2 preceding^)")
        .fails("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
    win("window w as ("
        + "order by deptno "
        + "rows between CURRENT ROW and ^2 preceding^)")
        .fails("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
    win("window w as ("
        + "order by deptno "
        + "rows between 2 following and ^CURRENT ROW^)")
        .fails("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");

    // Sql '03 rule 10 c) assertExceptionIsThrown("select deptno as d, sal
    // as s from emp window w as (partition by deptno order by sal), w2 as
    // (w partition by deptno)", null); checkWinClauseExp("window w as
    // (partition by sal order by deptno), w2 as (w partition by sal)",
    // null); d) valid because existing window does not have an ORDER BY
    // clause
    win("window w as (w2 range 2 preceding ), w2 as (order by sal)").ok();
    win("window w as ^(partition by sal)^, w2 as (w order by deptno)").ok();
    win("window w as (w2 partition by ^sal^), w2 as (order by deptno)")
        .fails("PARTITION BY not allowed with existing window reference");
    win("window w as (partition by sal order by deptno), w2 as (w order by ^deptno^)")
        .fails("ORDER BY not allowed in both base and referenced windows");

    // e)
    win("window w as (w2 order by deptno), w2 as (^range^ 100 preceding)")
        .fails("Referenced window cannot have framing declarations");

    // rule 12, todo: test scope of window assertExceptionIsThrown("select
    // deptno as d from emp window d as (partition by deptno)", null);

    // rule 13
    win("window w as (order by sal)").ok();
    win("window w as (order by ^non_exist_col^)")
        .fails("Column 'NON_EXIST_COL' not found in any table");
    win("window w as (partition by ^non_exist_col^ order by sal)")
        .fails("Column 'NON_EXIST_COL' not found in any table");
  }

  @Test void testWindowClause2() {
    // 7.10 syntax rule 2 <new window name> NWN1 shall not be contained in
    // the scope of another <new window name> NWN2 such that NWN1 and NWN2
    // are equivalent.
    win("window\n"
        + "w  as (partition by deptno order by empno rows 2 preceding),\n"
        + "w2 as ^(partition by deptno order by empno rows 2 preceding)^\n")
        .fails("Duplicate window specification not allowed in the same window clause");
  }

  @Test void testWindowClauseWithSubQuery() {
    sql("select * from\n"
        + "( select sum(empno) over w, sum(deptno) over w from emp\n"
        + "window w as (order by hiredate range interval '1' minute preceding))").ok();

    sql("select * from\n"
        + "( select sum(empno) over w, sum(deptno) over w, hiredate from emp)\n"
        + "window w as (order by hiredate range interval '1' minute preceding)").ok();

    sql("select * from\n"
        + "( select sum(empno) over w, sum(deptno) over w from emp)\n"
        + "window w as (order by ^hiredate^ range interval '1' minute preceding)")
        .fails("Column 'HIREDATE' not found in any table");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-754">[CALCITE-754]
   * Validator error when resolving OVER clause of JOIN query</a>. */
  @Test void testPartitionByColumnInJoinAlias() {
    sql("select sum(1) over(partition by t1.ename)\n"
        + "from emp t1, emp t2")
        .ok();
    sql("select sum(1) over(partition by emp.ename)\n"
        + "from emp, dept")
        .ok();
    sql("select sum(1) over(partition by ^deptno^)\n"
        + "from emp, dept")
        .fails("Column 'DEPTNO' is ambiguous");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1535">[CALCITE-1535]
   * Give error if column referenced in ORDER BY is ambiguous</a>. */
  @Test void testOrderByColumn() {
    sql("select emp.deptno from emp, dept order by emp.deptno")
        .ok();
    // Not ambiguous. There are two columns which could be referenced as
    // "deptno", but the one in the SELECT clause takes priority.
    sql("select emp.deptno from emp, dept order by deptno")
        .ok();
    sql("select emp.deptno as deptno from emp, dept order by deptno")
        .ok();
    sql("select emp.empno as deptno from emp, dept order by deptno")
        .ok();
    sql("select emp.deptno as n, dept.deptno as n from emp, dept order by ^n^")
        .fails("Column 'N' is ambiguous");
    sql("select emp.empno as deptno, dept.deptno from emp, dept\n"
        + "order by ^deptno^")
        .fails("Column 'DEPTNO' is ambiguous");
    sql("select emp.empno as deptno, dept.deptno from emp, dept\n"
        + "order by emp.deptno")
        .ok();
    sql("select emp.empno as deptno, dept.deptno from emp, dept order by 1, 2")
        .ok();
    sql("select empno as \"deptno\", deptno from emp order by deptno")
        .ok();
    sql("select empno as \"deptno\", deptno from emp order by \"deptno\"")
        .ok();
  }

  @Test void testWindowNegative() {
    // Do not fail when window has negative size. Allow
    final String negSize = null;
    checkNegWindow("rows between 2 preceding and 4 preceding", negSize);
    checkNegWindow("rows between 2 preceding and 3 preceding", negSize);
    checkNegWindow("rows between 2 preceding and 2 preceding", null);
    checkNegWindow("rows between unbounded preceding and current row",
        null);
    // Unbounded following IS supported
    final String unboundedFollowing =
        null;
    checkNegWindow("rows between unbounded preceding and unbounded following",
        unboundedFollowing);
    checkNegWindow("rows between current row and unbounded following",
        unboundedFollowing);
    checkNegWindow("rows between current row and 2 following", null);
    checkNegWindow("range between 2 preceding and 2 following", null);
    checkNegWindow("range between 2 preceding and -2 preceding", null);
    checkNegWindow("range between 4 following and 3 following", negSize);
    checkNegWindow("range between 4 following and 5 following", null);
    checkNegWindow("rows between 1 following and 0 following", negSize);
    checkNegWindow("rows between 0 following and 0 following", null);
  }

  private void checkNegWindow(String s, String msg) {
    String sql =
        "select sum(deptno) over ^(order by empno "
            + s
            + ")^ from emp";
    sql(sql).failsIf(msg != null, msg);
  }

  @Test void testWindowPartial() {
    sql("select sum(deptno) over (\n"
        + "order by deptno, empno rows 2 preceding disallow partial)\n"
        + "from emp").ok();

    // cannot do partial over logical window
    sql("select sum(deptno) over (\n"
        + "  partition by deptno\n"
        + "  order by empno\n"
        + "  range between 2 preceding and 3 following\n"
        + "  ^disallow partial^)\n"
        + "from emp")
        .fails("Cannot use DISALLOW PARTIAL with window based on RANGE");
  }

  @Test void testOneWinFunc() {
    win("window w as (partition by sal order by deptno rows 2 preceding)")
        .ok();
  }

  @Test void testNameResolutionInValuesClause() {
    final String emps =
        "(select 1 as empno, 'x' as name, 10 as deptno, 'M' as gender, 'San Francisco' as city, 30 as empid, 25 as age from (values (1)))";
    final String depts =
        "(select 10 as deptno, 'Sales' as name from (values (1)))";

    sql("select * from " + emps + " join " + depts + "\n"
        + " on ^emps^.deptno = deptno")
        .fails("Table 'EMPS' not found");

    // this is ok
    sql("select * from " + emps + " as e\n"
        + " join " + depts + " as d\n"
        + " on e.deptno = d.deptno").ok();

    // fail: ambiguous column in WHERE
    sql("select * from " + emps + " as emps,\n"
        + " " + depts + "\n"
        + "where ^deptno^ > 5")
        .fails("Column 'DEPTNO' is ambiguous");

    // fail: ambiguous column reference in ON clause
    sql("select * from " + emps + " as e\n"
        + " join " + depts + " as d\n"
        + " on e.deptno = ^deptno^")
        .fails("Column 'DEPTNO' is ambiguous");

    // ok: column 'age' is unambiguous
    sql("select * from " + emps + " as e\n"
        + " join " + depts + " as d\n"
        + " on e.deptno = age").ok();

    // ok: reference to derived column
    sql("select * from " + depts + "\n"
        + " join (select mod(age, 30) as agemod from " + emps + ")\n"
        + "on deptno = agemod").ok();

    // fail: deptno is ambiguous
    sql("select name from " + depts + "\n"
        + "join (select mod(age, 30) as agemod, deptno from " + emps + ")\n"
        + "on ^deptno^ = agemod")
        .fails("Column 'DEPTNO' is ambiguous");

    // fail: lateral reference
    sql("select * from " + emps + " as e,\n"
        + " (select 1, ^e^.deptno from (values(true))) as d")
        .fails("Table 'E' not found");

    // fail: deptno is ambiguous
    sql("select ^deptno^, name from " + depts + "as d\n"
        + "join " + emps +  "as e\n"
        + "on d.deptno = e.deptno")
        .fails("Column 'DEPTNO' is ambiguous");
  }

  @Test void testNestedFrom() {
    sql("values (true)")
        .columnType("BOOLEAN NOT NULL");
    sql("select * from (values(true))")
        .columnType("BOOLEAN NOT NULL");
    sql("select * from (select * from (values(true)))")
        .columnType("BOOLEAN NOT NULL");
    sql("select * from (select * from (select * from (values(true))))")
        .columnType("BOOLEAN NOT NULL");
    sql("select * from ("
        + "  select * from ("
        + "    select * from (values(true))"
        + "    union"
        + "    select * from (values (false)))"
        + "  except"
        + "  select * from (values(true)))")
        .columnType("BOOLEAN NOT NULL");
  }

  @Test void testAmbiguousColumn() {
    sql("select * from emp join dept\n"
        + " on emp.deptno = ^deptno^")
        .fails("Column 'DEPTNO' is ambiguous");

    // this is ok
    sql("select * from emp as e\n"
        + " join dept as d\n"
        + " on e.deptno = d.deptno").ok();

    // fail: ambiguous column in WHERE
    sql("select * from emp as emps, dept\n"
        + "where ^deptno^ > 5")
        .fails("Column 'DEPTNO' is ambiguous");

    // fail: alias 'd' obscures original table name 'dept'
    sql("select * from emp as emps, dept as d\n"
        + "where ^dept^.deptno > 5")
        .fails("Table 'DEPT' not found");

    // fail: ambiguous column reference in ON clause
    sql("select * from emp as e\n"
        + " join dept as d\n"
        + " on e.deptno = ^deptno^")
        .fails("Column 'DEPTNO' is ambiguous");

    // ok: column 'comm' is unambiguous
    sql("select * from emp as e\n"
        + " join dept as d\n"
        + " on e.deptno = comm").ok();

    // ok: reference to derived column
    sql("select * from dept\n"
        + " join (select mod(comm, 30) as commmod from emp)\n"
        + "on deptno = commmod").ok();

    // fail: deptno is ambiguous
    sql("select name from dept\n"
        + "join (select mod(comm, 30) as commmod, deptno from emp)\n"
        + "on ^deptno^ = commmod")
        .fails("Column 'DEPTNO' is ambiguous");

    // fail: lateral reference
    sql("select * from emp as e,\n"
        + " (select 1, ^e^.deptno from (values(true))) as d")
        .fails("Table 'E' not found");
  }

  @Test void testExpandStar() {
    // dtbug 282 -- "select r.* from sales.depts" gives NPE.
    // dtbug 318 -- error location should be ^r^ not ^r.*^.
    sql("select ^r^.* from dept")
        .fails("Unknown identifier 'R'");

    sql("select e.* from emp as e").ok();
    sql("select emp.* from emp").ok();

    // Error message could be better (EMPNO does exist, but it's a column).
    sql("select ^empno^ .  * from emp")
        .fails("Not a record type. The '\\*' operator requires a record");
    sql("select ^emp.empno^ .  * from emp")
        .fails("Not a record type. The '\\*' operator requires a record");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-546">[CALCITE-546]
   * Allow table, column and field called '*'</a>.
   */
  @Test void testStarIdentifier() {
    sql("SELECT * FROM (VALUES (0, 0)) AS T(A, \"*\")")
        .type("RecordType(INTEGER NOT NULL A, INTEGER NOT NULL *) NOT NULL");
  }

  @Test void testStarAliasFails() {
    sql("select emp.^*^ AS x from emp")
        .fails("Unknown field '\\*'");
  }

  @Test void testNonLocalStar() {
    // MySQL allows this, and now so do we
    sql("select * from emp e where exists (\n"
        + "  select e.* from dept where dept.deptno = e.deptno)")
        .type(EMP_RECORD_TYPE);
  }

  /**
   * Parser does not allow "*" in FROM clause.
   * Parser allows "*" in column expressions, but throws if they are outside
   * of the SELECT clause.
   *
   * @see #testStarIdentifier()
   */
  @Test void testStarInFromFails() {
    sql("select emp.empno AS x from sales^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select * from emp^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select emp.empno AS x from emp^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select emp.empno from emp where emp.^*^ is not null")
        .fails("Unknown field '\\*'");
  }

  @Test void testStarDotIdFails() {
    // Fails in parser
    sql("select emp.^*^.\"EXPR$1\" from emp")
        .fails("(?s).*Unknown field '\\*'");
    sql("select emp.^*^.foo from emp")
        .fails("(?s).*Unknown field '\\*'");
    // Parser does not allow star dot identifier.
    sql("select *^.^foo from emp")
        .fails("(?s).*Encountered \".\" at .*");
  }

  @Test void testAsColumnList() {
    sql("select d.a, b from dept as d(a, b)").ok();
    sql("select d.^deptno^ from dept as d(a, b)")
        .fails("(?s).*Column 'DEPTNO' not found in table 'D'.*");
    sql("select 1 from dept as d(^a, b, c^)")
        .fails("(?s).*List of column aliases must have same degree as table; "
            + "table has 2 columns \\('DEPTNO', 'NAME'\\), "
            + "whereas alias list has 3 columns.*");
    sql("select * from dept as d(a, b)")
        .type("RecordType(INTEGER NOT NULL A, VARCHAR(10) NOT NULL B) NOT NULL");
    sql("select * from (values ('a', 1), ('bc', 2)) t (a, b)")
        .type("RecordType(CHAR(2) NOT NULL A, INTEGER NOT NULL B) NOT NULL");
  }

  @Test void testAmbiguousColumnInIn() {
    // ok: cyclic reference
    sql("select * from emp as e\n"
        + "where e.deptno in (\n"
        + "  select 1 from (values(true)) where e.empno > 10)").ok();

    // ok: cyclic reference
    sql("select * from emp as e\n"
        + "where e.deptno in (\n"
        + "  select e.deptno from (values(true)))").ok();
  }

  @Test void testInList() {
    sql("select * from emp where empno in (10,20)").ok();

    // "select * from emp where empno in ()" is invalid -- see parser test
    sql("select * from emp\n"
        + "where empno in (10 + deptno, cast(null as integer))").ok();
    sql("select * from emp where empno in (10, '20')").ok();
    sql("select * from emp where empno in ^(10, '20')^")
        .withTypeCoercion(false)
        .fails(ERR_IN_VALUES_INCOMPATIBLE);

    expr("1 in (2, 3, 4)")
        .columnType("BOOLEAN NOT NULL");
    expr("cast(null as integer) in (2, 3, 4)")
        .columnType("BOOLEAN");
    expr("1 in (2, cast(null as integer) , 4)")
        .columnType("BOOLEAN");
    expr("1 in (2.5, 3.14)")
        .columnType("BOOLEAN NOT NULL");
    expr("true in (false, unknown)")
        .columnType("BOOLEAN");
    expr("true in (false, false or unknown)")
        .columnType("BOOLEAN");
    expr("true in (false, true)")
        .columnType("BOOLEAN NOT NULL");
    expr("(1,2) in ((1,2), (3,4))")
        .columnType("BOOLEAN NOT NULL");
    expr("'medium' in (cast(null as varchar(10)), 'bc')")
        .columnType("BOOLEAN");

    // nullability depends on nullability of both sides
    sql("select empno in (1, 2) from emp")
        .columnType("BOOLEAN NOT NULL");
    sql("select nullif(empno,empno) in (1, 2) from emp")
        .columnType("BOOLEAN");
    sql("select empno in (1, nullif(empno,empno), 2) from emp")
        .columnType("BOOLEAN");

    expr("1 in (2, 'c')").ok();
    expr("1 in ^(2, 'c')^")
        .withTypeCoercion(false)
        .fails(ERR_IN_VALUES_INCOMPATIBLE);
    expr("1 in ^((2), (3,4))^")
        .fails(ERR_IN_VALUES_INCOMPATIBLE);
    expr("false and ^1 in ('b', 'c')^").ok();
    expr("false and ^1 in (date '2012-01-02', date '2012-01-04')^")
        .fails(ERR_IN_OPERANDS_INCOMPATIBLE);
    expr("1 > 5 or ^(1, 2) in (3, 4)^")
        .fails(ERR_IN_OPERANDS_INCOMPATIBLE);
  }

  @Test void testInSubQuery() {
    sql("select * from emp where deptno in (select deptno from dept)").ok();
    sql("select * from emp where (empno,deptno)"
        + " in (select deptno,deptno from dept)").ok();

    // NOTE: jhyde: The closing caret should be one character to the right
    // ("dept)^"), but it's difficult to achieve, because parentheses are
    // discarded during the parsing process.
    sql("select * from emp where ^deptno in "
        + "(select deptno,deptno from dept^)")
        .fails("Values passed to IN operator must have compatible types");
  }

  @Test void testAnyList() {
    sql("select * from emp where empno = any (10,20)").ok();

    sql("select * from emp\n"
        + "where empno < any (10 + deptno, cast(null as integer))").ok();
    sql("select * from emp where empno < any (10, '20')").ok();
    sql("select * from emp where empno < any ^(10, '20')^")
        .withTypeCoercion(false)
        .fails(ERR_IN_VALUES_INCOMPATIBLE);

    expr("1 < all (2, 3, 4)")
        .columnType("BOOLEAN NOT NULL");
    expr("cast(null as integer) < all (2, 3, 4)")
        .columnType("BOOLEAN");
    expr("1 > some (2, cast(null as integer) , 4)")
        .columnType("BOOLEAN");
    expr("1 > any (2.5, 3.14)")
        .columnType("BOOLEAN NOT NULL");
    expr("true = any (false, unknown)")
        .columnType("BOOLEAN");
    expr("true = any (false, false or unknown)")
        .columnType("BOOLEAN");
    expr("true <> any (false, true)")
        .columnType("BOOLEAN NOT NULL");
    expr("(1,2) = any ((1,2), (3,4))")
        .columnType("BOOLEAN NOT NULL");
    expr("(1,2) < any ((1,2), (3,4))")
        .columnType("BOOLEAN NOT NULL");
    expr("'abc' < any (cast(null as varchar(10)), 'bc')")
        .columnType("BOOLEAN");

    // nullability depends on nullability of both sides
    sql("select empno < any (1, 2) from emp")
        .columnType("BOOLEAN NOT NULL");
    sql("select nullif(empno,empno) > all (1, 2) from emp")
        .columnType("BOOLEAN");
    sql("select empno in (1, nullif(empno,empno), 2) from emp")
        .columnType("BOOLEAN");

    expr("1 = any (2, 'c')").ok();
    expr("1 = any ^(2, 'c')^")
        .withTypeCoercion(false)
        .fails(ERR_IN_VALUES_INCOMPATIBLE);
    expr("1 > all ^((2), (3,4))^")
        .fails(ERR_IN_VALUES_INCOMPATIBLE);
    expr("false and 1 = any ('b', 'c')").ok();
    expr("false and ^1 = any (date '2012-01-02', date '2012-01-04')^")
        .fails(ERR_IN_OPERANDS_INCOMPATIBLE);
    expr("1 > 5 or ^(1, 2) < any (3, 4)^")
        .fails(ERR_IN_OPERANDS_INCOMPATIBLE);
  }

  @Test void testDoubleNoAlias() {
    sql("select * from emp join dept on true").ok();
    sql("select * from emp, dept").ok();
    sql("select * from emp cross join dept").ok();
  }

  @Test void testDuplicateColumnAliasIsOK() {
    // duplicate column aliases are daft, but SQL:2003 allows them
    sql("select 1 as a, 2 as b, 3 as a from emp").ok();
  }

  @Test void testDuplicateTableAliasFails() {
    // implicit alias clashes with implicit alias
    sql("select 1 from emp, ^emp^")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // implicit alias clashes with implicit alias, using join syntax
    sql("select 1 from emp join ^emp^ on emp.empno = emp.mgrno")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // explicit alias clashes with implicit alias
    sql("select 1 from emp join ^dept as emp^ on emp.empno = emp.deptno")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // implicit alias does not clash with overridden alias
    sql("select 1 from emp as e join emp on emp.empno = e.deptno").ok();

    // explicit alias does not clash with overridden alias
    sql("select 1 from emp as e join dept as emp on e.empno = emp.deptno").ok();

    // more than 2 in from clause
    sql("select 1 from emp, dept, emp as e, ^dept as emp^, emp")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // alias applied to sub-query
    sql("select 1 from emp, (^select 1 as x from (values (true))) as emp^")
        .fails("Duplicate relation name 'EMP' in FROM clause");
    sql("select 1 from emp, (^values (true,false)) as emp (b, c)^, dept as emp")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // alias applied to table function. doesn't matter that table fn
    // doesn't exist - should find the alias problem first
    sql("select 1 from emp, ^table(foo()) as emp^")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // explicit table
    sql("select 1 from emp, ^(table foo.bar.emp) as emp^")
        .fails("Duplicate relation name 'EMP' in FROM clause");

    // alias does not clash with alias inherited from enclosing context
    sql("select 1 from emp, dept where exists (\n"
        + "  select 1 from emp where emp.empno = emp.deptno)").ok();
  }

  @Test void testSchemaTableStar() {
    sql("select ^sales.e^.* from sales.emp as e")
        .fails("Unknown identifier 'SALES\\.E'");
    sql("select sales.dept.* from sales.dept")
        .type("RecordType(INTEGER NOT NULL DEPTNO,"
            + " VARCHAR(10) NOT NULL NAME) NOT NULL");
    sql("select sales.emp.* from emp").ok();
    sql("select sales.emp.* from emp as emp").ok();
    // MySQL gives: "Unknown table 'emp'"
    // (consistent with MySQL)
    sql("select ^sales.emp^.* from emp as e")
        .fails("Unknown identifier 'SALES.EMP'");
  }

  @Test void testSchemaTableColumn() {
    sql("select emp.empno from sales.emp").ok();
    sql("select sales.emp.empno from sales.emp").ok();
    sql("select sales.emp.empno from sales.emp\n"
        + "where sales.emp.deptno > 0").ok();
    sql("select 1 from sales.emp where sales.emp.^bad^ < 0")
        .fails("Column 'BAD' not found in table 'SALES.EMP'");
    sql("select ^sales.bad^.empno from sales.emp\n"
        + "where sales.emp.deptno > 0")
        .fails("Table 'SALES\\.BAD' not found");
    sql("select sales.emp.deptno from sales.emp").ok();
    sql("select 1 from sales.emp where sales.emp.deptno = 10").ok();
    sql("select 1 from sales.emp order by sales.emp.deptno").ok();
    // alias does not hide the fully-qualified name if same
    // (consistent with MySQL)
    sql("select sales.emp.deptno from sales.emp as emp").ok();
    // alias hides the fully-qualified name
    // (consistent with MySQL)
    sql("select ^sales.emp^.deptno from sales.emp as e")
        .fails("Table 'SALES\\.EMP' not found");
    sql("select sales.emp.deptno from sales.emp, ^sales.emp^")
        .fails("Duplicate relation name 'EMP' in FROM clause");
    // Table exists but not used in FROM clause
    sql("select ^sales.emp^.deptno from sales.dept as d1, sales.dept")
        .fails("Table 'SALES.EMP' not found");
    // Table does not exist
    sql("select ^sales.bad^.deptno from sales.dept as d1, sales.dept")
        .fails("Table 'SALES.BAD' not found");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-881">[CALCITE-881]
   * Allow schema.table.column references in GROUP BY</a>. */
  @Test void testSchemaTableColumnInGroupBy() {
    sql("select 1 from sales.emp group by sales.emp.deptno").ok();
    sql("select deptno from sales.emp group by sales.emp.deptno").ok();
    sql("select deptno + 1 from sales.emp group by sales.emp.deptno").ok();
  }

  @Test void testInvalidGroupBy() {
    sql("select ^empno^, deptno from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testInvalidGroupBy2() {
    sql("select count(*) from emp group by ^deptno + 'a'^")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply '\\+' to arguments of type.*");
    sql("select count(*) from emp group by deptno + 'a'").ok();
  }

  @Test void testInvalidGroupBy3() {
    sql("select deptno / 2 + 1, count(*) as c\n"
        + "from emp\n"
        + "group by rollup(deptno / 2, sal), rollup(empno, ^deptno + 'a'^)")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply '\\+' to arguments of type.*");
    sql("select deptno / 2 + 1, count(*) as c\n"
        + "from emp\n"
        + "group by rollup(deptno / 2, sal), rollup(empno, ^deptno + 'a'^)").ok();
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-3003">[CALCITE-3003]
   * AssertionError when GROUP BY nested field</a>.
   *
   * <p>Make sure table name of GROUP BY item with nested field could be
   * properly validated.
   */
  @Test void testInvalidGroupByWithInvalidTableName() {
    final String sql =
        "select\n"
            + "  coord.x,\n"
            + "  avg(coord.y)\n"
            + "from\n"
            + "  customer.contact_peek\n"
            + "group by\n"
            + "  ^unknown_table_alias.coord^.x";
    sql(sql)
        .fails("Table 'UNKNOWN_TABLE_ALIAS.COORD' not found");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1781">[CALCITE-1781]
   * Allow expression in CUBE and ROLLUP</a>. */
  @Test void testCubeExpression() {
    final String sql = "select deptno + 1\n"
        + "from emp\n"
        + "group by cube(deptno + 1)";
    sql(sql).ok();
    final String sql2 = "select deptno + 2 - 2\n"
        + "from emp\n"
        + "group by cube(deptno + 2, empno)";
    sql(sql2).ok();
    final String sql3 = "select ^deptno^\n"
        + "from emp\n"
        + "group by cube(deptno + 1)";
    sql(sql3).fails("Expression 'DEPTNO' is not being grouped");
    final String sql4 = "select ^deptno^ + 10\n"
        + "from emp\n"
        + "group by rollup(empno, deptno + 10 - 10)";
    sql(sql4).fails("Expression 'DEPTNO' is not being grouped");
    final String sql5 = "select deptno + 10\n"
        + "from emp\n"
        + "group by rollup(deptno + 10 - 10, deptno)";
    sql(sql5).ok();
  }

  /** Unit test for
   * {@link org.apache.calcite.sql.validate.SqlValidatorUtil#rollup}. */
  @Test void testRollupBitSets() {
    assertThat(rollup(ImmutableBitSet.of(1), ImmutableBitSet.of(3)).toString(),
        equalTo("[{1, 3}, {1}, {}]"));
    assertThat(rollup(ImmutableBitSet.of(1), ImmutableBitSet.of(3, 4))
            .toString(),
        equalTo("[{1, 3, 4}, {1}, {}]"));
    assertThat(rollup(ImmutableBitSet.of(1, 3), ImmutableBitSet.of(4))
            .toString(),
        equalTo("[{1, 3, 4}, {1, 3}, {}]"));
    assertThat(rollup(ImmutableBitSet.of(1, 4), ImmutableBitSet.of(3))
            .toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {}]"));
    // non-disjoint bit sets
    assertThat(rollup(ImmutableBitSet.of(1, 4), ImmutableBitSet.of(3, 4))
            .toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {}]"));
    // some bit sets are empty
    assertThat(
        rollup(ImmutableBitSet.of(1, 4), ImmutableBitSet.of(),
            ImmutableBitSet.of(3, 4), ImmutableBitSet.of()).toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {}]"));
    assertThat(rollup(ImmutableBitSet.of(1)).toString(),
        equalTo("[{1}, {}]"));
    // one empty bit set
    assertThat(rollup(ImmutableBitSet.of()).toString(),
        equalTo("[{}]"));
    // no bit sets
    assertThat(rollup().toString(),
        equalTo("[{}]"));
  }

  private ImmutableList<ImmutableBitSet> rollup(ImmutableBitSet... sets) {
    return SqlValidatorUtil.rollup(ImmutableList.copyOf(sets));
  }

  /** Unit test for
   * {@link org.apache.calcite.sql.validate.SqlValidatorUtil#cube}. */
  @Test void testCubeBitSets() {
    assertThat(cube(ImmutableBitSet.of(1), ImmutableBitSet.of(3)).toString(),
        equalTo("[{1, 3}, {1}, {3}, {}]"));
    assertThat(cube(ImmutableBitSet.of(1), ImmutableBitSet.of(3, 4)).toString(),
        equalTo("[{1, 3, 4}, {1}, {3, 4}, {}]"));
    assertThat(cube(ImmutableBitSet.of(1, 3), ImmutableBitSet.of(4)).toString(),
        equalTo("[{1, 3, 4}, {1, 3}, {4}, {}]"));
    assertThat(cube(ImmutableBitSet.of(1, 4), ImmutableBitSet.of(3)).toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {3}, {}]"));
    // non-disjoint bit sets
    assertThat(
        cube(ImmutableBitSet.of(1, 4), ImmutableBitSet.of(3, 4)).toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {3, 4}, {}]"));
    // some bit sets are empty, and there are duplicates
    assertThat(
        cube(ImmutableBitSet.of(1, 4),
            ImmutableBitSet.of(),
            ImmutableBitSet.of(1, 4),
            ImmutableBitSet.of(3, 4),
            ImmutableBitSet.of()).toString(),
        equalTo("[{1, 3, 4}, {1, 4}, {3, 4}, {}]"));
    assertThat(cube(ImmutableBitSet.of(1)).toString(),
        equalTo("[{1}, {}]"));
    assertThat(cube(ImmutableBitSet.of()).toString(),
        equalTo("[{}]"));
    assertThat(cube().toString(),
        equalTo("[{}]"));
  }

  private ImmutableList<ImmutableBitSet> cube(ImmutableBitSet... sets) {
    return SqlValidatorUtil.cube(ImmutableList.copyOf(sets));
  }

  @Test void testGrouping() {
    sql("select deptno, grouping(deptno) from emp group by deptno").ok();
    sql("select deptno, grouping(deptno, deptno) from emp group by deptno")
        .ok();
    sql("select deptno / 2, grouping(deptno / 2),\n"
        + " ^grouping(deptno / 2, empno)^\n"
        + "from emp group by deptno / 2, empno")
        .ok();
    sql("select deptno, grouping(^empno^) from emp group by deptno")
        .fails("Argument to GROUPING operator must be a grouped expression");
    sql("select deptno, grouping(deptno, ^empno^) from emp group by deptno")
        .fails("Argument to GROUPING operator must be a grouped expression");
    sql("select deptno, grouping(^empno^, deptno) from emp group by deptno")
        .fails("Argument to GROUPING operator must be a grouped expression");
    sql("select deptno, grouping(^deptno + 1^) from emp group by deptno")
        .fails("Argument to GROUPING operator must be a grouped expression");
    sql("select deptno, grouping(emp.^xxx^) from emp")
        .fails("Column 'XXX' not found in table 'EMP'");
    sql("select deptno, ^grouping(deptno)^ from emp")
        .fails("GROUPING operator may only occur in an aggregate query");
    sql("select deptno, sum(^grouping(deptno)^) over () from emp")
        .fails("GROUPING operator may only occur in an aggregate query");
    sql("select deptno from emp group by deptno having grouping(deptno) < 5")
        .ok();
    sql("select deptno from emp group by deptno order by grouping(deptno)")
        .ok();
    sql("select deptno as xx from emp group by deptno order by grouping(xx)")
        .ok();
    sql("select deptno as empno from emp\n"
        + "group by deptno order by grouping(empno)")
        .ok();
    sql("select 1 as deptno from emp\n"
        + "group by deptno order by grouping(^deptno^)")
        .fails("Argument to GROUPING operator must be a grouped expression");
    sql("select deptno from emp group by deptno order by grouping(emp.deptno)")
        .ok();
    sql("select ^deptno^ from emp group by empno order by grouping(deptno)")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select deptno from emp order by ^grouping(deptno)^")
        .fails("GROUPING operator may only occur in an aggregate query");
    sql("select deptno from emp where ^grouping(deptno)^ = 1")
        .fails("GROUPING operator may only occur in an aggregate query");
    sql("select deptno from emp where ^grouping(deptno)^ = 1 group by deptno")
        .fails("GROUPING operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp group by deptno, ^grouping(deptno)^")
        .fails("GROUPING operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by grouping sets(deptno, ^grouping(deptno)^)")
        .fails("GROUPING operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by cube(empno, ^grouping(deptno)^)")
        .fails("GROUPING operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by rollup(empno, ^grouping(deptno)^)")
        .fails("GROUPING operator may only occur in SELECT, HAVING or ORDER BY clause");
  }

  @Test void testGroupingId() {
    sql("select deptno, grouping_id(deptno) from emp group by deptno").ok();
    sql("select deptno, grouping_id(deptno, deptno) from emp group by deptno")
        .ok();
    sql("select deptno / 2, grouping_id(deptno / 2),\n"
        + " ^grouping_id(deptno / 2, empno)^\n"
        + "from emp group by deptno / 2, empno")
        .ok();
    sql("select deptno / 2, ^grouping_id()^\n"
        + "from emp group by deptno / 2, empno")
        .fails("Invalid number of arguments to function 'GROUPING_ID'. Was expecting 1 arguments");
    sql("select deptno, grouping_id(^empno^) from emp group by deptno")
        .fails("Argument to GROUPING_ID operator must be a grouped expression");
    sql("select deptno, grouping_id(^deptno + 1^) from emp group by deptno")
        .fails("Argument to GROUPING_ID operator must be a grouped expression");
    sql("select deptno, grouping_id(emp.^xxx^) from emp")
        .fails("Column 'XXX' not found in table 'EMP'");
    sql("select deptno, ^grouping_id(deptno)^ from emp")
        .fails("GROUPING_ID operator may only occur in an aggregate query");
    sql("select deptno, sum(^grouping_id(deptno)^) over () from emp")
        .fails("GROUPING_ID operator may only occur in an aggregate query");
    sql("select deptno from emp group by deptno having grouping_id(deptno) < 5")
        .ok();
    sql("select deptno from emp group by deptno order by grouping_id(deptno)")
        .ok();
    sql("select deptno as xx from emp group by deptno order by grouping_id(xx)")
        .ok();
    sql("select deptno as empno from emp\n"
        + "group by deptno order by grouping_id(empno)")
        .ok();
    sql("select 1 as deptno from emp\n"
        + "group by deptno order by grouping_id(^deptno^)")
        .fails("Argument to GROUPING_ID operator must be a grouped expression");
    sql("select deptno from emp group by deptno\n"
        + "order by grouping_id(emp.deptno)")
        .ok();
    sql("select ^deptno^ from emp group by empno order by grouping_id(deptno)")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select deptno from emp order by ^grouping_id(deptno)^")
        .fails("GROUPING_ID operator may only occur in an aggregate query");
    sql("select deptno from emp where ^grouping_id(deptno)^ = 1")
        .fails("GROUPING_ID operator may only occur in an aggregate query");
    sql("select deptno from emp where ^grouping_id(deptno)^ = 1\n"
        + "group by deptno")
        .fails("GROUPING_ID operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp group by deptno, ^grouping_id(deptno)^")
        .fails("GROUPING_ID operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by grouping sets(deptno, ^grouping_id(deptno)^)")
        .fails("GROUPING_ID operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by cube(empno, ^grouping_id(deptno)^)")
        .fails("GROUPING_ID operator may only occur in SELECT, HAVING or ORDER BY clause");
    sql("select deptno from emp\n"
        + "group by rollup(empno, ^grouping_id(deptno)^)")
        .fails("GROUPING_ID operator may only occur in SELECT, HAVING or ORDER BY clause");
  }

  @Test void testGroupId() {
    final String groupIdOnlyInAggregate =
        "GROUP_ID operator may only occur in an aggregate query";
    final String groupIdWrongClause =
        "GROUP_ID operator may only occur in SELECT, HAVING or ORDER BY clause";

    sql("select deptno, group_id() from emp group by deptno").ok();
    sql("select deptno, ^group_id^ as x from emp group by deptno")
        .fails("Column 'GROUP_ID' not found in any table");
    sql("select deptno, ^group_id(deptno)^ from emp group by deptno")
        .fails("Invalid number of arguments to function 'GROUP_ID'\\. "
            + "Was expecting 0 arguments");
    // Oracle throws "GROUPING function only supported with GROUP BY CUBE or
    // ROLLUP"
    sql("select ^group_id()^ from emp")
        .fails(groupIdOnlyInAggregate);
    sql("select deptno from emp order by ^group_id(deptno)^")
        .fails(groupIdOnlyInAggregate);
    // Oracle throws "GROUPING function only supported with GROUP BY CUBE or
    // ROLLUP"
    sql("select 1 from emp order by ^group_id()^")
        .fails(groupIdOnlyInAggregate);
    sql("select 1 from emp order by ^grouping(deptno)^")
        .fails("GROUPING operator may only occur in an aggregate query");
    // Oracle throws "group function is not allowed here"
    sql("select deptno from emp where ^group_id()^ = 1")
        .fails(groupIdOnlyInAggregate);
    // Oracle throws "group function is not allowed here"
    sql("select deptno from emp group by ^group_id()^")
        .fails(groupIdWrongClause);
    sql("select deptno from emp where ^group_id()^ = 1 group by deptno")
        .fails(groupIdWrongClause);
    sql("select deptno from emp group by deptno, ^group_id()^")
        .fails(groupIdWrongClause);
    sql("select deptno from emp\n"
        + "group by grouping sets(deptno, ^group_id()^)")
        .fails(groupIdWrongClause);
    sql("select deptno from emp\n"
        + "group by cube(empno, ^group_id()^)")
        .fails(groupIdWrongClause);
    sql("select deptno from emp\n"
        + "group by rollup(empno, ^group_id()^)")
        .fails(groupIdWrongClause);
    sql("select grouping(^group_id()^) from emp")
        .fails(groupIdOnlyInAggregate);
    // Oracle throws "not a GROUP BY expression"
    sql("select grouping(^group_id()^) from emp group by deptno")
        .fails(groupIdWrongClause);
    sql("select ^grouping(sum(empno))^ from emp group by deptno")
        .fails("Aggregate expressions cannot be nested");
  }

  @Test void testCubeGrouping() {
    sql("select deptno, grouping(deptno) from emp group by cube(deptno)").ok();
    sql("select deptno, grouping(^deptno + 1^) from emp\n"
        + "group by cube(deptno, empno)")
        .fails("Argument to GROUPING operator must be a grouped expression");
  }

  @Test void testSumInvalidArgs() {
    sql("select ^sum(ename)^, deptno from emp group by deptno")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(20\\)>\\)'\\. .*");
    sql("select sum(ename), deptno from emp group by deptno")
        .type("RecordType(DECIMAL(19, 9) NOT NULL EXPR$0, INTEGER NOT NULL DEPTNO) NOT NULL");
  }

  @Test void testSumTooManyArgs() {
    sql("select ^sum(empno, deptno)^, deptno from emp group by deptno")
        .fails("Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
  }

  @Test void testSumTooFewArgs() {
    sql("select ^sum()^, deptno from emp group by deptno")
        .fails("Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
  }

  @Test void testSingleNoAlias() {
    sql("select * from emp").ok();
  }

  @Test void testObscuredAliasFails() {
    // It is an error to refer to a table which has been given another
    // alias.
    sql("select * from emp as e where exists (\n"
        + "  select 1 from dept where dept.deptno = ^emp^.deptno)")
        .fails("Table 'EMP' not found");
  }

  @Test void testFromReferenceFails() {
    // You cannot refer to a table ('e2') in the parent scope of a query in
    // the from clause.
    sql("select * from emp as e1 where exists (\n"
        + "  select * from emp as e2,\n"
        + "    (select * from dept where dept.deptno = ^e2^.deptno))")
        .fails("Table 'E2' not found");
  }

  @Test void testWhereReference() {
    // You can refer to a table ('e1') in the parent scope of a query in
    // the from clause.
    //
    // Note: Oracle10g does not allow this query.
    sql("select * from emp as e1 where exists (\n"
        + "  select * from emp as e2,\n"
        + "    (select * from dept where dept.deptno = e1.deptno))").ok();
  }

  @Test void testUnionNameResolution() {
    sql("select * from emp as e1 where exists (\n"
        + "  select * from emp as e2,\n"
        + "  (select deptno from dept as d\n"
        + "   union\n"
        + "   select deptno from emp as e3 where deptno = ^e2^.deptno))")
        .fails("Table 'E2' not found");

    sql("select * from emp\n"
        + "union\n"
        + "select * from dept where ^empno^ < 10")
        .fails("Column 'EMPNO' not found in any table");
  }

  @Test void testUnionCountMismatchFails() {
    sql("select 1,2 from emp\n"
        + "union\n"
        + "select ^3^ from dept")
        .fails("Column count mismatch in UNION");
  }

  @Test void testUnionCountMismatcWithValuesFails() {
    sql("select * from ( values (1))\n"
        + "union\n"
        + "select ^*^ from ( values (1,2))")
        .fails("Column count mismatch in UNION");

    sql("select * from ( values (1))\n"
        + "union\n"
        + "select ^*^ from emp")
        .fails("Column count mismatch in UNION");

    sql("select * from emp\n"
        + "union\n"
        + "select ^*^ from ( values (1))")
        .fails("Column count mismatch in UNION");
  }

  @Test void testUnionTypeMismatchFails() {
    sql("select 1, ^2^ from emp union select deptno, name from dept")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 2 of UNION");

    sql("select 1, 2 from emp union select deptno, ^name^ from dept").ok();

    sql("select ^slacker^ from emp union select name from dept")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 1 of UNION");

    sql("select ^slacker^ from emp union select name from dept").ok();
  }

  @Test void testUnionTypeMismatchWithStarFails() {
    sql("select ^*^ from dept union select 1, 2 from emp")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 2 of UNION");

    sql("select * from dept union select 1, 2 from emp").ok();

    sql("select ^dept.*^ from dept union select 1, 2 from emp")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 2 of UNION");

    sql("select dept.* from dept union select 1, 2 from emp").ok();
  }

  @Test void testUnionTypeMismatchWithValuesFails() {
    sql("values (1, ^2^, 3), (3, 4, 5), (6, 7, 8) union\n"
        + "select deptno, name, deptno from dept")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 2 of UNION");

    sql("values (1, 2, 3), (3, 4, 5), (6, 7, 8) union\n"
        + "select deptno, ^name^, deptno from dept").ok();

    sql("select 1 from (values (^'x'^)) union\n"
        + "select 'a' from (values ('y'))")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 1 of UNION");

    sql("select 1 from (values ('x')) union\n"
        + "select 'a' from (values ('y'))").ok();

    sql("select 1 from (values (^'x'^)) union\n"
        + "(values ('a'))")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 1 of UNION");

    sql("select 1 from (values ('x')) union\n"
        + "(values ('a'))").ok();

    sql("select 1, ^2^, 3 union\n "
        + "select deptno, name, deptno from dept")
        .withTypeCoercion(false)
        .fails("Type mismatch in column 2 of UNION");

    sql("select 1, 2, 3 union\n "
        + "select deptno, name, deptno from dept").ok();
  }

  @Test void testValuesTypeMismatchFails() {
    sql("^values (1), ('a')^")
        .fails("Values passed to VALUES operator must have compatible types");
  }

  @Test void testNaturalCrossJoinFails() {
    sql("select * from emp natural cross ^join^ dept")
        .fails("Cannot specify condition \\(NATURAL keyword, or ON or USING "
            + "clause\\) following CROSS JOIN");
  }

  @Test void testCrossJoinUsingFails() {
    sql("select * from emp cross join dept ^using^ (deptno)")
        .fails("Cannot specify condition \\(NATURAL keyword, or ON or USING "
            + "clause\\) following CROSS JOIN");
  }

  @Test void testJoinUsing() {
    final String empDeptType = "RecordType(INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL EMPNO,"
        + " VARCHAR(20) NOT NULL ENAME,"
        + " VARCHAR(10) NOT NULL JOB,"
        + " INTEGER MGR,"
        + " TIMESTAMP(0) NOT NULL HIREDATE,"
        + " INTEGER NOT NULL SAL,"
        + " INTEGER NOT NULL COMM,"
        + " BOOLEAN NOT NULL SLACKER,"
        + " VARCHAR(10) NOT NULL NAME) NOT NULL";
    sql("select * from emp join dept using (deptno)").ok()
        .type(empDeptType);

    // NATURAL has same effect as USING
    sql("select * from emp natural join dept").ok()
        .type(empDeptType);

    // fail: comm exists on one side not the other
    // todo: The error message could be improved.
    sql("select * from emp join dept using (deptno, ^comm^)")
        .fails("Column 'COMM' not found in any table");

    sql("select * from emp join dept using (^empno^)")
        .fails("Column 'EMPNO' not found in any table");

    sql("select * from dept join emp using (^empno^)")
        .fails("Column 'EMPNO' not found in any table");

    // not on either side
    sql("select * from dept join emp using (^abc^)")
        .fails("Column 'ABC' not found in any table");

    // column exists, but wrong case
    sql("select * from dept join emp using (^\"deptno\"^)")
        .fails("Column 'deptno' not found in any table");

    // ok to repeat (ok in Oracle10g too)
    final String sql = "select * from emp join dept using (deptno, deptno)";
    sql(sql).ok()
        .type(empDeptType);

    // inherited column, not found in either side of the join, in the
    // USING clause
    sql("select * from dept where exists (\n"
        + "select 1 from emp join bonus using (^dname^))")
        .fails("Column 'DNAME' not found in any table");

    // inherited column, found in only one side of the join, in the
    // USING clause
    sql("select * from dept where exists (\n"
        + "select 1 from emp join bonus using (^deptno^))")
        .fails("Column 'DEPTNO' not found in any table");

    // cannot qualify common column in Oracle.
    sql("select ^emp.deptno^ from emp join dept using (deptno)")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("Cannot qualify common column 'EMP.DEPTNO'")
        .withConformance(SqlConformanceEnum.ORACLE_12)
        .fails("Cannot qualify common column 'EMP.DEPTNO'");

    sql("select ^emp.deptno^ from emp natural join dept")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("Cannot qualify common column 'EMP.DEPTNO'")
        .withConformance(SqlConformanceEnum.ORACLE_12)
        .fails("Cannot qualify common column 'EMP.DEPTNO'");

    sql("select count(^emp.deptno^) from emp join dept using (deptno)")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("Cannot qualify common column 'EMP.DEPTNO'")
        .withConformance(SqlConformanceEnum.ORACLE_12)
        .fails("Cannot qualify common column 'EMP.DEPTNO'");

    sql("select emp.deptno from emp join dept using (deptno)")
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok()
        .withConformance(SqlConformanceEnum.MYSQL_5)
        .ok();

    sql("select emp.empno from emp natural join dept")
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok()
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .ok()
        .withConformance(SqlConformanceEnum.ORACLE_12)
        .ok();

    sql("select emp.empno from emp join dept using (deptno)")
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok()
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .ok()
        .withConformance(SqlConformanceEnum.ORACLE_12)
        .ok();
  }

  @Test void testCrossJoinOnFails() {
    sql("select * from emp cross join dept\n"
        + " ^on^ emp.deptno = dept.deptno")
        .fails("Cannot specify condition \\(NATURAL keyword, or ON or "
            + "USING clause\\) following CROSS JOIN");
  }

  @Test void testInnerJoinWithoutUsingOrOnFails() {
    sql("select * from emp inner ^join^ dept\n"
        + "where emp.deptno = dept.deptno")
        .fails("INNER, LEFT, RIGHT or FULL join requires a condition "
            + "\\(NATURAL keyword or ON or USING clause\\)");
  }

  @Test void testNaturalJoinWithOnFails() {
    sql("select * from emp natural join dept on ^emp.deptno = dept.deptno^")
        .fails("Cannot specify NATURAL keyword with ON or USING clause");
  }

  @Test void testNaturalJoinWithUsing() {
    sql("select * from emp natural join dept ^using (deptno)^")
        .fails("Cannot specify NATURAL keyword with ON or USING clause");
  }

  @Test void testNaturalJoinCaseSensitive() {
    // With case-insensitive match, more columns are recognized as join columns
    // and therefore "*" expands to fewer columns.
    final String sql = "select *\n"
        + "from (select empno, deptno from emp)\n"
        + "natural join (select deptno as \"deptno\", name from dept)";
    final String type0 = "RecordType(INTEGER NOT NULL EMPNO,"
        + " INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL deptno,"
        + " VARCHAR(10) NOT NULL NAME) NOT NULL";
    final String type1 = "RecordType(INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL EMPNO,"
        + " VARCHAR(10) NOT NULL NAME) NOT NULL";
    sql(sql)
        .type(type0)
        .withCaseSensitive(false)
        .type(type1);
  }

  @Test void testNaturalJoinIncompatibleDatatype() {
    sql("select *\n"
        + "from (select ename as name, hiredate as deptno from emp)\n"
        + "natural ^join^\n"
        + "(select deptno, name as sal from dept)")
        .fails("Column 'DEPTNO' matched using NATURAL keyword or USING clause "
            + "has incompatible types: "
            + "cannot compare 'TIMESTAMP\\(0\\)' to 'INTEGER'");

    // INTEGER and VARCHAR are comparable: VARCHAR implicit converts to INTEGER
    sql("select * from emp natural ^join^\n"
        + "(select deptno, name as sal from dept)").ok();

    // make sal occur more than once on rhs, it is ignored and therefore
    // there is no error about incompatible types
    sql("select * from emp natural join\n"
        + " (select deptno, name as sal, 'foo' as sal2 from dept)").ok();
  }

  @Test void testJoinUsingIncompatibleDatatype() {
    sql("select *\n"
        + "from (select ename as name, hiredate as deptno from emp)\n"
        + "join (select deptno, name as sal from dept) using (^deptno^, sal)")
        .fails("Column 'DEPTNO' matched using NATURAL keyword or USING clause "
            + "has incompatible types: "
            + "cannot compare 'TIMESTAMP\\(0\\)' to 'INTEGER'");

    // INTEGER and VARCHAR are comparable: VARCHAR implicit converts to INTEGER
    final String sql = "select * from emp\n"
        + "join (select deptno, name as sal from dept) using (deptno, sal)";
    sql(sql).ok();
  }

  @Test void testJoinUsingInvalidColsFails() {
    // todo: Improve error msg
    sql("select * from emp left join dept using (^gender^)")
        .fails("Column 'GENDER' not found in any table");
  }

  @Test void testJoinUsingDupColsFails() {
    sql("select * from emp left join (select deptno, name as deptno from dept) using (^deptno^)")
        .fails("Column name 'DEPTNO' in USING clause is not unique on one side of join");
  }

  @Test void testJoinRowType() {
    sql("select * from emp left join dept on emp.deptno = dept.deptno")
        .type("RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER,"
            + " INTEGER DEPTNO0,"
            + " VARCHAR(10) NAME) NOT NULL");

    sql("select * from emp right join dept on emp.deptno = dept.deptno")
        .type("RecordType(INTEGER EMPNO,"
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

    sql("select * from emp full join dept on emp.deptno = dept.deptno")
        .type("RecordType(INTEGER EMPNO,"
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
  public void _testJoinUsing() {
    sql("select * from (emp join bonus using (job))\n"
        + "join dept using (deptno)").ok();

    // cannot alias a JOIN (actually this is a parser error, but who's
    // counting?)
    sql("select * from (emp join bonus using (job)) as x\n"
        + "join dept using (deptno)")
        .fails("as wrong here");
    sql("select * from (emp join bonus using (job))\n"
        + "join dept using (^dname^)")
        .fails("dname not found in lhs");

    // Needs real Error Message and error marks in query
    sql("select * from (emp join bonus using (job))\n"
        + "join (select 1 as job from (true)) using (job)")
        .fails("ambig");
  }

  @Disabled("bug: should fail if sub-query does not have alias")
  @Test void testJoinSubQuery() {
    // Sub-queries require alias
    sql("select * from (select 1 as uno from emp)\n"
        + "join (values (1), (2)) on true")
        .fails("require alias");
  }

  @Test void testJoinOnIn() {
    final String sql = "select * from emp join dept\n"
        + "on dept.deptno in (select deptno from emp)";
    sql(sql).ok();
  }

  @Test void testJoinOnInCorrelated() {
    final String sql = "select * from emp as e join dept\n"
        + "on dept.deptno in (select deptno from emp where deptno < e.deptno)";
    sql(sql).ok();
  }

  @Test void testJoinOnInCorrelatedFails() {
    final String sql = "select * from emp as e join dept as d\n"
        + "on d.deptno in (select deptno from emp where deptno < d.^empno^)";
    sql(sql).fails("Column 'EMPNO' not found in table 'D'");
  }

  @Test void testJoinOnExistsCorrelated() {
    final String sql = "select * from emp as e join dept\n"
        + "on exists (select 1, 2 from emp where deptno < e.deptno)";
    sql(sql).ok();
  }

  @Test void testJoinOnScalarCorrelated() {
    final String sql = "select * from emp as e join dept d\n"
        + "on d.deptno = (select 1 from emp where deptno < e.deptno)";
    sql(sql).ok();
  }

  @Test void testJoinOnScalarFails() {
    final String sql = "select * from emp as e join dept d\n"
        + "on d.deptno = (^select 1, 2 from emp where deptno < e.deptno^)";
    final String expected = "(?s)Cannot apply '\\$SCALAR_QUERY' to arguments "
        + "of type '\\$SCALAR_QUERY\\(<RECORDTYPE\\(INTEGER EXPR\\$0, INTEGER "
        + "EXPR\\$1\\)>\\)'\\. Supported form\\(s\\).*";
    sql(sql).fails(expected);
  }

  @Test void testJoinUsingThreeWay() {
    final String sql0 = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "join emp as e2 using (empno)";
    sql(sql0).ok();

    final String sql1 = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "join dept as d2 using (^deptno^)";
    final String expected = "Column name 'DEPTNO' in USING clause is not "
        + "unique on one side of join";
    sql(sql1).fails(expected);

    final String sql2 = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)";
    final String type2 = "RecordType(INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL EMPNO,"
        + " VARCHAR(20) NOT NULL ENAME,"
        + " VARCHAR(10) NOT NULL JOB,"
        + " INTEGER MGR,"
        + " TIMESTAMP(0) NOT NULL HIREDATE,"
        + " INTEGER NOT NULL SAL,"
        + " INTEGER NOT NULL COMM,"
        + " BOOLEAN NOT NULL SLACKER,"
        + " VARCHAR(10) NOT NULL NAME) NOT NULL";
    sql(sql2).type(type2);

    final String sql3 = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "join (values (10, 1000)) as b (empno, bonus) using (empno)";
    final String type3 = "RecordType(INTEGER NOT NULL EMPNO,"
        + " INTEGER NOT NULL DEPTNO,"
        + " VARCHAR(20) NOT NULL ENAME,"
        + " VARCHAR(10) NOT NULL JOB,"
        + " INTEGER MGR,"
        + " TIMESTAMP(0) NOT NULL HIREDATE,"
        + " INTEGER NOT NULL SAL,"
        + " INTEGER NOT NULL COMM,"
        + " BOOLEAN NOT NULL SLACKER,"
        + " VARCHAR(10) NOT NULL NAME,"
        + " INTEGER NOT NULL BONUS) NOT NULL";
    sql(sql3).type(type3);
  }

  @Test void testWhere() {
    sql("select * from emp where ^sal^")
        .fails("WHERE clause must be a condition");
  }

  @Test void testOn() {
    sql("select * from emp e1 left outer join emp e2 on ^e1.sal^")
        .fails("ON clause must be a condition");
  }

  @Test void testHaving() {
    sql("select * from emp having ^sum(sal)^")
        .fails("HAVING clause must be a condition");
    sql("select ^*^ from emp having sum(sal) > 10")
        .fails("Expression 'EMP\\.EMPNO' is not being grouped");

    // agg in select and having, no group by
    sql("select sum(sal + sal) from emp having sum(sal) > 10").ok();
    sql("SELECT deptno FROM emp GROUP BY deptno HAVING ^sal^ > 10")
        .fails("Expression 'SAL' is not being grouped");
  }

  @Test void testHavingBetween() {
    // FRG-115: having clause with between not working
    sql("select deptno from emp group by deptno\n"
        + "having deptno between 10 and 12").ok();

    // this worked even before FRG-115 was fixed
    sql("select deptno from emp group by deptno having deptno + 5 > 10").ok();
  }

  /** Tests the {@code WITH} clause, also called common table expressions. */
  @Test void testWith() {
    // simplest possible
    sql("with emp2 as (select * from emp)\n"
        + "select * from emp2")
        .type(EMP_RECORD_TYPE);

    // degree of emp2 column list does not match its query
    sql("with emp2 ^(x, y)^ as (select * from emp)\n"
        + "select * from emp2")
        .fails("Number of columns must match number of query columns");

    // duplicate names in column list
    sql("with emp2 (x, y, ^y^, x) as (select sal, deptno, ename, empno from emp)\n"
        + "select * from emp2")
        .fails("Duplicate name 'Y' in column list");

    // column list required if aliases are not unique
    sql("with emp2 as (^select empno as e, sal, deptno as e from emp^)\n"
        + "select * from emp2")
        .fails("Column has duplicate column name 'E' and no column list specified");

    // forward reference
    sql("with emp3 as (select * from ^emp2^),\n"
        + " emp2 as (select * from emp)\n"
        + "select * from emp3")
        .fails("Object 'EMP2' not found");

    // forward reference in with-item not used; should still fail
    sql("with emp3 as (select * from ^emp2^),\n"
        + " emp2 as (select * from emp)\n"
        + "select * from emp2")
        .fails("Object 'EMP2' not found");

    // table not used is ok
    sql("with emp2 as (select * from emp),\n"
        + " emp3 as (select * from emp2)\n"
        + "select * from emp2")
        .type(EMP_RECORD_TYPE);

    // self-reference is not ok, even in table not used
    sql("with emp2 as (select * from emp),\n"
        + " emp3 as (select * from ^emp3^)\n"
        + "values (1)")
        .fails("Object 'EMP3' not found");

    // self-reference not ok
    sql("with emp2 as (select * from ^emp2^)\n"
        + "select * from emp2 where false")
        .fails("Object 'EMP2' not found");

    // refer to 2 previous tables, not just immediately preceding
    sql("with emp2 as (select * from emp),\n"
        + " dept2 as (select * from dept),\n"
        + " empDept as (select emp2.empno, dept2.deptno from dept2 join emp2 using (deptno))\n"
        + "select 1 as uno from empDept")
        .type("RecordType(INTEGER NOT NULL UNO) NOT NULL");
  }

  /** Tests the {@code WITH} clause with UNION. */
  @Test void testWithUnion() {
    // nested WITH (parentheses required - and even with parentheses SQL
    // standard doesn't allow sub-query to have WITH)
    sql("with emp2 as (select * from emp)\n"
        + "select * from emp2 union all select * from emp")
        .type(EMP_RECORD_TYPE);
  }

  /** Tests the {@code WITH} clause and column aliases. */
  @Test void testWithColumnAlias() {
    sql("with w(x, y) as (select * from dept)\n"
        + "select * from w")
        .type("RecordType(INTEGER NOT NULL X, VARCHAR(10) NOT NULL Y) NOT NULL");
    sql("with w(x, y) as (select * from dept)\n"
        + "select * from w, w as w2")
        .type("RecordType(INTEGER NOT NULL X, VARCHAR(10) NOT NULL Y, "
            + "INTEGER NOT NULL X0, VARCHAR(10) NOT NULL Y0) NOT NULL");
    sql("with w(x, y) as (select * from dept)\n"
        + "select ^deptno^ from w")
        .fails("Column 'DEPTNO' not found in any table");
    sql("with w(x, ^x^) as (select * from dept)\n"
        + "select * from w")
        .fails("Duplicate name 'X' in column list");
  }

  /** Tests the {@code WITH} clause in sub-queries. */
  @Test void testWithSubQuery() {
    // nested WITH (parentheses required - and even with parentheses SQL
    // standard doesn't allow sub-query to have WITH)
    sql("with emp2 as (select * from emp)\n"
        + "(\n"
        + "  with dept2 as (select * from dept)\n"
        + "  (\n"
        + "    with empDept as (select emp2.empno, dept2.deptno from dept2 join emp2 using (deptno))\n"
        + "    select 1 as uno from empDept))")
        .type("RecordType(INTEGER NOT NULL UNO) NOT NULL");

    // WITH inside WHERE can see enclosing tables
    sql("select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)")
        .type(EMP_RECORD_TYPE);

    // WITH inside FROM cannot see enclosing tables
    sql("select * from emp\n"
        + "join (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= ^emp^.deptno)\n"
        + "  select * from dept2) as d on true")
        .fails("Table 'EMP' not found");

    // as above, using USING
    sql("select * from emp\n"
        + "join (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= ^emp^.deptno)\n"
        + "  select * from dept2) as d using (deptno)")
        .fails("Table 'EMP' not found");

    // WITH inside FROM
    sql("select e.empno, d.* from emp as e\n"
        + "join (\n"
        + "  with dept2 as (select * from dept where dept.deptno > 10)\n"
        + "  select deptno, 1 as uno from dept2) as d using (deptno)")
        .type("RecordType(INTEGER NOT NULL EMPNO,"
            + " INTEGER NOT NULL DEPTNO,"
            + " INTEGER NOT NULL UNO) NOT NULL");

    sql("select ^e^.empno, d.* from emp\n"
        + "join (\n"
        + "  with dept2 as (select * from dept where dept.deptno > 10)\n"
        + "  select deptno, 1 as uno from dept2) as d using (deptno)")
        .fails("Table 'E' not found");
  }

  @Test void testWithOrderAgg() {
    sql("select count(*) from emp order by count(*)").ok();
    sql("with q as (select * from emp)\n"
        + "select count(*) from q group by deptno order by count(*)").ok();
    sql("with q as (select * from emp)\n"
        + "select count(*) from q order by count(*)").ok();

    // ORDER BY on UNION would produce a similar parse tree,
    // SqlOrderBy(SqlUnion(SqlSelect ...)), but is not valid SQL.
    sql("select count(*) from emp\n"
        + "union all\n"
        + "select count(*) from emp\n"
        + "order by ^count(*)^")
        .fails("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4092">[CALCITE-4092]
   * NPE using WITH clause without a corresponding SELECT FROM</a>. */
  @Test void testWithNotSelected() {
    final String sql = "with emp2 as (select max(empno) as empno from emp)\n"
        + "select * from emp where empno < ^emp2^.empno";
    sql(sql).fails("Table 'EMP2' not found");
  }

  /**
   * Tests a large scalar expression, which will expose any O(n^2) algorithms
   * lurking in the validation process.
   */
  @Test void testLarge() {
    checkLarge(700, sql -> sql(sql).ok());
  }

  static void checkLarge(int x, Consumer<String> f) {
    if (System.getProperty("os.name").startsWith("Windows")) {
      // NOTE jvs 1-Nov-2006:  Default thread stack size
      // on Windows is too small, so avoid stack overflow
      x /= 3;
    }

    // E.g. large = "deptno * 1 + deptno * 2 + deptno * 3".
    String large = list(" + ", "deptno * ", x);
    f.accept("select " + large + "from emp");
    f.accept("select distinct " + large + "from emp");
    f.accept("select " + large + " from emp " + "group by deptno");
    f.accept("select * from emp where " + large + " > 5");
    f.accept("select * from emp order by " + large + " desc");
    f.accept("select " + large + " from emp order by 1");
    f.accept("select distinct " + large + " from emp order by " + large);

    // E.g. "in (0, 1, 2, ...)"
    f.accept("select * from emp where deptno in (" + list(", ", "", x) + ")");

    // E.g. "where x = 1 or x = 2 or x = 3 ..."
    f.accept("select * from emp where " + list(" or ", "deptno = ", x));

    // E.g. "select x1, x2 ... from (
    // select 'a' as x1, 'a' as x2, ... from emp union
    // select 'bb' as x1, 'bb' as x2, ... from dept)"
    f.accept("select " + list(", ", "x", x)
        + " from (select " + list(", ", "'a' as x", x) + " from emp "
        + "union all select " + list(", ", "'bb' as x", x) + " from dept)");
  }

  private static String list(String sep, String before, int count) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        buf.append(sep);
      }
      buf.append(before).append(i);
    }
    return buf.toString();
  }

  @Test void testAbstractConformance()
      throws InvocationTargetException, IllegalAccessException {
    final SqlAbstractConformance c0 = new SqlAbstractConformance() {
    };
    final SqlConformance c1 = SqlConformanceEnum.DEFAULT;
    for (Method method : SqlConformance.class.getMethods()) {
      final Object o0 = method.invoke(c0);
      final Object o1 = method.invoke(c1);
      assertThat(method.toString(), Objects.equals(o0, o1), is(true));
    }
  }

  @Test void testUserDefinedConformance() {
    final SqlAbstractConformance custom =
        new SqlDelegatingConformance(SqlConformanceEnum.DEFAULT) {
          public boolean isBangEqualAllowed() {
            return true;
          }
        };

    // Our conformance behaves differently from ORACLE_10 for FROM-less query.
    sql("^select 2+2^")
        .withConformance(custom)
        .ok()
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok()
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("SELECT must have a FROM clause");

    // Our conformance behaves like ORACLE_10 for "!=" operator.
    sql("select * from (values 1) where 1 ^!=^ 2")
        .withConformance(custom)
        .ok()
        .withConformance(SqlConformanceEnum.DEFAULT)
        .fails("Bang equal '!=' is not allowed under the current SQL conformance level")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .ok();

    sql("select * from (values 1) where 1 ^!=^ any (2, 3)")
        .withConformance(custom)
        .ok()
        .withConformance(SqlConformanceEnum.DEFAULT)
        .fails("Bang equal '!=' is not allowed under the current SQL conformance level")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .ok();
  }

  @Test void testOrder() {
    final SqlConformance conformance = tester.getConformance();
    sql("select empno as x from emp order by empno").ok();

    // invalid use of 'asc'
    sql("select empno, sal from emp order by ^asc^")
        .fails("Column 'ASC' not found in any table");

    // In sql92, empno is obscured by the alias.
    // Otherwise valid.
    // Checked Oracle10G -- is it valid.
    sql("select empno as x from emp order by empno")
        .failsIf(conformance.isSortByAliasObscures(),
            "unknown column empno");

    // valid in oracle and pre-99 sql,
    // invalid in sql:2003
    sql("select empno as x from emp order by ^x^")
        .failsIf(!conformance.isSortByAlias(),
            "Column 'X' not found in any table");

    // invalid in oracle and pre-99;
    // valid from sql:99 onwards (but sorting by constant achieves
    // nothing!)
    sql("select empno as x from emp order by ^10^")
        .failsIf(conformance.isSortByOrdinal(),
            "Ordinal out of range");

    // Has different meanings in different dialects (which makes it very
    // confusing!) but is always valid.
    sql("select empno + 1 as empno from emp order by empno").ok();

    // Always fails
    sql("select empno as x from emp, dept order by ^deptno^")
        .fails("Column 'DEPTNO' is ambiguous");

    sql("select empno + 1 from emp order by deptno asc, empno + 1 desc").ok();

    // Alias 'deptno' is closer in scope than 'emp.deptno'
    // and 'dept.deptno', and is therefore not ambiguous.
    // Checked Oracle10G -- it is valid.
    // Ambiguous in SQL:2003
    sql("select empno as deptno from emp, dept order by deptno")
        .failsIf(!conformance.isSortByAlias(),
            "col ambig");

    sql("select deptno from dept\n"
        + "union\n"
        + "select empno from emp\n"
        + "order by deptno").ok();

    sql("select deptno from dept\n"
        + "union\n"
        + "select empno from emp\n"
        + "order by ^empno^")
        .fails("Column 'EMPNO' not found in any table");

    // invalid in oracle and pre-99
    sql("select deptno from dept\n"
        + "union\n"
        + "select empno from emp\n"
        + "order by ^10^")
        .failsIf(conformance.isSortByOrdinal(),
            "Ordinal out of range");

    // Sort by scalar sub-query
    sql("select * from emp\n"
        + "order by (select name from dept where deptno = emp.deptno)").ok();
    sql("select * from emp\n"
        + "order by (select name from dept where deptno = emp.^foo^)")
        .fails("Column 'FOO' not found in table 'EMP'");

    // REVIEW jvs 10-Apr-2008:  I disabled this because I don't
    // understand what it means; see
    // testAggregateInOrderByFails for the discrimination I added
    // (SELECT should be aggregating for this to make sense).
/*
        // Sort by aggregate. Oracle allows this.
        check("select 1 from emp order by sum(sal)");
*/

    // ORDER BY and SELECT *
    sql("select * from emp order by empno").ok();
    sql("select * from emp order by ^nonExistent^, deptno")
        .fails("Column 'NONEXISTENT' not found in any table");

    // Overriding expression has different type.
    sql("select 'foo' as empno from emp order by ^empno + 5^")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply '\\+' to arguments of type '<CHAR\\(3\\)> \\+ <INTEGER>'\\..*");

    sql("select 'foo' as empno from emp order by empno + 5").ok();
  }

  @Test void testOrderJoin() {
    sql("select * from emp as e, dept as d order by e.empno").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-633">[CALCITE-633]
   * WITH ... ORDER BY cannot find table</a>. */
  @Test void testWithOrder() {
    sql("with e as (select * from emp)\n"
        + "select * from e as e1 order by e1.empno").ok();
    sql("with e as (select * from emp)\n"
        + "select * from e as e1, e as e2 order by e1.empno").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-662">[CALCITE-662]
   * Query validation fails when an ORDER BY clause is used with WITH
   * CLAUSE</a>. */
  @Test void testWithOrderInParentheses() {
    sql("with e as (select * from emp)\n"
        + "(select e.empno from e order by e.empno)").ok();
    sql("with e as (select * from emp)\n"
        + "(select e.empno from e order by 1)").ok();
    sql("with e as (select * from emp)\n"
        + "(select ee.empno from e as ee order by ee.deptno)").ok();
    // worked even before CALCITE-662 fixed
    sql("with e as (select * from emp)\n"
        + "(select e.empno from e)").ok();
  }

  @Test void testOrderUnion() {
    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by empno").ok();

    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by ^asc^")
        .fails("Column 'ASC' not found in any table");

    // name belongs to emp but is not projected so cannot sort on it
    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by ^ename^ desc")
        .fails("Column 'ENAME' not found in any table");

    // empno is not an alias in the first select in the union
    sql("select deptno, deptno as no2 from dept "
        + "union all "
        + "select empno, sal from emp "
        + "order by deptno asc, ^empno^")
        .fails("Column 'EMPNO' not found in any table");

    // ordinals ok
    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by 2").ok();

    // ordinal out of range -- if 'order by <ordinal>' means something in
    // this dialect
    if (tester.getConformance().isSortByOrdinal()) {
      sql("select empno, sal from emp "
          + "union all "
          + "select deptno, deptno from dept "
          + "order by ^3^")
          .fails("Ordinal out of range");
    }

    // Expressions made up of aliases are OK.
    // (This is illegal in Oracle 10G.)
    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by empno * sal + 2").ok();

    sql("select empno, sal from emp "
        + "union all "
        + "select deptno, deptno from dept "
        + "order by 'foobar'").ok();
  }

  /**
   * Tests validation of the ORDER BY clause when GROUP BY is present.
   */
  @Test void testOrderGroup() {
    // Group by
    sql("select 1 from emp group by deptno order by ^empno^")
        .fails("Expression 'EMPNO' is not being grouped");

    // order by can contain aggregate expressions
    sql("select empno from emp "
        + "group by empno, deptno "
        + "order by deptno * sum(sal + 2)").ok();

    // Having

    sql("select sum(sal) from emp having count(*) > 3 order by ^empno^")
        .fails("Expression 'EMPNO' is not being grouped");

    sql("select sum(sal) from emp having count(*) > 3 order by sum(deptno)").ok();

    // Select distinct

    sql("select distinct deptno from emp group by deptno order by ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");

    sql("select distinct deptno from emp group by deptno order by deptno, ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");

    sql("select distinct deptno from emp group by deptno order by deptno").ok();

    // UNION of SELECT DISTINCT and GROUP BY behaves just like a UNION.
    sql("select distinct deptno from dept "
        + "union all "
        + "select empno from emp group by deptno, empno "
        + "order by deptno").ok();

    // order by can contain a mixture of aliases and aggregate expressions
    sql("select empno as x "
        + "from emp "
        + "group by empno, deptno "
        + "order by x * sum(sal + 2)").ok();

    sql("select empno as x "
        + "from emp "
        + "group by empno, deptno "
        + "order by empno * sum(sal + 2)")
        .failsIf(tester.getConformance().isSortByAliasObscures(), "xxxx");
  }

  /**
   * Tests validation of the aliases in GROUP BY.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1306">[CALCITE-1306]
   * Allow GROUP BY and HAVING to reference SELECT expressions by ordinal and
   * alias</a>.
   *
   * @see SqlConformance#isGroupByAlias()
   */
  @Test void testAliasInGroupBy() {
    final SqlConformanceEnum lenient = SqlConformanceEnum.LENIENT;
    final SqlConformanceEnum strict = SqlConformanceEnum.STRICT_2003;

    // Group by
    sql("select empno as e from emp group by ^e^")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select empno as e from emp group by ^e^")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select emp.empno as e from emp group by ^e^")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select e.empno from emp as e group by e.empno")
        .withConformance(strict).ok()
        .withConformance(lenient).ok();
    sql("select e.empno as eno from emp as e group by ^eno^")
        .withConformance(strict).fails("Column 'ENO' not found in any table")
        .withConformance(lenient).ok();
    sql("select deptno as dno from emp group by cube(^dno^)")
        .withConformance(strict).fails("Column 'DNO' not found in any table")
        .withConformance(lenient).ok();
    sql("select deptno as dno, ename name, sum(sal) from emp\n"
        + "group by grouping sets ((^dno^), (name, deptno))")
        .withConformance(strict).fails("Column 'DNO' not found in any table")
        .withConformance(lenient).ok();
    sql("select ename as deptno from emp as e join dept as d on "
        + "e.deptno = d.deptno group by ^deptno^")
        .withConformance(lenient).ok();
    sql("select t.e, count(*) from (select empno as e from emp) t group by e")
        .withConformance(strict).ok()
        .withConformance(lenient).ok();

    // The following 2 tests have the same SQL but fail for different reasons.
    sql("select t.e, count(*) as c from "
        + " (select empno as e from emp) t group by e,^c^")
        .withConformance(strict).fails("Column 'C' not found in any table");
    sql("select t.e, ^count(*)^ as c from "
        + " (select empno as e from emp) t group by e,c")
        .withConformance(lenient).fails(ERR_AGG_IN_GROUP_BY);

    sql("select t.e, e + ^count(*)^ as c from "
        + " (select empno as e from emp) t group by e,c")
        .withConformance(lenient).fails(ERR_AGG_IN_GROUP_BY);
    sql("select t.e, e + ^count(*)^ as c from "
        + " (select empno as e from emp) t group by e,2")
        .withConformance(lenient).fails(ERR_AGG_IN_GROUP_BY)
        .withConformance(strict).ok();

    sql("select deptno,(select empno + 1 from emp) eno\n"
        + "from dept group by deptno,^eno^")
        .withConformance(strict).fails("Column 'ENO' not found in any table")
        .withConformance(lenient).ok();
    sql("select empno as e, deptno as e\n"
        + "from emp group by ^e^")
        .withConformance(lenient).fails("Column 'E' is ambiguous");
    sql("select empno, ^count(*)^ c from emp group by empno, c")
        .withConformance(lenient).fails(ERR_AGG_IN_GROUP_BY);
    sql("select deptno + empno as d, deptno + empno + mgr from emp"
        + " group by d,mgr")
        .withConformance(lenient).ok();
    // When alias is equal to one or more columns in the query then giving
    // priority to alias. But Postgres may throw ambiguous column error or give
    // priority to column name.
    sql("select count(*) from (\n"
        + "  select ename AS deptno FROM emp GROUP BY deptno) t")
        .withConformance(lenient).ok();
    sql("select count(*) from "
        + "(select ename AS deptno FROM emp, dept GROUP BY deptno) t")
        .withConformance(lenient).ok();
    sql("select empno + deptno AS \"z\" FROM emp GROUP BY \"Z\"")
        .withConformance(lenient).withCaseSensitive(false).ok();
    sql("select empno + deptno as c, ^c^ + mgr as d from emp group by c, d")
        .withConformance(lenient).fails("Column 'C' not found in any table");
    // Group by alias with strict conformance should fail.
    sql("select empno as e from emp group by ^e^")
        .withConformance(strict).fails("Column 'E' not found in any table");
  }

  /**
   * Tests validation of ordinals in GROUP BY.
   *
   * @see SqlConformance#isGroupByOrdinal()
   */
  @Test void testOrdinalInGroupBy() {
    final SqlConformanceEnum lenient = SqlConformanceEnum.LENIENT;
    final SqlConformanceEnum strict = SqlConformanceEnum.STRICT_2003;

    sql("select ^empno^,deptno from emp group by 1, deptno")
        .withConformance(strict).fails("Expression 'EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^emp.empno^ as e from emp group by 1")
        .withConformance(strict).fails("Expression 'EMP.EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select 2 + ^emp.empno^ + 3 as e from emp group by 1")
        .withConformance(strict).fails("Expression 'EMP.EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^e.empno^ from emp as e group by 1")
        .withConformance(strict).fails("Expression 'E.EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select e.empno from emp as e group by 1, empno")
        .withConformance(strict).ok()
        .withConformance(lenient).ok();
    sql("select ^e.empno^ as eno from emp as e group by 1")
        .withConformance(strict).fails("Expression 'E.EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^deptno^ as dno from emp group by cube(1)")
        .withConformance(strict).fails("Expression 'DEPTNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select 1 as dno from emp group by cube(1)")
        .withConformance(strict).ok()
        .withConformance(lenient).ok();
    sql("select deptno as dno, ename name, sum(sal) from emp\n"
        + "group by grouping sets ((1), (^name^, deptno))")
        .withConformance(strict).fails("Column 'NAME' not found in any table")
        .withConformance(lenient).ok();
    sql("select ^e.deptno^ from emp as e\n"
        + "join dept as d on e.deptno = d.deptno group by 1")
        .withConformance(strict).fails("Expression 'E.DEPTNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^deptno^,(select empno from emp) eno from dept"
        + " group by 1,2")
        .withConformance(strict).fails("Expression 'DEPTNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^empno^, count(*) from emp group by 1 order by 1")
        .withConformance(strict).fails("Expression 'EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select ^empno^ eno, count(*) from emp group by 1 order by 1")
        .withConformance(strict).fails("Expression 'EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    sql("select count(*) from (select 1 from emp"
        + " group by substring(ename from 2 for 3))")
        .withConformance(strict).ok()
        .withConformance(lenient).ok();
    sql("select deptno from emp group by deptno, ^100^")
        .withConformance(lenient).fails("Ordinal out of range")
        .withConformance(strict).ok();
    // Calcite considers integers in GROUP BY to be constants, so test passes.
    // Postgres considers them ordinals and throws out of range position error.
    sql("select deptno from emp group by ^100^, deptno")
        .withConformance(lenient).fails("Ordinal out of range")
        .withConformance(strict).ok();
  }

  /**
   * Tests validation of the aliases in HAVING.
   *
   * @see SqlConformance#isHavingAlias()
   */
  @Test void testAliasInHaving() {
    final SqlConformanceEnum lenient = SqlConformanceEnum.LENIENT;
    final SqlConformanceEnum strict = SqlConformanceEnum.STRICT_2003;

    sql("select count(empno) as e from emp having ^e^ > 10")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select emp.empno as e from emp group by ^e^ having e > 10")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select emp.empno as e from emp group by empno having ^e^ > 10")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
    sql("select e.empno from emp as e group by 1 having ^e.empno^ > 10")
        .withConformance(strict).fails("Expression 'E.EMPNO' is not being grouped")
        .withConformance(lenient).ok();
    // When alias is equal to one or more columns in the query then giving
    // priority to alias, but PostgreSQL throws ambiguous column error or gives
    // priority to column name.
    sql("select count(empno) as deptno from emp having ^deptno^ > 10")
        .withConformance(strict).fails("Expression 'DEPTNO' is not being grouped")
        .withConformance(lenient).ok();
    // Alias in aggregate is not allowed.
    sql("select empno as e from emp having max(^e^) > 10")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).fails("Column 'E' not found in any table");
    sql("select count(empno) as e from emp having ^e^ > 10")
        .withConformance(strict).fails("Column 'E' not found in any table")
        .withConformance(lenient).ok();
  }

  /**
   * Tests validation of the ORDER BY clause when DISTINCT is present.
   */
  @Test void testOrderDistinct() {
    // Distinct on expressions with attempts to order on a column in
    // the underlying table
    sql("select distinct cast(empno as bigint) "
        + "from emp order by ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");
    sql("select distinct cast(empno as bigint) "
        + "from emp order by ^emp.empno^")
        .fails("Expression 'EMP\\.EMPNO' is not in the select clause");
    sql("select distinct cast(empno as bigint) as empno "
        + "from emp order by ^emp.empno^")
        .fails("Expression 'EMP\\.EMPNO' is not in the select clause");
    sql("select distinct cast(empno as bigint) as empno "
        + "from emp as e order by ^e.empno^")
        .fails("Expression 'E\\.EMPNO' is not in the select clause");

    // These tests are primarily intended to test cases where sorting by
    // an alias is allowed.  But for instances that don't support sorting
    // by alias, the tests also verify that a proper exception is thrown.
    final SqlConformance conformance = tester.getConformance();
    sql("select distinct cast(empno as bigint) as empno "
        + "from emp order by ^empno^")
        .failsIf(!conformance.isSortByAlias(),
            "Expression 'EMPNO' is not in the select clause");
    sql("select distinct cast(empno as bigint) as eno "
        + "from emp order by ^eno^")
        .failsIf(!conformance.isSortByAlias(),
            "Column 'ENO' not found in any table");
    sql("select distinct cast(empno as bigint) as empno "
        + "from emp e order by ^empno^")
        .failsIf(!conformance.isSortByAlias(),
            "Expression 'EMPNO' is not in the select clause");

    // Distinct on expressions, sorting using ordinals.
    if (conformance.isSortByOrdinal()) {
      sql("select distinct cast(empno as bigint) from emp order by 1").ok();
      sql("select distinct cast(empno as bigint) as empno "
          + "from emp order by 1").ok();
      sql("select distinct cast(empno as bigint) as empno "
          + "from emp as e order by 1").ok();
    }

    // Distinct on expressions with ordering on expressions as well
    sql("select distinct cast(empno as varchar(10)) from emp "
        + "order by cast(empno as varchar(10))").ok();
    sql("select distinct cast(empno as varchar(10)) as eno from emp "
        + " order by upper(^eno^)")
        .failsIf(!conformance.isSortByAlias(),
            "Column 'ENO' not found in any table");
  }

  /**
   * Tests validation of the ORDER BY clause when DISTINCT and GROUP BY are
   * present.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-634">[CALCITE-634]
   * Allow ORDER BY aggregate function in SELECT DISTINCT, provided that it
   * occurs in SELECT clause</a>.
   */
  @Test void testOrderGroupDistinct() {
    // Order by an aggregate function,
    // which exists in select-clause with distinct being present
    sql("select distinct count(empno) AS countEMPNO from emp\n"
        + "group by empno\n"
        + "order by 1").ok();

    sql("select distinct count(empno) from emp\n"
        + "group by empno\n"
        + "order by 1").ok();

    sql("select distinct count(empno) AS countEMPNO from emp\n"
        + "group by empno\n"
        + "order by count(empno)").ok();

    sql("select distinct count(empno) from emp\n"
        + "group by empno\n"
        + "order by count(empno)").ok();

    sql("select distinct count(empno) from emp\n"
        + "group by empno\n"
        + "order by count(empno) desc").ok();

    sql("SELECT DISTINCT deptno from emp\n"
        + "ORDER BY deptno, ^sum(empno)^")
        .fails("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT");
    sql("SELECT DISTINCT deptno from emp\n"
        + "GROUP BY deptno ORDER BY deptno, ^sum(empno)^")
        .fails("Expression 'SUM\\(`EMPNO`\\)' is not in the select clause");
    sql("SELECT DISTINCT deptno, min(empno) from emp\n"
        + "GROUP BY deptno ORDER BY deptno, ^sum(empno)^")
        .fails("Expression 'SUM\\(`EMPNO`\\)' is not in the select clause");
    sql("SELECT DISTINCT deptno, sum(empno) from emp\n"
        + "GROUP BY deptno ORDER BY deptno, sum(empno)").ok();
  }

  @Test void testGroup() {
    sql("select empno from emp where ^sum(sal)^ > 50")
        .fails("Aggregate expression is illegal in WHERE clause");

    sql("select ^empno^ from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");

    sql("select ^*^ from emp group by deptno")
        .fails("Expression 'EMP\\.EMPNO' is not being grouped");

    // If we're grouping on ALL columns, 'select *' is ok.
    // Checked on Oracle10G.
    sql("select * from (select empno,deptno from emp) group by deptno,empno").ok();

    // This query tries to reference an agg expression from within a
    // sub-query as a correlating expression, but the SQL syntax rules say
    // that the agg function SUM always applies to the current scope.
    // As it happens, the query is valid.
    sql("select deptno\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having exists (select sum(emp.sal) > 10 from (values(true)))").ok();

    // if you reference a column from a sub-query, it must be a group col
    sql("select deptno "
        + "from emp "
        + "group by deptno "
        + "having exists (select 1 from (values(true)) where emp.deptno = 10)").ok();

    // Needs proper error message text and error markers in query
    if (TODO) {
      sql("select deptno "
          + "from emp "
          + "group by deptno "
          + "having exists (select 1 from (values(true)) where emp.empno = 10)")
          .fails("xx");
    }

    // constant expressions
    sql("select cast(1 as integer) + 2 from emp group by deptno").ok();
    sql("select localtime, deptno + 3 from emp group by deptno").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-886">[CALCITE-886]
   * System functions in GROUP BY clause</a>. */
  @Test void testGroupBySystemFunction() {
    sql("select CURRENT_USER from emp group by CURRENT_USER").ok();
    sql("select CURRENT_USER from emp group by rollup(CURRENT_USER)").ok();
    sql("select CURRENT_USER from emp group by rollup(CURRENT_USER, ^x^)")
        .fails("Column 'X' not found in any table");
    sql("select CURRENT_USER from emp group by deptno").ok();
  }

  @Test void testGroupingSets() {
    sql("select count(1), ^empno^ from emp group by grouping sets (deptno)")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))\n"
        + "order by 2").ok();

    // duplicate (and heavily nested) GROUPING SETS
    sql("select sum(sal) from emp\n"
        + "group by deptno,\n"
        + "  grouping sets (deptno,\n"
        + "    grouping sets (deptno, ename),\n"
        + "      (ename)),\n"
        + "  ()").ok();
  }

  @Test void testRollup() {
    // DEPTNO is not null in database, but rollup introduces nulls
    sql("select deptno, count(*) as c, sum(sal) as s\n"
        + "from emp\n"
        + "group by rollup(deptno)")
        .ok()
        .type("RecordType(INTEGER DEPTNO, BIGINT NOT NULL C, INTEGER NOT NULL S) NOT NULL");

    // EMPNO stays NOT NULL because it is not rolled up
    sql("select deptno, empno\n"
        + "from emp\n"
        + "group by empno, rollup(deptno)")
        .ok()
        .type("RecordType(INTEGER DEPTNO, INTEGER NOT NULL EMPNO) NOT NULL");

    // DEPTNO stays NOT NULL because it is not rolled up
    sql("select deptno, empno\n"
        + "from emp\n"
        + "group by rollup(empno), deptno")
        .ok()
        .type("RecordType(INTEGER NOT NULL DEPTNO, INTEGER EMPNO) NOT NULL");
  }

  @Test void testGroupByCorrelatedColumn() {
    // This is not sql 2003 standard; see sql2003 part2,  7.9
    // But the extension seems harmless.
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "where exists (select count(*) from dept group by emp.empno)";
    sql(sql).ok();
  }

  @Test void testGroupExpressionEquivalence() {
    // operator equivalence
    sql("select empno + 1 from emp group by empno + 1").ok();
    sql("select 1 + ^empno^ from emp group by empno + 1")
        .fails("Expression 'EMPNO' is not being grouped");

    // datatype equivalence
    sql("select cast(empno as VARCHAR(10)) from emp\n"
        + "group by cast(empno as VARCHAR(10))").ok();
    sql("select cast(^empno^ as VARCHAR(11)) from emp group by cast(empno as VARCHAR(10))")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testGroupExpressionEquivalenceId() {
    // identifier equivalence
    sql("select case empno when 10 then deptno else null end from emp "
        + "group by case empno when 10 then deptno else null end").ok();

    // matches even when one column is qualified (checked on Oracle10.1)
    sql("select case empno when 10 then deptno else null end from emp "
        + "group by case empno when 10 then emp.deptno else null end").ok();
    sql("select case empno when 10 then deptno else null end from emp "
        + "group by case emp.empno when 10 then emp.deptno else null end").ok();
    sql("select case emp.empno when 10 then deptno else null end from emp "
        + "group by case empno when 10 then emp.deptno else null end").ok();

    // emp.deptno is different to dept.deptno (even though there is an '='
    // between them)
    sql("select case ^emp.empno^ when 10 then emp.deptno else null end "
        + "from emp join dept on emp.deptno = dept.deptno "
        + "group by case emp.empno when 10 then dept.deptno else null end")
        .fails("Expression 'EMP\\.EMPNO' is not being grouped");
  }

  // todo: enable when correlating variables work
  public void _testGroupExpressionEquivalenceCorrelated() {
    // dname comes from dept, so it is constant within the sub-query, and
    // is so is a valid expr in a group-by query
    sql("select * from dept where exists ("
        + "select dname from emp group by empno)").ok();
    sql("select * from dept where exists ("
        + "select dname + empno + 1 from emp group by empno, dept.deptno)").ok();
  }

  // todo: enable when params are implemented
  public void _testGroupExpressionEquivalenceParams() {
    sql("select cast(? as integer) from emp group by cast(? as integer)").ok();
  }

  @Test void testGroupExpressionEquivalenceLiteral() {
    // The purpose of this test is to see whether the validator
    // regards a pair of constants as equivalent. If we just used the raw
    // constants the validator wouldn't care ('SELECT 1 FROM emp GROUP BY
    // 2' is legal), so we combine a column and a constant into the same
    // CASE expression.

    // literal equivalence
    sql("select case empno when 10 then date '1969-04-29' else null end\n"
        + "from emp\n"
        + "group by case empno when 10 then date '1969-04-29' else null end").ok();

    // this query succeeds in oracle 10.1 because 1 and 1.0 have the same
    // type
    sql("select case ^empno^ when 10 then 1 else null end from emp "
        + "group by case empno when 10 then 1.0 else null end")
        .fails("Expression 'EMPNO' is not being grouped");

    // 3.1415 and 3.14150 are different literals (I don't care either way)
    sql("select case ^empno^ when 10 then 3.1415 else null end from emp "
        + "group by case empno when 10 then 3.14150 else null end")
        .fails("Expression 'EMPNO' is not being grouped");

    // 3 and 03 are the same literal (I don't care either way)
    sql("select case empno when 10 then 03 else null end from emp "
        + "group by case empno when 10 then 3 else null end").ok();
    sql("select case ^empno^ when 10 then 1 else null end from emp "
        + "group by case empno when 10 then 2 else null end")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select case empno when 10 then timestamp '1969-04-29 12:34:56.0'\n"
        + "       else null end from emp\n"
        + "group by case empno when 10 then timestamp '1969-04-29 12:34:56'\n"
        + "              else null end")
        .ok();
  }

  @Test void testGroupExpressionEquivalenceStringLiteral() {
    sql("select case empno when 10 then 'foo bar' else null end from emp "
        + "group by case empno when 10 then 'foo bar' else null end").ok();

    if (Bug.FRG78_FIXED) {
      final String sql = "select case empno when 10\n"
          + "      then _iso-8859-1'foo bar' collate latin1$en$1\n"
          + "      else null end\n"
          + "from emp\n"
          + "group by case empno when 10\n"
          + "      then _iso-8859-1'foo bar' collate latin1$en$1\n"
          + "      else null end";
      sql(sql).ok();
    }

    sql("select case ^empno^ when 10 then _iso-8859-1'foo bar' else null end from emp "
        + "group by case empno when 10 then _UTF16'foo bar' else null end")
        .fails("Expression 'EMPNO' is not being grouped");

    if (Bug.FRG78_FIXED) {
      sql("select case ^empno^ when 10 then 'foo bar' collate latin1$en$1 else null end from emp "
          + "group by case empno when 10 then 'foo bar' collate latin1$fr$1 else null end")
          .fails("Expression 'EMPNO' is not being grouped");
    }
  }

  @Test void testGroupByAliasedColumn() {
    // alias in GROUP BY query has been known to cause problems
    sql("select deptno as d, count(*) as c from emp group by deptno").ok();
  }

  @Test void testNestedAggFails() {
    // simple case
    sql("select ^sum(max(empno))^ from emp")
        .fails(ERR_NESTED_AGG);

    // should still fail with intermediate expression
    sql("select ^sum(2*max(empno))^ from emp")
        .fails(ERR_NESTED_AGG);

    // make sure it fails with GROUP BY too
    sql("select ^sum(max(empno))^ from emp group by deptno")
        .fails(ERR_NESTED_AGG);

    // make sure it fails in HAVING too
    sql("select count(*) from emp group by deptno "
        + "having ^sum(max(empno))^=3")
        .fails(ERR_NESTED_AGG);

    // double-nesting should fail too; bottom-up validation currently
    // causes us to flag the intermediate level
    sql("select sum(^max(min(empno))^) from emp")
        .fails(ERR_NESTED_AGG);
  }

  @Test void testNestedAggOver() {
    sql("select sum(max(empno))\n"
        + " OVER (order by ^deptno^ ROWS 2 PRECEDING)\n"
        + " from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select sum(max(empno)) OVER w\n"
        + " from emp\n"
        + " window w as (order by ^deptno^ ROWS 2 PRECEDING)")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select sum(max(empno)) OVER w2, sum(deptno) OVER w1\n"
        + " from emp group by deptno\n"
        + " window w1 as (partition by ^empno^ ROWS 2 PRECEDING),\n"
        + " w2 as (order by deptno ROWS 2 PRECEDING)")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(count(empno)) over w\n"
        + "from emp group by deptno\n"
        + "window w as (partition by deptno order by ^empno^)")
        .fails("Expression 'EMPNO' is not being grouped");

    // in OVER clause with more than one level of nesting
    sql("select ^avg(sum(min(sal)))^ OVER (partition by deptno)\n"
        + "from emp group by deptno")
        .fails(ERR_NESTED_AGG);

    // OVER clause columns not appearing in GROUP BY clause NOT OK
    sql("select avg(^sal^) OVER (), avg(count(empno)) OVER (partition by 1)\n"
        + " from emp")
        .fails("Expression 'SAL' is not being grouped");
    sql("select avg(^sal^) OVER (), avg(count(empno)) OVER (partition by deptno)\n"
        + " from emp group by deptno")
        .fails("Expression 'SAL' is not being grouped");
    sql("select avg(deptno) OVER (partition by ^empno^),\n"
        + " avg(count(empno)) OVER (partition by deptno)\n"
        + " from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(deptno) OVER (order by ^empno^),\n"
        + " avg(count(empno)) OVER (partition by deptno)\n"
        + " from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by ^empno^)\n"
        + " from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by ^empno^)\n"
        + "from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by ^empno^)\n"
        + " from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by ^empno^)\n"
        + "from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by deptno order by ^empno^)\n"
        + "from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(^sal^) OVER (),\n"
        + " avg(count(empno)) OVER (partition by min(deptno))\n"
        + " from emp")
        .fails("Expression 'SAL' is not being grouped");
    sql("select avg(deptno) OVER (),\n"
        + " avg(count(empno)) OVER (partition by deptno)"
        + " from emp group by deptno").ok();

    // COUNT(*), PARTITION BY(CONST) with GROUP BY is OK
    sql("select avg(count(*)) OVER ()\n"
        + " from emp group by deptno").ok();
    sql("select count(*) OVER ()\n"
        + " from emp group by deptno").ok();
    sql("select count(deptno) OVER ()\n"
        + " from emp group by deptno").ok();
    sql("select count(deptno, deptno + 1) OVER ()\n"
        + " from emp group by deptno").ok();
    sql("select count(deptno, deptno + 1, ^empno^) OVER ()\n"
        + " from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select count(emp.^*^)\n"
        + "from emp group by deptno")
        .fails("Unknown field '\\*'");
    sql("select count(emp.^*^) OVER ()\n"
        + "from emp group by deptno")
        .fails("Unknown field '\\*'");
    sql("select avg(count(*)) OVER (partition by 1)\n"
        + " from emp group by deptno").ok();
    sql("select avg(sum(sal)) OVER (partition by 1)\n"
        + " from emp").ok();
    sql("select avg(sal), avg(count(empno)) OVER (partition by 1)\n"
        + " from emp").ok();

    // expression is OK
    sql("select avg(sum(sal)) OVER (partition by 10 - deptno\n"
        + "   order by deptno / 2 desc)\n"
        + "from emp group by deptno").ok();
    sql("select avg(sal),\n"
        + " avg(count(empno)) OVER (partition by min(deptno))\n"
        + " from emp").ok();
    sql("select avg(min(sal)) OVER (),\n"
        + " avg(count(empno)) OVER (partition by min(deptno))\n"
        + " from emp").ok();

    // expression involving non-GROUP column is not OK
    sql("select avg(2+^sal^) OVER (partition by deptno)\n"
        + "from emp group by deptno")
        .fails("Expression 'SAL' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by deptno + ^empno^)\n"
        + "from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by empno + deptno)\n"
        + "from emp group by empno + deptno").ok();
    sql("select avg(sum(sal)) OVER (partition by empno + deptno + 1)\n"
        + "from emp group by empno + deptno").ok();
    sql("select avg(sum(sal)) OVER (partition by ^deptno^ + 1)\n"
        + "from emp group by empno + deptno")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select avg(empno + deptno) OVER (partition by empno + deptno + 1),\n"
        + " count(empno + deptno) OVER (partition by empno + deptno + 1)\n"
        + " from emp group by empno + deptno").ok();

    // expression is NOT OK (AS clause)
    sql("select sum(max(empno))\n"
        + " OVER (order by ^deptno^ ROWS 2 PRECEDING) AS sumEmpNo\n"
        + " from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select sum(max(empno)) OVER w AS sumEmpNo\n"
        + " from emp\n"
        + " window w AS (order by ^deptno^ ROWS 2 PRECEDING)")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select avg(^sal^) OVER () AS avgSal,"
        + " avg(count(empno)) OVER (partition by 1) AS avgEmpNo\n"
        + " from emp")
        .fails("Expression 'SAL' is not being grouped");
    sql("select count(deptno, deptno + 1, ^empno^) OVER () AS cntDeptEmp\n"
        + " from emp group by deptno")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select avg(sum(sal)) OVER (partition by ^deptno^ + 1) AS avgSal\n"
        + "from emp group by empno + deptno")
        .fails("Expression 'DEPTNO' is not being grouped");

    // OVER in clause
    sql("select ^sum(max(empno) OVER (order by deptno ROWS 2 PRECEDING))^ from emp")
        .fails(ERR_NESTED_AGG);
  }

  @Test void testAggregateInGroupByFails() {
    sql("select count(*) from emp group by ^sum(empno)^")
        .fails(ERR_AGG_IN_GROUP_BY);
  }

  @Test void testAggregateInNonGroupBy() {
    sql("select count(1), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select count(*) from emp")
        .columnType("BIGINT NOT NULL");
    sql("select count(deptno) from emp")
        .columnType("BIGINT NOT NULL");

    // Even though deptno is not null, its sum may be, because emp may be empty.
    sql("select sum(deptno) from emp")
        .columnType("INTEGER");
    sql("select sum(deptno) from emp group by ()")
        .columnType("INTEGER");
    sql("select sum(deptno) from emp group by empno")
        .columnType("INTEGER NOT NULL");
  }

  @Test void testAggregateInOrderByFails() {
    sql("select empno from emp order by ^sum(empno)^")
        .fails(ERR_AGG_IN_ORDER_BY);

    // but this should be OK
    sql("select sum(empno) from emp group by deptno order by sum(empno)").ok();

    // this should also be OK
    sql("select sum(empno) from emp order by sum(empno)").ok();
  }

  @Test void testAggregateFilter() {
    sql("select sum(empno) filter (where deptno < 10) as s from emp")
        .type("RecordType(INTEGER S) NOT NULL");
  }

  @Test void testAggregateFilterNotBoolean() {
    sql("select sum(empno) filter (where ^deptno + 10^) from emp")
        .fails("FILTER clause must be a condition");
  }

  @Test void testAggregateFilterInHaving() {
    sql("select sum(empno) as s from emp\n"
        + "group by deptno\n"
        + "having sum(empno) filter (where deptno < 20) > 10")
        .ok();
  }

  @Test void testAggregateFilterContainsAggregate() {
    sql("select sum(empno) filter (where ^count(*) < 10^) from emp")
        .fails("FILTER must not contain aggregate expression");
  }

  @Test void testWithinGroup() {
    sql("select deptno,\n"
        + " collect(empno) within group(order by 1)\n"
        + "from emp\n"
        + "group by deptno").ok();
    sql("select collect(empno) within group(order by 1)\n"
        + "from emp\n"
        + "group by ()").ok();
    sql("select deptno,\n"
        + " collect(empno) within group(order by deptno)\n"
        + "from emp\n"
        + "group by deptno").ok();
    sql("select deptno,\n"
        + " collect(empno) within group(order by deptno, hiredate desc)\n"
        + "from emp\n"
        + "group by deptno").ok();
    sql("select deptno,\n"
        + " collect(empno) within group(\n"
        + "  order by cast(deptno as varchar), hiredate desc)\n"
        + "from emp\n"
        + "group by deptno").ok();
    sql("select collect(empno) within group(order by 1)\n"
        + "from emp\n"
        + "group by deptno").ok();
    sql("select collect(empno) within group(order by 1)\n"
        + "from emp").ok();
    sql("select ^power(deptno, 1) within group(order by 1)^ from emp")
        .fails("(?s).*WITHIN GROUP not allowed with POWER function.*");
    sql("select ^collect(empno)^ within group(order by count(*))\n"
        + "from emp\n"
        + "group by deptno")
        .fails("WITHIN GROUP must not contain aggregate expression");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4644">[CALCITE-4644]
   * Add PERCENTILE_CONT and PERCENTILE_DISC aggregate functions</a>. */
  @Test void testPercentile() {
    final String sql = "select\n"
        + " percentile_cont(0.25) within group (order by sal) as c,\n"
        + " percentile_disc(0.5) within group (order by sal desc) as d\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql)
        .type("RecordType(DOUBLE NOT NULL C, DOUBLE NOT NULL D) NOT NULL");
  }

  /** Tests that {@code PERCENTILE_CONT} only allows numeric fields. */
  @Test void testPercentileContMustOrderByNumeric() {
    final String sql = "select\n"
        + " percentile_cont(0.25) within group (^order by ename^)\n"
        + "from emp";
    sql(sql)
        .fails("Invalid type 'VARCHAR' in ORDER BY clause of "
            + "'PERCENTILE_CONT' function. Only NUMERIC types are supported");
  }

  /** Tests that {@code PERCENTILE_CONT} only allows one sort key. */
  @Test void testPercentileContMultipleOrderByFields() {
    final String sql = "select\n"
        + " percentile_cont(0.25) within group (^order by deptno, empno^)\n"
        + "from emp";
    sql(sql)
        .fails("'PERCENTILE_CONT' requires precisely one ORDER BY key");
  }

  @Test void testPercentileContFractionMustBeLiteral() {
    final String sql = "select\n"
        + " ^percentile_cont(deptno)^ within group (order by empno)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql)
        .fails("Argument to function 'PERCENTILE_CONT' must be a literal");
  }

  @Test void testPercentileContFractionOutOfRange() {
    final String sql = "select\n"
        + " ^percentile_cont(1.5)^ within group (order by deptno)\n"
        + "from emp";
    sql(sql)
        .fails("Argument to function 'PERCENTILE_CONT' must be a numeric "
            + "literal between 0 and 1");
  }

  /** Tests that {@code PERCENTILE_DISC} only allows numeric fields. */
  @Test void testPercentileDiscMustOrderByNumeric() {
    final String sql = "select\n"
        + " percentile_disc(0.25) within group (^order by ename^)\n"
        + "from emp";
    sql(sql)
        .fails("Invalid type 'VARCHAR' in ORDER BY clause of "
            + "'PERCENTILE_DISC' function. Only NUMERIC types are supported");
  }

  /** Tests that {@code PERCENTILE_DISC} only allows one sort key. */
  @Test void testPercentileDiscMultipleOrderByFields() {
    final String sql = "select\n"
        + " percentile_disc(0.25) within group (^order by deptno, empno^)\n"
        + "from emp";
    sql(sql)
        .fails("'PERCENTILE_DISC' requires precisely one ORDER BY key");
  }

  @Test void testCorrelatingVariables() {
    // reference to unqualified correlating column
    sql("select * from emp where exists (\n"
        + "select * from dept where deptno = sal)").ok();

    // reference to qualified correlating column
    sql("select * from emp where exists (\n"
        + "select * from dept where deptno = emp.sal)").ok();
  }

  @Test void testIntervalCompare() {
    expr("interval '1' hour = interval '1' day")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' hour <> interval '1' hour")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' hour < interval '1' second")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' hour <= interval '1' minute")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' minute > interval '1' second")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' second >= interval '1' day")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' year >= interval '1' year")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' month = interval '1' year")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' month <> interval '1' month")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '1' year >= interval '1' month")
        .columnType("BOOLEAN NOT NULL");

    wholeExpr("interval '1' second >= interval '1' year")
        .fails("(?s).*Cannot apply '>=' to arguments of type "
            + "'<INTERVAL SECOND> >= <INTERVAL YEAR>'.*");
    wholeExpr("interval '1' month = interval '1' day")
        .fails("(?s).*Cannot apply '=' to arguments of type "
            + "'<INTERVAL MONTH> = <INTERVAL DAY>'.*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-613">[CALCITE-613]
   * Implicitly convert strings in comparisons</a>. */
  @Test void testDateCompare() {
    // can convert character value to date, time, timestamp, interval
    // provided it is on one side of a comparison operator (=, <, >, BETWEEN)
    expr("date '2015-03-17' < '2015-03-18'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' > '2015-03-18'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' = '2015-03-18'")
        .columnType("BOOLEAN NOT NULL");
    expr("'2015-03-17' < date '2015-03-18'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' between '2015-03-16' and '2015-03-19'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' between '2015-03-16' and '2015-03'||'-19'")
        .columnType("BOOLEAN NOT NULL");
    expr("'2015-03-17' between date '2015-03-16' and date '2015-03-19'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' between date '2015-03-16' and '2015-03-19'")
        .columnType("BOOLEAN NOT NULL");
    expr("date '2015-03-17' between '2015-03-16' and date '2015-03-19'")
        .columnType("BOOLEAN NOT NULL");
    expr("time '12:34:56' < '12:34:57'")
        .columnType("BOOLEAN NOT NULL");
    expr("timestamp '2015-03-17 12:34:56' < '2015-03-17 12:34:57'")
        .columnType("BOOLEAN NOT NULL");
    expr("interval '2' hour < '2:30'")
        .columnType("BOOLEAN NOT NULL");

    // can convert to exact and approximate numeric
    expr("123 > '72'")
        .columnType("BOOLEAN NOT NULL");
    expr("12.3 > '7.2'")
        .columnType("BOOLEAN NOT NULL");

    // can convert to boolean
    expr("true = 'true'")
        .columnType("BOOLEAN NOT NULL");
    expr("^true and 'true'^")
        .fails("Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <CHAR\\(4\\)>'\\..*");
  }

  @Test void testOverlaps() {
    expr("(date '1-2-3', date '1-2-3')\n"
        + " overlaps (date '1-2-3', date '1-2-3')")
        .columnType("BOOLEAN NOT NULL");
    expr("period (date '1-2-3', date '1-2-3')\n"
        + " overlaps period (date '1-2-3', date '1-2-3')")
        .columnType("BOOLEAN NOT NULL");
    expr("(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' "
        + "year)").ok();
    expr("(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time"
        + " '1:2:3')").ok();
    expr("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps "
        + "(timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)").ok();

    wholeExpr("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' )\n"
        + " overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    wholeExpr("(time '4:5:6', timestamp '1-2-3 4:5:6' )\n"
        + " overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    wholeExpr("(time '4:5:6', time '4:5:6' )\n"
        + " overlaps (time '4:5:6', date '1-2-3')")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    wholeExpr("1 overlaps 2")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type "
            + "'<INTEGER> OVERLAPS <INTEGER>'\\. Supported form.*");

    expr("true\n"
        + "or (date '1-2-3', date '1-2-3')\n"
        + "   overlaps (date '1-2-3', date '1-2-3')\n"
        + "or false")
        .columnType("BOOLEAN NOT NULL");
    // row with 3 arguments as left argument to overlaps
    expr("true\n"
        + "or ^(date '1-2-3', date '1-2-3', date '1-2-3')\n"
        + "   overlaps (date '1-2-3', date '1-2-3')^\n"
        + "or false")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    // row with 3 arguments as right argument to overlaps
    expr("true\n"
        + "or ^(date '1-2-3', date '1-2-3')\n"
        + "  overlaps (date '1-2-3', date '1-2-3', date '1-2-3')^\n"
        + "or false")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    expr("^period (date '1-2-3', date '1-2-3')\n"
        + "   overlaps (date '1-2-3', date '1-2-3', date '1-2-3')^")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    expr("true\n"
        + "or ^(1, 2) overlaps (2, 3)^\n"
        + "or false")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");

    // Other operators with similar syntax
    String[] ops = {
        "overlaps", "contains", "equals", "precedes", "succeeds",
        "immediately precedes", "immediately succeeds"
    };
    for (String op : ops) {
      expr("period (date '1-2-3', date '1-2-3')\n"
          + " " + op + " period (date '1-2-3', date '1-2-3')")
          .columnType("BOOLEAN NOT NULL");
      expr("(date '1-2-3', date '1-2-3')\n"
          + " " + op + " (date '1-2-3', date '1-2-3')")
          .columnType("BOOLEAN NOT NULL");
    }
  }

  @Test void testContains() {
    final String cannotApply =
        "(?s).*Cannot apply 'CONTAINS' to arguments of type .*";

    expr("(date '1-2-3', date '1-2-3')\n"
        + " contains (date '1-2-3', date '1-2-3')")
        .columnType("BOOLEAN NOT NULL");
    expr("period (date '1-2-3', date '1-2-3')\n"
        + " contains period (date '1-2-3', date '1-2-3')")
        .columnType("BOOLEAN NOT NULL");
    expr("(date '1-2-3', date '1-2-3')\n"
        + "  contains (date '1-2-3', interval '1' year)").ok();
    expr("(time '1:2:3', interval '1' second)\n"
        + " contains (time '23:59:59', time '1:2:3')").ok();
    expr("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6')\n"
        + " contains (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to "
        + "second)").ok();

    // period contains point
    expr("(date '1-2-3', date '1-2-3')\n"
        + "  contains date '1-2-3'").ok();
    // same, with "period" keyword
    expr("period (date '1-2-3', date '1-2-3')\n"
        + "  contains date '1-2-3'").ok();
    // point contains period
    wholeExpr("date '1-2-3'\n"
        + "  contains (date '1-2-3', date '1-2-3')")
        .fails(cannotApply);
    // same, with "period" keyword
    wholeExpr("date '1-2-3'\n"
        + "  contains period (date '1-2-3', date '1-2-3')")
        .fails(cannotApply);
    // point contains point
    wholeExpr("date '1-2-3' contains date '1-2-3'")
        .fails(cannotApply);

    wholeExpr("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' )\n"
        + " contains (time '4:5:6', interval '1 2:3:4.5' day to second)")
        .fails(cannotApply);
    wholeExpr("(time '4:5:6', timestamp '1-2-3 4:5:6' )\n"
        + " contains (time '4:5:6', interval '1 2:3:4.5' day to second)")
        .fails(cannotApply);
    wholeExpr("(time '4:5:6', time '4:5:6' )\n"
        + "  contains (time '4:5:6', date '1-2-3')")
        .fails(cannotApply);
    wholeExpr("1 contains 2")
        .fails(cannotApply);
    // row with 3 arguments
    expr("true\n"
        + "or ^(date '1-2-3', date '1-2-3', date '1-2-3')\n"
        + "  contains (date '1-2-3', date '1-2-3')^\n"
        + "or false")
        .fails(cannotApply);

    expr("true\n"
        + "or (date '1-2-3', date '1-2-3')\n"
        + "  contains (date '1-2-3', date '1-2-3')\n"
        + "or false")
        .columnType("BOOLEAN NOT NULL");
    // second argument is a point
    expr("true\n"
        + "or (date '1-2-3', date '1-2-3')\n"
        + "  contains date '1-2-3'\n"
        + "or false")
        .columnType("BOOLEAN NOT NULL");
    // first argument may be null, so result may be null
    expr("true\n"
        + "or (date '1-2-3',\n"
        + "     case 1 when 2 then date '1-2-3' else null end)\n"
        + "  contains date '1-2-3'\n"
        + "or false")
        .columnType("BOOLEAN");
    // second argument may be null, so result may be null
    expr("true\n"
        + "or (date '1-2-3', date '1-2-3')\n"
        + "  contains case 1 when 1 then date '1-2-3' else null end\n"
        + "or false")
        .columnType("BOOLEAN");
    expr("true\n"
        + "or ^period (date '1-2-3', date '1-2-3')\n"
        + "  contains period (date '1-2-3', time '4:5:6')^\n"
        + "or false")
        .fails(cannotApply);
    expr("true\n"
        + "or ^(1, 2) contains (2, 3)^\n"
        + "or false")
        .fails(cannotApply);
  }

  @Test void testExtract() {
    // TODO: Need to have extract return decimal type for seconds
    // so we can have seconds fractions
    expr("extract(year from interval '1-2' year to month)")
        .columnType("BIGINT NOT NULL");
    expr("extract(minute from interval '1.1' second)").ok();
    expr("extract(year from DATE '2008-2-2')").ok();

    wholeExpr("extract(minute from interval '11' month)")
        .fails("(?s).*Cannot apply.*");
    wholeExpr("extract(year from interval '11' second)")
        .fails("(?s).*Cannot apply.*");
  }

  @Test void testCastToInterval() {
    expr("cast(interval '1' hour as varchar(20))")
        .columnType("VARCHAR(20) NOT NULL");
    expr("cast(interval '1' hour as bigint)")
        .columnType("BIGINT NOT NULL");
    expr("cast(1000 as interval hour)")
        .columnType("INTERVAL HOUR NOT NULL");

    expr("cast(interval '1' month as interval year)")
        .columnType("INTERVAL YEAR NOT NULL");
    expr("cast(interval '1-1' year to month as interval month)")
        .columnType("INTERVAL MONTH NOT NULL");
    expr("cast(interval '1:1' hour to minute as interval day)")
        .columnType("INTERVAL DAY NOT NULL");
    expr("cast(interval '1:1' hour to minute as interval minute to second)")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");

    wholeExpr("cast(interval '1:1' hour to minute as interval month)")
        .fails("Cast function cannot convert value of type "
            + "INTERVAL HOUR TO MINUTE to type INTERVAL MONTH");
    wholeExpr("cast(interval '1-1' year to month as interval second)")
        .fails("Cast function cannot convert value of type "
            + "INTERVAL YEAR TO MONTH to type INTERVAL SECOND");
  }

  @Test void testMinusDateOperator() {
    expr("(CURRENT_DATE - CURRENT_DATE) HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("(CURRENT_DATE - CURRENT_DATE) YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    wholeExpr("(CURRENT_DATE - LOCALTIME) YEAR TO MONTH")
        .fails("(?s).*Parameters must be of the same type.*");
  }

  @Test void testBind() {
    sql("select * from emp where deptno = ?").ok();
    sql("select * from emp where deptno = ? and sal < 100000").ok();
    sql("select case when deptno = ? then 1 else 2 end from emp").ok();
    // It is not possible to infer type of ?, because SUBSTRING is overloaded
    sql("select deptno from emp group by substring(ename from ^?^ for ?)")
        .fails("Illegal use of dynamic parameter");
    // In principle we could infer that ? should be a VARCHAR
    sql("select count(*) from emp group by position(^?^ in ename)")
        .fails("Illegal use of dynamic parameter");
    sql("select ^deptno^ from emp\n"
        + "group by case when deptno = ? then 1 else 2 end")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select deptno from emp\n"
        + "group by deptno, case when deptno = ? then 1 else 2 end").ok();
    sql("select 1 from emp having sum(sal) < ?").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1310">[CALCITE-1310]
   * Infer type of arguments to BETWEEN operator</a>. */
  @Test void testBindBetween() {
    sql("select * from emp where ename between ? and ?").ok();
    sql("select * from emp where deptno between ? and ?").ok();
    sql("select * from emp where ? between deptno and ?").ok();
    sql("select * from emp where ? between ? and deptno").ok();
    sql("select * from emp where ^?^ between ? and ?")
        .fails("Illegal use of dynamic parameter");
  }

  @Test void testUnnest() {
    sql("select*from unnest(multiset[1])")
        .columnType("INTEGER NOT NULL");
    sql("select*from unnest(multiset[1, 2])")
        .columnType("INTEGER NOT NULL");
    sql("select*from unnest(multiset[321.3, 2.33])")
        .columnType("DECIMAL(5, 2) NOT NULL");
    sql("select*from unnest(multiset[321.3, 4.23e0])")
        .columnType("DOUBLE NOT NULL");
    sql("select*from unnest(multiset[43.2e1, cast(null as decimal(4,2))])")
        .columnType("DOUBLE");
    sql("select*from unnest(multiset[1, 2.3, 1])")
        .columnType("DECIMAL(11, 1) NOT NULL");
    sql("select*from unnest(multiset['1','22','333'])")
        .columnType("CHAR(3) NOT NULL");
    sql("select*from unnest(multiset['1','22','333','22'])")
        .columnType("CHAR(3) NOT NULL");
    sql("select*from ^unnest(1)^")
        .fails("(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    sql("select*from unnest(multiset(select*from dept))").ok();
    sql("select c from unnest(multiset(select deptno from dept)) as t(c)").ok();
    sql("select c from unnest(multiset(select * from dept)) as t(^c^)")
        .fails("List of column aliases must have same degree as table; "
            + "table has 2 columns \\('DEPTNO', 'NAME'\\), "
            + "whereas alias list has 1 columns");
    sql("select ^c1^ from unnest(multiset(select name from dept)) as t(c)")
        .fails("Column 'C1' not found in any table");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3789">[CALCITE-3789]
   * Support validation of UNNEST multiple array columns like Presto</a>.
   */
  @Test public void testAliasUnnestMultipleArrays() {
    // for accessing a field in STRUCT type unnested from array
    sql("select e.ENAME\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.employees) as t(e)")
        .withConformance(SqlConformanceEnum.PRESTO).columnType("VARCHAR(10) NOT NULL");

    // for unnesting multiple arrays at the same time
    sql("select d.deptno, e, k.empno, l.\"unit\", l.\"X\" * l.\"Y\"\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.admins, d.employees, d.offices) as t(e, k, l)")
        .withConformance(SqlConformanceEnum.PRESTO).ok();

    // Make sure validation fails properly given illegal select items
    sql("select d.deptno, ^e^.some_column, k.empno\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.admins, d.employees) as t(e, k)")
        .withConformance(SqlConformanceEnum.PRESTO)
        .fails("Table 'E' not found");
    sql("select d.deptno, e.detail, ^unknown^.detail\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.employees) as t(e)")
        .withConformance(SqlConformanceEnum.PRESTO).fails("Incompatible types");

    // Make sure validation fails without PRESTO conformance
    sql("select e.ENAME\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.employees) as t(^e^)")
        .fails("List of column aliases must have same degree as table; table has 3 columns "
            + "\\('EMPNO', 'ENAME', 'DETAIL'\\), whereas alias list has 1 columns");
  }

  @Test void testUnnestArray() {
    sql("select*from unnest(array[1])")
        .columnType("INTEGER NOT NULL");
    sql("select*from unnest(array[1, 2])")
        .columnType("INTEGER NOT NULL");
    sql("select*from unnest(array[321.3, 2.33])")
        .columnType("DECIMAL(5, 2) NOT NULL");
    sql("select*from unnest(array[321.3, 4.23e0])")
        .columnType("DOUBLE NOT NULL");
    sql("select*from unnest(array[43.2e1, cast(null as decimal(4,2))])")
        .columnType("DOUBLE");
    sql("select*from unnest(array[1, 2.3, 1])")
        .columnType("DECIMAL(11, 1) NOT NULL");
    sql("select*from unnest(array['1','22','333'])")
        .columnType("CHAR(3) NOT NULL");
    sql("select*from unnest(array['1','22','333','22'])")
        .columnType("CHAR(3) NOT NULL");
    sql("select*from unnest(array['1','22',null,'22'])")
        .columnType("CHAR(2)");
    sql("select*from ^unnest(1)^")
        .fails("(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    sql("select*from unnest(array(select*from dept))").ok();
    sql("select*from unnest(array[1,null])").ok();
    sql("select c from unnest(array(select deptno from dept)) as t(c)").ok();
    sql("select c from unnest(array(select * from dept)) as t(^c^)")
        .fails("List of column aliases must have same degree as table; "
            + "table has 2 columns \\('DEPTNO', 'NAME'\\), "
            + "whereas alias list has 1 columns");
    sql("select ^c1^ from unnest(array(select name from dept)) as t(c)")
        .fails("Column 'C1' not found in any table");
  }

  @Test void testArrayConstructor() {
    sql("select array[1,2] as a from (values (1))")
        .columnType("INTEGER NOT NULL ARRAY NOT NULL");
    sql("select array[1,cast(null as integer), 2] as a\n"
        + "from (values (1))")
        .columnType("INTEGER ARRAY NOT NULL");
    sql("select array[1,null,2] as a from (values (1))")
        .columnType("INTEGER ARRAY NOT NULL");
    sql("select array['1',null,'234',''] as a from (values (1))")
        .columnType("CHAR(3) ARRAY NOT NULL");
  }

  @Test void testCastAsCollectionType() {
    sql("select cast(array[1,null,2] as int array) from (values (1))")
        .columnType("INTEGER NOT NULL ARRAY NOT NULL");
    sql("select cast(array['1',null,'2'] as varchar(5) array) from (values (1))")
        .columnType("VARCHAR(5) NOT NULL ARRAY NOT NULL");
    // test array type.
    sql("select cast(\"intArrayType\" as int array) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("INTEGER NOT NULL ARRAY NOT NULL");
    sql("select cast(\"varchar5ArrayType\" as varchar(5) array) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("VARCHAR(5) NOT NULL ARRAY NOT NULL");
    sql("select cast(\"intArrayArrayType\" as int array array) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
    sql("select cast(\"varchar5ArrayArrayType\" as varchar(5) array array) "
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("VARCHAR(5) NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
    // test multiset type.
    sql("select cast(\"intMultisetType\" as int multiset) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("INTEGER NOT NULL MULTISET NOT NULL");
    sql("select cast(\"varchar5MultisetType\" as varchar(5) multiset) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("VARCHAR(5) NOT NULL MULTISET NOT NULL");
    sql("select cast(\"intMultisetArrayType\" as int multiset array) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("INTEGER NOT NULL MULTISET NOT NULL ARRAY NOT NULL");
    sql("select cast(\"varchar5MultisetArrayType\" as varchar(5) multiset array) "
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("VARCHAR(5) NOT NULL MULTISET NOT NULL ARRAY NOT NULL");
    // test row type nested in collection type.
    sql("select cast(\"rowArrayMultisetType\" as row(f0 int array multiset, "
        + "f1 varchar(5) array) array multiset) "
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType(INTEGER NOT NULL ARRAY NOT NULL MULTISET NOT NULL F0, "
            + "VARCHAR(5) NOT NULL ARRAY NOT NULL F1) NOT NULL "
            + "ARRAY NOT NULL MULTISET NOT NULL");
    // test UDT collection type.
    sql("select cast(a as ^MyUDT^ array multiset) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .fails("Unknown identifier 'MYUDT'");
  }

  @Test void testCastAsRowType() {
    sql("select cast(a as row(f0 int, f1 varchar)) from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType(INTEGER NOT NULL F0, VARCHAR NOT NULL F1) NOT NULL");
    sql("select cast(b as row(f0 int not null, f1 varchar null))\n"
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType(INTEGER NOT NULL F0, VARCHAR F1) NOT NULL");
    // test nested row type.
    sql("select "
        + "cast(c as row("
        + "f0 row(ff0 int not null, ff1 varchar null) null, "
        + "f1 timestamp not null))"
        + " from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType("
            + "RecordType(INTEGER FF0, VARCHAR FF1) F0, "
            + "TIMESTAMP(0) NOT NULL F1) NOT NULL");
    // test row type in collection data types.
    sql("select cast(d as row(f0 bigint not null, f1 decimal null) array)\n"
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType(BIGINT NOT NULL F0, DECIMAL(19, 0) F1) NOT NULL "
            + "ARRAY NOT NULL");
    sql("select cast(e as row(f0 varchar not null, f1 timestamp null) multiset)\n"
        + "from COMPLEXTYPES.CTC_T1")
        .withExtendedCatalog()
        .columnType("RecordType(VARCHAR NOT NULL F0, TIMESTAMP(0) F1) NOT NULL "
            + "MULTISET NOT NULL");
  }

  @Test void testMultisetConstructor() {
    sql("select multiset[1,null,2] as a from (values (1))")
        .columnType("INTEGER MULTISET NOT NULL");
  }

  @Test void testUnnestArrayColumn() {
    final String sql1 = "select d.deptno, e.*\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) as e";
    final String type = "RecordType(INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL EMPNO,"
        + " VARCHAR(10) NOT NULL ENAME,"
        + " RecordType(RecordType(VARCHAR(10) NOT NULL TYPE, VARCHAR(20) NOT NULL DESC,"
        + " RecordType(VARCHAR(10) NOT NULL A, VARCHAR(10) NOT NULL B) NOT NULL OTHERS)"
        + " NOT NULL ARRAY NOT NULL SKILLS) NOT NULL DETAIL) NOT NULL";
    sql(sql1).type(type);

    // equivalent query without table alias
    final String sql1b = "select d.deptno, e.*\n"
        + "from dept_nested as d,\n"
        + " UNNEST(employees) as e";
    sql(sql1b).type(type);

    // equivalent query using CROSS JOIN
    final String sql2 = "select d.deptno, e.*\n"
        + "from dept_nested as d CROSS JOIN\n"
        + " UNNEST(d.employees) as e";
    sql(sql2).type(type);

    // equivalent query using CROSS JOIN, without table alias
    final String sql2b = "select d.deptno, e.*\n"
        + "from dept_nested as d CROSS JOIN\n"
        + " UNNEST(employees) as e";
    sql(sql2b).type(type);

    // LATERAL works left-to-right
    final String sql3 = "select d.deptno, e.*\n"
        + "from UNNEST(^d^.employees) as e, dept_nested as d";
    sql(sql3).fails("Table 'D' not found");
  }

  @Test void testUnnestWithOrdinality() {
    sql("select*from unnest(array[1, 2]) with ordinality")
        .type("RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL ORDINALITY) NOT NULL");
    sql("select*from unnest(array[43.2e1, cast(null as decimal(4,2))]) with ordinality")
        .type("RecordType(DOUBLE EXPR$0, INTEGER NOT NULL ORDINALITY) NOT NULL");
    sql("select*from ^unnest(1) with ordinality^")
        .fails("(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    sql("select deptno\n"
        + "from unnest(array(select*from dept)) with ordinality\n"
        + "where ordinality < 5").ok();
    sql("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(^c^)")
        .fails("List of column aliases must have same degree as table; table has 2 "
            + "columns \\('DEPTNO', 'ORDINALITY'\\), "
            + "whereas alias list has 1 columns");
    sql("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(c, d)").ok();
    sql("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(^c, d, e^)")
        .fails("List of column aliases must have same degree as table; table has 2 "
            + "columns \\('DEPTNO', 'ORDINALITY'\\), "
            + "whereas alias list has 3 columns");
    sql("select c\n"
        + "from unnest(array(select * from dept)) with ordinality as t(^c, d, e, f^)")
        .fails("List of column aliases must have same degree as table; table has 3 "
            + "columns \\('DEPTNO', 'NAME', 'ORDINALITY'\\), "
            + "whereas alias list has 4 columns");
    sql("select ^name^ from unnest(array(select name from dept)) with ordinality as t(c, o)")
        .fails("Column 'NAME' not found in any table");
    sql("select ^ordinality^ from unnest(array(select name from dept)) with ordinality as t(c, o)")
        .fails("Column 'ORDINALITY' not found in any table");
  }

  @Test void unnestMapMustNameColumnsKeyAndValueWhenNotAliased() {
    sql("select * from unnest(map[1, 12, 2, 22])")
        .type("RecordType(INTEGER NOT NULL KEY, INTEGER NOT NULL VALUE) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4305">[CALCITE-4305]
   * Implicit column alias for single-column UNNEST</a>. */
  @Test void testUnnestAlias() {
    final String expectedType = "RecordType(CHAR(6) NOT NULL FRUIT) NOT NULL";

    // When UNNEST produces a single column, and you use an alias for the
    // relation, that alias becomes the name of the column.
    sql("select fruit.* from UNNEST(array ['apple', 'banana']) as fruit")
        .type(expectedType);

    // The magic doesn't happen if the query is not an UNNEST.
    // In this case, the query is a SELECT.
    sql("SELECT fruit.*\n"
        + "FROM (\n"
        + "  SELECT * FROM UNNEST(array ['apple', 'banana']) as x) as fruit")
        .type("RecordType(CHAR(6) NOT NULL X) NOT NULL");

    // The magic doesn't happen if the UNNEST yields more than one column.
    sql("select * from UNNEST(array [('apple', 1), ('banana', 2)]) as fruit")
        .type("RecordType(CHAR(6) NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) "
            + "NOT NULL");

    // VALUES gets the same treatment as ARRAY. (Unlike PostgreSQL.)
    sql("select * from (values ('apple'), ('banana')) as fruit")
        .type("RecordType(CHAR(6) NOT NULL FRUIT) NOT NULL");

    // UNNEST MULTISET gets the same treatment as UNNEST ARRAY.
    sql("select * from unnest(multiset [1, 2, 1]) as f")
        .type("RecordType(INTEGER NOT NULL F) NOT NULL");
  }

  @Test void testCorrelationJoin() {
    sql("select *,"
        + "         multiset(select * from emp where deptno=dept.deptno) "
        + "               as empset"
        + "      from dept").ok();
    sql("select*from unnest(select multiset[8] from dept)").ok();
    sql("select*from unnest(select multiset[deptno] from dept)").ok();
  }

  @Test void testStructuredTypes() {
    sql("values new address()")
        .columnType("ObjectSqlType(ADDRESS) NOT NULL");
    sql("select home_address from emp_address")
        .columnType("ObjectSqlType(ADDRESS) NOT NULL");
    sql("select ea.home_address.zip from emp_address ea")
        .columnType("INTEGER NOT NULL");
    sql("select ea.mailing_address.city from emp_address ea")
        .columnType("VARCHAR(20) NOT NULL");
  }

  @Test void testLateral() {
    sql("select * from emp, (select * from dept where ^emp^.deptno=dept.deptno)")
        .fails("Table 'EMP' not found");

    sql("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno)").ok();
    sql("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno) as ldt").ok();
    sql("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno) ldt").ok();
  }

  @Test void testCollect() {
    sql("select collect(deptno) from emp").ok();
    sql("select collect(multiset[3]) from emp").ok();
    sql("select collect(multiset[3]), ^deptno^ from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
  }

  @Test void testFusion() {
    sql("select ^fusion(deptno)^ from emp")
        .fails("(?s).*Cannot apply 'FUSION' to arguments of type 'FUSION.<INTEGER>.'.*");
    sql("select fusion(multiset[3]) from emp").ok();
    // todo. FUSION is an aggregate function. test that validator can only
    // take set operators in its select list once aggregation support is
    // complete
  }

  @Test void testCountFunction() {
    sql("select count(*) from emp").ok();
    sql("select count(ename) from emp").ok();
    sql("select count(sal) from emp").ok();
    sql("select count(1) from emp").ok();
    sql("select ^count()^ from emp")
        .fails("Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
  }

  @Test void testCountCompositeFunction() {
    sql("select count(ename, deptno) from emp").ok();
    sql("select count(ename, deptno, ^gender^) from emp")
        .fails("Column 'GENDER' not found in any table");
    sql("select count(ename, 1, deptno) from emp").ok();
    sql("select count(distinct ename, 1, deptno) from emp").ok();
    sql("select count(deptno, ^*^) from emp")
        .fails("(?s).*Encountered \"\\*\" at .*");
    sql("select count(*^,^ deptno) from emp")
        .fails("(?s).*Encountered \",\" at .*");
  }

  @Test void testLastFunction() {
    sql("select LAST_VALUE(sal) over (order by empno) from emp").ok();
    sql("select LAST_VALUE(ename) over (order by empno) from emp").ok();

    sql("select FIRST_VALUE(sal) over (order by empno) from emp").ok();
    sql("select FIRST_VALUE(ename) over (order by empno) from emp").ok();

    sql("select NTH_VALUE(sal, 2) over (order by empno) from emp").ok();
    sql("select NTH_VALUE(ename, 2) over (order by empno) from emp").ok();
  }

  @Test void testMinMaxFunctions() {
    sql("SELECT MIN(true) from emp").ok();
    sql("SELECT MAX(false) from emp").ok();

    sql("SELECT MIN(sal+deptno) FROM emp").ok();
    sql("SELECT MAX(ename) FROM emp").ok();
    sql("SELECT MIN(5.5) FROM emp").ok();
    sql("SELECT MAX(5) FROM emp").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4661">[CALCITE-4661]
   * Add MODE aggregate function</a>. */
  @Test void testModeFunction() {
    sql("select MODE(sal) from emp").ok();
    sql("select ^MODE(null)^ from emp").fails("Argument to function 'MODE' must not be NULL");
    sql("select MODE(sal) over (order by empno) from emp").ok();
    sql("select MODE(ename) from emp where sal=3000");
    sql("select MODE(sal) from emp group by deptno").ok();
    sql("select MODE(sal) from emp group by deptno order by deptno").ok();
    sql("select deptno,\n"
        + "^mode(empno)^ within group(order by 1)\n"
        + "from emp\n"
        + "group by deptno")
        .fails("Aggregate expression 'MODE' must not contain a WITHIN GROUP clause");
  }

  @Test void testSomeEveryAndIntersectionFunctions() {
    sql("select some(sal = 100), every(sal > 0), intersection(multiset[1,2]) from emp").ok();
    sql("select some(sal = 100), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select every(sal > 0), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select intersection(multiset[1]), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testAnyValueFunction() {
    sql("SELECT any_value(ename) from emp").ok();
  }

  @Test void testBoolAndBoolOrFunction() {
    final Sql s = sql("?")
        .withOperatorTable(operatorTableFor(SqlLibrary.POSTGRESQL));
    s.sql("SELECT bool_and(true) from emp").ok();
    s.sql("SELECT bool_or(true) from emp").ok();

    s.sql("select bool_and(col)\n"
        + "from (values(true), (false), (true)) as tbl(col)").ok();
    s.sql("select bool_or(col)\n"
        + "from (values(true), (false), (true)) as tbl(col)").ok();

    s.sql("select bool_and(col)\n"
        + "from (values(true), (false), (null)) as tbl(col)").ok();
    s.sql("select bool_or(col)\n"
        + "from (values(true), (false), (null)) as tbl(col)").ok();

    s.sql("SELECT ^bool_and(ename)^ from emp")
        .fails("(?s).*Cannot apply 'BOOL_AND' to arguments of type "
            + "'BOOL_AND\\(<VARCHAR\\(20\\)>\\)'.*");
    s.sql("SELECT ^bool_or(ename)^ from emp")
        .fails("(?s).*Cannot apply 'BOOL_OR' to arguments of type "
            + "'BOOL_OR\\(<VARCHAR\\(20\\)>\\)'.*");
  }

  @Test void testFunctionalDistinct() {
    sql("select count(distinct sal) from emp").ok();
    sql("select COALESCE(^distinct^ sal) from emp")
        .fails("DISTINCT/ALL not allowed with COALESCE function");
  }

  @Test void testColumnNotFound() {
    sql("select ^b0^ from sales.emp")
        .fails("Column 'B0' not found in any table");
  }

  @Test void testColumnNotFound2() {
    sql("select ^b0^ from sales.emp, sales.dept")
        .fails("Column 'B0' not found in any table");
  }

  @Test void testColumnNotFound3() {
    sql("select e.^b0^ from sales.emp as e")
        .fails("Column 'B0' not found in table 'E'");
  }

  @Test void testSelectDistinct() {
    sql("SELECT DISTINCT deptno FROM emp").ok();
    sql("SELECT DISTINCT deptno, sal FROM emp").ok();
    sql("SELECT DISTINCT deptno FROM emp GROUP BY deptno").ok();
    sql("SELECT DISTINCT ^deptno^ FROM emp GROUP BY sal")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT avg(sal) from emp").ok();
    sql("SELECT DISTINCT ^deptno^, avg(sal) from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT deptno, sal from emp GROUP BY sal, deptno").ok();
    sql("SELECT deptno FROM emp GROUP BY deptno HAVING deptno > 55").ok();
    sql("SELECT DISTINCT deptno, 33 FROM emp\n"
        + "GROUP BY deptno HAVING deptno > 55").ok();
    sql("SELECT DISTINCT deptno, 33 FROM emp HAVING ^deptno^ > 55")
        .fails("Expression 'DEPTNO' is not being grouped");
    // same query under a different conformance finds a different error first
    sql("SELECT DISTINCT ^deptno^, 33 FROM emp HAVING deptno > 55")
        .withConformance(SqlConformanceEnum.LENIENT)
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT 33 FROM emp HAVING ^deptno^ > 55")
        .fails("Expression 'DEPTNO' is not being grouped")
        .withConformance(SqlConformanceEnum.LENIENT)
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT * from emp").ok();
    sql("SELECT DISTINCT ^*^ from emp GROUP BY deptno")
        .fails("Expression 'EMP\\.EMPNO' is not being grouped");

    // similar validation for SELECT DISTINCT and GROUP BY
    sql("SELECT deptno FROM emp GROUP BY deptno ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("SELECT DISTINCT deptno from emp ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");
    sql("SELECT DISTINCT deptno from emp ORDER BY deptno + 2").ok();

    // The ORDER BY clause works on what is projected by DISTINCT - even if
    // GROUP BY is present.
    sql("SELECT DISTINCT deptno FROM emp GROUP BY deptno, empno ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");

    // redundant distinct; same query is in unitsql/optimizer/distinct.sql
    sql("select distinct * from (\n"
        + "  select distinct deptno from emp) order by 1").ok();

    sql("SELECT DISTINCT 5, 10+5, 'string' from emp").ok();
  }

  @Test void testSelectWithoutFrom() {
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok();
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("SELECT must have a FROM clause");
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.STRICT_2003)
        .fails("SELECT must have a FROM clause");
  }

  @Test void testSelectAmbiguousField() {
    sql("select ^t0^ from (select 1 as t0, 2 as T0 from dept)")
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)
        .fails("Column 't0' is ambiguous");
    sql("select ^t0^ from (select 1 as t0, 2 as t0,3 as t1,4 as t1, 5 as t2 from dept)")
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)
        .fails("Column 't0' is ambiguous");
    // t0 is not referenced,so this case is allowed
    sql("select 1 as t0, 2 as t0 from dept")
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)
        .ok();

    sql("select t0 from (select 1 as t0, 2 as T0 from DEPT)")
        .withCaseSensitive(true)
        .withUnquotedCasing(Casing.UNCHANGED)
        .ok();
  }

  @Test void testTableExtend() {
    sql("select * from dept extend (x int not null)")
        .type("RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10) NOT NULL NAME, "
            + "INTEGER NOT NULL X) NOT NULL");
    sql("select deptno + x as z\n"
        + "from dept extend (x int not null) as x\n"
        + "where x > 10")
        .type("RecordType(INTEGER NOT NULL Z) NOT NULL");
  }

  @Test void testExplicitTable() {
    final String empRecordType =
        "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL";
    sql("select * from (table emp)")
        .type(empRecordType);
    sql("table emp")
        .type(empRecordType);
    sql("table ^nonexistent^")
        .fails("Object 'NONEXISTENT' not found");
    sql("table ^sales.nonexistent^")
        .fails("Object 'NONEXISTENT' not found within 'SALES'");
    sql("table ^nonexistent.foo^")
        .fails("Object 'NONEXISTENT' not found");
  }

  @Test void testCollectionTable() {
    sql("select * from table(ramp(3))")
        .type("RecordType(INTEGER NOT NULL I) NOT NULL");

    sql("select * from table(^ramp('3')^)")
        .withTypeCoercion(false)
        .fails("Cannot apply 'RAMP' to arguments of type "
            + "'RAMP\\(<CHAR\\(1\\)>\\)'\\. "
            + "Supported form\\(s\\): 'RAMP\\(<NUMERIC>\\)'");

    sql("select * from table(ramp('3'))")
        .type("RecordType(INTEGER NOT NULL I) NOT NULL");

    sql("select * from table(^abs^(-1))")
        .fails("(?s)Encountered \"abs\" at .*");

    sql("select * from table(^1^ + 2)")
        .fails("(?s)Encountered \"1\" at .*");

    sql("select * from table(^nonExistentRamp('3')^)")
        .fails("No match found for function signature NONEXISTENTRAMP\\(<CHARACTER>\\)");

    sql("select * from table(^bad_ramp(3)^)")
        .fails("Argument must be a table function: BAD_RAMP");

    sql("select * from table(^bad_table_function(3)^)")
        .fails("Table function should have CURSOR type, not"
            + " RecordType\\(INTEGER I\\)");
  }

  /** Tests that Calcite gives an error if a table function is used anywhere
   * that a scalar expression is expected. */
  @Test void testTableFunctionAsExpression() {
    sql("select ^ramp(3)^ from (values (1))")
        .fails("Cannot call table function here: 'RAMP'");
    sql("select * from (values (1)) where ^ramp(3)^")
        .fails("WHERE clause must be a condition");
    sql("select * from (values (1)) where ^ramp(3) and 1 = 1^")
        .fails("Cannot apply 'AND' to arguments of type '<CURSOR> AND "
            + "<BOOLEAN>'\\. Supported form\\(s\\): '<BOOLEAN> AND <BOOLEAN>'");
    sql("select * from (values (1)) where ^ramp(3) is not null^")
        .fails("Cannot apply 'IS NOT NULL' to arguments of type '<CURSOR> IS"
            + " NOT NULL'\\. Supported form\\(s\\): '<ANY> IS NOT NULL'");
    sql("select ^sum(ramp(3))^ from (values (1))")
        .fails("Cannot apply 'SUM' to arguments of type 'SUM\\(<CURSOR>\\)'\\. "
            + "Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'");
    sql("select 0 from (values (1)) group by ^ramp(3)^")
        .fails("Cannot call table function here: 'RAMP'");
    sql("select count(*) from (values (1)) having ^ramp(3)^")
        .fails("HAVING clause must be a condition");
    sql("select * from (values (1)) order by ^ramp(3)^ asc, 1 desc")
        .fails("Cannot call table function here: 'RAMP'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1309">[CALCITE-1309]
   * Support LATERAL TABLE</a>. */
  @Test void testCollectionTableWithLateral() {
    final String expectedType = "RecordType(INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME, "
        + "INTEGER NOT NULL I) NOT NULL";
    sql("select * from dept, lateral table(ramp(dept.deptno))")
        .type(expectedType);
    sql("select * from dept cross join lateral table(ramp(dept.deptno))")
        .type(expectedType);
    sql("select * from dept join lateral table(ramp(dept.deptno)) on true")
        .type(expectedType);

    final String expectedType2 = "RecordType(INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME, "
        + "INTEGER I) NOT NULL";
    sql("select * from dept left join lateral table(ramp(dept.deptno)) on true")
        .type(expectedType2);

    sql("select * from dept, lateral table(^ramp(dept.name)^)")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'RAMP' to arguments of type 'RAMP\\(<VARCHAR\\(10\\)>\\)'.*");

    sql("select * from dept, lateral table(ramp(dept.name))")
        .type("RecordType(INTEGER NOT NULL DEPTNO, "
            + "VARCHAR(10) NOT NULL NAME, "
            + "INTEGER NOT NULL I) NOT NULL");

    sql("select * from lateral table(ramp(^dept^.deptno)), dept")
        .fails("Table 'DEPT' not found");
    final String expectedType3 = "RecordType(INTEGER NOT NULL I, "
        + "INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME) NOT NULL";
    sql("select * from lateral table(ramp(1234)), dept")
        .type(expectedType3);
  }

  @Test void testCollectionTableWithLateral2() {
    // The expression inside the LATERAL can only see tables before it in the
    // FROM clause. And it can't see itself.
    sql("select * from emp, lateral table(ramp(emp.deptno)), dept")
        .ok();
    sql("select * from emp, lateral table(ramp(^z^.i)) as z, dept")
        .fails("Table 'Z' not found");
    sql("select * from emp, lateral table(ramp(^dept^.deptno)), dept")
        .fails("Table 'DEPT' not found");
  }

  @Test void testCollectionTableWithCursorParam() {
    sql("select * from table(dedup(cursor(select * from emp),'ename'))")
        .type("RecordType(VARCHAR(1024) NOT NULL NAME) NOT NULL");
    sql("select * from table(dedup(cursor(select * from ^bloop^),'ename'))")
        .fails("Object 'BLOOP' not found");
  }

  @Test void testTemporalTable() {
    sql("select stream * from orders, ^products^ for system_time as of"
        + " TIMESTAMP '2011-01-02 00:00:00'")
        .fails("Table 'PRODUCTS' is not a temporal table, "
            + "can not be queried in system time period specification");

    sql("select stream * from orders, products_temporal "
        + "for system_time as of ^'2011-01-02 00:00:00'^")
        .fails("The system time period specification expects Timestamp type but is 'CHAR'");

    // verify inner join with a specific timestamp
    sql("select stream * from orders join products_temporal "
        + "for system_time as of timestamp '2011-01-02 00:00:00' "
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a timestamp field
    sql("select stream * from orders left join products_temporal "
        + "for system_time as of orders.rowtime "
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a timestamp expression
    sql("select stream * from orders left join products_temporal\n"
        + "for system_time as of orders.rowtime - INTERVAL '3' DAY\n"
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a datetime value function
    sql("select stream * from orders left join products_temporal\n"
        + "for system_time as of CURRENT_TIMESTAMP\n"
        + "on orders.productid = products_temporal.productid").ok();
  }

  @Test void testScalarSubQuery() {
    sql("SELECT  ename,(select name from dept where deptno=1) FROM emp").ok();
    sql("SELECT ename,(^select losal, hisal from salgrade where grade=1^) FROM emp")
        .fails("Cannot apply '\\$SCALAR_QUERY' to arguments of type "
            + "'\\$SCALAR_QUERY\\(<RECORDTYPE\\(INTEGER LOSAL, "
            + "INTEGER HISAL\\)>\\)'\\. Supported form\\(s\\): "
            + "'\\$SCALAR_QUERY\\(<RECORDTYPE\\(SINGLE FIELD\\)>\\)'");

    // Note that X is a field (not a record) and is nullable even though
    // EMP.NAME is NOT NULL.
    sql("SELECT  ename,(select name from dept where deptno=1) FROM emp")
        .type("RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) EXPR$1) NOT NULL");

    // scalar subqery inside AS operator
    sql("SELECT  ename,(select name from dept where deptno=1) as X FROM emp")
        .type("RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) X) NOT NULL");

    // scalar subqery inside + operator
    sql("SELECT  ename, 1 + (select deptno from dept where deptno=1) as X FROM emp")
        .type("RecordType(VARCHAR(20) NOT NULL ENAME, INTEGER X) NOT NULL");

    // scalar sub-query inside WHERE
    sql("select * from emp where (select true from dept)").ok();
  }

  @Disabled("not supported")
  @Test void testSubQueryInOnClause() {
    // Currently not supported. Should give validator error, but gives
    // internal error.
    sql("select * from emp as emps left outer join dept as depts\n"
        + "on emps.deptno = depts.deptno and emps.deptno = (\n"
        + "select min(deptno) from dept as depts2)").ok();
  }

  @Test void testRecordType() {
    // Have to qualify columns with table name.
    sql("SELECT ^coord^.x, coord.y FROM customer.contact")
        .fails("Table 'COORD' not found");

    sql("SELECT contact.coord.x, contact.coord.y FROM customer.contact")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

    // Qualifying with schema is OK.
    sql("SELECT customer.contact.coord.x, customer.contact.email,\n"
        + "  contact.coord.y\n"
        + "FROM customer.contact")
        .type("RecordType(INTEGER NOT NULL X, VARCHAR(20) NOT NULL EMAIL, "
            + "INTEGER NOT NULL Y) NOT NULL");
  }

  @Test void testArrayOfRecordType() {
    sql("SELECT name, dept_nested.employees[1].^ne^ as ne from dept_nested")
        .fails("Unknown field 'NE'");
    sql("SELECT name, dept_nested.employees[1].ename as ename from dept_nested")
        .type("RecordType(VARCHAR(10) NOT NULL NAME, VARCHAR(10) ENAME) NOT NULL");
    sql("SELECT dept_nested.employees[1].detail.skills[1].desc as DESCRIPTION\n"
        + "from dept_nested")
        .type("RecordType(VARCHAR(20) DESCRIPTION) NOT NULL");
    sql("SELECT dept_nested.employees[1].detail.skills[1].others.a as oa\n"
        + "from dept_nested")
        .type("RecordType(VARCHAR(10) OA) NOT NULL");
  }

  @Test void testItemOperatorException() {
    sql("select ^name[0]^ from dept")
        .fails("Cannot apply 'ITEM' to arguments of type 'ITEM\\(<VARCHAR\\(10\\)>, "
            +  "<INTEGER>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<ANY>\\]\n"
            + "<ROW>\\[<CHARACTER>\\|<INTEGER>\\].*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-497">[CALCITE-497]
   * Support optional qualifier for column name references</a>. */
  @Test void testRecordTypeElided() {
    sql("SELECT contact.^x^, contact.coord.y FROM customer.contact")
        .fails("Column 'X' not found in table 'CONTACT'");

    // Fully qualified works.
    sql("SELECT contact.coord.x, contact.coord.y FROM customer.contact")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

    // Because the types of CONTACT_PEEK.COORD and CONTACT_PEEK.COORD_NE are
    // marked "peek", the validator can see through them.
    sql("SELECT c.x, c.coord.y, c.m, c.b FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y, INTEGER NOT NULL M,"
            + " INTEGER NOT NULL B) NOT NULL");
    sql("SELECT c.coord.x, c.coord.y, c.coord_ne.m FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y, INTEGER NOT NULL M) NOT NULL");
    sql("SELECT x, a, c.coord.y FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL A, INTEGER NOT NULL Y) NOT NULL");

    // Because CONTACT_PEEK.COORD_NE is marked "peek no expand",
    // "select *" does not flatten it.
    sql("SELECT coord_ne.* FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL M, "
            + "RecordType:peek_no_expand(INTEGER NOT NULL A, INTEGER NOT NULL B) "
            + "NOT NULL SUB) NOT NULL");
    sql("SELECT * FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL CONTACTNO, VARCHAR(10) NOT NULL FNAME, "
            + "VARCHAR(10) NOT NULL LNAME, VARCHAR(20) NOT NULL EMAIL, INTEGER NOT NULL X, "
            + "INTEGER NOT NULL Y, VARCHAR(20) NOT NULL unit, "
            + "RecordType:peek_no_expand(INTEGER NOT NULL M, "
            + "RecordType:peek_no_expand(INTEGER NOT NULL A, INTEGER NOT NULL B) "
            + "NOT NULL SUB) NOT NULL COORD_NE) NOT NULL");

    // Qualifying with schema is OK.
    final String sql = "SELECT customer.contact_peek.x,\n"
        + " customer.contact_peek.email, contact_peek.coord.y\n"
        + "FROM customer.contact_peek";
    sql(sql).type("RecordType(INTEGER NOT NULL X, VARCHAR(20) NOT NULL EMAIL,"
        + " INTEGER NOT NULL Y) NOT NULL");
  }

  @Test void testSample() {
    // applied to table
    sql("SELECT * FROM emp TABLESAMPLE SUBSTITUTE('foo')").ok();
    sql("SELECT * FROM emp TABLESAMPLE BERNOULLI(50)").ok();
    sql("SELECT * FROM emp TABLESAMPLE SYSTEM(50)").ok();

    // applied to query
    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE SUBSTITUTE('bar') "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample substitute('SMALL')").ok();

    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(50) "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE BERNOULLI(50) "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(10) "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample bernoulli(10)").ok();

    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(50) "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE SYSTEM(50) "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(10) "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample system(10)").ok();
  }

  @Test void testRewriteWithoutIdentifierExpansion() {
    sql("select * from dept")
        .withValidatorIdentifierExpansion(false)
        .rewritesTo("SELECT *\n"
            + "FROM `DEPT`");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1238">[CALCITE-1238]
   * Unparsing LIMIT without ORDER BY after validation</a>. */
  @Test void testRewriteWithLimitWithoutOrderBy() {
    final String sql = "select name from dept limit 2";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithLimitWithDynamicParameters() {
    final String sql = "select name from dept offset ? rows fetch next ? rows only";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "OFFSET ? ROWS\n"
        + "FETCH NEXT ? ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithOffsetWithoutOrderBy() {
    final String sql = "select name from dept offset 2";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "OFFSET 2 ROWS";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithUnionFetchWithoutOrderBy() {
    final String sql =
        "select name from dept union all select name from dept limit 2";
    final String expected = "SELECT *\n"
        + "FROM (SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "UNION ALL\n"
        + "SELECT `NAME`\n"
        + "FROM `DEPT`)\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithIdentifierExpansion() {
    sql("select * from dept")
        .withValidatorIdentifierExpansion(true)
        .rewritesTo("SELECT `DEPT`.`DEPTNO`, `DEPT`.`NAME`\n"
            + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`");
  }

  @Test void testRewriteWithColumnReferenceExpansion() {
    // The names in the ORDER BY clause are not qualified.
    // This is because ORDER BY references columns in the SELECT clause
    // in preference to columns in tables in the FROM clause.

    sql("select name from dept where name = 'Moonracer' group by name"
        + " having sum(deptno) > 3 order by name")
        .withValidatorIdentifierExpansion(true)
        .withValidatorColumnReferenceExpansion(true)
        .rewritesTo("SELECT `DEPT`.`NAME`\n"
            + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`\n"
            + "WHERE `DEPT`.`NAME` = 'Moonracer'\n"
            + "GROUP BY `DEPT`.`NAME`\n"
            + "HAVING SUM(`DEPT`.`DEPTNO`) > 3\n"
            + "ORDER BY `NAME`");
  }

  @Test void testRewriteWithColumnReferenceExpansionAndFromAlias() {
    // In the ORDER BY clause, 'ename' is not qualified but 'deptno' and 'sal'
    // are. This is because 'ename' appears as an alias in the SELECT clause.
    // 'sal' is qualified in the ORDER BY clause, so remains qualified.

    final String sql = "select ename, sal from (select * from emp) as e"
        + " where ename = 'Moonracer' group by ename, deptno, sal"
        + " having sum(deptno) > 3 order by ename, deptno, e.sal";
    final String expected = "SELECT `E`.`ENAME`, `E`.`SAL`\n"
        + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`,"
        + " `EMP`.`MGR`, `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`,"
        + " `EMP`.`DEPTNO`, `EMP`.`SLACKER`\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`) AS `E`\n"
        + "WHERE `E`.`ENAME` = 'Moonracer'\n"
        + "GROUP BY `E`.`ENAME`, `E`.`DEPTNO`, `E`.`SAL`\n"
        + "HAVING SUM(`E`.`DEPTNO`) > 3\n"
        + "ORDER BY `ENAME`, `E`.`DEPTNO`, `E`.`SAL`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .withValidatorColumnReferenceExpansion(true)
        .rewritesTo(expected);
  }

  @Test void testRewriteExpansionOfColumnReferenceBeforeResolution() {
    SqlValidatorTester sqlValidatorTester = new SqlValidatorTester(
        SqlTestFactory.INSTANCE.withValidator((opTab, catalogReader, typeFactory, config) ->
             // Rewrites columnar sql identifiers 'UNEXPANDED'.'Something' to 'DEPT'.'Something',
             // where 'Something' is any string.
            new SqlValidatorImpl(opTab, catalogReader, typeFactory, config) {
              @Override public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
                SqlNode rewrittenNode = rewriteNode(expr);
                return super.expand(rewrittenNode, scope);
              }

              @Override public SqlNode expandSelectExpr(
                  SqlNode expr,
                  SelectScope scope,
                  SqlSelect select) {
                SqlNode rewrittenNode = rewriteNode(expr);
                return super.expandSelectExpr(rewrittenNode, scope, select);
              }

              @Override public SqlNode expandGroupByOrHavingExpr(
                  SqlNode expr,
                  SqlValidatorScope scope,
                  SqlSelect select,
                  boolean havingExpression) {
                SqlNode rewrittenNode = rewriteNode(expr);
                return super.expandGroupByOrHavingExpr(
                    rewrittenNode,
                    scope,
                    select,
                    havingExpression);
              }

              private SqlNode rewriteNode(SqlNode sqlNode) {
                return sqlNode.accept(new SqlShuttle() {
                  @Override public SqlNode visit(SqlIdentifier id) {
                    return rewriteIdentifier(id);
                  }
                });
              }

              private SqlIdentifier rewriteIdentifier(SqlIdentifier sqlIdentifier) {
                Preconditions.checkArgument(sqlIdentifier.names.size() == 2);
                if (sqlIdentifier.names.get(0).equals("UNEXPANDED")) {
                  return new SqlIdentifier(
                      asList("DEPT", sqlIdentifier.names.get(1)),
                      null,
                      sqlIdentifier.getParserPosition(),
                      asList(
                          sqlIdentifier.getComponentParserPosition(0),
                          sqlIdentifier.getComponentParserPosition(1)));
                } else if (sqlIdentifier.names.get(0).equals("DEPT")) {
                  //  Identifiers are expanded multiple times
                  return sqlIdentifier;
                } else {
                  throw new RuntimeException("Unknown Identifier " + sqlIdentifier);
                }
              }
            }));

    final String sql = "select unexpanded.deptno from dept \n"
        + " where unexpanded.name = 'Moonracer' \n"
        + " group by unexpanded.deptno\n"
        + " having sum(unexpanded.deptno) > 0\n"
        + " order by unexpanded.deptno";
    final String expectedSql = "SELECT `DEPT`.`DEPTNO`\n"
        + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`\n"
        + "WHERE `DEPT`.`NAME` = 'Moonracer'\n"
        + "GROUP BY `DEPT`.`DEPTNO`\n"
        + "HAVING SUM(`DEPT`.`DEPTNO`) > 0\n"
        + "ORDER BY `DEPT`.`DEPTNO`";
    new Sql(sqlValidatorTester, StringAndPos.of(sql), true, false)
        .withValidatorIdentifierExpansion(true)
        .withValidatorColumnReferenceExpansion(true)
        .withConformance(SqlConformanceEnum.LENIENT)
        .rewritesTo(expectedSql);
  }

  @Test void testCoalesceWithoutRewrite() {
    final String sql = "select coalesce(deptno, empno) from emp";
    final String expected1 = "SELECT COALESCE(`EMP`.`DEPTNO`, `EMP`.`EMPNO`)\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`";
    final String expected2 = "SELECT COALESCE(`DEPTNO`, `EMPNO`)\n"
        + "FROM `EMP`";
    sql(sql)
        .withValidatorCallRewrite(false)
        .rewritesTo(tester.getValidator().config().identifierExpansion()
            ? expected1 : expected2);
  }

  @Test void testCoalesceWithRewrite() {
    final String sql = "select coalesce(deptno, empno) from emp";
    final String expected1 = "SELECT CASE WHEN `EMP`.`DEPTNO` IS NOT NULL"
        + " THEN `EMP`.`DEPTNO` ELSE `EMP`.`EMPNO` END\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`";
    final String expected2 = "SELECT CASE WHEN `DEPTNO` IS NOT NULL"
        + " THEN `DEPTNO` ELSE `EMPNO` END\n"
        + "FROM `EMP`";
    sql(sql)
        .withValidatorCallRewrite(true)
        .rewritesTo(tester.getValidator().config().identifierExpansion()
            ? expected1 : expected2);
  }

  @Disabled
  @Test void testValuesWithAggFuncs() {
    sql("values(^count(1)^)")
        .fails("Call to xxx is invalid\\. Direct calls to aggregate "
            + "functions not allowed in ROW definitions\\.");
  }

  @Test void testFieldOrigin() {
    tester.checkFieldOrigin("select * from emp join dept on true",
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

    tester.checkFieldOrigin("select distinct emp.empno, hiredate, 1 as uno,\n"
            + " emp.empno * 2 as twiceEmpno\n"
            + "from emp join dept on true",
        "{CATALOG.SALES.EMP.EMPNO,"
            + " CATALOG.SALES.EMP.HIREDATE,"
            + " null,"
            + " null}");
  }

  @Test void testBrackets() {
    final Sql s = sql("?").withQuoting(Quoting.BRACKET);
    s.sql("select [e].EMPNO from [EMP] as [e]")
        .type("RecordType(INTEGER NOT NULL EMPNO) NOT NULL");

    s.sql("select ^e^.EMPNO from [EMP] as [e]")
        .fails("Table 'E' not found; did you mean 'e'\\?");

    s.sql("select ^x^ from (\n"
        + "  select [e].EMPNO as [x] from [EMP] as [e])")
        .fails("Column 'X' not found in any table; did you mean 'x'\\?");

    s.sql("select ^x^ from (\n"
        + "  select [e].EMPNO as [x ] from [EMP] as [e])")
        .fails("Column 'X' not found in any table");

    s.sql("select EMP^.^\"x\" from EMP")
        .fails("(?s).*Encountered \"\\. \\\\\"\" at line .*");

    s.sql("select [x[y]] z ] from (\n"
        + "  select [e].EMPNO as [x[y]] z ] from [EMP] as [e])").type(
        "RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  @Test void testLexJava() {
    final Sql s = sql("?").withLex(Lex.JAVA);
    s.sql("select e.EMPNO from EMP as e")
        .type("RecordType(INTEGER NOT NULL EMPNO) NOT NULL");

    s.sql("select ^e^.EMPNO from EMP as E")
        .fails("Table 'e' not found; did you mean 'E'\\?");

    s.sql("select ^E^.EMPNO from EMP as e")
        .fails("Table 'E' not found; did you mean 'e'\\?");

    s.sql("select ^x^ from (\n"
        + "  select e.EMPNO as X from EMP as e)")
        .fails("Column 'x' not found in any table; did you mean 'X'\\?");

    s.sql("select ^x^ from (\n"
        + "  select e.EMPNO as Xx from EMP as e)")
        .fails("Column 'x' not found in any table");

    // double-quotes are not valid in this lexical convention
    s.sql("select EMP^.^\"x\" from EMP")
        .fails("(?s).*Encountered \"\\. \\\\\"\" at line .*");

    // in Java mode, creating identifiers with spaces is not encouraged, but you
    // can use back-ticks if you really have to
    s.sql("select `x[y] z ` from (\n"
        + "  select e.EMPNO as `x[y] z ` from EMP as e)")
        .type("RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-145">[CALCITE-145]
   * Unexpected upper-casing of keywords when using java lexer</a>. */
  @Test void testLexJavaKeyword() {
    final Sql s = sql("?").withLex(Lex.JAVA);
    s.sql("select path, x from (select 1 as path, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select path, x from (select 1 as `path`, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select `path`, x from (select 1 as path, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select ^PATH^ from (select 1 as path from (values (true)))")
        .fails("Column 'PATH' not found in any table; did you mean 'path'\\?");
    s.sql("select t.^PATH^ from (select 1 as path from (values (true))) as t")
        .fails("Column 'PATH' not found in table 't'; did you mean 'path'\\?");
    s.sql("select t.x, t.^PATH^ from (values (true, 1)) as t(path, x)")
        .fails("Column 'PATH' not found in table 't'; did you mean 'path'\\?");

    // Built-in functions can be written in any case, even those with no args,
    // and regardless of spaces between function name and open parenthesis.
    s.sql("values (current_timestamp, floor(2.5), ceil (3.5))").ok();
    s.sql("values (CURRENT_TIMESTAMP, FLOOR(2.5), CEIL (3.5))").ok();
    s.sql("values (CURRENT_TIMESTAMP, CEIL (3.5))")
        .type("RecordType(TIMESTAMP(0) NOT NULL CURRENT_TIMESTAMP, "
            + "DECIMAL(2, 0) NOT NULL EXPR$1) NOT NULL");
  }

  @Test void testLexAndQuoting() {
    // in Java mode, creating identifiers with spaces is not encouraged, but you
    // can use double-quote if you really have to
    sql("?")
        .withLex(Lex.JAVA)
        .withQuoting(Quoting.DOUBLE_QUOTE)
        .sql("select \"x[y] z \" from (\n"
            + "  select e.EMPNO as \"x[y] z \" from EMP as e)")
        .type("RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  /** Tests using case-insensitive matching of identifiers. */
  @Test void testCaseInsensitive() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?")
        .withQuoting(Quoting.BRACKET);

    s.sql("select EMPNO from EMP").ok();
    s.sql("select empno from emp").ok();
    s.sql("select [empno] from [emp]").ok();
    s.sql("select [E].[empno] from [emp] as e").ok();
    s.sql("select t.[x] from (\n"
        + "  select [E].[empno] as x from [emp] as e) as [t]").ok();

    // correlating variable
    s.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = [E].deptno)").ok();
    sensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = ^[E]^.deptno)")
        .fails("(?s).*Table 'E' not found; did you mean 'e'\\?");

    sql("select count(1), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  /** Tests using case-insensitive matching of user-defined functions. */
  @Test void testCaseInsensitiveUdfs() {
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);
    final Sql insensitive = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(operatorTable);
    final Sql sensitive = sql("?")
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(operatorTable);

    // test table function lookup case-insensitively.
    insensitive.sql("select * from dept, lateral table(ramp(dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table(RAMP(dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table([RAMP](dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table([Ramp](dept.deptno))").ok();
    // test scalar function lookup case-insensitively.
    insensitive.sql("select myfun(EMPNO) from EMP").ok();
    insensitive.sql("select MYFUN(empno) from emp").ok();
    insensitive.sql("select [MYFUN]([empno]) from [emp]").ok();
    insensitive.sql("select [Myfun]([E].[empno]) from [emp] as e").ok();
    insensitive.sql("select t.[x] from (\n"
        + "  select [Myfun]([E].[empno]) as x from [emp] as e) as [t]").ok();

    // correlating variable
    insensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = myfun([E].deptno))").ok();
    sensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = ^[myfun]([e].deptno)^)")
        .fails("No match found for function signature myfun\\(<NUMERIC>\\).*");
  }

  /** Tests using case-sensitive matching of builtin functions. */
  @Test void testCaseSensitiveBuiltinFunction() {
    final Sql sensitive = sql("?")
        .withCaseSensitive(true)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(SqlStdOperatorTable.instance());

    sensitive.sql("select sum(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [sum](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [SUM](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select SUM(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select Sum(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select count(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [count](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [COUNT](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select COUNT(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select Count(EMPNO) from EMP group by ENAME, EMPNO").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-319">[CALCITE-319]
   * Table aliases should follow case-sensitivity policy</a>. */
  @Test void testCaseInsensitiveTableAlias() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?").withQuoting(Quoting.BRACKET);

    // Table aliases should follow case-sensitivity preference.
    //
    // In MySQL, table aliases are case-insensitive:
    // mysql> select `D`.day from DAYS as `d`, DAYS as `D`;
    // ERROR 1066 (42000): Not unique table/alias: 'D'
    s.sql("select count(*) from dept as [D], ^dept as [d]^")
        .fails("Duplicate relation name 'd' in FROM clause");
    sensitive.sql("select count(*) from dept as [D], dept as [d]").ok();
    sensitive.sql("select count(*) from dept as [D], ^dept as [D]^")
        .fails("Duplicate relation name 'D' in FROM clause");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1305">[CALCITE-1305]
   * Case-insensitive table aliases and GROUP BY</a>. */
  @Test void testCaseInsensitiveTableAliasInGroupBy() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED);
    s.sql("select deptno, count(*) from EMP AS emp\n"
        + "group by eMp.deptno").ok();
    s.sql("select deptno, count(*) from EMP AS EMP\n"
        + "group by eMp.deptno").ok();
    s.sql("select deptno, count(*) from EMP\n"
        + "group by eMp.deptno").ok();
    s.sql("select * from EMP where exists (\n"
        + "  select 1 from dept\n"
        + "  group by eMp.deptno)").ok();
    s.sql("select deptno, count(*) from EMP group by DEPTNO").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1549">[CALCITE-1549]
   * Improve error message when table or column not found</a>. */
  @Test void testTableNotFoundDidYouMean() {
    // No table in default schema
    sql("select * from ^unknownTable^")
        .fails("Object 'UNKNOWNTABLE' not found");

    // Similar table exists in default schema
    sql("select * from ^\"Emp\"^")
        .fails("Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // Schema correct, but no table in specified schema
    sql("select * from ^sales.unknownTable^")
        .fails("Object 'UNKNOWNTABLE' not found within 'SALES'");
    // Similar table exists in specified schema
    sql("select * from ^sales.\"Emp\"^")
        .fails("Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // No schema found
    sql("select * from ^unknownSchema.unknownTable^")
        .fails("Object 'UNKNOWNSCHEMA' not found");
    // Similar schema found
    sql("select * from ^\"sales\".emp^")
        .fails("Object 'sales' not found; did you mean 'SALES'\\?");
    sql("select * from ^\"saLes\".\"eMp\"^")
        .fails("Object 'saLes' not found; did you mean 'SALES'\\?");

    // Spurious after table
    sql("select * from ^emp.foo^")
        .fails("Object 'FOO' not found within 'SALES\\.EMP'");
    sql("select * from ^sales.emp.foo^")
        .fails("Object 'FOO' not found within 'SALES\\.EMP'");

    // Alias not found
    sql("select ^aliAs^.\"name\"\n"
        + "from sales.emp as \"Alias\"")
        .fails("Table 'ALIAS' not found; did you mean 'Alias'\\?");
    // Alias not found, fully-qualified
    sql("select ^sales.\"emp\"^.\"name\" from sales.emp")
        .fails("Table 'SALES\\.emp' not found; did you mean 'EMP'\\?");
  }

  @Test void testColumnNotFoundDidYouMean() {
    // Column not found
    sql("select ^\"unknownColumn\"^ from emp")
        .fails("Column 'unknownColumn' not found in any table");
    // Similar column in table, unqualified table name
    sql("select ^\"empNo\"^ from emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // Similar column in table, table name qualified with schema
    sql("select ^\"empNo\"^ from sales.emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // Similar column in table, table name qualified with catalog and schema
    sql("select ^\"empNo\"^ from catalog.sales.emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // With table alias
    sql("select e.^\"empNo\"^ from catalog.sales.emp as e")
        .fails("Column 'empNo' not found in table 'E'; did you mean 'EMPNO'\\?");
    // With fully-qualified table alias
    sql("select catalog.sales.emp.^\"empNo\"^\n"
        + "from catalog.sales.emp")
        .fails("Column 'empNo' not found in table 'CATALOG\\.SALES\\.EMP'; "
            + "did you mean 'EMPNO'\\?");
    // Similar column in table; multiple tables
    sql("select ^\"name\"^ from emp, dept")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table; table and a query
    sql("select ^\"name\"^ from emp,\n"
        + "  (select * from dept) as d")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table; table and an un-aliased query
    sql("select ^\"name\"^ from emp, (select * from dept)")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table, multiple tables
    sql("select ^\"deptno\"^ from emp,\n"
        + "  (select deptno as \"deptNo\" from dept)")
        .fails("Column 'deptno' not found in any table; "
            + "did you mean 'DEPTNO', 'deptNo'\\?");
    sql("select ^\"deptno\"^ from emp,\n"
        + "  (select * from dept) as t(\"deptNo\", name)")
        .fails("Column 'deptno' not found in any table; "
            + "did you mean 'DEPTNO', 'deptNo'\\?");
  }

  /** Tests matching of built-in operator names. */
  @Test void testUnquotedBuiltInFunctionNames() {
    final Sql mysql = sql("?")
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BACK_TICK)
        .withCaseSensitive(false);
    final Sql oracle = sql("?")
        .withUnquotedCasing(Casing.TO_UPPER)
        .withCaseSensitive(true);

    // Built-in functions are always case-insensitive.
    oracle.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    oracle.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    oracle.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    oracle.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    oracle.sql("select current_time from dept").ok();
    oracle.sql("select Current_Time from dept").ok();
    oracle.sql("select CURRENT_TIME from dept").ok();

    mysql.sql("select sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    mysql.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    mysql.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    mysql.sql("select current_time from dept").ok();
    mysql.sql("select Current_Time from dept").ok();
    mysql.sql("select CURRENT_TIME from dept").ok();

    // MySQL assumes that a quoted function name is not a built-in.
    //
    // mysql> select `sum`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.sum does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select `SUM`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.SUM does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select SUM(`day`) from days;
    // +------------+
    // | SUM(`day`) |
    // +------------+
    // |         28 |
    // +------------+
    // 1 row in set (0.00 sec)
    //
    // We do not follow MySQL in this regard. `count` is preserved in
    // lower-case, and is matched case-insensitively because it is a built-in.
    // So, the query succeeds.
    oracle.sql("select \"count\"(*) from dept").ok();
    mysql.sql("select `count`(*) from dept").ok();
  }

  /** Sanity check: All built-ins are upper-case. We rely on this. */
  @Test void testStandardOperatorNamesAreUpperCase() {
    for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
      final String name = op.getName();
      switch (op.getSyntax()) {
      case SPECIAL:
      case INTERNAL:
        break;
      default:
        assertThat(name.toUpperCase(Locale.ROOT), equalTo(name));
        break;
      }
    }
  }

  private static int prec(SqlOperator op) {
    return Math.max(op.getLeftPrec(), op.getRightPrec());
  }

  /** Tests that operators, sorted by precedence, are in a sane order. Each
   * operator has a {@link SqlOperator#getLeftPrec() left} and
   * {@link SqlOperator#getRightPrec()} right} precedence, but we would like
   * the order to remain the same even if we tweak particular operators'
   * precedences. If you need to update the expected output, you might also
   * need to change
   * <a href="http://calcite.apache.org/docs/reference.html#operator-precedence">
   * the documentation</a>. */
  @Test void testOperatorsSortedByPrecedence() {
    final StringBuilder b = new StringBuilder();
    final Comparator<SqlOperator> comparator = (o1, o2) -> {
      int c = Integer.compare(prec(o1), prec(o2));
      if (c != 0) {
        return -c;
      }
      c = o1.getName().compareTo(o2.getName());
      if (c != 0) {
        return c;
      }
      return o1.getSyntax().compareTo(o2.getSyntax());
    };
    final List<SqlOperator> operators =
        SqlStdOperatorTable.instance().getOperatorList();
    int p = -1;
    for (SqlOperator op : Ordering.from(comparator).sortedCopy(operators)) {
      final String type;
      switch (op.getSyntax()) {
      case FUNCTION:
      case FUNCTION_ID:
      case FUNCTION_STAR:
      case INTERNAL:
        continue;
      case PREFIX:
        type = "pre";
        break;
      case POSTFIX:
        type = "post";
        break;
      case BINARY:
        if (op.getLeftPrec() < op.getRightPrec()) {
          type = "left";
        } else {
          type = "right";
        }
        break;
      default:
        if (op instanceof SqlSpecialOperator) {
          type = "-";
        } else {
          continue;
        }
      }
      if (prec(op) != p) {
        b.append('\n');
        p = prec(op);
      }
      b.append(op.getName())
          .append(' ')
          .append(type)
          .append('\n');
    }
    final String expected = "\n"
        + "ARRAY -\n"
        + "ARRAY -\n"
        + "COLUMN_LIST -\n"
        + "CURSOR -\n"
        + "LATERAL -\n"
        + "MAP -\n"
        + "MAP -\n"
        + "MULTISET -\n"
        + "MULTISET -\n"
        + "ROW -\n"
        + "TABLE -\n"
        + "UNNEST -\n"
        + "\n"
        + "CURRENT_VALUE -\n"
        + "DEFAULT -\n"
        + "DOT -\n"
        + "ITEM -\n"
        + "NEXT_VALUE -\n"
        + "PATTERN_EXCLUDE -\n"
        + "PATTERN_PERMUTE -\n"
        + "WITHIN DISTINCT left\n"
        + "WITHIN GROUP left\n"
        + "\n"
        + "PATTERN_QUANTIFIER -\n"
        + "\n"
        + " left\n"
        + "$LiteralChain -\n"
        + "+ pre\n"
        + "- pre\n"
        + "FINAL pre\n"
        + "RUNNING pre\n"
        + "\n"
        + "| left\n"
        + "\n"
        + "% left\n"
        + "* left\n"
        + "/ left\n"
        + "/INT left\n"
        + "|| left\n"
        + "\n"
        + "+ left\n"
        + "+ -\n"
        + "- left\n"
        + "- -\n"
        + "EXISTS pre\n"
        + "\n"
        + "< ALL left\n"
        + "< SOME left\n"
        + "<= ALL left\n"
        + "<= SOME left\n"
        + "<> ALL left\n"
        + "<> SOME left\n"
        + "= ALL left\n"
        + "= SOME left\n"
        + "> ALL left\n"
        + "> SOME left\n"
        + ">= ALL left\n"
        + ">= SOME left\n"
        + "BETWEEN ASYMMETRIC -\n"
        + "BETWEEN SYMMETRIC -\n"
        + "IN left\n"
        + "LIKE -\n"
        + "NEGATED POSIX REGEX CASE INSENSITIVE left\n"
        + "NEGATED POSIX REGEX CASE SENSITIVE left\n"
        + "NOT BETWEEN ASYMMETRIC -\n"
        + "NOT BETWEEN SYMMETRIC -\n"
        + "NOT IN left\n"
        + "NOT LIKE -\n"
        + "NOT SIMILAR TO -\n"
        + "POSIX REGEX CASE INSENSITIVE left\n"
        + "POSIX REGEX CASE SENSITIVE left\n"
        + "SIMILAR TO -\n"
        + "\n"
        + "$IS_DIFFERENT_FROM left\n"
        + "< left\n"
        + "<= left\n"
        + "<> left\n"
        + "= left\n"
        + "> left\n"
        + ">= left\n"
        + "CONTAINS left\n"
        + "EQUALS left\n"
        + "IMMEDIATELY PRECEDES left\n"
        + "IMMEDIATELY SUCCEEDS left\n"
        + "IS DISTINCT FROM left\n"
        + "IS NOT DISTINCT FROM left\n"
        + "MEMBER OF left\n"
        + "NOT SUBMULTISET OF left\n"
        + "OVERLAPS left\n"
        + "PRECEDES left\n"
        + "SUBMULTISET OF left\n"
        + "SUCCEEDS left\n"
        + "\n"
        + "FORMAT JSON post\n"
        + "IS A SET post\n"
        + "IS EMPTY post\n"
        + "IS FALSE post\n"
        + "IS JSON ARRAY post\n"
        + "IS JSON OBJECT post\n"
        + "IS JSON SCALAR post\n"
        + "IS JSON VALUE post\n"
        + "IS NOT A SET post\n"
        + "IS NOT EMPTY post\n"
        + "IS NOT FALSE post\n"
        + "IS NOT JSON ARRAY post\n"
        + "IS NOT JSON OBJECT post\n"
        + "IS NOT JSON SCALAR post\n"
        + "IS NOT JSON VALUE post\n"
        + "IS NOT NULL post\n"
        + "IS NOT TRUE post\n"
        + "IS NOT UNKNOWN post\n"
        + "IS NULL post\n"
        + "IS TRUE post\n"
        + "IS UNKNOWN post\n"
        + "\n"
        + "NOT pre\n"
        + "\n"
        + "AND left\n"
        + "\n"
        + "OR left\n"
        + "\n"
        + "=> -\n"
        + "AS -\n"
        + "DESC post\n"
        + "FILTER left\n"
        + "IGNORE NULLS -\n"
        + "OVER left\n"
        + "RESPECT NULLS -\n"
        + "TABLESAMPLE -\n"
        + "\n"
        + "INTERSECT left\n"
        + "INTERSECT ALL left\n"
        + "MULTISET INTERSECT ALL left\n"
        + "MULTISET INTERSECT DISTINCT left\n"
        + "NULLS FIRST post\n"
        + "NULLS LAST post\n"
        + "\n"
        + "EXCEPT left\n"
        + "EXCEPT ALL left\n"
        + "MULTISET EXCEPT ALL left\n"
        + "MULTISET EXCEPT DISTINCT left\n"
        + "MULTISET UNION ALL left\n"
        + "MULTISET UNION DISTINCT left\n"
        + "UNION left\n"
        + "UNION ALL left\n"
        + "\n"
        + "$throw -\n"
        + "Reinterpret -\n"
        + "TABLE pre\n"
        + "VALUES -\n"
        + "\n"
        + "CALL pre\n"
        + "ESCAPE -\n"
        + "NEW pre\n";
    assertThat(b.toString(), is(expected));
  }

  /** Tests that it is an error to insert into the same column twice, even using
   * case-insensitive matching. */
  @Test void testCaseInsensitiveInsert() {
    sql("insert into EMP ([EMPNO], deptno, ^[empno]^)\n"
        + " values (1, 1, 1)")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET)
        .fails("Target column 'EMPNO' is assigned more than once");
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. (The standard says it should be an error, but we don't right
   * now.) */
  @Test void testCaseInsensitiveSubQuery() {
    final Sql insensitive = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?")
        .withCaseSensitive(true)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BRACKET);
    String sql = "select [e] from (\n"
        + "select EMPNO as [e], DEPTNO as d, 1 as [e2] from EMP)";
    sensitive.sql(sql).ok();
    insensitive.sql(sql).ok();

    String sql1 = "select e2 from (\n"
        + "select EMPNO as [e2], DEPTNO as d, 1 as [E] from EMP)";
    insensitive.sql(sql1).ok();
    sensitive.sql(sql1).ok();
  }

  /** Tests using case-insensitive matching of table names. */
  @Test void testCaseInsensitiveTables() {
    final Sql mssql = sql("?").withLex(Lex.SQL_SERVER);
    mssql.sql("select eMp.* from (select * from emp) as EmP").ok();
    mssql.sql("select ^eMp^.* from (select * from emp as EmP)")
        .fails("Unknown identifier 'eMp'");
    mssql.sql("select eMp.* from (select * from emP) as EmP").ok();
    mssql.sql("select eMp.empNo from (select * from emP) as EmP").ok();
    mssql.sql("select empNo from (select Empno from emP) as EmP").ok();
    mssql.sql("select empNo from (select Empno from emP)").ok();
  }

  @Test void testInsert() {
    sql("insert into empnullables (empno, ename)\n"
        + "values (1, 'Ambrosia')").ok();
    sql("insert into empnullables (empno, ename)\n"
        + "select 1, 'Ardy' from (values 'a')").ok();

    final String sql = "insert into emp\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,\n"
        + "  1, false)";
    sql(sql).ok();

    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "select 1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00',\n"
        + "  1, 1, 1, false\n"
        + "from (values 'a')";
    sql(sql2).ok();

    sql("insert into empnullables (ename, empno, deptno)\n"
        + "values ('Pat', 1, null)").ok();

    final String sql3 = "insert into empnullables (\n"
        + "  empno, ename, job, hiredate)\n"
        + "values (1, 'Jim', 'Baker', timestamp '1970-01-01 00:00:00')";
    sql(sql3).ok();

    sql("insert into empnullables (empno, ename)\n"
        + "select 1, 'b' from (values 'a')").ok();

    sql("insert into empnullables (empno, ename)\n"
        + "values (1, 'Karl')").ok();
  }

  @Test void testInsertWithNonEqualSourceSinkFieldsNum() {
    sql("insert into ^dept^ select sid, ename, deptno "
        + "from "
        + "(select sum(empno) as sid, ename, deptno, sal "
        + "from emp group by ename, deptno, sal)")
        .fails("Number of INSERT target columns \\(2\\) "
            + "does not equal number of source items \\(3\\)");
  }

  @Test void testInsertSubset() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    final String sql1 = "insert into empnullables\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00')";
    s.sql(sql1).ok();

    final String sql2 = "insert into empnullables\n"
        + "values (1, 'nom', null, 0, null)";
    s.sql(sql2).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1510">[CALCITE-1510]
   * INSERT/UPSERT should allow fewer values than columns</a>,
   * check for default value only when target field is null. */
  @Test void testInsertShouldNotCheckForDefaultValue() {
    final int c = CountingFactory.THREAD_CALL_COUNT.get().get();
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    final String sql1 = "insert into emp values(1, 'nom', 'job', 0, "
        + "timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    s.sql(sql1).ok();
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now add a list of target columns, keeping the query otherwise the same.
    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    s.sql(sql2).ok();
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now remove SLACKER, which is NOT NULL, from the target list.
    final String sql3 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1)";
    s.sql(sql3)
        .fails("Column 'SLACKER' has no default value and does not allow NULLs");
    assertThat("Should not check for default value, even if if column is "
            + "missing from INSERT and nullable",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));

    // Now remove DEPTNO, which has a default value, from the target list.
    // Will generate an extra call to newColumnDefaultValue at sql-to-rel time,
    // just not yet.
    final String sql4 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, false)";
    s.sql(sql4).ok();
    assertThat("Missing DEFAULT column generates a call to factory",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));
  }

  @Test void testInsertView() {
    sql("insert into empnullables_20 (ename, empno, comm)\n"
        + "values ('Karl', 1, 1)").ok();
  }

  @Test void testInsertSubsetView() {
    sql("insert into empnullables_20\n"
        + "values (1, 'Karl')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
  }

  @Test void testInsertModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW (empno, ename, job)\n"
        + "values (1, 'Arthur', 'clown')").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, job, extra)\n"
        + "values (1, 'Arthur', 'clown', true)").ok();
  }

  @Test void testInsertSubsetModifiableView() {
    final Sql s = sql("?").withExtendedCatalog()
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    s.sql("insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1)").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1, 'Knight', 20, false, 99999, true, timestamp '1370-01-01 00:00:00',"
        + " 1, 100)").ok();
  }

  @Test void testInsertBind() {
    // VALUES
    final String sql0 = "insert into empnullables (empno, ename, deptno)\n"
        + "values (?, ?, ?)";
    sql(sql0).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, INTEGER ?2)");

    // multiple VALUES
    final String sql1 = "insert into empnullables (empno, ename, deptno)\n"
        + "values (?, 'Pat', 1), (2, ?, ?), (3, 'Tod', ?), (4, 'Arthur', null)";
    sql(sql1).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, INTEGER ?2, INTEGER ?3)");

    // VALUES with expression
    sql("insert into empnullables (ename, empno) values (?, ? + 1)")
        .ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // SELECT
    sql("insert into empnullables (ename, empno) select ?, ? from (values (1))")
        .ok().bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // WITH
    final String sql3 = "insert into empnullables (ename, empno)\n"
        + "with v as (values ('a'))\n"
        + "select ?, ? from (values (1))";
    sql(sql3).ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // UNION
    final String sql2 = "insert into empnullables (ename, empno)\n"
        + "select ?, ? from (values (1))\n"
        + "union all\n"
        + "select ?, ? from (values (time '1:2:3'))";
    final String expected2 = "RecordType(VARCHAR(20) ?0, INTEGER ?1,"
        + " VARCHAR(20) ?2, INTEGER ?3)";
    sql(sql2).ok().bindType(expected2);
  }


  @Test void testInsertWithExtendedColumns() {
    final String sql0 = "insert into empnullables\n"
        + " (empno, ename, \"f.dc\" ^varchar(10)^)\n"
        + "values (?, ?, ?)";
    sql(sql0).withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql1 = "insert into empnullables\n"
        + " (empno, ename, dynamic_column ^double^ not null)\n"
        + "values (?, ?, ?)";
    sql(sql1)
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, DOUBLE ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql2 = "insert into struct.t_extend\n"
        + " (f0.c0, f1.c1, \"F2\".\"C2\" ^varchar(20)^ not null)\n"
        + "values (?, ?, ?)";
    sql(sql2)
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");
  }

  @Test void testInsertBindSubset() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    // VALUES
    final String sql0 = "insert into empnullables\n"
        + "values (?, ?, ?)";
    s.sql(sql0).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)");

    // multiple VALUES
    final String sql1 = "insert into empnullables\n"
        + "values (?, 'Pat', 'Tailor'), (2, ?, ?),\n"
        + " (3, 'Tod', ?), (4, 'Arthur', null)";
    s.sql(sql1).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2, "
            + "VARCHAR(10) ?3)");

    // VALUES with expression
    s.sql("insert into empnullables values (? + 1, ?)").ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // SELECT
    s.sql("insert into empnullables select ?, ? from (values (1))").ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // WITH
    final String sql3 = "insert into empnullables\n"
        + "with v as (values ('a'))\n"
        + "select ?, ? from (values (1))";
    s.sql(sql3).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // UNION
    final String sql2 = "insert into empnullables\n"
        + "select ?, ? from (values (1))\n"
        + "union all\n"
        + "select ?, ? from (values (time '1:2:3'))";
    final String expected2 = "RecordType(INTEGER ?0, VARCHAR(20) ?1,"
        + " INTEGER ?2, VARCHAR(20) ?3)";
    s.sql(sql2).ok().bindType(expected2);
  }

  @Test void testInsertBindView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (mgr, empno, ename)"
        + " values (?, ?, ?)";
    sql(sql).withExtendedCatalog().ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)");
  }

  @Test void testInsertModifiableViewPassConstraint() {
    sql("insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename, extra)"
        + " values (20, 100, 'Lex', true)")
        .withExtendedCatalog()
        .ok();
    sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, extra)"
        + " values (100, 'Lex', true)")
        .withExtendedCatalog()
        .ok();

    sql("insert into EMP_MODIFIABLEVIEW2 values ('Edward', 20)")
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
  }

  @Test void testInsertModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename)"
        + " values (^21^, 100, 'Lex')";
    final String error0 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename)"
        + " values (^19+1^, 100, 'Lex')";
    final String error1 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql1).fails(error1);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1, 'Knight', ^27^, false, 99999, true,"
        + "timestamp '1370-01-01 00:00:00', 1, 100)";
    final String error2 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql2).fails(error2);
  }

  @Test void testUpdateModifiableViewPassConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2"
        + " set deptno = 20, empno = 99"
        + " where ename = 'Lex'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2"
        + " set empno = 99"
        + " where ename = 'Lex'").ok();
  }

  @Test void testUpdateModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update EMP_MODIFIABLEVIEW2"
        + " set deptno = ^21^, empno = 99"
        + " where ename = 'Lex'";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW2"
        + " set deptno = ^19 + 1^, empno = 99"
        + " where ename = 'Lex'";
    s.sql(sql1).fails(error);
  }

  @Test void testInsertTargetTableWithVirtualColumns() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into VIRTUALCOLUMNS.VC_T1\n"
        + "select a, b, c from VIRTUALCOLUMNS.VC_T2").ok();

    final String sql0 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', 3, 4)";
    final String error0 = "Cannot INSERT into generated column 'D'";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT, DEFAULT)";
    s.sql(sql1).ok();

    final String sql2 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT)";
    final String error2 = "(?s).*Number of INSERT target columns \\(5\\) "
        + "does not equal number of source items \\(4\\).*";
    s.sql(sql2).fails(error2);

    final String sql3 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT, DEFAULT, DEFAULT)";
    final String error3 = "(?s).*Number of INSERT target columns \\(5\\) "
        + "does not equal number of source items \\(6\\).*";
    s.sql(sql3).fails(error3);

    final String sql4 = "insert into VIRTUALCOLUMNS.VC_T1\n"
        + "^values(1, '2', 'abc')^";
    final String error4 = "(?s).*Cannot assign to target field 'B' of type BIGINT "
        + "from source field 'EXPR\\$1' of type CHAR\\(1\\).*";
    s.sql(sql4).withTypeCoercion(false).fails(error4);
    s.sql(sql4).ok();
  }

  @Test void testInsertFailNullability() {
    sql("insert into ^emp^ (ename) values ('Kevin')")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^emp^ (empno) values (10)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into emp (empno, ename, deptno) ^values (5, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertSubsetFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^emp^ values (1)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into emp ^values (null, 'Liam')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into emp ^values (45, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertViewFailNullability() {
    sql("insert into ^emp_20^ (ename) values ('Jake')")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^emp_20^ (empno) values (9)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into emp_20 (empno, ename, mgr) ^values (5, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertSubsetViewFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^EMP_20^ values (1)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into EMP_20 ^values (null, 'Liam')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into EMP_20 ^values (45, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertBindFailNullability() {
    sql("insert into ^emp^ (ename) values (?)")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^emp^ (empno) values (?)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into emp (empno, ename, deptno) ^values (?, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertBindSubsetFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^emp^ values (?)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into emp ^values (null, ?)^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into emp ^values (?, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertSubsetDisallowed() {
    sql("insert into ^emp^ values (1)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (null)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (1, 'Kevin')")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test void testInsertSubsetViewDisallowed() {
    sql("insert into ^emp_20^ values (1)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp_20^ values (null)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp_20^ values (?, ?)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test void testInsertBindSubsetDisallowed() {
    sql("insert into ^emp^ values (?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (?, ?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test void testSelectExtendedColumnDuplicate() {
    sql("select deptno, extra from emp (extra int, \"extra\" boolean)").ok();
    sql("select deptno, extra from emp (extra int, \"extra\" int)").ok();
    sql("select deptno, extra from emp (extra int, ^extra^ int)")
        .fails("Duplicate name 'EXTRA' in column list");
    sql("select deptno, extra from emp (extra int, ^extra^ boolean)")
        .fails("Duplicate name 'EXTRA' in column list");
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select deptno, extra\n"
        + "from EMP_MODIFIABLEVIEW (extra int, ^extra^ int)";
    s.sql(sql0).fails("Duplicate name 'EXTRA' in column list");

    final String sql1 = "select deptno, extra from EMP_MODIFIABLEVIEW"
        + " (extra int, ^extra^ boolean)";
    s.sql(sql1).fails("Duplicate name 'EXTRA' in column list");
  }

  @Test void testSelectViewFailExcludedColumn() {
    final String sql = "select ^deptno^, empno from EMP_MODIFIABLEVIEW";
    final String error = "Column 'DEPTNO' not found in any table";
    sql(sql).withExtendedCatalog().fails(error);
  }

  @Test void testSelectViewExtendedColumnCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (SAL int)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, \"Sal\", HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"Sal\" VARCHAR)\n"
        + " where SAL = 20").ok();
  }

  @Test void testSelectViewExtendedColumnExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, EXTRA,"
        + " \"EXtra\"\n"
        + " from EMP_MODIFIABLEVIEW2 extend (\"EXtra\" VARCHAR)\n"
        + " where SAL = 20").ok();
  }

  @Test void testSelectViewExtendedColumnUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMP_MODIFIABLEVIEW3 extend (COMM int)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comM\"\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"comM\" BOOLEAN)\n"
        + " where SAL = 20").ok();
  }

  @Test void testSelectExtendedColumnCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (COMM int)\n"
        + " where SAL = 20").ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM, \"ComM\"\n"
        + " from EMPDEFAULTS extend (\"ComM\" int)\n"
        + " where SAL = 20").ok();
  }

  @Test void testSelectExtendedColumnFailCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^COMM^ boolean)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'COMM' of type INTEGER "
            + "from source field 'COMM' of type BOOLEAN");
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^EMPNO^ integer)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL "
            + "from source field 'EMPNO' of type INTEGER");
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^\"EMPNO\"^ integer)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL "
            + "from source field 'EMPNO' of type INTEGER");
  }

  @Test void testSelectViewExtendedColumnFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^SLACKER^ integer)\n"
        + " where SAL = 20";
    final String error0 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^EMPNO^ integer)\n"
        + " where SAL = 20";
    final String error1 = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type INTEGER";
    s.sql(sql1).fails(error1);
  }

  @Test void testSelectViewExtendedColumnFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^EXTRA^ integer)\n"
        + " where SAL = 20";
    final String error = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    s.sql(sql0).fails(error);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^\"EXTRA\"^ integer)\n"
        + " where SAL = 20";
    s.sql(sql1).fails(error);
  }

  @Test void testSelectViewExtendedColumnFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW3 extend (^COMM^ boolean)\n"
        + "where SAL = 20";
    final String error = "Cannot assign to target field 'COMM' of type INTEGER"
        + " from source field 'COMM' of type BOOLEAN";
    s.sql(sql0).fails(error);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW3 extend (^\"COMM\"^ boolean)\n"
        + " where SAL = 20";
    s.sql(sql1).fails(error);
  }

  @Test void testSelectFailCaseSensitivity() {
    sql("select ^\"empno\"^, ename, deptno from EMP")
        .fails("Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
    sql("select ^\"extra\"^, ename, deptno from EMP (extra boolean)")
        .fails("Column 'extra' not found in any table; did you mean 'EXTRA'\\?");
    sql("select ^extra^, ename, deptno from EMP (\"extra\" boolean)")
        .fails("Column 'EXTRA' not found in any table; did you mean 'extra'\\?");
  }

  @Test void testInsertFailCaseSensitivity() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW"
        + " (^\"empno\"^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql0).fails("Unknown target column 'empno'");

    final String sql1 = "insert into EMP_MODIFIABLEVIEW (\"extra\" int)"
        + " (^extra^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql1).fails("Unknown target column 'EXTRA'");

    final String sql2 = "insert into EMP_MODIFIABLEVIEW (extra int)"
        + " (^\"extra\"^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql2).fails("Unknown target column 'extra'");
  }

  @Test void testInsertFailExcludedColumn() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = ""
        + "insert into EMP_MODIFIABLEVIEW (empno, ename, ^deptno^)"
        + " values (45, 'Jake', 5)";
    s.sql(sql).fails("Unknown target column 'DEPTNO'");
  }

  @Test void testInsertBindViewFailExcludedColumn() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "insert into EMP_MODIFIABLEVIEW (empno, ename, ^deptno^)"
        + " values (?, ?, ?)";
    s.sql(sql).fails("Unknown target column 'DEPTNO'");
  }

  @Test void testInsertWithCustomInitializerExpressionFactory() {
    sql("insert into empdefaults (deptno) values (1)").ok();
    sql("insert into empdefaults (ename, empno) values ('Quan', 50)").ok();
    sql("insert into empdefaults (ename, deptno) ^values (null, 1)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into ^empdefaults^ values (null, 'Tod')")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into empdefaults values (101)").ok();
    s.sql("insert into empdefaults values (101, 'Coral')").ok();
    s.sql("insert into empdefaults ^values (null, 'Tod')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into empdefaults ^values (78, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test void testInsertBindWithCustomInitializerExpressionFactory() {
    sql("insert into empdefaults (deptno) values (?)").ok()
        .bindType("RecordType(INTEGER ?0)");
    sql("insert into empdefaults (ename, empno) values (?, ?)").ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");
    sql("insert into empdefaults (ename, deptno) ^values (null, ?)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into ^empdefaults^ values (null, ?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    s.sql("insert into empdefaults values (101, ?)").ok()
        .bindType("RecordType(VARCHAR(20) ?0)");
    s.sql("insert into empdefaults ^values (null, ?)^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
  }

  @Test void testInsertBindWithCustomColumnResolving() {
    final SqlConformanceEnum pragmatic = SqlConformanceEnum.PRAGMATIC_2003;

    final String sql = "insert into struct.t\n"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    final String expected = "RecordType(VARCHAR(20) ?0, VARCHAR(20) ?1,"
        + " INTEGER ?2, BOOLEAN ?3, INTEGER ?4, INTEGER ?5, INTEGER ?6,"
        + " INTEGER ?7, INTEGER ?8)";
    sql(sql).ok().bindType(expected);

    final String sql2 =
        "insert into struct.t_nullables (c0, c2, c1) values (?, ?, ?)";
    final String expected2 =
        "RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)";
    sql(sql2).withConformance(pragmatic).ok().bindType(expected2);

    final String sql3 =
        "insert into struct.t_nullables (f1.c0, f1.c2, f0.c1) values (?, ?, ?)";
    final String expected3 =
        "RecordType(INTEGER ?0, INTEGER ?1, INTEGER ?2)";
    sql(sql3).withConformance(pragmatic).ok().bindType(expected3);

    sql("insert into struct.t_nullables (c0, ^c4^, c1) values (?, ?, ?)")
        .withConformance(pragmatic)
        .fails("Unknown target column 'C4'");
    sql("insert into struct.t_nullables (^a0^, c2, c1) values (?, ?, ?)")
        .withConformance(pragmatic)
        .fails("Unknown target column 'A0'");
    final String sql4 = "insert into struct.t_nullables (\n"
        + "  f1.c0, ^f0.a0^, f0.c1) values (?, ?, ?)";
    sql(sql4).withConformance(pragmatic)
        .fails("Unknown target column 'F0.A0'");
    final String sql5 = "insert into struct.t_nullables (\n"
        + "  f1.c0, f1.c2, ^f1.c0^) values (?, ?, ?)";
    sql(sql5).withConformance(pragmatic)
        .fails("Target column '\"F1\".\"C0\"' is assigned more than once");
  }

  @Test void testUpdateBind() {
    final String sql = "update emp\n"
        + "set ename = ?\n"
        + "where deptno = ?";
    sql(sql).ok().bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");
  }

  @Test void testDeleteBind() {
    final String sql = "delete from emp\n"
        + "where deptno = ?\n"
        + "or ename = ?";
    sql(sql).ok().bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");
  }

  @Test void testStream() {
    sql("select stream * from orders").ok();
    sql("select stream * from ^emp^")
        .fails(cannotConvertToStream("EMP"));
    sql("select * from ^orders^")
        .fails(cannotConvertToRelation("ORDERS"));
  }

  @Test void testStreamWhere() {
    sql("select stream * from orders where productId < 10").ok();
    sql("select stream * from ^emp^ where deptno = 10")
        .fails(cannotConvertToStream("EMP"));
    sql("select stream * from ^emp^ as e where deptno = 10")
        .fails(cannotConvertToStream("E"));
    sql("select stream * from (^select * from emp as e1^) as e\n"
        + "where deptno = 10")
        .fails("Cannot convert table 'E' to stream");
    sql("select * from ^orders^ where productId > 10")
        .fails(cannotConvertToRelation("ORDERS"));
  }

  @Test void testStreamGroupBy() {
    sql("select stream rowtime, productId, count(*) as c\n"
        + "from orders\n"
        + "group by productId, rowtime").ok();
    sql("select stream floor(rowtime to hour) as rowtime, productId,\n"
        + " count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to hour), productId").ok();
    sql("select stream productId, count(*) as c\n"
        + "from orders\n"
        + "^group by productId^")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream ^count(*)^ as c\n"
        + "from orders")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream count(*) as c\n"
        + "from orders ^group by ()^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  @Test void testStreamHaving() {
    sql("select stream rowtime, productId, count(*) as c\n"
        + "from orders\n"
        + "group by productId, rowtime\n"
        + "having count(*) > 5").ok();
    sql("select stream floor(rowtime to hour) as rowtime, productId,\n"
        + " count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to hour), productId\n"
        + "having false").ok();
    sql("select stream productId, count(*) as c\n"
        + "from orders\n"
        + "^group by productId^\n"
        + "having count(*) > 5")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream 1\n"
        + "from orders\n"
        + "having ^count(*) > 3^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  /** Tests that various expressions are monotonic. */
  @Test void testMonotonic() {
    sql("select stream floor(rowtime to hour) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream ceil(rowtime to minute) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(minute from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);
    sql("select stream (rowtime - timestamp '1970-01-01 00:00:00') hour from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer) / 15\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "mod(cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer), 15)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // constant
    sql("select stream 1 - 2 from orders")
        .monotonic(SqlMonotonicity.CONSTANT);
    sql("select stream 1 + 2 from orders")
        .monotonic(SqlMonotonicity.CONSTANT);

    // extract(YEAR) is monotonic, extract(other time unit) is not
    sql("select stream extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(month from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> - constant
    sql("select stream extract(year from rowtime) - 3 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);

    // <monotonic> / constant
    sql("select stream extract(year from rowtime) / -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream extract(year from rowtime) / 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) / 0 from orders")
        .monotonic(SqlMonotonicity.CONSTANT); // +inf is constant!
    sql("select stream extract(year from rowtime) / null from orders")
        .monotonic(SqlMonotonicity.CONSTANT);
    sql("select stream null / extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.CONSTANT);
    sql("select stream extract(year from rowtime) / cast(null as integer) from orders")
        .monotonic(SqlMonotonicity.CONSTANT);
    sql("select stream cast(null as integer) / extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.CONSTANT);

    // constant / <monotonic> is not monotonic (we don't know whether sign of
    // expression ever changes)
    sql("select stream 5 / extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> * constant
    sql("select stream extract(year from rowtime) * -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream extract(year from rowtime) * 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * 0 from orders")
        .monotonic(SqlMonotonicity.CONSTANT); // 0 is constant!

    // constant * <monotonic>
    sql("select stream -5 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream 5 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream 0 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.CONSTANT);

    // <monotonic> - <monotonic>
    sql("select stream\n"
        + "extract(year from rowtime) - extract(year from rowtime)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> + <monotonic>
    sql("select stream\n"
        + "extract(year from rowtime) + extract(year from rowtime)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
  }

  @Test void testStreamUnionAll() {
    sql("select orderId\n"
        + "from ^orders^\n"
        + "union all\n"
        + "select orderId\n"
        + "from shipments")
        .fails(cannotConvertToRelation("ORDERS"));
    sql("select stream orderId\n"
        + "from orders\n"
        + "union all\n"
        + "^select orderId\n"
        + "from shipments^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("select empno\n"
        + "from emp\n"
        + "union all\n"
        + "^select stream orderId\n"
        + "from orders^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("select stream orderId\n"
        + "from orders\n"
        + "union all\n"
        + "select stream orderId\n"
        + "from shipments").ok();
    sql("select stream rowtime, orderId\n"
        + "from orders\n"
        + "union all\n"
        + "select stream rowtime, orderId\n"
        + "from shipments\n"
        + "order by rowtime").ok();
  }

  @Test void testStreamValues() {
    sql("select stream * from (^values 1) as e^")
        .fails(cannotConvertToStream("E"));
    sql("select stream * from (^values 1) as e (c)^")
        .fails(cannotConvertToStream("E"));
    sql("select stream orderId from orders\n"
        + "union all\n"
        + "^values 1^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("values 1, 2\n"
        + "union all\n"
        + "^select stream orderId from orders^\n")
        .fails(STR_SET_OP_INCONSISTENT);
  }

  @Test void testStreamOrderBy() {
    sql("select stream *\n"
        + "from orders\n"
        + "order by rowtime").ok();
    sql("select stream *\n"
        + "from orders\n"
        + "order by floor(rowtime to hour)").ok();
    sql("select stream floor(rowtime to minute), productId\n"
        + "from orders\n"
        + "order by floor(rowtime to hour)").ok();
    sql("select stream floor(rowtime to minute), productId\n"
        + "from orders\n"
        + "order by floor(rowtime to minute), productId desc").ok();
    sql("select stream *\n"
        + "from orders\n"
        + "order by ^productId^, rowtime")
        .fails(STR_ORDER_REQUIRES_MONO);
    sql("select stream *\n"
        + "from orders\n"
        + "order by ^rowtime desc^")
        .fails(STR_ORDER_REQUIRES_MONO);
    sql("select stream *\n"
        + "from orders\n"
        + "order by floor(rowtime to hour), rowtime desc").ok();
  }

  @Test void testStreamJoin() {
    sql("select stream\n"
        + " orders.rowtime as rowtime, orders.orderId as orderId,\n"
        + " products.supplierId as supplierId\n"
        + "from orders\n"
        + "join products on orders.productId = products.productId").ok();
    sql("^select stream *\n"
        + "from products\n"
        + "join suppliers on products.supplierId = suppliers.supplierId^")
        .fails(cannotStreamResultsForNonStreamingInputs("PRODUCTS, SUPPLIERS"));
  }

  @Test void testDummy() {
    // (To debug individual statements, paste them into this method.)
    expr("true\n"
        + "or ^(date '1-2-3', date '1-2-3', date '1-2-3')\n"
        + "   overlaps (date '1-2-3', date '1-2-3')^\n"
        + "or false")
        .fails("(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
  }

  @Test void testCustomColumnResolving() {
    checkCustomColumnResolving("T");
  }

  @Test void testCustomColumnResolvingWithView() {
    checkCustomColumnResolving("T_10");
  }

  private void checkCustomColumnResolving(String table) {
    // Table STRUCT.T is defined as: (
    //   K0 VARCHAR(20) NOT NULL,
    //   C1 VARCHAR(20) NOT NULL,
    //   RecordType:PEEK_FIELDS_DEFAULT(
    //     C0 INTEGER NOT NULL,
    //     C1 INTEGER NOT NULL) F0,
    //   RecordType:PEEK_FIELDS(
    //      C0 INTEGER,
    //      C2 INTEGER NOT NULL,
    //      A0 INTEGER NOT NULL) F1,
    //   RecordType:PEEK_FIELDS(
    //      C3 INTEGER NOT NULL,
    //      A0 BOOLEAN NOT NULL) F2)
    //
    // The labels 'PEEK_FIELDS_DEFAULT' and 'PEEK_FIELDS' mean that F0, F1 and
    // F2 can all be transparent. F0 has default struct priority; F1 and F2 have
    // lower priority.

    sql("select * from struct." + table).ok();

    // Resolve K0 as top-level column K0.
    sql("select k0 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL K0) NOT NULL");

    // Resolve C2 as secondary-level column F1.C2.
    sql("select c2 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F1.C2 as fully qualified column F1.C2.
    sql("select f1.c2 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve C1 as top-level column C1 as opposed to F0.C1.
    sql("select c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Resolve C0 as secondary-level column F0.C0 as opposed to F1.C0, since F0
    // has the default priority.
    sql("select c0 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C0) NOT NULL");

    // Resolve F1.C0 as fully qualified column F1.C0 (as evidenced by "INTEGER"
    // rather than "INTEGER NOT NULL")
    sql("select f1.c0 from struct." + table)
        .type("RecordType(INTEGER C0) NOT NULL");

    // Fail ambiguous column reference A0, since F1.A0 and F2.A0 both exist with
    // the same resolving priority.
    sql("select ^a0^ from struct." + table)
        .fails("Column 'A0' is ambiguous");

    // Resolve F2.A0 as fully qualified column F2.A0.
    sql("select f2.a0 from struct." + table)
        .type("RecordType(BOOLEAN NOT NULL A0) NOT NULL");

    // Resolve T0.K0 as top-level column K0, since T0 is recognized as the table
    // alias.
    sql("select t0.k0 from struct." + table + " t0")
        .type("RecordType(VARCHAR(20) NOT NULL K0) NOT NULL");

    // Resolve T0.C2 as secondary-level column F1.C2, since T0 is recognized as
    // the table alias here.
    sql("select t0.c2 from struct." + table + " t0")
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F0.C2 as secondary-level column F1.C2, since F0 is recognized as
    // the table alias here.
    sql("select f0.c2 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F0.C1 as top-level column C1 as opposed to F0.C1, since F0 is
    // recognized as the table alias here.
    sql("select f0.c1 from struct." + table + " f0")
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Resolve C1 as inner INTEGER column not top-level VARCHAR column.
    sql("select f0.f0.c1 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Resolve <table>.C1 as top-level column C1 as opposed to F0.C1, since <table> is
    // recognized as the table name.
    sql("select " + table + ".c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Alias "f0" obscures table name "<table>"
    sql("select ^" + table + "^.c1 from struct." + table + " f0")
        .fails("Table '" + table + "' not found");

    // Resolve STRUCT.<table>.C1 as top-level column C1 as opposed to F0.C1, since
    // STRUCT.<table> is recognized as the schema and table name.
    sql("select struct." + table + ".c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Table alias "f0" obscures table name "STRUCT.<table>"
    sql("select ^struct." + table + "^.c1 from struct." + table + " f0")
        .fails("Table 'STRUCT." + table + "' not found");

    // Resolve F0.F0.C1 as secondary-level column F0.C1, since the first F0 is
    // recognized as the table alias here.
    sql("select f0.f0.c1 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Resolve <table>.F0.C1 as secondary-level column F0.C1, since <table> is
    // recognized as the table name.
    sql("select " + table + ".f0.c1 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Table alias obscures
    sql("select ^" + table + ".f0^.c1 from struct." + table + " f0")
        .fails("Table '" + table + ".F0' not found");

    // Resolve STRUCT.<table>.F0.C1 as secondary-level column F0.C1, since
    // STRUCT.<table> is recognized as the schema and table name.
    sql("select struct." + table + ".f0.c1 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Table alias "f0" obscures table name "STRUCT.<table>"
    sql("select ^struct." + table + ".f0^.c1 from struct." + table + " f0")
        .fails("Table 'STRUCT." + table + ".F0' not found");

    // Resolve struct type F1 with wildcard.
    sql("select f1.* from struct." + table)
        .type("RecordType(INTEGER NOT NULL A0, INTEGER C0,"
            + " INTEGER NOT NULL C2) NOT NULL");

    // Resolve struct type F1 with wildcard.
    sql("select " + table + ".f1.* from struct." + table)
        .type("RecordType(INTEGER NOT NULL A0, INTEGER C0,"
            + " INTEGER NOT NULL C2) NOT NULL");

    // Fail non-existent column B0.
    sql("select ^b0^ from struct." + table)
        .fails("Column 'B0' not found in any table");

    // A column family can only be referenced with a star expansion.
    sql("select ^f1^ from struct." + table)
        .fails("Column 'F1' not found in any table");

    // If we fail to find a column, give an error based on the shortest prefix
    // that fails.
    sql("select " + table + ".^f0.notFound^.a.b.c.d from struct." + table)
        .fails("Column 'F0\\.NOTFOUND' not found in table '" + table + "'");
    sql("select " + table + ".^f0.notFound^ from struct." + table)
        .fails("Column 'F0\\.NOTFOUND' not found in table '" + table + "'");
    sql("select " + table + ".^f0.c1.notFound^ from struct." + table)
        .fails("Column 'F0\\.C1\\.NOTFOUND' not found in table '" + table + "'");
  }

  @Test void testDescriptor() {
    sql("select * from table(tumble(table orders, descriptor(rowtime), interval '2' hour))").ok();
    sql("select * from table(tumble(table orders, descriptor(^column_not_exist^), "
        + "interval '2' hour))")
        .fails("Unknown identifier 'COLUMN_NOT_EXIST'");
  }

  @Test public void testTumbleTableFunction() {
    sql("select rowtime, productid, orderid, 'window_start', 'window_end' from table(\n"
        + "tumble(table orders, descriptor(rowtime), interval '2' hour))").ok();
    sql("select rowtime, productid, orderid, 'window_start', 'window_end' from table(\n"
        + "tumble(table orders, descriptor(rowtime), interval '2' hour, interval '1' hour))").ok();
    // test named params.
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "tumble(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "size => interval '2' hour))").ok();
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "tumble(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "size => interval '2' hour,\n"
        + "\"OFFSET\" => interval '1' hour))").ok();
    // negative tests.
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "^tumble(table orders, descriptor(productid), interval '2' hour)^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE table_name, "
            + "DESCRIPTOR\\(timecol\\), datetime interval\\[, datetime interval\\]\\)");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "^tumble(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(productid),\n"
        + "size => interval '2' hour)^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE table_name, "
            + "DESCRIPTOR\\(timecol\\), datetime interval\\[, datetime interval\\]\\)");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "tumble(\n"
        + "^\"data\"^ => table orders,\n"
        + "TIMECOL => descriptor(rowtime),\n"
        + "SIZE => interval '2' hour))")
        .fails("Param 'data' not found in function 'TUMBLE'; did you mean 'DATA'\\?");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "^tumble(\n"
        + "data => table orders,\n"
        + "SIZE => interval '2' hour)^)")
        .fails("Invalid number of arguments to function 'TUMBLE'. Was expecting 3 arguments");
    sql("select * from table(\n"
        + "^tumble(table orders, descriptor(rowtime))^)")
        .fails("Invalid number of arguments to function 'TUMBLE'. Was expecting 3 arguments");
    sql("select * from table(\n"
        + "^tumble(table orders, descriptor(rowtime), 'test')^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>,"
            + " <CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE "
            + "table_name, DESCRIPTOR\\(timecol\\), datetime interval"
            + "\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "^tumble(table orders, 'test', interval '2' hour)^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <CHAR\\"
            + "(4\\)>, <INTERVAL HOUR>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE "
            + "table_name, DESCRIPTOR\\(timecol\\), datetime interval"
            + "\\[, datetime interval\\]\\)");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end' from table(\n"
        + "^tumble(table orders, descriptor(rowtime), interval '2' hour, 'test')^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>,"
            + " <INTERVAL HOUR>, <CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE "
            + "table_name, DESCRIPTOR\\(timecol\\), datetime interval"
            + "\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "tumble(TABLE ^tabler_not_exist^, descriptor(rowtime), interval '2' hour))")
        .fails("Object 'TABLER_NOT_EXIST' not found");
  }

  @Test public void testHopTableFunction() {
    sql("select * from table(\n"
        + "hop(table orders, descriptor(rowtime), interval '2' hour, interval '1' hour))").ok();
    sql("select * from table(\n"
        + "hop(table orders, descriptor(rowtime), interval '2' hour, interval '1' hour, "
        + "interval '20' minute))").ok();
    // test named params.
    sql("select * from table(\n"
        + "hop(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "slide => interval '2' hour,\n"
        + "size => interval '1' hour))").ok();
    sql("select * from table(\n"
        + "hop(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "slide => interval '2' hour,\n"
        + "size => interval '1' hour,\n"
        + "\"OFFSET\" => interval '20' minute))").ok();
    // negative tests.
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "^hop(table orders, descriptor(productid), interval '2' hour, interval '1' hour)^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>, <INTERVAL HOUR>\\)'\\. Supported form\\(s\\): "
            + "HOP\\(TABLE table_name, DESCRIPTOR\\(timecol\\), "
            + "datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end'\n"
        + "from table(\n"
        + "^hop(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(productid),\n"
        + "size => interval '2' hour,\n"
        + "slide => interval '1' hour)^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>, <INTERVAL HOUR>\\)'\\. Supported form\\(s\\): "
            + "HOP\\(TABLE table_name, DESCRIPTOR\\(timecol\\), "
            + "datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "hop(\n"
        + "^\"data\"^ => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "slide => interval '2' hour,\n"
        + "size => interval '1' hour))")
        .fails("Param 'data' not found in function 'HOP'; did you mean 'DATA'\\?");
    sql("select * from table(\n"
        + "^hop(\n"
        + "data => table orders,\n"
        + "slide => interval '2' hour,\n"
        + "size => interval '1' hour)^)")
        .fails("Invalid number of arguments to function 'HOP'. Was expecting 4 arguments");
    sql("select * from table(\n"
        + "^hop(table orders, descriptor(rowtime), interval '2' hour)^)")
        .fails("Invalid number of arguments to function 'HOP'. Was expecting 4 arguments");
    sql("select * from table(\n"
        + "^hop(table orders, descriptor(rowtime), interval '2' hour, 'test')^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\(TIMESTAMP\\(0\\) "
            + "ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, <INTERVAL HOUR>, "
            + "<CHAR\\(4\\)>\\)'. Supported form\\(s\\): HOP\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "^hop(table orders, descriptor(rowtime), 'test', interval '2' hour)^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\(TIMESTAMP\\(0\\) "
            + "ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, <CHAR\\(4\\)>, "
            + "<INTERVAL HOUR>\\)'. Supported form\\(s\\): HOP\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "^hop(table orders, 'test', interval '2' hour, interval '2' hour)^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\(TIMESTAMP\\(0\\) "
            + "ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <CHAR\\(4\\)>, <INTERVAL HOUR>, "
            + "<INTERVAL HOUR>\\)'. Supported form\\(s\\): HOP\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "^hop(table orders, descriptor(rowtime), interval '2' hour, interval '1' hour, 'test')^)")
        .fails("Cannot apply 'HOP' to arguments of type 'HOP\\(<RECORDTYPE\\(TIMESTAMP\\(0\\) "
            + "ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, <INTERVAL HOUR>, "
            + "<INTERVAL HOUR>, <CHAR\\(4\\)>\\)'. Supported form\\(s\\): HOP\\(TABLE table_name, "
            + "DESCRIPTOR\\(timecol\\), datetime interval, datetime interval\\[, datetime interval\\]\\)");
    sql("select * from table(\n"
        + "hop(TABLE ^tabler_not_exist^, descriptor(rowtime), interval '2' hour, interval '1' hour))")
        .fails("Object 'TABLER_NOT_EXIST' not found");
  }

  @Test public void testSessionTableFunction() {
    sql("select * from table(\n"
        + "session(table orders, descriptor(rowtime), descriptor(productid), interval '1' hour))")
        .ok();
    // test without key descriptor
    sql("select * from table(\n"
        + "session(table orders, descriptor(rowtime), interval '2' hour))").ok();
    // test named params.
    sql("select * from table(\n"
        + "session(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "key => descriptor(productid),\n"
        + "size => interval '1' hour))")
        .ok();
    sql("select * from table(\n"
        + "session(\n"
        + "data => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "size => interval '1' hour))")
        .ok();
    // negative tests.
    sql("select * from table(\n"
        + "^session(\n"
        + "data => table orders,\n"
        + "key => descriptor(productid),\n"
        + "size => interval '1' hour)^)")
        .fails("Cannot apply 'SESSION' to arguments of type 'SESSION\\(<RECORDTYPE\\(TIMESTAMP\\("
            + "0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>\\)'. Supported form\\(s\\): SESSION\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), DESCRIPTOR\\(key\\) optional, datetime interval\\)");
    sql("select * from table(\n"
        + "session(\n"
        + "^\"data\"^ => table orders,\n"
        + "timecol => descriptor(rowtime),\n"
        + "key => descriptor(productid),\n"
        + "size => interval '1' hour))")
        .fails("Param 'data' not found in function 'SESSION'; did you mean 'DATA'\\?");
    sql("select * from table(\n"
        + "^session(table orders, descriptor(rowtime), descriptor(productid), 'test')^)")
        .fails("Cannot apply 'SESSION' to arguments of type 'SESSION\\(<RECORDTYPE\\(TIMESTAMP\\("
            + "0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, <COLUMN_LIST>, "
            + "<CHAR\\(4\\)>\\)'. Supported form\\(s\\): SESSION\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), DESCRIPTOR\\(key\\) optional, datetime interval\\)");
    sql("select * from table(\n"
        + "^session(table orders, descriptor(rowtime), 'test', interval '2' hour)^)")
        .fails("Cannot apply 'SESSION' to arguments of type 'SESSION\\(<RECORDTYPE\\(TIMESTAMP\\("
            + "0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>, <CHAR\\(4\\)>, "
            + "<INTERVAL HOUR>\\)'. Supported form\\(s\\): SESSION\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), DESCRIPTOR\\(key\\) optional, datetime interval\\)");
    sql("select * from table(\n"
        + "^session(table orders, 'test', descriptor(productid), interval '2' hour)^)")
        .fails("Cannot apply 'SESSION' to arguments of type 'SESSION\\(<RECORDTYPE\\(TIMESTAMP\\("
            + "0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <CHAR\\(4\\)>, <COLUMN_LIST>, "
            + "<INTERVAL HOUR>\\)'. Supported form\\(s\\): SESSION\\(TABLE table_name, DESCRIPTOR\\("
            + "timecol\\), DESCRIPTOR\\(key\\) optional, datetime interval\\)");
    sql("select * from table(\n"
        + "session(TABLE ^tabler_not_exist^, descriptor(rowtime), descriptor(productid), interval '1' hour))")
        .fails("Object 'TABLER_NOT_EXIST' not found");
  }

  @Test public void testStreamTumble() {
    // TUMBLE
    sql("select stream tumble_end(rowtime, interval '2' hour) as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId").ok();
    sql("select stream ^tumble(rowtime, interval '2' hour)^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId")
        .fails("Group function '\\$TUMBLE' can only appear in GROUP BY clause");
    // TUMBLE with align argument
    sql("select stream\n"
        + "  tumble_end(rowtime, interval '2' hour, time '00:12:00') as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour, time '00:12:00')").ok();
    // TUMBLE_END without corresponding TUMBLE
    sql("select stream\n"
        + "  ^tumble_end(rowtime, interval '2' hour, time '00:13:00')^ as rowtime\n"
        + "from orders\n"
        + "group by floor(rowtime to hour)")
        .fails("Call to auxiliary group function 'TUMBLE_END' must have "
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
    // Arguments to TUMBLE_END are slightly different to arguments to TUMBLE
    sql("select stream\n"
        + "  ^tumble_start(rowtime, interval '2' hour, time '00:13:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour, time '00:12:00')")
        .fails("Call to auxiliary group function 'TUMBLE_START' must have "
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
    // Even though align defaults to TIME '00:00:00', we need structural
    // equivalence, not semantic equivalence.
    sql("select stream\n"
        + "  ^tumble_end(rowtime, interval '2' hour, time '00:00:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour)")
        .fails("Call to auxiliary group function 'TUMBLE_END' must have "
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
    // TUMBLE query produces no monotonic column - OK
    sql("select stream productId\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId").ok();
    sql("select stream productId\n"
        + "from orders\n"
        + "^group by productId,\n"
        + "  tumble(timestamp '1990-03-04 12:34:56', interval '2' hour)^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  @Test void testStreamHop() {
    // HOP
    sql("select stream\n"
        + "  hop_start(rowtime, interval '1' hour, interval '3' hour) as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)").ok();
    sql("select stream\n"
        + "  ^hop_start(rowtime, interval '1' hour, interval '2' hour)^,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)")
        .fails("Call to auxiliary group function 'HOP_START' must have "
            + "matching call to group function '\\$HOP' in GROUP BY clause");
    // HOP with align
    sql("select stream\n"
        + "  hop_start(rowtime, interval '1' hour, interval '3' hour,\n"
        + "    time '12:34:56') as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour,\n"
        + "    time '12:34:56')").ok();
  }

  @Test void testStreamSession() {
    // SESSION
    sql("select stream session_start(rowtime, interval '1' hour) as rowtime,\n"
        + "  session_end(rowtime, interval '1' hour),\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by session(rowtime, interval '1' hour)").ok();
  }

  @Test void testInsertExtendedColumn() {
    sql("insert into empdefaults(extra BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra, note) values (1, 10, '2', true, 'ok')").ok();
    sql("insert into emp(\"rank\" INT, extra BOOLEAN)"
        + " values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  1, false, 100, false)").ok();
  }

  @Test void testInsertBindExtendedColumn() {
    sql("insert into empdefaults(extra BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra, note) values (1, 10, '2', ?, 'ok')").ok();
    sql("insert into emp(\"rank\" INT, extra BOOLEAN)"
        + " values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  1, false, ?, ?)").ok();
  }

  @Test void testInsertExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (20, 10, '2', true, 'ok')";
    s.sql(sql0).ok();
    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"rank\" INT,"
        + " extra2 BOOLEAN)\n"
        + "values ('nom', 1, 'job', 20, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1, false)";
    s.sql(sql1).ok();
  }

  @Test void testInsertBindExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra2, note) values (20, 10, '2', true, ?)").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)"
        + " values ('nom', 1, 'job', 20, true, 0, false, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  ?, false)").ok();
  }

  @Test void testInsertExtendedColumnModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (^1^, 10, '2', true, 'ok')";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (^?^, 10, '2', true, 'ok')";
    s.sql(sql1).fails(error);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"rank\" INT,"
        + " extra2 BOOLEAN)\n"
        + "values ('nom', 1, 'job', ^0^, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1, false)";
    s.sql(sql2).fails(error);
  }

  @Test void testInsertExtendedColumnModifiableViewFailColumnCount() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into ^EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)^"
        + " values ('nom', 1, 'job', 0, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1)";
    final String error0 = "Number of INSERT target columns \\(12\\) does not"
        + " equal number of source items \\(11\\)";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into ^EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)^"
        + " (deptno, empno, ename, extra2, \"rank\") values (?, 10, '2', true)";
    final String error1 = "Number of INSERT target columns \\(5\\) does not"
        + " equal number of source items \\(4\\)";
    s.sql(sql1).fails(error1);
  }

  @Test void testInsertExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN)\n"
        + "values ('nom', 1, 'job', 0, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1)";
    final String error = "Duplicate name 'EXTCOL' in column list";
    s.sql(sql0).fails(error);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN) (extcol) values (1)";
    s.sql(sql1).fails(error);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN) (extcol) values (false)";
    s.sql(sql2).fails(error);

    final String sql3 = "insert into EMP(extcol INT, ^extcol^ BOOLEAN)"
        + " (extcol) values (1)";
    s.sql(sql3).fails(error);

    final String sql4 = "insert into EMP(extcol INT, ^extcol^ BOOLEAN)"
        + " (extcol) values (false)";
    s.sql(sql4).fails(error);
  }

  @Test void testUpdateExtendedColumn() {
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10").ok();
    sql("update empdefaults(extra BOOLEAN)"
        + " set extra = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
    sql("update empdefaults(\"empNo\" VARCHAR)"
        + " set \"empNo\" = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test void testInsertFailDataType() {
    sql("insert into empnullables ^values ('5', 'bob')^")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .withTypeCoercion(false)
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    sql("insert into empnullables values ('5', 'bob')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
    sql("insert into empnullables (^empno^, ename) values ('5', 'bob')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .withTypeCoercion(false)
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    sql("insert into empnullables (empno, ename) values ('5', 'bob')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
    sql("insert into empnullables(extra BOOLEAN)"
        + " (empno, ename, ^extra^) values (5, 'bob', 'true')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .withTypeCoercion(false)
        .fails("Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR\\$2' of type CHAR\\(4\\)");
    sql("insert into empnullables(extra BOOLEAN)"
        + " (empno, ename, ^extra^) values (5, 'bob', 'true')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
  }

  @Disabled("CALCITE-1727")
  @Test void testUpdateFailDataType() {
    sql("update emp"
        + " set ^empNo^ = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR$0' of type CHAR(1)");
    sql("update emp(extra boolean)"
        + " set ^extra^ = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR$0' of type CHAR(1)");
  }

  @Disabled("CALCITE-1727")
  @Test void testUpdateFailCaseSensitivity() {
    sql("update empdefaults"
        + " set empNo = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
  }

  @Test void testUpdateExtendedColumnFailCaseSensitivity() {
    sql("update empdefaults(\"extra\" BOOLEAN)"
        + " set ^extra^ = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Unknown target column 'EXTRA'");
    sql("update empdefaults(extra BOOLEAN)"
        + " set ^\"extra\"^ = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Unknown target column 'extra'");
  }

  @Test void testUpdateBindExtendedColumn() {
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = ?"
        + " where deptno = 10").ok();
    sql("update empdefaults(extra BOOLEAN)"
        + " set extra = ?, deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test void testUpdateExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " set deptno = 20, extra2 = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where ename = 'Jane'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = true, ename = 'Bob'"
        + " where ename = 'Jane'").ok();
  }

  @Test void testUpdateBindExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " set deptno = 20, extra2 = true, empno = 20, ename = 'Bob', note = ?"
        + " where ename = 'Jane'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = ?, ename = 'Bob'"
        + " where ename = 'Jane'").ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR)\n"
        + "set deptno = ^1^, extra2 = true, empno = 20, ename = 'Bob',"
        + " note = 'legion'\n"
        + "where ename = 'Jane'";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = true, deptno = ^1^, ename = 'Bob'"
        + " where ename = 'Jane'";
    s.sql(sql1).fails(error);
  }

  @Test void testUpdateExtendedColumnCollision() {
    sql("update empdefaults(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 1, empno = 20, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL,"
        + " deptno INTEGER)\n"
        + "set deptno = 20, empno = 20, ename = 'Bob'\n"
        + "where empno = 10").ok();
    s.sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL,"
        + " \"deptno\" BOOLEAN)\n"
        + "set \"deptno\" = true, empno = 20, ename = 'Bob'\n"
        + "where empno = 10").ok();
  }

  @Test void testUpdateExtendedColumnFailCollision() {
    final String sql = "update empdefaults(^empno^ BOOLEAN, deptno INTEGER)\n"
        + "set deptno = 1, empno = false, ename = 'Bob'\n"
        + "where deptno = 10";
    final String expected = "Cannot assign to target field 'EMPNO' of type "
        + "INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    sql(sql).fails(expected);
  }

  @Disabled("CALCITE-1727")
  @Test void testUpdateExtendedColumnFailCollision2() {
    final String sql = "update empdefaults(^\"deptno\"^ BOOLEAN)\n"
        + "set \"deptno\" = 1, empno = 1, ename = 'Bob'\n"
        + "where deptno = 10";
    final String expected = "Cannot assign to target field 'deptno' of type "
        + "BOOLEAN NOT NULL from source field 'deptno' of type INTEGER";
    sql(sql).fails(expected);
  }

  @Test void testUpdateExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "update EMP_MODIFIABLEVIEW3(^empno^ BOOLEAN,"
        + " deptno INTEGER)\n"
        + "set deptno = 1, empno = false, ename = 'Bob'\n"
        + "where deptno = 10";
    final String error = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    s.sql(sql).fails(error);
  }

  @Test void testUpdateExtendedColumnModifiableViewFailExtendedCollision() {
    final String error = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    final String sql = "update EMP_MODIFIABLEVIEW2(^extra^ INTEGER,"
        + " deptno INTEGER)\n"
        + "set deptno = 20, empno = 20, ename = 'Bob', extra = 5\n"
        + "where empno = 10";
    sql(sql).withExtendedCatalog().fails(error);
  }

  @Test void testUpdateExtendedColumnModifiableViewFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "update EMP_MODIFIABLEVIEW3(^comm^ BOOLEAN,"
        + " deptno INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = true\n"
        + "where deptno = 10";
    final String error = "Cannot assign to target field 'COMM' of type"
        + " INTEGER from source field 'COMM' of type BOOLEAN";
    s.sql(sql).fails(error);
  }

  @Test void testUpdateExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update emp(comm BOOLEAN, ^comm^ INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = 1\n"
        + "where deptno = 10";
    final String error = "Duplicate name 'COMM' in column list";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW3(comm BOOLEAN,"
        + " ^comm^ INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = true\n"
        + "where deptno = 10";
    s.sql(sql1).fails(error);
  }

  @Test void testInsertExtendedColumnCollision() {
    sql("insert into EMPDEFAULTS(^comm^ INTEGER) (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', 5)").ok();
  }

  @Test void testInsertExtendedColumnModifiableViewCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW3(^sal^ INTEGER)\n"
        + " (empno, ename, job, sal)\n"
        + "values (1, 'Arthur', 'clown', 5)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test void testInsertExtendedColumnModifiableViewExtendedCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(^extra^ BOOLEAN)"
        + " (empno, ename, job, extra)\n"
        + "values (1, 'Arthur', 'clown', true)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test void testInsertExtendedColumnModifiableViewUnderlyingCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW3(^comm^ INTEGER)\n"
        + " (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', 5)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test void testInsertExtendedColumnFailCollision() {
    sql("insert into EMPDEFAULTS(^comm^ BOOLEAN)"
        + " (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', true)")
        .fails("Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'COMM' of type BOOLEAN");
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^comm^)\n"
        + "values (1, 'Arthur', 'clown', true)")
        .withTypeCoercion(false)
        .fails("Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'EXPR\\$3' of type BOOLEAN");
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', true)")
        .ok();
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^\"comm\"^)\n"
        + "values (1, 'Arthur', 'clown', 1)")
        .withTypeCoercion(false)
        .fails("Cannot assign to target field 'comm' of type BOOLEAN"
            + " from source field 'EXPR\\$3' of type INTEGER");
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, \"comm\")\n"
        + "values (1, 'Arthur', 'clown', 1)")
        .ok();
  }

  @Test void testInsertExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(^slacker^ INTEGER)"
        + " (empno, ename, job, slacker) values (1, 'Arthur', 'clown', true)";
    final String error0 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER)"
        + " (empno, ename, job, ^slacker^) values (1, 'Arthur', 'clown', 1)";
    final String error1 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql1).withTypeCoercion(false).fails(error1);
    s.sql(sql1).ok();

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER)"
        + " (empno, ename, job, ^\"slacker\"^)\n"
        + "values (1, 'Arthur', 'clown', true)";
    final String error2 = "Cannot assign to target field 'slacker' of type"
        + " INTEGER from source field 'EXPR\\$3' of type BOOLEAN";
    s.sql(sql2).withTypeCoercion(false).fails(error2);
    s.sql(sql2).ok();
  }

  @Test void testInsertExtendedColumnModifiableViewFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(^extra^ INTEGER)"
        + " (empno, ename, job, extra) values (1, 'Arthur', 'clown', true)";
    final String error0 = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, ^extra^) values (1, 'Arthur', 'clown', 1)";
    final String error1 = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql1).withTypeCoercion(false).fails(error1);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, extra) values (1, 'Arthur', 'clown', 1)";
    s.sql(sql2).ok();

    final String sql3 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, ^\"extra\"^)\n"
        + "values (1, 'Arthur', 'clown', true)";
    final String error3 = "Cannot assign to target field 'extra' of type"
        + " INTEGER from source field 'EXPR\\$3' of type BOOLEAN";
    s.sql(sql3).withTypeCoercion(false).fails(error3);

    final String sql4 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, \"extra\")\n"
        + "values (1, 'Arthur', 'clown', true)";
    s.sql(sql4).ok();
  }

  @Test void testInsertExtendedColumnModifiableViewFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String error0 = "Cannot assign to target field 'COMM' of type"
        + " INTEGER from source field 'COMM' of type BOOLEAN";
    final String sql0 = "insert into EMP_MODIFIABLEVIEW3(^comm^ BOOLEAN)"
        + " (empno, ename, job, comm) values (1, 'Arthur', 'clown', true)";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW3(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^comm^) values (1, 'Arthur', 'clown', 5)";
    final String error1 = "Unknown target column 'COMM'";
    s.sql(sql1).fails(error1);


    final String sql2 = "insert into EMP_MODIFIABLEVIEW3(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^\"comm\"^) values (1, 'Arthur', 'clown', 1)";
    final String error2 = "Cannot assign to target field 'comm' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql2).withTypeCoercion(false).fails(error2);
    s.sql(sql2).ok();
  }

  @Test void testDelete() {
    sql("delete from empdefaults where deptno = 10").ok();
  }

  @Test void testDeleteExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = 10").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = false").ok();
  }

  @Test void testDeleteBindExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = ?").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = ?").ok();
  }

  @Test void testDeleteModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where deptno = 10").ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where deptno = 20").ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where empno = 30").ok();
  }

  @Test void testDeleteExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("delete from EMP_MODIFIABLEVIEW2(extra BOOLEAN) where sal > 10")
        .ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2(note BOOLEAN) where note = 'fired'")
        .ok();
  }

  @Test void testDeleteExtendedColumnCollision() {
    final String sql =
        "delete from emp(empno INTEGER NOT NULL) where sal > 10";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test void testDeleteExtendedColumnModifiableViewCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW2("
        + "empno INTEGER NOT NULL) where sal > 10";
    s.sql(sql0).ok();
    final String sql1 = "delete from EMP_MODIFIABLEVIEW2(\"empno\" INTEGER)\n"
        + "where sal > 10";
    s.sql(sql1).ok();
    final String sql2 = "delete from EMP_MODIFIABLEVIEW2(extra BOOLEAN)\n"
        + "where sal > 10";
    s.sql(sql2).ok();
    final String sql3 = "delete from EMP_MODIFIABLEVIEW2(\"extra\" VARCHAR)\n"
        + "where sal > 10";
    s.sql(sql3).ok();
    final String sql4 = "delete from EMP_MODIFIABLEVIEW3(comm INTEGER)\n"
        + "where sal > 10";
    s.sql(sql4).ok();
    final String sql5 = "delete from EMP_MODIFIABLEVIEW3(\"comm\" BIGINT)\n"
        + "where sal > 10";
    s.sql(sql5).ok();
  }

  @Test void testDeleteExtendedColumnFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW2(^empno^ BOOLEAN)\n"
        + "where sal > 10";
    final String error0 = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    s.sql(sql0).fails(error0);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW2(^empno^ INTEGER)\n"
        + "where sal > 10";
    final String error = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type INTEGER";
    s.sql(sql1).fails(error);
    final String sql2 = "delete from EMP_MODIFIABLEVIEW2(^\"EMPNO\"^ INTEGER)"
        + " where sal > 10";
    s.sql(sql2).fails(error);
    s.sql(sql1).fails(error);
  }

  @Test void testDeleteExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW(^deptno^ BOOLEAN)\n"
        + "where sal > 10";
    final String error = "Cannot assign to target field 'DEPTNO' of type"
        + " INTEGER from source field 'DEPTNO' of type BOOLEAN";
    s.sql(sql0).fails(error);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW(^\"DEPTNO\"^ BOOLEAN)"
        + " where sal > 10";
    s.sql(sql1).fails(error);
  }

  @Test void testDeleteExtendedColumnModifiableViewFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String error = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    final String sql0 = "delete from EMP_MODIFIABLEVIEW(^slacker^ INTEGER)\n"
        + "where sal > 10";
    s.sql(sql0).fails(error);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW(^\"SLACKER\"^ INTEGER)"
        + " where sal > 10";
    s.sql(sql1).fails(error);
  }

  @Test void testDeleteExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    sql("delete from emp (extra VARCHAR, ^extra^ VARCHAR)")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^extra^ VARCHAR)"
        + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^\"EXTRA\"^ VARCHAR)"
        + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1804">[CALCITE-1804]
   * Cannot assign NOT NULL array to nullable array</a>. */
  @Test void testArrayAssignment() {
    final SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType bigintNullable =
        typeFactory.createTypeWithNullability(bigint, true);
    final RelDataType bigintNotNull =
        typeFactory.createTypeWithNullability(bigint, false);
    final RelDataType date = typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType dateNotNull =
        typeFactory.createTypeWithNullability(date, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintNullable, bigintNotNull),
        is(true));
    assertThat(SqlTypeUtil.canAssignFrom(bigintNullable, dateNotNull),
        is(false));
    final RelDataType bigintNullableArray =
        typeFactory.createArrayType(bigintNullable, -1);
    final RelDataType bigintArrayNullable =
        typeFactory.createTypeWithNullability(bigintNullableArray, true);
    final RelDataType bigintNotNullArray =
        new ArraySqlType(bigintNotNull, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintArrayNullable, bigintNotNullArray),
        is(true));
    final RelDataType dateNotNullArray =
        new ArraySqlType(dateNotNull, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintArrayNullable, dateNotNullArray),
        is(false));
  }

  @Test void testSelectRolledUpColumn() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in SELECT";

    sql("select ^slackingmin^ from emp_r")
        .fails(error);

    sql("select a from (select ^slackingmin^ from emp_r)")
        .fails(error);

    sql("select ^slackingmin^ as b from emp_r")
        .fails(error);

    sql("select empno, ^slackingmin^ from emp_r")
        .fails(error);

    sql("select slackingmin from (select empno as slackingmin from emp_r)").ok();

    sql("select ^emp_r.slackingmin^ from emp_r")
        .fails(error);

    sql("select ^sales.emp_r.slackingmin^ from sales.emp_r")
        .fails(error);

    sql("select ^sales.emp_r.slackingmin^ from emp_r")
        .fails(error);

    sql("select ^catalog.sales.emp_r.slackingmin^ from emp_r")
        .fails(error);

    sql("select (select ^slackingmin^ from emp_r), a from (select empno as a from emp_r)")
        .fails(error);

    sql("select (((^slackingmin^))) from emp_r")
        .fails(error);

    sql("select ^slackingmin^ from nest.emp_r")
        .fails(error);

    sql("with emp_r as (select 1 as slackingmin) select slackingmin from emp_r")
        .ok();

    sql("with emp_r as (select ^slackingmin^ from emp_r) select slackingmin from emp_r")
        .fails(error);

    sql("with emp_r1 as (select 1 as slackingmin) select emp_r1.slackingmin from emp_r, emp_r1")
        .ok();

    sql("with emp_r1 as (select 1 as slackingmin) select ^emp_r.slackingmin^ from emp_r, emp_r1")
        .fails(error);
  }

  @Test void testSelectAggregateOnRolledUpColumn() {
    final String maxError = "Rolled up column 'SLACKINGMIN' is not allowed in MAX";
    final String plusError = "Rolled up column 'SLACKINGMIN' is not allowed in PLUS";

    sql("select max(^slackingmin^) from emp_r")
        .fails(maxError);

    sql("select count(slackingmin) from emp_r").ok();

    sql("select count(empno, deptno, slackingmin) from emp_r").ok();

    sql("select sum(slackingmin) from emp_r").ok();

    sql("select empno, min(slackingmin) from emp_r group by empno").ok();

    sql("select count(distinct slackingmin) from emp_r").ok();

    sql("select sum(empno + ^slackingmin^) from emp_r")
        .fails(plusError);

    sql("select max(^slackingmin^) over t as a "
        + "from emp_r window t as (partition by empno order by empno)")
        .fails(maxError);
  }

  @Test void testRolledUpColumnInWhere() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in GREATER_THAN";

    // Fire these slackers!!
    sql("select empno from emp_r where slacker and ^slackingmin^ > 60")
        .fails(error);

    sql("select sum(slackingmin) filter (where slacker and ^slackingmin^ > 60) from emp_r")
        .fails(error);
  }

  @Test void testRolledUpColumnInHaving() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in SUM";

    sql("select deptno, sum(slackingmin) from emp_r group "
        + "by deptno having sum(^slackingmin^) > 1000")
        .fails(error);
  }

  @Test void testRollUpInWindow() {
    final String partitionError = "Rolled up column 'SLACKINGMIN' is not allowed in PARTITION BY";
    final String orderByError = "Rolled up column 'SLACKINGMIN' is not allowed in ORDER BY";

    sql("select empno, sum(slackingmin) over (partition by ^slackingmin^) from emp_r")
        .fails(partitionError);

    sql("select empno, sum(slackingmin) over (partition by empno, ^slackingmin^) from emp_r")
        .fails(partitionError);

    sql("select empno, sum(slackingmin) over (partition by empno order by ^slackingmin^) "
        + "from emp_r")
        .fails(orderByError);

    sql("select empno, sum(slackingmin) over slackingmin "
        + "from emp_r window slackingmin as (partition by ^slackingmin^)")
        .fails(partitionError);

    sql("select sum(slackingmin) over t "
        + "from emp_r window t as (partition by empno order by ^slackingmin^, empno)")
        .fails(orderByError);

    sql("select sum(slackingmin) over t as a "
        + "from emp_r window t as (partition by empno order by ^slackingmin^, empno)")
        .fails(orderByError);
  }

  @Test void testRollUpInGroupBy() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in GROUP BY";

    sql("select empno, count(distinct empno) from emp_r group by empno, ^slackingmin^")
        .fails(error);

    sql("select empno from emp_r group by grouping sets (empno, ^slackingmin^)")
        .fails(error);
  }

  @Test void testRollUpInOrderBy() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in ORDER BY";

    sql("select empno from emp_r order by ^slackingmin^ asc")
        .fails(error);

    sql("select slackingmin from (select empno as slackingmin from emp_r)\n"
        + "order by slackingmin")
        .ok();

    sql("select empno, sum(slackingmin) from emp_r group by empno\n"
        + "order by sum(slackingmin)").ok();
  }

  @Test void testRollUpInJoin() {
    final String onError = "Rolled up column 'SLACKINGMIN' is not allowed in ON";
    final String usingError = "Rolled up column 'SLACKINGMIN' is not allowed in USING";
    final String selectError = "Rolled up column 'SLACKINGMIN' is not allowed in SELECT";

    sql("select * from (select deptno, ^slackingmin^ from emp_r)\n"
        + " join dept using (deptno)")
        .fails(selectError);

    sql("select * from dept as a\n"
        + "join (select deptno, ^slackingmin^ from emp_r) using (deptno)")
        .fails(selectError);

    sql("select * from emp_r as a\n"
        + "join dept_r as b using (deptno, ^slackingmin^)")
        .fails(usingError);

    // Even though the emp_r.slackingmin column will be under the SqlNode for '=',
    // The error should say it happened in 'ON' instead
    sql("select * from emp_r\n"
        + "join dept_r on (^emp_r.slackingmin^ = dept_r.slackingmin)")
        .fails(onError);
  }

  @Test void testJsonValueExpressionOperator() {
    expr("'{}' format json").ok();
    expr("'{}' format json encoding utf8").ok();
    expr("'{}' format json encoding utf16").ok();
    expr("'{}' format json encoding utf32").ok();
    expr("'{}' format json")
        .columnType("ANY NOT NULL");
    expr("'null' format json")
        .columnType("ANY NOT NULL");
    expr("cast(null as varchar) format json")
        .columnType("ANY");
    expr("null format json")
        .columnType("ANY");
    expr("^null^ format json")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
  }

  @Test void testJsonExists() {
    expr("json_exists('{}', 'lax $')").ok();
    expr("json_exists('{}', 'lax $')")
        .columnType("BOOLEAN");
  }

  @Test void testJsonValue() {
    expr("json_value('{\"foo\":\"bar\"}', 'lax $.foo')").ok();
    expr("json_value('{\"foo\":\"bar\"}', 123)").ok();
    expr("json_value('{\"foo\":\"bar\"}', 'lax $.foo')")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo')")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer)")
        .columnType("INTEGER");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer default 0 on empty default 0 on error)")
        .columnType("INTEGER");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer default null on empty default null on error)")
        .columnType("INTEGER");

    expr("json_value('{\"foo\":true}', 'lax $.foo'"
        + "returning boolean default 100 on empty default 100 on error)")
        .columnType("BOOLEAN");

    // test type inference of default value
    expr("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on empty)")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo' returning boolean"
        + " default 100 on empty)")
        .columnType("BOOLEAN");
  }

  @Test void testJsonQuery() {
    expr("json_query('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_query('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' WITH WRAPPER)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY OBJECT ON EMPTY)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY ARRAY ON ERROR)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY OBJECT ON EMPTY "
        + "EMPTY ARRAY ON ERROR EMPTY ARRAY ON EMPTY NULL ON ERROR)")
        .columnType("VARCHAR(2000)");
  }

  @Test void testJsonArray() {
    expr("json_array()").ok();
    expr("json_array('foo', 'bar')").ok();
    expr("json_array('foo', 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonArrayAgg() {
    sql("select json_arrayagg(ename) from emp").ok();
    expr("json_arrayagg('foo')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonObject() {
    expr("json_object()").ok();
    expr("json_object('foo': 'bar')").ok();
    expr("json_object('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
    expr("^json_object(100: 'bar')^")
        .fails("(?s).*Expected a character type*");
  }

  @Test void testJsonPretty() {
    sql("select json_pretty(ename) from emp").ok();
    expr("json_pretty('{\"foo\":\"bar\"}')").ok();
    expr("json_pretty('{\"foo\":\"bar\"}')")
        .columnType("VARCHAR(2000)");
    sql("select json_pretty(^NULL^) from emp")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
    sql("select json_pretty(NULL) from emp").ok();

    if (!Bug.CALCITE_2869_FIXED) {
      // the case should throw an error but currently validation
      // is done during sql-to-rel process.
      //
      // see StandardConvertletTable.JsonOperatorValueExprConvertlet
      return;
    }
    sql("select json_pretty(^1^) from emp")
        .fails("(.*)JSON_VALUE_EXPRESSION(.*)");
  }

  @Test void testJsonStorageSize() {
    sql("select json_storage_size(ename) from emp").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
  }

  @Test void testJsonType() {
    sql("select json_type(ename) from emp").ok();
    expr("json_type('{\"foo\":\"bar\"}')").ok();
    expr("json_type('{\"foo\":\"bar\"}')")
        .columnType("VARCHAR(20)");

    if (!Bug.CALCITE_2869_FIXED) {
      return;
    }
    sql("select json_type(^1^) from emp")
        .fails("(.*)JSON_VALUE_EXPRESSION(.*)");
  }

  @Test void testJsonDepth() {
    sql("select json_depth(ename) from emp").ok();
    expr("json_depth('{\"foo\":\"bar\"}')").ok();
    expr("json_depth('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");

    if (!Bug.CALCITE_2869_FIXED) {
      return;
    }
    sql("select json_depth(^1^) from emp")
        .fails("(.*)JSON_VALUE_EXPRESSION(.*)");
  }

  @Test void testJsonLength() {
    expr("json_length('{\"foo\":\"bar\"}')").ok();
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_length('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("INTEGER");
  }

  @Test void testJsonKeys() {
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("VARCHAR(2000)");
    expr("json_keys('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("VARCHAR(2000)");
  }

  @Test void testJsonRemove() {
    expr("json_remove('{\"foo\":\"bar\"}', '$')").ok();
    expr("json_remove('{\"foo\":\"bar\"}', '$')")
        .columnType("VARCHAR(2000)");
    expr("json_remove('{\"foo\":\"bar\"}', 1, '2', 3)")
        .columnType("VARCHAR(2000)");
    expr("json_remove('{\"foo\":\"bar\"}', 1, 2, 3)")
        .columnType("VARCHAR(2000)");
    sql("select ^json_remove('{\"foo\":\"bar\"}')^")
        .fails("(?s).*Invalid number of arguments.*");
  }

  @Test void testJsonObjectAgg() {
    sql("select json_objectagg(ename: empno) from emp").ok();
    sql("select json_objectagg(empno: ename) from emp").ok();
    sql("select ^json_objectagg(empno: ename)^ from emp")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    expr("json_objectagg('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonPredicate() {
    expr("'{}' is json")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is json value")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is json object")
        .columnType("BOOLEAN NOT NULL");
    expr("'[]' is json array")
        .columnType("BOOLEAN NOT NULL");
    expr("'100' is json scalar")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json value")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json object")
        .columnType("BOOLEAN NOT NULL");
    expr("'[]' is not json array")
        .columnType("BOOLEAN NOT NULL");
    expr("'100' is not json scalar")
        .columnType("BOOLEAN NOT NULL");
    expr("100 is json value")
        .columnType("BOOLEAN NOT NULL");
    expr("^100 is json value^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
  }

  @Test void testRegexpReplace() {
    final SqlOperatorTable opTable = operatorTableFor(SqlLibrary.ORACLE);

    expr("REGEXP_REPLACE('a b c', 'a', 'X')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2)")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3)")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def GHI', '[a-z]+', 'X', 1, 3, 'c')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    // Implicit type coercion.
    expr("REGEXP_REPLACE(null, '(-)', '###')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('100-200', null, '###')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('100-200', '(-)', null)")
        .withOperatorTable(opTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '2')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '1', '3')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
    // The last argument to REGEXP_REPLACE should be specific character, but with
    // implicit type coercion, the validation still passes.
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '1', '3', '1')")
        .withOperatorTable(opTable)
        .columnType("VARCHAR NOT NULL");
  }

  @Test void testInvalidFunctionCall() {
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);

    // With implicit type coercion.
    expr("^unknown_udf(1, 2)^")
        .fails("(?s).*No match found for function signature "
            + "UNKNOWN_UDF\\(<NUMERIC>, <NUMERIC>\\).*");
    expr("^power(cast(1 as timestamp), cast(2 as timestamp))^")
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER\\(<TIMESTAMP\\(0\\)>, <TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(cast('124' as timestamp))^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'MYFUN' to arguments of type "
            + "'MYFUN\\(<TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(1, 2)^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "MYFUN\\(<NUMERIC>, <NUMERIC>\\).*");

    // Without implicit type coercion.
    expr("^unknown_udf(1, 2)^")
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "UNKNOWN_UDF\\(<NUMERIC>, <NUMERIC>\\).*");
    expr("^power(cast(1 as timestamp), cast(2 as timestamp))^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER\\(<TIMESTAMP\\(0\\)>, <TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(cast('124' as timestamp))^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'MYFUN' to arguments of type "
            + "'MYFUN\\(<TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(1, 2)^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "MYFUN\\(<NUMERIC>, <NUMERIC>\\).*");
  }

  @Test void testPositionalAggregateWithExpandedCurrentDateFunction() {
    SqlConformance defaultPlusOrdinalGroupBy =
        new SqlDelegatingConformance(SqlConformanceEnum.DEFAULT) {
          @Override public boolean isGroupByOrdinal() {
            return true;
          }
        };
    sql("SELECT HIREDATE >= CURRENT_DATE, COUNT(*) "
        + "FROM EMP GROUP BY 1")
        .withConformance(defaultPlusOrdinalGroupBy)
        .ok();
  }

  @Test void testValidatorReportsOriginalQueryUsingReader()
      throws Exception {
    final String sql = "select a from b";
    final SqlParser.Config config = SqlParser.config();
    final String messagePassingSqlString;
    try {
      final SqlParser sqlParserReader = SqlParser.create(sql, config);
      final SqlNode node = sqlParserReader.parseQuery();
      final SqlValidator validator = tester.getValidator();
      final SqlNode x = validator.validate(node);
      fail("expecting an error, got " + x);
      return;
    } catch (CalciteContextException error) {
      // we want the exception to report column and line
      assertThat(error.getMessage(),
          is("At line 1, column 15: Object 'B' not found"));
      messagePassingSqlString = error.getMessage();
      // the error does not contain the original query (even using a String)
      assertNull(error.getOriginalStatement());
    }

    // parse SQL text using a java.io.Reader and not a java.lang.String
    try {
      final SqlParser sqlParserReader =
          SqlParser.create(new StringReader(sql), config);
      final SqlNode node = sqlParserReader.parseQuery();
      final SqlValidator validator = tester.getValidator();
      final SqlNode x = validator.validate(node);
      fail("expecting an error, got " + x);
    } catch (CalciteContextException error) {
      // we want exactly the same error as with java.lang.String input
      assertThat(error.getMessage(), is(messagePassingSqlString));
      // the error does not contain the original query (using a Reader)
      assertThat(error.getOriginalStatement(), nullValue());
    }
  }

  @Test void testValidateParameterizedExpression() throws SqlParseException {
    final SqlParser.Config config = SqlParser.config();
    final SqlValidator validator = tester.getValidator();
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intTypeNull = typeFactory.createTypeWithNullability(intType, true);
    final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
    nameToTypeMap.put("A", intType);
    nameToTypeMap.put("B", intTypeNull);
    final String expr = "a + b";
    final SqlParser parser = SqlParser.create(expr, config);
    final SqlNode sqlNode = parser.parseExpression();
    final SqlNode validated = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    final RelDataType resultType = validator.getValidatedNodeType(validated);
    assertThat(resultType.toString(), is("INTEGER"));
  }

  @Test void testAccessingNestedFieldsOfNullableRecord() {
    sql("select ROW_COLUMN_ARRAY[0].NOT_NULL_FIELD from NULLABLEROWS.NR_T1")
        .withExtendedCatalog()
        .type("RecordType(BIGINT EXPR$0) NOT NULL");
    sql("select ROW_COLUMN_ARRAY[0]['NOT_NULL_FIELD'] from NULLABLEROWS.NR_T1")
        .withExtendedCatalog()
        .type("RecordType(BIGINT EXPR$0) NOT NULL");

    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);
    sql("select * FROM TABLE(ROW_FUNC()) AS T(a, b)")
        .withOperatorTable(operatorTable)
        .type("RecordType(BIGINT NOT NULL A, BIGINT B) NOT NULL");
  }
}
