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
package org.apache.calcite.sql.test;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.advise.SqlSimpleParser;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.test.SqlValidatorFixture;
import org.apache.calcite.test.SqlValidatorTestCase;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing unit tests
 * for SqlAdvisor.
 */
@SuppressWarnings({"unchecked", "ArraysAsListWithZeroOrOneArgument"})
class SqlAdvisorTest extends SqlValidatorTestCase {
  public static final SqlTestFactory ADVISOR_NEW_TEST_FACTORY =
      SqlTestFactory.INSTANCE.withValidator(SqlAdvisorValidator::new);

  static final Fixture LOCAL_FIXTURE =
      new Fixture(SqlValidatorTester.DEFAULT, ADVISOR_NEW_TEST_FACTORY,
          StringAndPos.of("?"), false, false);

  private static final List<String> STAR_KEYWORD =
      Collections.singletonList(
          "KEYWORD(*)");

  protected static final List<String> FROM_KEYWORDS =
      Arrays.asList(
          "KEYWORD(()",
          "KEYWORD(LATERAL)",
          "KEYWORD(TABLE)",
          "KEYWORD(UNNEST)");

  protected static final List<String> SALES_TABLES =
      Arrays.asList(
          "SCHEMA(CATALOG.SALES)",
          "SCHEMA(CATALOG.SALES.NEST)",
          "TABLE(CATALOG.SALES.EMP)",
          "TABLE(CATALOG.SALES.EMPDEFAULTS)",
          "TABLE(CATALOG.SALES.EMPNULLABLES)",
          "TABLE(CATALOG.SALES.EMP_B)",
          "TABLE(CATALOG.SALES.EMP_20)",
          "TABLE(CATALOG.SALES.EMPNULLABLES_20)",
          "TABLE(CATALOG.SALES.EMP_ADDRESS)",
          "TABLE(CATALOG.SALES.DEPT)",
          "TABLE(CATALOG.SALES.DEPT_NESTED)",
          "TABLE(CATALOG.SALES.DEPT_NESTED_EXPANDED)",
          "TABLE(CATALOG.SALES.BONUS)",
          "TABLE(CATALOG.SALES.ORDERS)",
          "TABLE(CATALOG.SALES.SALGRADE)",
          "TABLE(CATALOG.SALES.SHIPMENTS)",
          "TABLE(CATALOG.SALES.PRODUCTS)",
          "TABLE(CATALOG.SALES.PRODUCTS_TEMPORAL)",
          "TABLE(CATALOG.SALES.SUPPLIERS)",
          "TABLE(CATALOG.SALES.EMP_R)",
          "TABLE(CATALOG.SALES.DEPT_R)");

  private static final List<String> SCHEMAS =
      Arrays.asList(
          "CATALOG(CATALOG)",
          "SCHEMA(CATALOG.SALES)",
          "SCHEMA(CATALOG.STRUCT)",
          "SCHEMA(CATALOG.CUSTOMER)",
          "SCHEMA(CATALOG.SALES.NEST)");

  private static final List<String> AB_TABLES =
      Arrays.asList(
          "TABLE(A)",
          "TABLE(B)");

  private static final List<String> EMP_TABLE =
      Collections.singletonList(
          "TABLE(EMP)");

  protected static final List<String> FETCH_OFFSET =
      Arrays.asList(
          "KEYWORD(FETCH)",
          "KEYWORD(LIMIT)",
          "KEYWORD(OFFSET)");

  protected static final List<String> EXPR_KEYWORDS =
      Arrays.asList(
          "KEYWORD(()",
          "KEYWORD(+)",
          "KEYWORD(-)",
          "KEYWORD(?)",
          "KEYWORD(ABS)",
          "KEYWORD(ARRAY)",
          "KEYWORD(AVG)",
          "KEYWORD(CARDINALITY)",
          "KEYWORD(CASE)",
          "KEYWORD(CAST)",
          "KEYWORD(CEIL)",
          "KEYWORD(CEILING)",
          "KEYWORD(CHAR)",
          "KEYWORD(CHARACTER_LENGTH)",
          "KEYWORD(CHAR_LENGTH)",
          "KEYWORD(CLASSIFIER)",
          "KEYWORD(COALESCE)",
          "KEYWORD(COLLECT)",
          "KEYWORD(CONVERT)",
          "KEYWORD(COUNT)",
          "KEYWORD(COVAR_POP)",
          "KEYWORD(COVAR_SAMP)",
          "KEYWORD(CUME_DIST)",
          "KEYWORD(CURRENT)",
          "KEYWORD(CURRENT_CATALOG)",
          "KEYWORD(CURRENT_DATE)",
          "KEYWORD(CURRENT_DEFAULT_TRANSFORM_GROUP)",
          "KEYWORD(CURRENT_PATH)",
          "KEYWORD(CURRENT_ROLE)",
          "KEYWORD(CURRENT_SCHEMA)",
          "KEYWORD(CURRENT_TIME)",
          "KEYWORD(CURRENT_TIMESTAMP)",
          "KEYWORD(CURRENT_USER)",
          "KEYWORD(CURSOR)",
          "KEYWORD(DATE)",
          "KEYWORD(DENSE_RANK)",
          "KEYWORD(ELEMENT)",
          "KEYWORD(EVERY)",
          "KEYWORD(EXISTS)",
          "KEYWORD(EXP)",
          "KEYWORD(EXTRACT)",
          "KEYWORD(FALSE)",
          "KEYWORD(FIRST_VALUE)",
          "KEYWORD(FLOOR)",
          "KEYWORD(FUSION)",
          "KEYWORD(GROUPING)",
          "KEYWORD(HOUR)",
          "KEYWORD(INTERSECTION)",
          "KEYWORD(INTERVAL)",
          "KEYWORD(JSON_ARRAY)",
          "KEYWORD(JSON_ARRAYAGG)",
          "KEYWORD(JSON_EXISTS)",
          "KEYWORD(JSON_OBJECT)",
          "KEYWORD(JSON_OBJECTAGG)",
          "KEYWORD(JSON_QUERY)",
          "KEYWORD(JSON_VALUE)",
          "KEYWORD(LAG)",
          "KEYWORD(LAST_VALUE)",
          "KEYWORD(LEAD)",
          "KEYWORD(LEFT)",
          "KEYWORD(LN)",
          "KEYWORD(LOCALTIME)",
          "KEYWORD(LOCALTIMESTAMP)",
          "KEYWORD(LOWER)",
          "KEYWORD(MATCH_NUMBER)",
          "KEYWORD(MAX)",
          "KEYWORD(MIN)",
          "KEYWORD(MINUTE)",
          "KEYWORD(MOD)",
          "KEYWORD(MONTH)",
          "KEYWORD(MULTISET)",
          "KEYWORD(NEW)",
          "KEYWORD(NEXT)",
          "KEYWORD(NOT)",
          "KEYWORD(NTH_VALUE)",
          "KEYWORD(NTILE)",
          "KEYWORD(NULL)",
          "KEYWORD(NULLIF)",
          "KEYWORD(OCTET_LENGTH)",
          "KEYWORD(OVERLAY)",
          "KEYWORD(PERCENTILE_CONT)",
          "KEYWORD(PERCENTILE_DISC)",
          "KEYWORD(PERCENT_RANK)",
          "KEYWORD(PERIOD)",
          "KEYWORD(POSITION)",
          "KEYWORD(POWER)",
          "KEYWORD(PREV)",
          "KEYWORD(RANK)",
          "KEYWORD(REGR_COUNT)",
          "KEYWORD(REGR_SXX)",
          "KEYWORD(REGR_SYY)",
          "KEYWORD(RIGHT)",
          "KEYWORD(ROW)",
          "KEYWORD(ROW_NUMBER)",
          "KEYWORD(RUNNING)",
          "KEYWORD(SECOND)",
          "KEYWORD(SESSION_USER)",
          "KEYWORD(SOME)",
          "KEYWORD(SPECIFIC)",
          "KEYWORD(SQRT)",
          "KEYWORD(SUBSTRING)",
          "KEYWORD(STDDEV_POP)",
          "KEYWORD(STDDEV_SAMP)",
          "KEYWORD(SUM)",
          "KEYWORD(SYSTEM_USER)",
          "KEYWORD(TIME)",
          "KEYWORD(TIMESTAMP)",
          "KEYWORD(TRANSLATE)",
          "KEYWORD(TRIM)",
          "KEYWORD(TRUE)",
          "KEYWORD(TRUNCATE)",
          "KEYWORD(UNIQUE)",
          "KEYWORD(UNKNOWN)",
          "KEYWORD(UPPER)",
          "KEYWORD(USER)",
          "KEYWORD(VAR_POP)",
          "KEYWORD(VAR_SAMP)",
          "KEYWORD(YEAR)");

  protected static final List<String> QUANTIFIERS =
      Arrays.asList(
          "KEYWORD(ALL)",
          "KEYWORD(ANY)",
          "KEYWORD(SOME)");

  protected static final List<String> SELECT_KEYWORDS =
      Arrays.asList(
          "KEYWORD(ALL)",
          "KEYWORD(DISTINCT)",
          "KEYWORD(STREAM)",
          "KEYWORD(*)",
          "KEYWORD(/*+)");

  private static final List<String> ORDER_KEYWORDS =
      Arrays.asList(
          "KEYWORD(,)",
          "KEYWORD(ASC)",
          "KEYWORD(DESC)",
          "KEYWORD(NULLS)");

  private static final List<String> EMP_COLUMNS =
      Arrays.asList(
          "COLUMN(EMPNO)",
          "COLUMN(ENAME)",
          "COLUMN(JOB)",
          "COLUMN(MGR)",
          "COLUMN(HIREDATE)",
          "COLUMN(SAL)",
          "COLUMN(COMM)",
          "COLUMN(DEPTNO)",
          "COLUMN(SLACKER)");

  private static final List<String> EMP_COLUMNS_E =
      Arrays.asList(
          "COLUMN(EMPNO)",
          "COLUMN(ENAME)");

  private static final List<String> DEPT_COLUMNS =
      Arrays.asList(
          "COLUMN(DEPTNO)",
          "COLUMN(NAME)");

  protected static final List<String> PREDICATE_KEYWORDS =
      Arrays.asList(
          "KEYWORD(()",
          "KEYWORD(*)",
          "KEYWORD(+)",
          "KEYWORD(-)",
          "KEYWORD(.)",
          "KEYWORD(/)",
          "KEYWORD(%)",
          "KEYWORD(<)",
          "KEYWORD(<=)",
          "KEYWORD(<>)",
          "KEYWORD(!=)",
          "KEYWORD(=)",
          "KEYWORD(>)",
          "KEYWORD(>=)",
          "KEYWORD(AND)",
          "KEYWORD(BETWEEN)",
          "KEYWORD(CONTAINS)",
          "KEYWORD(EQUALS)",
          "KEYWORD(FORMAT)",
          "KEYWORD(ILIKE)",
          "KEYWORD(RLIKE)",
          "KEYWORD(IMMEDIATELY)",
          "KEYWORD(IN)",
          "KEYWORD(IS)",
          "KEYWORD(LIKE)",
          "KEYWORD(MEMBER)",
          "KEYWORD(MULTISET)",
          "KEYWORD(NOT)",
          "KEYWORD(OR)",
          "KEYWORD(OVERLAPS)",
          "KEYWORD(PRECEDES)",
          "KEYWORD(SIMILAR)",
          "KEYWORD(SUBMULTISET)",
          "KEYWORD(SUCCEEDS)",
          "KEYWORD([)",
          "KEYWORD(||)");

  private static final List<String> WHERE_KEYWORDS =
      Arrays.asList(
          "KEYWORD(EXCEPT)",
          "KEYWORD(MINUS)",
          "KEYWORD(FETCH)",
          "KEYWORD(OFFSET)",
          "KEYWORD(LIMIT)",
          "KEYWORD(GROUP)",
          "KEYWORD(HAVING)",
          "KEYWORD(INTERSECT)",
          "KEYWORD(ORDER)",
          "KEYWORD(UNION)",
          "KEYWORD(WINDOW)");

  private static final List<String> A_TABLE =
      Collections.singletonList(
          "TABLE(A)");

  protected static final List<String> JOIN_KEYWORDS =
      Arrays.asList(
          "KEYWORD(FETCH)",
          "KEYWORD(FOR)",
          "KEYWORD(OFFSET)",
          "KEYWORD(LIMIT)",
          "KEYWORD(UNION)",
          "KEYWORD(FULL)",
          "KEYWORD(ORDER)",
          "KEYWORD(()",
          "KEYWORD(EXTEND)",
          "KEYWORD(/*+)",
          "KEYWORD(AS)",
          "KEYWORD(USING)",
          "KEYWORD(OUTER)",
          "KEYWORD(RIGHT)",
          "KEYWORD(GROUP)",
          "KEYWORD(CROSS)",
          "KEYWORD(,)",
          "KEYWORD(NATURAL)",
          "KEYWORD(INNER)",
          "KEYWORD(HAVING)",
          "KEYWORD(LEFT)",
          "KEYWORD(EXCEPT)",
          "KEYWORD(MATCH_RECOGNIZE)",
          "KEYWORD(MINUS)",
          "KEYWORD(JOIN)",
          "KEYWORD(WINDOW)",
          "KEYWORD(.)",
          "KEYWORD(TABLESAMPLE)",
          "KEYWORD(ON)",
          "KEYWORD(INTERSECT)",
          "KEYWORD(WHERE)");

  private static final List<String> SETOPS =
      Arrays.asList(
          "KEYWORD(EXCEPT)",
          "KEYWORD(MINUS)",
          "KEYWORD(INTERSECT)",
          "KEYWORD(ORDER)",
          "KEYWORD(UNION)");

  private static final String EMPNO_EMP =
      "COLUMN(EMPNO)\n"
          + "TABLE(EMP)\n";

  @Override public Fixture fixture() {
    return LOCAL_FIXTURE;
  }

  protected List<String> getFromKeywords() {
    return FROM_KEYWORDS;
  }

  protected List<String> getSelectKeywords() {
    return SELECT_KEYWORDS;
  }

  /**
   * Returns a list of the tables in the SALES schema. Derived classes with
   * extended SALES schemas may override.
   *
   * @return list of tables in the SALES schema
   */
  protected List<String> getSalesTables() {
    return SALES_TABLES;
  }

  protected List<String> getJoinKeywords() {
    return JOIN_KEYWORDS;
  }

  @Test void testFrom() {
    final Fixture f = fixture();

    String sql = "select a.empno, b.deptno from ^dummy a, sales.dummy b";
    f.withSql(sql)
        .assertHint(SCHEMAS, getSalesTables(), getFromKeywords()); // join

    sql = "select a.empno, b.deptno from ^";
    f.withSql(sql).assertComplete(SCHEMAS, getSalesTables(), getFromKeywords());
    sql = "select a.empno, b.deptno from ^, sales.dummy b";
    f.withSql(sql).assertComplete(SCHEMAS, getSalesTables(), getFromKeywords());
    sql = "select a.empno, b.deptno from ^a";
    f.withSql(sql).assertComplete(SCHEMAS, getSalesTables(), getFromKeywords());

    sql = "select a.empno, b.deptno from dummy a, ^sales.dummy b";
    f.withSql(sql)
        .assertHint(SCHEMAS, getSalesTables(), getFromKeywords()); // join
  }

  @Test void testFromComplete() {
    String sql = "select a.empno, b.deptno from dummy a, sales.^";
    fixture().withSql(sql).assertComplete(getSalesTables());
  }

  @Test void testGroup() {
    // This test is hard because the statement is not valid if you replace
    // '^' with a dummy identifier.
    String sql = "select a.empno, b.deptno from emp group ^";
    fixture().withSql(sql).assertComplete(Arrays.asList("KEYWORD(BY)"));
  }

  @Test void testJoin() {
    final Fixture f = fixture();
    String sql;

    // from
    sql =
        "select a.empno, b.deptno from ^dummy a join sales.dummy b "
            + "on a.deptno=b.deptno where empno=1";
    f.withSql(sql).assertHint(getFromKeywords(), SCHEMAS, getSalesTables());

    // from
    sql = "select a.empno, b.deptno from ^ a join sales.dummy b";
    f.withSql(sql).assertComplete(getFromKeywords(), SCHEMAS, getSalesTables());

    // REVIEW: because caret is before 'sales', should it ignore schema
    // name and present all schemas and all tables in the default schema?
    // join
    sql =
        "select a.empno, b.deptno from dummy a join ^sales.dummy b "
            + "on a.deptno=b.deptno where empno=1";
    f.withSql(sql).assertHint(getFromKeywords(), SCHEMAS, getSalesTables());

    sql = "select a.empno, b.deptno from dummy a join sales.^";
    f.withSql(sql).assertComplete(getSalesTables()); // join
    sql = "select a.empno, b.deptno from dummy a join sales.^ on";
    f.withSql(sql).assertComplete(getSalesTables()); // join

    // unfortunately cannot complete this case: syntax is too broken
    sql = "select a.empno, b.deptno from dummy a join sales.^ on a.deptno=";
    f.withSql(sql).assertComplete(QUANTIFIERS, EXPR_KEYWORDS); // join
  }

  @Test void testJoinKeywords() {
    // variety of keywords possible
    List<String> list = getJoinKeywords();
    String sql = "select * from dummy join sales.emp ^";
    fixture().withSql(sql)
        .assertSimplify("SELECT * FROM dummy JOIN sales.emp _suggest_")
        .assertComplete(list);
  }

  @Test void testSimplifyStarAlias() {
    String sql = "select ax^ from (select * from dummy a)";
    fixture().withSql(sql)
        .assertSimplify("SELECT ax _suggest_ FROM ( SELECT * FROM dummy a )");
  }

  @Test void testSimplifySubQueryStar() {
    final Fixture f = fixture();
    String sql;

    sql = "select ax^ from (select (select * from dummy) axc from dummy a)";
    f.withSql(sql)
        .assertSimplify("SELECT ax _suggest_ FROM ("
            + " SELECT ( SELECT * FROM dummy ) axc FROM dummy a )")
        .assertComplete("COLUMN(AXC)\n", "ax");

    sql = "select ax^ from (select a.x+0 axa, b.x axb,"
        + " (select * from dummy) axbc from dummy a, dummy b)";
    f.withSql(sql)
        .assertSimplify("SELECT ax _suggest_ FROM ( SELECT a.x+0 axa , b.x axb ,"
            + " ( SELECT * FROM dummy ) axbc FROM dummy a , dummy b )")
        .assertComplete("COLUMN(AXA)\nCOLUMN(AXB)\nCOLUMN(AXBC)\n", "ax");

    sql = "select ^ from (select * from dummy)";
    f.withSql(sql)
        .assertSimplify("SELECT _suggest_ FROM ( SELECT * FROM dummy )");

    sql = "select ^ from (select x.* from dummy x)";
    f.withSql(sql)
        .assertSimplify("SELECT _suggest_ FROM ( SELECT x.* FROM dummy x )");

    sql = "select ^ from (select a.x + b.y from dummy a, dummy b)";
    f.withSql(sql)
        .assertSimplify("SELECT _suggest_ FROM ( "
            + "SELECT a.x + b.y FROM dummy a , dummy b )");
  }

  @Test void testSimplifySubQueryMultipleFrom() {
    final Fixture f = fixture();
    String sql;

    // "dummy b" should be removed
    sql = "select axc\n"
        + "from (select (select ^ from dummy) axc from dummy a), dummy b";
    f.withSql(sql)
        .assertSimplify("SELECT * FROM ("
            + " SELECT ( SELECT _suggest_ FROM dummy ) axc FROM dummy a )");

    // "dummy b" should be removed
    sql = "select axc\n"
        + "from dummy b, (select (select ^ from dummy) axc from dummy a)";
    f.withSql(sql)
        .assertSimplify("SELECT * FROM ("
            + " SELECT ( SELECT _suggest_ FROM dummy ) axc FROM dummy a )");
  }

  @Test void testSimplifyMinus() {
    final Fixture f = fixture();
    String sql;

    sql = "select ^ from dummy a minus select * from dummy b";
    f.withSql(sql).assertSimplify("SELECT _suggest_ FROM dummy a");

    sql = "select * from dummy a minus select ^ from dummy b";
    f.withSql(sql).assertSimplify("SELECT _suggest_ FROM dummy b");
  }

  @Test void testOnCondition() {
    final Fixture f = fixture();
    String sql;

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on ^a.deptno=b.dummy where empno=1";
    f.withSql(sql).assertHint(AB_TABLES, EXPR_KEYWORDS); // on left

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.^";
    f.withSql(sql).assertComplete(EMP_COLUMNS); // on left

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=^b.dummy where empno=1";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, QUANTIFIERS, AB_TABLES); // on right

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.^ where empno=1";
    f.withSql(sql).assertComplete(DEPT_COLUMNS); // on right

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.^";
    f.withSql(sql).assertComplete(DEPT_COLUMNS); // on right
  }

  @Test void testFromWhere() {
    final Fixture f = fixture();
    String sql;

    sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
        + "where b.deptno=^a.dummy";
    f.withSql(sql)
        .assertHint(AB_TABLES, EXPR_KEYWORDS, QUANTIFIERS); // where list

    sql = "select a.empno, b.deptno from sales.emp a, sales.dept b\n"
        + "where b.deptno=a.^";
    f.withSql(sql)
        .assertComplete(ImmutableMap.of("COLUMN(COMM)", "COMM"),
            EMP_COLUMNS); // where list

    sql =
        "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where b.deptno=a.e^";
    f.withSql(sql)
        .assertComplete(ImmutableMap.of("COLUMN(ENAME)", "ename"),
            EMP_COLUMNS_E); // where list

    // hints contain no columns, only table aliases, because there are >1
    // aliases
    sql =
        "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^dummy=1";
    f.withSql(sql)
        .assertComplete(
            ImmutableMap.of("KEYWORD(CURRENT_TIMESTAMP)", "CURRENT_TIMESTAMP"),
            AB_TABLES, EXPR_KEYWORDS); // where list

    sql =
        "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^";
    f.withSql(sql)
        .assertComplete(AB_TABLES, EXPR_KEYWORDS); // where list

    // If there's only one table alias, we allow both the alias and the
    // unqualified columns
    sql = "select a.empno, a.deptno from sales.emp a "
        + "where ^";
    f.withSql(sql)
        .assertComplete(A_TABLE, EMP_COLUMNS, EXPR_KEYWORDS);
  }

  @Test void testWhereList() {
    final Fixture f = fixture();
    String sql;

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^dummy=1";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, AB_TABLES); // where list

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^";
    f.withSql(sql).assertComplete(EXPR_KEYWORDS, AB_TABLES); // where list

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^a.dummy=1";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, AB_TABLES); // where list

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where a.^";
    f.withSql(sql).assertComplete(EMP_COLUMNS);

    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where a.empno ^ ";
    f.withSql(sql).assertComplete(PREDICATE_KEYWORDS, WHERE_KEYWORDS);
  }

  @Test void testSelectList() {
    final Fixture f = fixture();
    String sql;

    sql =
        "select ^dummy, b.dummy from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where empno=1";
    f.withSql(sql).assertHint(getSelectKeywords(), EXPR_KEYWORDS, AB_TABLES);

    sql = "select ^ from (values (1))";
    f.withSql(sql)
        .assertComplete(getSelectKeywords(), EXPR_KEYWORDS,
            Arrays.asList("TABLE(EXPR$0)", "COLUMN(EXPR$0)"));

    sql = "select ^ from (values (1)) as t(c)";
    f.withSql(sql)
        .assertComplete(getSelectKeywords(), EXPR_KEYWORDS,
            Arrays.asList("TABLE(T)", "COLUMN(C)"));

    sql = "select ^, b.dummy from sales.emp a join sales.dept b ";
    f.withSql(sql)
        .assertComplete(getSelectKeywords(), EXPR_KEYWORDS, AB_TABLES);

    sql =
        "select dummy, ^b.dummy from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where empno=1";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, STAR_KEYWORD, AB_TABLES);

    sql = "select dummy, b.^ from sales.emp a join sales.dept b on true";
    f.withSql(sql).assertComplete(STAR_KEYWORD, DEPT_COLUMNS);

    // REVIEW: Since 'b' is not a valid alias, should it suggest anything?
    // We don't get through validation, so the only suggestion, '*', comes
    // from the parser.
    sql = "select dummy, b.^ from sales.emp a";
    f.withSql(sql).assertComplete(STAR_KEYWORD);

    sql = "select ^emp.dummy from sales.emp";
    f.withSql(sql)
        .assertHint(getSelectKeywords(),
            EXPR_KEYWORDS,
            EMP_COLUMNS,
            Arrays.asList("TABLE(EMP)"));

    // Suggest columns for a table name or table alias in the SELECT clause.
    final Consumer<String> c = sql_ ->
        f.withSql(sql_).assertComplete(EMP_COLUMNS, STAR_KEYWORD);
    c.accept("select emp.^ from sales.emp");
    c.accept("select emp.^ from sales.emp as emp");
    c.accept("select emp.^ from sales.emp emp");
    c.accept("select e.^ from sales.emp as e");
    c.accept("select e.^ from sales.emp e");
    c.accept("select e.^ from sales.emp e, sales.dept d");
    c.accept("select e.^ from sales.emp e cross join sales.dept d");
    c.accept("select e.^ from sales.emp e where deptno = 20");
    c.accept("select e.^ from sales.emp e order by deptno");
  }

  @Test void testOrderByList() {
    final Fixture f = fixture();
    String sql;

    sql = "select emp.empno from sales.emp where empno=1 order by ^dummy";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, EMP_COLUMNS, EMP_TABLE);

    sql = "select emp.empno from sales.emp where empno=1 order by ^";
    f.withSql(sql).assertComplete(EXPR_KEYWORDS, EMP_COLUMNS, EMP_TABLE);

    sql =
        "select emp.empno\n"
            + "from sales.emp as e(\n"
            + "  mpno,name,ob,gr,iredate,al,omm,eptno,lacker)\n"
            + "where e.mpno=1 order by ^";
    f.withSql(sql)
        .assertComplete(EXPR_KEYWORDS,
            Arrays.asList("COLUMN(MPNO)",
                "COLUMN(NAME)",
                "COLUMN(OB)",
                "COLUMN(GR)",
                "COLUMN(IREDATE)",
                "COLUMN(AL)",
                "COLUMN(OMM)",
                "COLUMN(EPTNO)",
                "COLUMN(LACKER)"),
            Arrays.asList("TABLE(E)"));

    sql =
        "select emp.empno from sales.emp where empno=1 order by empno ^, deptno";
    f.withSql(sql)
        .assertComplete(PREDICATE_KEYWORDS, ORDER_KEYWORDS, FETCH_OFFSET);
  }

  @Test void testSubQuery() {
    final Fixture f = fixture();
    String sql;
    final List<String> xyColumns =
        Arrays.asList(
            "COLUMN(X)",
            "COLUMN(Y)");
    final List<String> tTable =
        Arrays.asList(
            "TABLE(T)");

    sql = "select ^t.dummy from (\n"
        + "  select 1 as x, 2 as y from sales.emp) as t\n"
        + "where t.dummy=1";
    f.withSql(sql)
        .assertHint(EXPR_KEYWORDS, getSelectKeywords(), xyColumns, tTable);

    sql = "select t.^ from (select 1 as x, 2 as y from sales.emp) as t";
    f.withSql(sql).assertComplete(xyColumns, STAR_KEYWORD);

    sql = "select t.x from (select 1 as x, 2 as y from sales.emp) as t "
        + "where ^t.dummy=1";
    f.withSql(sql).assertHint(EXPR_KEYWORDS, tTable, xyColumns);

    sql = "select t.x\n"
        + "from (select 1 as x, 2 as y from sales.emp) as t\n"
        + "where t.^";
    f.withSql(sql).assertComplete(xyColumns);

    sql = "select t.x from (select 1 as x, 2 as y from sales.emp) as t where ^";
    f.withSql(sql).assertComplete(EXPR_KEYWORDS, tTable, xyColumns);

    // with extra from item, aliases are ambiguous, so columns are not
    // offered
    sql = "select a.x\n"
        + "from (select 1 as x, 2 as y from sales.emp) as a,\n"
        + "  dept as b\n"
        + "where ^";
    f.withSql(sql).assertComplete(EXPR_KEYWORDS, AB_TABLES);

    // note that we get hints even though there's a syntax error in
    // select clause ('t.')
    sql = "select t.\n"
        + "from (select 1 as x, 2 as y from (select x from sales.emp)) as t\n"
        + "where ^";
    String simplified = "SELECT * "
        + "FROM ( SELECT 1 as x , 2 as y FROM ( SELECT x FROM sales.emp ) ) as t "
        + "WHERE _suggest_";
    f.withSql(sql)
        .assertSimplify(simplified)
        .assertComplete(EXPR_KEYWORDS, tTable, xyColumns);

    sql = "select t.x from (select 1 as x, 2 as y from sales.^) as t";
    f.withSql(sql).assertComplete(getSalesTables());

    // CALCITE-3474:SqlSimpleParser toke.s equals NullPointerException
    sql = "select ^ from (select * from sales.emp) as t";
    f.withSql(sql)
        .assertComplete(getSelectKeywords(), tTable, EMP_COLUMNS,
            EXPR_KEYWORDS);
  }

  @Test void testSubQueryInWhere() {
    // Aliases from enclosing sub-queries are inherited: hence A from
    // enclosing, B from same scope.
    // The raw columns from dept are suggested (because they can
    // be used unqualified in the inner scope) but the raw
    // columns from emp are not (because they would need to be qualified
    // with A).
    String sql = "select * from sales.emp a where deptno in ("
        + "select * from sales.dept b where ^)";
    String simplifiedSql = "SELECT * FROM sales.emp a WHERE deptno in ("
        + " SELECT * FROM sales.dept b WHERE _suggest_ )";
    fixture().withSql(sql)
        .assertSimplify(simplifiedSql)
        .assertComplete(
            AB_TABLES,
            DEPT_COLUMNS,
            EXPR_KEYWORDS);
  }

  @Test void testSimpleParserTokenizer() {
    String sql =
        "select"
            + " 12"
            + " "
            + "*"
            + " 1.23e45"
            + " "
            + "("
            + "\"an id\""
            + ","
            + " "
            + "\"an id with \"\"quotes' inside\""
            + ","
            + " "
            + "/* a comment, with 'quotes', over\nmultiple lines\nand select keyword */"
            + "\n "
            + "("
            + " "
            + "a"
            + " "
            + "different"
            + " "
            + "// comment\n\r"
            + "//and a comment /* containing comment */ and then some more\r"
            + ")"
            + " "
            + "from"
            + " "
            + "t"
            + ")"
            + ")"
            + "/* a comment after close paren */"
            + " "
            + "("
            + "'quoted'"
            + " "
            + "'string with ''single and \"double\"\" quote'"
            + ")";
    String expected =
        "SELECT\n"
            + "ID(12)\n"
            + "ID(*)\n"
            + "ID(1.23e45)\n"
            + "LPAREN\n"
            + "DQID(\"an id\")\n"
            + "COMMA\n"
            + "DQID(\"an id with \"\"quotes' inside\")\n"
            + "COMMA\n"
            + "COMMENT\n"
            + "LPAREN\n"
            + "ID(a)\n"
            + "ID(different)\n"
            + "COMMENT\n"
            + "COMMENT\n"
            + "RPAREN\n"
            + "FROM\n"
            + "ID(t)\n"
            + "RPAREN\n"
            + "RPAREN\n"
            + "COMMENT\n"
            + "LPAREN\n"
            + "SQID('quoted')\n"
            + "SQID('string with ''single and \"double\"\" quote')\n"
            + "RPAREN\n";
    final Fixture f = fixture();
    f.withSql(sql).assertTokenizesTo(expected);

    // Tokenizer should be lenient if input ends mid-token
    f.withSql("select /* unfinished comment")
        .assertTokenizesTo("SELECT\nCOMMENT\n");
    f.withSql("select // unfinished comment")
        .assertTokenizesTo("SELECT\nCOMMENT\n");
    f.withSql("'starts with string'")
        .assertTokenizesTo("SQID('starts with string')\n");
    f.withSql("'unfinished string")
        .assertTokenizesTo("SQID('unfinished string)\n");
    f.withSql("\"unfinished double-quoted id")
        .assertTokenizesTo("DQID(\"unfinished double-quoted id)\n");
    f.withSql("123")
        .assertTokenizesTo("ID(123)\n");
  }

  @Test void testSimpleParser() {
    final Fixture f = fixture();
    String sql;
    String expected;

    // from
    sql = "select * from ^where";
    expected = "SELECT * FROM _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // from
    sql = "select a.empno, b.deptno from ^";
    expected = "SELECT * FROM _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // select list
    sql = "select ^ from (values (1))";
    expected = "SELECT _suggest_ FROM ( values ( 1 ) )";
    f.withSql(sql).assertSimplify(expected);

    sql = "select emp.^ from sales.emp";
    expected = "SELECT emp. _suggest_ FROM sales.emp";
    f.withSql(sql).assertSimplify(expected);

    sql = "select ^from sales.emp";
    expected = "SELECT _suggest_ FROM sales.emp";
    f.withSql(sql).assertSimplify(expected);

    // remove other expressions in select clause
    sql = "select a.empno ,^  from sales.emp a , sales.dept b";
    expected = "SELECT _suggest_ FROM sales.emp a , sales.dept b";
    f.withSql(sql).assertSimplify(expected);

    sql = "select ^, a.empno from sales.emp a , sales.dept b";
    expected = "SELECT _suggest_ FROM sales.emp a , sales.dept b";
    f.withSql(sql).assertSimplify(expected);

    sql = "select dummy, b.^ from sales.emp a , sales.dept b";
    expected = "SELECT b. _suggest_ FROM sales.emp a , sales.dept b";
    f.withSql(sql).assertSimplify(expected);

    // join
    sql = "select a.empno, b.deptno from dummy a join ^on where empno=1";
    expected = "SELECT * FROM dummy a JOIN _suggest_ ON TRUE";
    f.withSql(sql).assertSimplify(expected);

    // join
    sql =
        "select a.empno, b.deptno from dummy a join sales.^ where empno=1";
    expected = "SELECT * FROM dummy a JOIN sales. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // on
    sql =
        "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=^";
    expected =
        "SELECT * FROM sales.emp a JOIN sales.dept b "
            + "ON a.deptno= _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // where
    sql =
        "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^";
    expected = "SELECT * FROM sales.emp a , sales.dept b WHERE _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // order by
    sql = "select emp.empno from sales.emp where empno=1 order by ^";
    expected = "SELECT emp.empno FROM sales.emp ORDER BY _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // sub-query in from
    sql =
        "select t.^ from (select 1 as x, 2 as y from sales.emp) as t "
            + "where t.dummy=1";
    expected =
        "SELECT t. _suggest_ "
            + "FROM ( SELECT 1 as x , 2 as y FROM sales.emp ) as t";
    f.withSql(sql).assertSimplify(expected);

    sql =
        "select t. from (select 1 as x, 2 as y from "
            + "(select x from sales.emp)) as t where ^";
    expected =
        "SELECT * FROM ( SELECT 1 as x , 2 as y FROM "
            + "( SELECT x FROM sales.emp ) ) as t WHERE _suggest_";
    f.withSql(sql).assertSimplify(expected);

    sql =
        "select ^from (select 1 as x, 2 as y from sales.emp), "
            + "(select 2 as y from (select m from n where)) as t "
            + "where t.dummy=1";
    expected =
        "SELECT _suggest_ FROM ( SELECT 1 as x , 2 as y FROM sales.emp ) "
            + ", ( SELECT 2 as y FROM ( SELECT m FROM n ) ) as t";
    f.withSql(sql).assertSimplify(expected);

    // Note: completes the missing close paren; wipes out select clause of
    // both outer and inner queries since not relevant.
    sql = "select t.x from ( select 1 as x, 2 as y from sales.^";
    expected = "SELECT * FROM ( SELECT * FROM sales. _suggest_ )";
    f.withSql(sql).assertSimplify(expected);

    sql = "select t.^ from (select 1 as x, 2 as y from sales)";
    expected =
        "SELECT t. _suggest_ FROM ( SELECT 1 as x , 2 as y FROM sales )";
    f.withSql(sql).assertSimplify(expected);

    // sub-query in where; note that:
    // 1. removes the SELECT clause of sub-query in WHERE clause;
    // 2. keeps SELECT clause of sub-query in FROM clause;
    // 3. removes GROUP BY clause of sub-query in FROM clause;
    // 4. removes SELECT clause of outer query.
    sql =
        "select x + y + 32 from "
            + "(select 1 as x, 2 as y from sales group by invalid stuff) as t "
            + "where x in (select deptno from emp where foo + t.^ < 10)";
    expected =
        "SELECT * FROM ( SELECT 1 as x , 2 as y FROM sales ) as t "
            + "WHERE x in ( SELECT * FROM emp WHERE foo + t. _suggest_ < 10 )";
    f.withSql(sql).assertSimplify(expected);

    // if hint is in FROM, can remove other members of FROM clause
    sql = "select a.empno, b.deptno from dummy a, sales.^";
    expected = "SELECT * FROM sales. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // function
    sql = "select count(1) from sales.emp a where ^";
    expected = "SELECT * FROM sales.emp a WHERE _suggest_";
    f.withSql(sql).assertSimplify(expected);

    sql =
        "select count(1) from sales.emp a "
            + "where substring(a.^ FROM 3 for 6) = '1234'";
    expected =
        "SELECT * FROM sales.emp a "
            + "WHERE substring ( a. _suggest_ FROM 3 for 6 ) = '1234'";
    f.withSql(sql).assertSimplify(expected);

    // missing ')' following sub-query
    sql =
        "select * from sales.emp a where deptno in ("
            + "select * from sales.dept b where ^";
    expected =
        "SELECT * FROM sales.emp a WHERE deptno in ("
            + " SELECT * FROM sales.dept b WHERE _suggest_ )";
    f.withSql(sql).assertSimplify(expected);

    // keyword embedded in single and double quoted string should be
    // ignored
    sql =
        "select 'a cat from a king' as foobar, 1 / 2 \"where\" from t "
            + "group by t.^ order by 123";
    expected = "SELECT * FROM t GROUP BY t. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // skip comments
    sql = "select /* here is from */ 'cat' as foobar, 1 as x\n"
        + "from t group by t.^ order by 123";
    expected = "SELECT * FROM t GROUP BY t. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // skip comments
    sql = "select // here is from clause\n"
        + " 'cat' as foobar, 1 as x from t group by t.^ order by 123";
    expected = "SELECT * FROM t GROUP BY t. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // skip comments
    sql = "select -- here is from clause\n"
        + " 'cat' as foobar, 1 as x from t group by t.^ order by 123";
    expected = "SELECT * FROM t GROUP BY t. _suggest_";
    f.withSql(sql).assertSimplify(expected);

    // skip comments
    sql = "-- test test\n"
        + "select -- here is from\n"
        + "'cat' as foobar, 1 as x from t group by t.^ order by 123";
    expected = "SELECT * FROM t GROUP BY t. _suggest_";
    f.withSql(sql).assertSimplify(expected);
  }

  @Test void testSimpleParserQuotedIdSqlServer() {
    checkSimpleParserQuotedIdImpl(fixture().withLex(Lex.SQL_SERVER));
  }

  @Test void testSimpleParserQuotedIdMySql() {
    checkSimpleParserQuotedIdImpl(fixture().withLex(Lex.MYSQL));
  }

  @Test void testSimpleParserQuotedIdJava() {
    checkSimpleParserQuotedIdImpl(fixture().withLex(Lex.JAVA));
  }

  @Test void testSimpleParserQuotedIdDefault() {
    checkSimpleParserQuotedIdImpl(fixture());
  }

  private String replaceQuotes(SqlParser.Config parserConfig, String sql) {
    char openQuote = parserConfig.quoting().string.charAt(0);
    char closeQuote = openQuote == '[' ? ']' : openQuote;
    return sql.replace('[', openQuote).replace(']', closeQuote);
  }

  private void checkSimpleParserQuotedIdImpl(Fixture fixture) {
    SqlParser.Config parserConfig = fixture.parserConfig();
    String sql;
    String expected;

    // unclosed double-quote
    sql = replaceQuotes(parserConfig, "select * from t where [^");
    expected = replaceQuotes(parserConfig, "SELECT * FROM t WHERE _suggest_");
    fixture.withSql(sql).assertSimplify(expected);

    // closed double-quote
    sql = replaceQuotes(parserConfig, "select * from t where [^] and x = y");
    expected = replaceQuotes(parserConfig,
        "SELECT * FROM t WHERE _suggest_ and x = y");
    fixture.withSql(sql).assertSimplify(expected);

    // closed double-quote containing extra stuff
    sql = replaceQuotes(parserConfig, "select * from t where [^foo] and x = y");
    expected = replaceQuotes(parserConfig,
        "SELECT * FROM t WHERE _suggest_ and x = y");
    fixture.withSql(sql).assertSimplify(expected);

    // escaped double-quote containing extra stuff
    sql = replaceQuotes(parserConfig,
        "select * from t where [^f]]oo] and x = y");
    expected = replaceQuotes(parserConfig,
        "SELECT * FROM t WHERE _suggest_ and x = y");
    fixture.withSql(sql).assertSimplify(expected);
  }

  @Test void testPartialIdentifier() {
    final Fixture f = fixture();
    String sql = "select * from emp where e^ and emp.deptno = 10";
    String expected =
        "COLUMN(EMPNO)\n"
            + "COLUMN(ENAME)\n"
            + "KEYWORD(ELEMENT)\n"
            + "KEYWORD(EVERY)\n"
            + "KEYWORD(EXISTS)\n"
            + "KEYWORD(EXP)\n"
            + "KEYWORD(EXTRACT)\n"
            + "TABLE(EMP)\n";
    f.withSql(sql)
        .assertComplete(expected, "e",
            ImmutableMap.of("KEYWORD(EXISTS)", "exists",
                "TABLE(EMP)", "emp"));

    sql = "select * from emp where \"e^ and emp.deptno = 10";
    expected =
        "COLUMN(EMPNO)\n"
            + "COLUMN(ENAME)\n"
            + "KEYWORD(ELEMENT)\n"
            + "KEYWORD(EVERY)\n"
            + "KEYWORD(EXISTS)\n"
            + "KEYWORD(EXP)\n"
            + "KEYWORD(EXTRACT)\n"
            + "TABLE(EMP)\n";
    f.withSql(sql)
        .assertComplete(expected, "\"e",
            ImmutableMap.of("KEYWORD(EXISTS)", "exists",
                "TABLE(EMP)", "\"EMP\""));

    sql = "select * from emp where E^ and emp.deptno = 10";
    expected =
        "COLUMN(EMPNO)\n"
            + "COLUMN(ENAME)\n"
            + "KEYWORD(ELEMENT)\n"
            + "KEYWORD(EVERY)\n"
            + "KEYWORD(EXISTS)\n"
            + "KEYWORD(EXP)\n"
            + "KEYWORD(EXTRACT)\n"
            + "TABLE(EMP)\n";
    f.withSql(sql)
        .assertComplete(expected, "E",
            ImmutableMap.of("KEYWORD(EXISTS)", "EXISTS",
                "TABLE(EMP)", "EMP"));

    // cursor in middle of word and at end
    sql = "select * from emp where e^";
    f.withSql(sql)
        .assertComplete(expected, null);

    // longer completion
    sql = "select * from emp where em^";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, null,
            ImmutableMap.of("COLUMN(EMPNO)", "empno"));

    // word after punctuation
    sql = "select deptno,em^ from emp where 1+2<3+4";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, null,
            ImmutableMap.of("COLUMN(EMPNO)", "empno"));

    // inside double-quotes, no terminating double-quote.
    // Only identifiers should be suggested (no keywords),
    // and suggestion should include double-quotes
    sql = "select deptno,\"EM^ from emp where 1+2<3+4";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, "\"EM",
            ImmutableMap.of("COLUMN(EMPNO)", "\"EMPNO\""));

    // inside double-quotes, match is case-insensitive as well
    sql = "select deptno,\"em^ from emp where 1+2<3+4";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, "\"em",
            ImmutableMap.of("COLUMN(EMPNO)", "\"EMPNO\""));

    // when input strings has mixed casing, match should be case-sensitive
    sql = "select deptno,eM^ from emp where 1+2<3+4";
    f.withSql(sql).assertComplete("", "eM");

    // when input strings has mixed casing, match should be case-sensitive
    sql = "select deptno,\"eM^ from emp where 1+2<3+4";
    f.withSql(sql).assertComplete("", "\"eM");

    // eat up following double-quote
    sql = "select deptno,\"EM^ps\" from emp where 1+2<3+4";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, "\"EM",
            ImmutableMap.of("COLUMN(EMPNO)", "\"EMPNO\""));

    // closing double-quote is at very end of string
    sql = "select * from emp where 5 = \"EM^xxx\"";
    f.withSql(sql)
        .assertComplete(EMPNO_EMP, "\"EM",
            ImmutableMap.of("COLUMN(EMPNO)", "\"EMPNO\""));

    // just before dot
    sql = "select emp.^name from emp";
    f.withSql(sql).assertComplete(EMP_COLUMNS, STAR_KEYWORD);
  }

  @Test void testAdviceKeywordsJava() {
    String sql = "select deptno, exi^ from emp where 1+2<3+4";
    fixture().withSql(sql).withLex(Lex.JAVA)
        .assertComplete("KEYWORD(EXISTS)\n", "exi",
            ImmutableMap.of("KEYWORD(EXISTS)", "exists"));
  }

  @Test void testAdviceMixedCase() {
    String sql = "select is^ from (select 1 isOne from emp)";
    fixture().withSql(sql).withLex(Lex.JAVA)
        .assertComplete("COLUMN(isOne)\n", "is",
            ImmutableMap.of("COLUMN(isOne)", "isOne"));
  }

  @Test void testAdviceExpression() {
    String sql = "select s.`count`+s.co^ from (select 1 `count` from emp) s";
    fixture().withSql(sql).withLex(Lex.JAVA)
        .assertComplete("COLUMN(count)\n", "co",
            ImmutableMap.of("COLUMN(count)", "`count`"));
  }

  @Test void testAdviceEmptyFrom() {
    String sql = "select * from^";
    fixture().withSql(sql).withLex(Lex.JAVA)
        .assertComplete("KEYWORD(FROM)\n", "from",
            ImmutableMap.of("KEYWORD(FROM)", "from"));
  }

  @Disabled("Inserts are not supported by SimpleParser yet")
  @Test void testInsert() {
    final Fixture f = fixture();
    String sql;

    sql = "insert into emp(empno, mgr) select ^ from dept a";
    f.withSql(sql)
        .assertComplete(getSelectKeywords(),
            EXPR_KEYWORDS,
            A_TABLE,
            DEPT_COLUMNS,
            SETOPS,
            FETCH_OFFSET);

    sql = "insert into emp(empno, mgr) values (123, 3 + ^)";
    f.withSql(sql).assertComplete(EXPR_KEYWORDS);

    // Wish we could do better here. Parser gives error 'Non-query
    // expression encountered in illegal context' and cannot suggest
    // possible tokens.
    sql = "insert into emp(empno, mgr) ^";
    f.withSql(sql).assertComplete("", null);
  }

  @Test void testNestSchema() {
    final Fixture f = fixture();
    String sql;

    sql = "select * from sales.n^";
    f.withSql(sql)
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "n",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "nest"));

    sql = "select * from sales.\"n^asfasdf";
    f.withSql(sql)
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "\"n",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "\"NEST\""));

    sql = "select * from sales.n^est";
    f.withSql(sql)
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "n",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "nest"));

    sql = "select * from sales.nu^";
    f.withSql(sql).assertComplete("", "nu");
  }

  @Disabled("The set of completion results is empty")
  @Test void testNestTable1() {
    final Fixture f = fixture();
    String sql;

    // select scott.emp.deptno from scott.emp; # valid
    sql = "select catalog.sales.emp.em^ from catalog.sales.emp";
    f.withSql(sql)
        .assertComplete("COLUMN(EMPNO)\n", "em",
            ImmutableMap.of("COLUMN(EMPNO)", "empno"));

    sql = "select catalog.sales.em^ from catalog.sales.emp";
    f.withSql(sql)
        .assertComplete("TABLE(EMP)\n", "em",
            ImmutableMap.of("TABLE(EMP)", "emp"));
  }

  @Test void testNestTable2() {
    // select scott.emp.deptno from scott.emp as e; # not valid
    String sql = "select catalog.sales.emp.em^ from catalog.sales.emp as e";
    fixture().withSql(sql)
        .assertComplete("", "em");
  }


  @Disabled("The set of completion results is empty")
  @Test void testNestTable3() {
    String sql;

    // select scott.emp.deptno from emp; # valid
    sql = "select catalog.sales.emp.em^ from emp";
    fixture().withSql(sql)
        .assertComplete("COLUMN(EMPNO)\n", "em",
            ImmutableMap.of("COLUMN(EMP)", "empno"));

    sql = "select catalog.sales.em^ from emp";
    fixture().withSql(sql)
        .assertComplete("TABLE(EMP)\n", "em",
            ImmutableMap.of("TABLE(EMP)", "emp"));
  }

  @Test void testNestTable4() {
    // select scott.emp.deptno from emp as emp; # not valid
    String sql = "select catalog.sales.emp.em^ from catalog.sales.emp as emp";
    fixture().withSql(sql)
        .assertComplete("", "em");
  }

  @Test void testNestTableSchemaMustMatch() {
    String sql;

    // select foo.emp.deptno from emp; # not valid
    sql = "select sales.nest.em^ from catalog.sales.emp_r";
    fixture().withSql(sql)
        .assertComplete("", "em");
  }

  @Test void testNestSchemaSqlServer() {
    final Fixture f = fixture().withLex(Lex.SQL_SERVER);
    f.withSql("select * from SALES.N^")
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "N",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "NEST"));

    f.withSql("select * from SALES.[n^asfasdf")
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "[n",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "[NEST]"));

    f.withSql("select * from SALES.[N^est")
        .assertComplete("SCHEMA(CATALOG.SALES.NEST)\n", "[N",
            ImmutableMap.of("SCHEMA(CATALOG.SALES.NEST)", "[NEST]"));

    f.withSql("select * from SALES.NU^")
        .assertComplete("", "NU");
  }

  @Test void testUnion() {
    // we simplify set ops such as UNION by removing other queries -
    // thereby avoiding validation errors due to mismatched select lists
    String sql =
        "select 1 from emp union select 2 from dept a where ^ and deptno < 5";
    String simplified =
        "SELECT * FROM dept a WHERE _suggest_ and deptno < 5";
    final Fixture f = fixture();
    f.withSql(sql)
        .assertSimplify(simplified)
        .assertComplete(EXPR_KEYWORDS, A_TABLE, DEPT_COLUMNS);

    // UNION ALL
    sql = "select 1 from emp\n"
        + "union all\n"
        + "select 2 from dept a where ^ and deptno < 5";
    f.withSql(sql).assertSimplify(simplified);

    // hint is in first query
    sql = "select 1 from emp group by ^ except select 2 from dept a";
    simplified = "SELECT * FROM emp GROUP BY _suggest_";
    f.withSql(sql).assertSimplify(simplified);
  }

  @Test void testMssql() {
    String sql = "select 1 from [emp]\n"
        + "union\n"
        + "select 2 from [DEPT] a where ^ and deptno < 5";
    String simplified =
        "SELECT * FROM [DEPT] a WHERE _suggest_ and deptno < 5";
    fixture()
        .withLex(Lex.SQL_SERVER)
        .withSql(sql)
        .assertSimplify(simplified)
        .assertComplete(EXPR_KEYWORDS, Collections.singletonList("TABLE(a)"),
            DEPT_COLUMNS);
  }

  @Test void testFilterComment() {
    // SqlSimpleParser.Tokenizer#nextToken() lines 401 - 423
    // is used to recognize the sql of TokenType.ID or some keywords
    // if a certain segment of characters is continuously composed of Token,
    // the function of this code may be wrong
    // E.g :
    // (1)select * from a where price> 10.0--comment
    // 【10.0--comment】should be recognize as TokenType.ID("10.0") and TokenType.COMMENT
    // but it recognize as TokenType.ID("10.0--comment")
    // (2)select * from a where column_b='/* this is not comment */'
    // 【/* this is not comment */】should be recognize as
    // TokenType.SQID("/* this is not comment */"), but it was not

    final String baseOriginSql = "select * from a ";
    final String baseResultSql = "SELECT * FROM a ";
    String originSql;

    // when SqlSimpleParser.Tokenizer#nextToken() method parse sql,
    // ignore the  "--" after 10.0, this is a comment,
    // but Tokenizer#nextToken() does not recognize it
    originSql = baseOriginSql + "where price > 10.0-- this is comment "
        + System.lineSeparator() + " -- comment ";
    assertSimplifySql(originSql, baseResultSql + "WHERE price > 10.0");

    originSql = baseOriginSql + "where column_b='/* this is not comment */'";
    assertSimplifySql(originSql,
        baseResultSql + "WHERE column_b= '/* this is not comment */'");

    originSql = baseOriginSql + "where column_b='2021 --this is not comment'";
    assertSimplifySql(originSql,
        baseResultSql + "WHERE column_b= '2021 --this is not comment'");

    originSql = baseOriginSql + "where column_b='2021--this is not comment'";
    assertSimplifySql(originSql,
        baseResultSql + "WHERE column_b= '2021--this is not comment'");
  }

  /**
   * Tests that the simplified originSql is consistent with expectedSql.
   *
   * @param originSql   a string sql to simplify.
   * @param expectedSql Expected result after simplification.
   */
  private void assertSimplifySql(String originSql, String expectedSql) {
    SqlSimpleParser simpleParser =
        new SqlSimpleParser("_suggest_", SqlParser.Config.DEFAULT);

    String actualSql = simpleParser.simplifySql(originSql);
    assertThat("simpleParser.simplifySql(" + originSql + ")",
        actualSql, equalTo(expectedSql));
  }

  /** Fixture for the advisor test. */
  static class Fixture extends SqlValidatorFixture {
    protected Fixture(SqlTester tester, SqlTestFactory factory,
        StringAndPos sap, boolean expression, boolean whole) {
      super(tester, factory, sap, expression, whole);
    }

    @SuppressWarnings("deprecation")
    @Override public Fixture withTester(UnaryOperator<SqlTester> transform) {
      final SqlTester tester = transform.apply(this.tester);
      return new Fixture(tester, factory, sap, expression, whole);
    }

    @Override public Fixture withFactory(
        UnaryOperator<SqlTestFactory> transform) {
      final SqlTestFactory factory = transform.apply(this.factory);
      return new Fixture(tester, factory, sap, expression, whole);
    }

    @Override public Fixture withLex(Lex lex) {
      return (Fixture) super.withLex(lex);
    }

    @Override public Fixture withSql(String sql) {
      return new Fixture(tester, factory, StringAndPos.of(sql), false, false);
    }

    private void assertTokenizesTo(String expected) {
      SqlSimpleParser.Tokenizer tokenizer =
          new SqlSimpleParser.Tokenizer(sap.sql, "xxxxx",
              factory.parserConfig().quoting());
      StringBuilder buf = new StringBuilder();
      while (true) {
        SqlSimpleParser.Token token = tokenizer.nextToken();
        if (token == null) {
          break;
        }
        buf.append(token).append("\n");
      }
      assertEquals(expected, buf.toString());
    }

    protected void assertHint(List<String>... expectedLists) {
      List<String> expectedList = plus(expectedLists);
      final String expected = toString(new TreeSet<>(expectedList));
      assertHint(expected);
    }

    /**
     * Checks that a given SQL statement yields the expected set of completion
     * hints.
     *
     * @param expectedResults Expected list of hints
     */
    protected void assertHint(String expectedResults) {
      SqlAdvisor advisor = factory.createAdvisor();

      List<SqlMoniker> results =
          advisor.getCompletionHints(
              sap.sql,
              requireNonNull(sap.pos, "sap.pos"));
      assertEquals(
          expectedResults, convertCompletionHints(results));
    }

    /**
     * Tests that a given SQL statement simplifies to the salesTables result.
     *
     * @param expected Expected result after simplification.
     */
    protected Fixture assertSimplify(String expected) {
      SqlAdvisor advisor = factory.createAdvisor();

      String actual = advisor.simplifySql(sap.sql, sap.cursor);
      assertEquals(expected, actual);
      return this;
    }

    protected void assertComplete(List<String>... expectedResults) {
      assertComplete(null, expectedResults);
    }

    protected void assertComplete(Map<String, String> replacements,
        List<String>... expectedResults) {
      List<String> expectedList = plus(expectedResults);
      String expected = toString(new TreeSet<>(expectedList));
      assertComplete(expected, null, replacements);
    }

    protected void assertComplete(String expectedResults,
        @Nullable String expectedWord) {
      assertComplete(expectedResults, expectedWord, null);
    }

    /**
     * Tests that a given SQL which may be invalid or incomplete simplifies
     * itself and yields the salesTables set of completion hints. This is an
     * integration test of {@link #assertHint} and {@link #assertSimplify}.
     *
     * @param expectedResults Expected list of hints
     * @param expectedWord    Word that we expect to be replaced, or null if we
     *                        don't care
     */
    protected void assertComplete(String expectedResults,
        @Nullable String expectedWord,
        @Nullable Map<String, String> replacements) {
      SqlAdvisor advisor = factory.createAdvisor();

      final String[] replaced = {null};
      List<SqlMoniker> results =
          advisor.getCompletionHints(sap.sql, sap.cursor, replaced);
      assertEquals(expectedResults, convertCompletionHints(results),
          () -> "Completion hints for " + sap);
      if (expectedWord != null) {
        assertEquals(expectedWord, replaced[0],
            "replaced[0] for " + sap);
      } else {
        assertNotNull(replaced[0]);
      }
      assertReplacements(replacements, advisor, replaced[0], results);
    }

    private void assertReplacements(@Nullable Map<String, String> replacements,
        SqlAdvisor advisor, String word, List<SqlMoniker> results) {
      if (replacements == null) {
        return;
      }
      Set<String> missingReplacemenets = new HashSet<>(replacements.keySet());
      for (SqlMoniker result : results) {
        String id = result.id();
        String expectedReplacement = replacements.get(id);
        if (expectedReplacement == null) {
          continue;
        }
        missingReplacemenets.remove(id);
        String actualReplacement = advisor.getReplacement(result, word);
        assertEquals(expectedReplacement, actualReplacement,
            () -> sap + ", replacement of " + word + " with " + id);
      }
      if (missingReplacemenets.isEmpty()) {
        return;
      }
      fail("Sql " + sap + " did not produce replacement hints "
          + missingReplacemenets);
    }

    private String convertCompletionHints(List<SqlMoniker> hints) {
      List<String> list = new ArrayList<>();
      for (SqlMoniker hint : hints) {
        if (hint.getType() != SqlMonikerType.FUNCTION) {
          list.add(hint.id());
        }
      }
      Collections.sort(list);
      return toString(list);
    }

    /**
     * Converts a list to a string, one item per line.
     *
     * @param list List
     * @return String with one item of the list per line
     */
    private static <T> String toString(Collection<T> list) {
      StringBuilder buf = new StringBuilder();
      for (T t : list) {
        buf.append(t).append("\n");
      }
      return buf.toString();
    }

    /**
     * Concatenates several lists of the same type into a single list.
     *
     * @param lists Lists to concatenate
     * @return Sum list
     */
    protected static <T> List<T> plus(List<T>... lists) {
      final List<T> result = new ArrayList<>();
      for (List<T> list : lists) {
        result.addAll(list);
      }
      return result;
    }
  }
}
