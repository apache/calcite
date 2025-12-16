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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for Babel framework.
 */
class BabelTest {

  static final String URL = "jdbc:calcite:";

  private static UnaryOperator<CalciteAssert.PropBuilder> useParserFactory() {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.PARSER_FACTORY,
            SqlBabelParserImpl.class.getName() + "#FACTORY");
  }

  private static UnaryOperator<CalciteAssert.PropBuilder> useLibraryList(
      String libraryList) {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.FUN, libraryList);
  }

  private static UnaryOperator<CalciteAssert.PropBuilder> useLenientOperatorLookup(
      boolean lenient) {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP,
            Boolean.toString(lenient));
  }

  static SqlOperatorTable operatorTableFor(SqlLibrary library) {
    return SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
        SqlLibrary.STANDARD, library);
  }

  static Connection connect() throws SQLException {
    return connect(UnaryOperator.identity());
  }

  static Connection connect(UnaryOperator<CalciteAssert.PropBuilder> propBuild)
      throws SQLException {
    final CalciteAssert.PropBuilder propBuilder = CalciteAssert.propBuilder();
    final Properties info =
        propBuild.andThen(useParserFactory())
            .andThen(useLenientOperatorLookup(true))
            .apply(propBuilder)
            .build();
    return DriverManager.getConnection(URL, info);
  }

  @Test void testInfixCast() throws SQLException {
    try (Connection connection = connect(useLibraryList("standard,postgresql"));
         Statement statement = connection.createStatement()) {
      checkInfixCast(statement, "integer", Types.INTEGER);
      checkInfixCast(statement, "varchar", Types.VARCHAR);
      checkInfixCast(statement, "boolean", Types.BOOLEAN);
      checkInfixCast(statement, "double", Types.DOUBLE);
      checkInfixCast(statement, "bigint", Types.BIGINT);
    }
  }

  private void checkInfixCast(Statement statement, String typeName, int sqlType)
      throws SQLException {
    final String sql = "SELECT x::" + typeName + "\n"
        + "FROM (VALUES ('1', '2')) as tbl(x, y)";
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat("Invalid column count", metaData.getColumnCount(), is(1));
      assertThat("Invalid column type", metaData.getColumnType(1),
          is(sqlType));
    }
  }

  @Test void testPosixRegex() {
    final SqlValidatorFixture f = Fixtures.forValidator()
        .withParserConfig(p -> p.withParserFactory(SqlBabelParserImpl.FACTORY));
    f.withSql("select null !~ 'ab[cd]'").ok();
    f.withSql("select 'abcd' !~ null").ok();
    f.withSql("select null !~ null").ok();
    f.withSql("select null !~* 'ab[cd]'").ok();
    f.withSql("select 'abcd' !~* null").ok();
    f.withSql("select null !~* null").ok();
    f.withSql("select null ~* null").ok();
    f.withSql("select 'abcd' ~* null").ok();
    f.withSql("select null ~* 'ab[cd]'").ok();
    f.withSql("select null ~ null").ok();
    f.withSql("select 'abcd' ~ null").ok();
    f.withSql("select null ~ 'ab[cd]'").ok();
    f.withSql("select 'abcd' !~* 'ab[CD]'").ok();
  }

  /** Tests that you can run tests via {@link Fixtures}. */
  @Test void testFixtures() {
    final SqlValidatorFixture v = Fixtures.forValidator();
    v.withSql("select ^1 + date '2002-03-04'^")
        .fails("(?s).*Cannot apply '\\+' to arguments of"
            + " type '<INTEGER> \\+ <DATE>'.*");

    v.withSql("select 1 + 2 as three")
        .type("RecordType(INTEGER NOT NULL THREE) NOT NULL");

    // 'as' as identifier is invalid with Core parser
    final SqlParserFixture p = Fixtures.forParser();
    p.sql("select ^as^ from t")
        .fails("(?s)Encountered \"as\".*");

    // 'as' as identifier is invalid if you use Babel's tester and Core parser
    p.sql("select ^as^ from t")
        .withTester(new BabelParserTest.BabelTesterImpl())
        .fails("(?s)Encountered \"as\".*");

    // 'as' as identifier is valid with Babel parser
    p.withConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY))
        .sql("select as from t")
        .ok("SELECT `AS`\n"
            + "FROM `T`");

    // Postgres cast is invalid with core parser
    p.sql("select 1 ^:^: integer as x")
        .fails("(?s).*Encountered \":\" at .*");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7310">
   * [CALCITE-7310] Support the syntax SELECT * EXCLUDE(columns)</a>. */
  @Test void testStarExcludeValidation() {
    final SqlValidatorFixture fixture = Fixtures.forValidator()
        .withParserConfig(p -> p.withParserFactory(SqlBabelParserImpl.FACTORY));

    fixture.withSql("select * exclude(empno, deptno) from emp")
        .type(type -> {
          final List<String> names = type.getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());
          assertThat(
              names, is(
                  ImmutableList.of("ENAME", "JOB", "MGR", "HIREDATE", "SAL", "COMM", "SLACKER")));
        });

    fixture.withSql("select * exclude (empno, ^foo^) from emp")
        .fails("SELECT \\* EXCLUDE list contains unknown column\\(s\\): FOO");

    fixture.withSql("select e.* exclude(e.empno, e.ename, e.job, e.mgr)"
            + " from emp e join dept d on e.deptno = d.deptno")
        .type(type -> {
          final List<String> names = type.getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());
          assertThat(
              names, is(
                  ImmutableList.of("HIREDATE", "SAL", "COMM", "DEPTNO", "SLACKER")));
        });

    fixture.withSql("select e.* exclude(e.empno, e.ename, e.job, e.mgr, ^d.deptno^)"
            + " from emp e join dept d on e.deptno = d.deptno")
        .fails("SELECT \\* EXCLUDE list contains unknown column\\(s\\): D.DEPTNO");

    fixture.withSql("select e.* exclude(e.empno, e.ename, e.job, e.mgr), d.* exclude(d.name)"
            + " from emp e join dept d on e.deptno = d.deptno")
        .type(type -> {
          final List<String> names = type.getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());
          assertThat(
              names, is(
                  ImmutableList.of("HIREDATE", "SAL", "COMM", "DEPTNO", "SLACKER", "DEPTNO0")));
        });
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7332">[CALCITE-7332]
   * SELECT * EXCLUDE list should error when it excludes all columns</a>. */
  @Test void testStarExcludeWithEmptyColumn() {
    final SqlValidatorFixture fixture = Fixtures.forValidator()
        .withParserConfig(p -> p.withParserFactory(SqlBabelParserImpl.FACTORY));

    // To verify the scenario where all columns in the exclude list exist
    // and the number of columns in the list is equal to the number of columns in the table.
    fixture.withSql("select * exclude(deptno, deptno) from dept")
        .type(type -> {
          final List<String> names = type.getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());
          assertThat(names, is(ImmutableList.of("NAME")));
        });

    // To verify that the exclude list contains all columns in the table
    fixture.withSql("select ^*^ exclude(deptno, name) from dept")
        .fails("SELECT \\* EXCLUDE list cannot exclude all columns");
  }

  /** Tests that DATEADD, DATEDIFF, DATEPART, DATE_PART allow custom time
   * frames. */
  @Test void testTimeFrames() {
    final SqlValidatorFixture f = Fixtures.forValidator()
        .withParserConfig(p -> p.withParserFactory(SqlBabelParserImpl.FACTORY))
        .withOperatorTable(operatorTableFor(SqlLibrary.MSSQL))
        .withFactory(tf ->
            tf.withTypeSystem(typeSystem ->
                new DelegatingTypeSystem(typeSystem) {
                  @Override public TimeFrameSet deriveTimeFrameSet(
                      TimeFrameSet frameSet) {
                    return TimeFrameSet.builder()
                        .addAll(frameSet)
                        .addDivision("minute15", 4, "HOUR")
                        .build();
                  }
                }));

    final String ts = "timestamp '2020-06-27 12:34:56'";
    final String ts2 = "timestamp '2020-06-27 13:45:56'";
    f.withSql("SELECT DATEADD(YEAR, 3, " + ts + ")").ok();
    f.withSql("SELECT DATEADD(HOUR^.^A, 3, " + ts + ")")
        .fails("(?s).*Encountered \".\" at .*");
    f.withSql("SELECT DATEADD(^A^, 3, " + ts + ")")
        .fails("'A' is not a valid time frame");
    f.withSql("SELECT DATEADD(minute15, 3, " + ts + ")")
        .ok();
    f.withSql("SELECT DATEDIFF(^A^, " + ts + ", " + ts2 + ")")
        .fails("'A' is not a valid time frame");
    f.withSql("SELECT DATEDIFF(minute15, " + ts + ", " + ts2 + ")")
        .ok();
    f.withSql("SELECT DATEPART(^A^, " + ts + ")")
        .fails("'A' is not a valid time frame");
    f.withSql("SELECT DATEPART(minute15, " + ts + ")")
        .ok();

    // Where DATEPART is MSSQL, DATE_PART is Postgres
    f.withSql("SELECT ^DATE_PART(A, " + ts + ")^")
        .fails("No match found for function signature "
            + "DATE_PART\\(<INTERVAL_DAY_TIME>, <TIMESTAMP>\\)");
    final SqlValidatorFixture f2 =
        f.withOperatorTable(operatorTableFor(SqlLibrary.POSTGRESQL));
    f2.withSql("SELECT ^DATEPART(A, " + ts + ")^")
        .fails("No match found for function signature "
            + "DATEPART\\(<INTERVAL_DAY_TIME>, <TIMESTAMP>\\)");
    f2.withSql("SELECT DATE_PART(^A^, " + ts + ")")
        .fails("'A' is not a valid time frame");
    f2.withSql("SELECT DATE_PART(minute15, " + ts + ")")
        .ok();
  }

  @Test void testNullSafeEqual() {
    // x <=> y
    checkSqlResult("mysql", "SELECT 1 <=> NULL", "EXPR$0=false\n");
    checkSqlResult("mysql", "SELECT NULL <=> NULL", "EXPR$0=true\n");
    // (a, b) <=> (x, y)
    checkSqlResult("mysql",
        "SELECT (CAST(NULL AS Integer), 1) <=> (1, CAST(NULL AS Integer))",
        "EXPR$0=false\n");
    checkSqlResult("mysql",
        "SELECT (CAST(NULL AS Integer), CAST(NULL AS Integer))\n"
            + "<=> (CAST(NULL AS Integer), CAST(NULL AS Integer))",
        "EXPR$0=true\n");
    // the higher precedence
    checkSqlResult("mysql",
        "SELECT x <=> 1 + 3 FROM (VALUES (1, 2)) as tbl(x,y)",
        "EXPR$0=false\n");
    // the lower precedence
    checkSqlResult("mysql",
        "SELECT NOT x <=> 1 FROM (VALUES (1, 2)) as tbl(x,y)",
        "EXPR$0=false\n");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6030">
   * [CALCITE-6030] DATE_PART is not handled by the RexToLixTranslator</a>. */
  @Test void testDatePart() {
    checkSqlResult("postgresql", "SELECT DATE_PART(second, TIME '10:10:10')",
        "EXPR$0=10\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5816">[CALCITE-5816]
   * Query with LEFT SEMI JOIN should not refer to RHS columns</a>. */
  @Test public void testLeftSemiJoin() {
    final SqlValidatorFixture v = Fixtures.forValidator()
        .withParserConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY))
        .withConformance(SqlConformanceEnum.BABEL);

    v.withSql("SELECT * FROM dept LEFT SEMI JOIN emp ON emp.deptno = dept.deptno")
        .type("RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10) NOT NULL NAME) NOT NULL");

    v.withSql("SELECT deptno FROM dept LEFT SEMI JOIN emp ON emp.deptno = dept.deptno")
        .type("RecordType(INTEGER NOT NULL DEPTNO) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5962">[CALCITE-5962]
   * Support parse Spark-style syntax "LEFT ANTI JOIN" in Babel parser</a>. */
  @Test public void testLeftAntiJoin() {
    final SqlValidatorFixture v = Fixtures.forValidator()
        .withParserConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY))
        .withConformance(SqlConformanceEnum.BABEL);

    v.withSql("SELECT * FROM dept LEFT ANTI JOIN emp ON emp.deptno = dept.deptno")
        .type("RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10) NOT NULL NAME) NOT NULL");

    v.withSql("SELECT name FROM dept LEFT ANTI JOIN emp ON emp.deptno = dept.deptno")
        .type("RecordType(VARCHAR(10) NOT NULL NAME) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5347">[CALCITE-5347]
   * Support parse "SELECT ... BY" in Babel parser</a>. */
  @Test void testByClause() {
    final SqlValidatorFixture v = Fixtures.forValidator()
        .withParserConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY))
        .withConformance(SqlConformanceEnum.BABEL);

    // Test basic BY clause: SELECT a BY b is sugar for SELECT b, a GROUP BY b ORDER BY b
    v.withSql("select ename, empno by deptno from emp").ok();
    // Test BY clause with alias
    v.withSql("select ename, empno by deptno as dept from emp").ok();
    // Test BY clause with DESC modifier
    v.withSql("select ename, empno by deptno DESC from emp").ok();
    // Test BY clause with multiple columns
    v.withSql("select ename, empno by deptno, job from emp").ok();
    // Test complex BY clause example from the feature proposal
    v.withSql("select e.ename, e.empno by d.name as dept DESC, e.job as title "
        + "from emp as e join dept as d on e.deptno = d.deptno where d.name = 'SALES'")
        .ok();

    // Test SELECT BY cannot be used with GROUP BY
    v.withSql("select ename by deptno from emp ^group by empno^")
        .fails("SELECT BY cannot be used with GROUP BY");
    // Test SELECT BY cannot be used with ORDER BY
    v.withSql("select ename by deptno from emp ^order by empno^")
        .fails("SELECT BY cannot be used with ORDER BY");
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5347">[CALCITE-5347]
   * Add 'SELECT ... BY', a syntax extension that is shorthand for GROUP BY and ORDER BY</a>. */
  @Test void testByClauseConversion() {
    // Test basic BY clause: SELECT a BY b is sugar for SELECT b, a GROUP BY b ORDER BY b
    final String sql = "select ename, empno by deptno from emp";
    final String expected = "SELECT \"DEPTNO\","
        + " ANY_VALUE(\"ENAME\") AS \"ENAME\","
        + " ANY_VALUE(\"EMPNO\") AS \"EMPNO\"\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "ORDER BY \"DEPTNO\"";
    checkSqlConversion(sql, expected);

    // Test BY clause with alias
    final String sql2 = "select ename, empno by deptno as dept from emp";
    final String expected2 = "SELECT \"DEPTNO\" AS \"DEPT\","
        + " ANY_VALUE(\"ENAME\") AS \"ENAME\","
        + " ANY_VALUE(\"EMPNO\") AS \"EMPNO\"\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "ORDER BY \"DEPTNO\"";
    checkSqlConversion(sql2, expected2);

    // Test BY clause with DESC modifier
    final String sql3 = "select ename, empno by deptno DESC from emp";
    final String expected3 = "SELECT \"DEPTNO\","
        + " ANY_VALUE(\"ENAME\") AS \"ENAME\","
        + " ANY_VALUE(\"EMPNO\") AS \"EMPNO\"\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "ORDER BY \"DEPTNO\" DESC";
    checkSqlConversion(sql3, expected3);

    // Test BY clause with multiple columns
    final String sql4 = "select ename, empno by deptno, job from emp";
    final String expected4 = "SELECT \"DEPTNO\", \"JOB\","
        + " ANY_VALUE(\"ENAME\") AS \"ENAME\","
        + " ANY_VALUE(\"EMPNO\") AS \"EMPNO\"\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\", \"JOB\"\n"
        + "ORDER BY \"DEPTNO\", \"JOB\"";
    checkSqlConversion(sql4, expected4);

    // Test complex BY clause example from the feature proposal
    final String sql5 = "SELECT e.ename, e.empno BY d.dname AS dept DESC, e.job AS title\n"
        + "FROM emp AS e\n"
        + "  JOIN dept AS d ON e.deptno = d.deptno\n"
        + "WHERE d.loc = 'CHICAGO'";
    final String expected5 = "SELECT \"DEPT\".\"DNAME\" AS \"DEPT\","
        + " \"EMP\".\"JOB\" AS \"TITLE\","
        + " ANY_VALUE(\"EMP\".\"ENAME\") AS \"ENAME\","
        + " ANY_VALUE(\"EMP\".\"EMPNO\") AS \"EMPNO\"\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "INNER JOIN \"SCOTT\".\"DEPT\" ON \"EMP\".\"DEPTNO\" = \"DEPT\".\"DEPTNO\"\n"
        + "WHERE \"DEPT\".\"LOC\" = 'CHICAGO'\n"
        + "GROUP BY \"DEPT\".\"DNAME\", \"EMP\".\"JOB\"\n"
        + "ORDER BY \"DEPT\".\"DNAME\" DESC, \"EMP\".\"JOB\"";
    checkSqlConversion(sql5, expected5);
  }

  private void checkSqlConversion(String sql, String expected) {
    try {
      final SqlParser.Config parserConfig = SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY);

      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final SchemaPlus defaultSchema =
          CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.JDBC_SCOTT);

      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(parserConfig)
          .defaultSchema(defaultSchema)
          .programs(Programs.standard())
          .build();

      final Planner planner = Frameworks.getPlanner(config);
      final SqlNode parse = planner.parse(sql);
      final SqlNode validate = planner.validate(parse);
      RelNode rel = planner.rel(validate).project();

      final SqlDialect dialect = CalciteSqlDialect.DEFAULT;
      final RelToSqlConverter converter = new RelToSqlConverter(dialect);
      final SqlNode sqlNode = converter.visitRoot(rel).asStatement();
      final String actual = sqlNode.toSqlString(c ->
          c.withDialect(dialect)
              .withAlwaysUseParentheses(false)
              .withSelectListItemsOnSeparateLines(false)
              .withUpdateSetListNewline(false)
              .withIndentation(0))
          .getSql();

      assertThat(actual, isLinux(expected));
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
  }

  private void checkSqlResult(String funLibrary, String query, String result) {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.PARSER_FACTORY,
            SqlBabelParserImpl.class.getName() + "#FACTORY")
        .with(CalciteConnectionProperty.FUN, funLibrary)
        .query(query)
        .returns(result);
  }
}
