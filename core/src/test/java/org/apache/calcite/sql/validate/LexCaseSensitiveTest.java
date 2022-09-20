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
package org.apache.calcite.sql.validate;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testing {@link SqlValidator} and {@link Lex}.
 */
class LexCaseSensitiveTest {

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  private static void runProjectQueryWithLex(Lex lex, String sql)
      throws Exception {
    Config javaLex = SqlParser.config().withLex(lex);
    Planner planner = getPlanner(null, javaLex, Programs.ofRules(Programs.RULE_SET));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet =
        convert.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(transform, instanceOf(EnumerableProject.class));
    List<String> fieldNames = transform.getRowType().getFieldNames();
    assertThat(fieldNames.size(), is(2));
    if (lex.caseSensitive) {
      assertThat(fieldNames.get(0), is("EMPID"));
      assertThat(fieldNames.get(1), is("empid"));
    } else {
      assertThat(fieldNames.get(0) + "-" + fieldNames.get(1),
          anyOf(is("EMPID-empid0"), is("EMPID0-empid")));
    }
  }

  @Test void testCalciteCaseOracle() throws Exception {
    String sql = "select \"empid\" as EMPID, \"empid\" from\n"
        + " (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test void testCalciteCaseOracleException() {
    assertThrows(ValidationException.class, () -> {
      // Oracle is case sensitive, so EMPID should not be found.
      String sql = "select EMPID, \"empid\" from\n"
          + " (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
      runProjectQueryWithLex(Lex.ORACLE, sql);
    });
  }

  @Test void testCalciteCaseMySql() throws Exception {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by `EMPS`.DEPTNO)";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test void testCalciteCaseMySqlNoException() throws Exception {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test void testCalciteCaseMySqlAnsi() throws Exception {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by EMPS.DEPTNO)";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test void testCalciteCaseMySqlAnsiNoException() throws Exception {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test void testCalciteCaseSqlServer() throws Exception {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by EMPS.DEPTNO)";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test void testCalciteCaseSqlServerNoException() throws Exception {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test void testCalciteCaseJava() throws Exception {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }

  @Test void testCalciteCaseJavaException() {
    assertThrows(ValidationException.class, () -> {
      // JAVA is case sensitive, so EMPID should not be found.
      String sql = "select EMPID, empid from\n"
          + " (select empid from emps order by emps.deptno)";
      runProjectQueryWithLex(Lex.JAVA, sql);
    });
  }

  @Test void testCalciteCaseJoinOracle() throws Exception {
    String sql = "select t.\"empid\" as EMPID, s.\"empid\" from\n"
        + "(select * from \"emps\" where \"emps\".\"deptno\" > 100) t join\n"
        + "(select * from \"emps\" where \"emps\".\"deptno\" < 200) s\n"
        + "on t.\"empid\" = s.\"empid\"";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test void testCalciteCaseJoinMySql() throws Exception {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test void testCalciteCaseJoinMySqlAnsi() throws Exception {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test void testCalciteCaseJoinSqlServer() throws Exception {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test void testCalciteCaseJoinJava() throws Exception {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }

  @Test void testCalciteCaseBigQuery() throws Exception {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by EMPS.DEPTNO)";
    runProjectQueryWithLex(Lex.BIG_QUERY, sql);
  }

  @Test void testCalciteCaseBigQueryNoException() throws Exception {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.BIG_QUERY, sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5291">[CALCITE-5291]
   * Make BigQuery lexical policy case insensitive</a>.
   *
   * <p>In reality, BigQuery treats table names as case-sensitive, column names
   * and aliases as case-insensitive. Calcite cannot currently support a hybrid
   * policy, so it treats table names as case-insensitive. */
  @Test void testCalciteCaseJoinBigQuery() throws Exception {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.BIG_QUERY, sql);
  }
}
