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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.test.schemata.hr.Department;
import org.apache.calcite.test.schemata.hr.DepartmentPlus;
import org.apache.calcite.test.schemata.hr.Dependent;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.Event;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.test.schemata.hr.Location;
import org.apache.calcite.test.schemata.hr.NullableTest;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration tests for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
@Tag("slow")
public class MaterializationTest {
  private static final Consumer<ResultSet> CONTAINS_M0 =
      CalciteAssert.checkResultContains(
          "EnumerableTableScan(table=[[hr, m0]])");

  private static final Consumer<ResultSet> CONTAINS_LOCATIONS =
      CalciteAssert.checkResultContains(
          "EnumerableTableScan(table=[[hr, locations]])");

  private static final Ordering<Iterable<String>> CASE_INSENSITIVE_LIST_COMPARATOR =
      Ordering.from(String.CASE_INSENSITIVE_ORDER).lexicographical();

  private static final Ordering<Iterable<List<String>>> CASE_INSENSITIVE_LIST_LIST_COMPARATOR =
      CASE_INSENSITIVE_LIST_COMPARATOR.lexicographical();

  private static final String HR_FKUK_SCHEMA = "{\n"
      + "       type: 'custom',\n"
      + "       name: 'hr',\n"
      + "       factory: '"
      + ReflectiveSchema.Factory.class.getName()
      + "',\n"
      + "       operand: {\n"
      + "         class: '" + HrFKUKSchema.class.getName() + "'\n"
      + "       }\n"
      + "     }\n";

  private static final String HR_FKUK_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'hr',\n"
      + "   schemas: [\n"
      + HR_FKUK_SCHEMA
      + "   ]\n"
      + "}";

  @Test void testScan() {
    CalciteAssert.that()
        .withMaterializations(
            "{\n"
                + "  version: '1.0',\n"
                + "  defaultSchema: 'SCOTT_CLONE',\n"
                + "  schemas: [ {\n"
                + "    name: 'SCOTT_CLONE',\n"
                + "    type: 'custom',\n"
                + "    factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',\n"
                + "    operand: {\n"
                + "      jdbcDriver: '" + JdbcTest.SCOTT.driver + "',\n"
                + "      jdbcUser: '" + JdbcTest.SCOTT.username + "',\n"
                + "      jdbcPassword: '" + JdbcTest.SCOTT.password + "',\n"
                + "      jdbcUrl: '" + JdbcTest.SCOTT.url + "',\n"
                + "      jdbcSchema: 'SCOTT'\n"
                + "   } } ]\n"
                + "}",
            "m0",
            "select empno, deptno from emp order by deptno")
        .query(
            "select empno, deptno from emp")
        .enableMaterializations(true)
        .explainContains("EnumerableTableScan(table=[[SCOTT_CLONE, m0]])")
        .sameResultWithMaterializationsDisabled();
  }

  /** Model with a clone of the scott schema. */
  private static final String SCOTT_CLONE_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'SCOTT_CLONE',\n"
      + "  schemas: [ {\n"
      + "    name: 'SCOTT_CLONE',\n"
      + "    type: 'custom',\n"
      + "    factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',\n"
      + "    operand: {\n"
      + "      jdbcDriver: '" + JdbcTest.SCOTT.driver + "',\n"
      + "      jdbcUser: '" + JdbcTest.SCOTT.username + "',\n"
      + "      jdbcPassword: '" + JdbcTest.SCOTT.password + "',\n"
      + "      jdbcUrl: '" + JdbcTest.SCOTT.url + "',\n"
      + "      jdbcSchema: 'SCOTT'\n"
      + "   } } ]\n"
      + "}";

  /** Runs {@code query} with the materialized view {@code mv} on the scott
   * schema, and checks the result. */
  private static void checkScottMaterialization(String mv, String query,
      String expected) {
    CalciteAssert.that()
        .withMaterializations(SCOTT_CLONE_MODEL, "m0", mv)
        .query(query)
        .enableMaterializations(true)
        .returns(expected);
  }

  /** MaterializedViewAggregateRule answers SUM WITHIN DISTINCT from a plain
   * SUM in the view (gave 310).
   * Result validated with PostgreSQL 14, using SUM(DISTINCT deptno). */
  @Test void testMaterializationWithinDistinct() {
    checkScottMaterialization(
        "select deptno, empno, sum(cast(deptno as integer)) as s\n"
            + "from emp group by deptno, empno",
        "select sum(cast(deptno as integer)) within distinct (deptno) as s\n"
            + "from emp",
        "S=60\n");
  }

  /** MaterializedViewAggregateRule answers LISTAGG WITHIN GROUP from an
   * unordered LISTAGG in the view.
   * Result validated with PostgreSQL 14, using
   * STRING_AGG(ename, ',' ORDER BY sal DESC, ename). */
  @Test void testMaterializationWithinGroup() {
    checkScottMaterialization(
        "select deptno, listagg(ename) as l from emp group by deptno",
        "select deptno,\n"
            + " listagg(ename) within group (order by sal desc, ename) as l\n"
            + "from emp group by deptno order by deptno",
        "DEPTNO=10; L=KING,CLARK,MILLER\n"
            + "DEPTNO=20; L=FORD,SCOTT,JONES,ADAMS,SMITH\n"
            + "DEPTNO=30; L=BLAKE,ALLEN,TURNER,MARTIN,WARD,JAMES\n");
  }

  /** Materialized view substitution rolls up COUNT WITHIN DISTINCT from the
   * view as if it were a plain COUNT (gave 14).
   * Result validated with PostgreSQL 14, using COUNT(DISTINCT deptno). */
  @Test void testMaterializationRollUpWithinDistinct() {
    checkScottMaterialization(
        "select empno, count(*) within distinct (deptno) as c\n"
            + "from emp group by empno",
        "select count(*) within distinct (deptno) as c from emp",
        "C=3\n");
  }

  /** MaterializedViewAggregateRule answers a plain COUNT from a COUNT WITHIN
   * DISTINCT in the view.
   * Result validated with PostgreSQL 14, using COUNT(*). */
  @Test void testMaterializationViewWithinDistinct() {
    checkScottMaterialization(
        "select deptno, count(*) within distinct (job) as c\n"
            + "from emp group by deptno",
        "select deptno, count(*) as c from emp group by deptno order by deptno",
        "DEPTNO=10; C=3\n"
            + "DEPTNO=20; C=5\n"
            + "DEPTNO=30; C=6\n");
  }

  /** The view groups by DEPTNO; the query has no GROUP BY.
   * SubstitutionVisitor rolls up COUNT WITHIN DISTINCT with SUM0 over the
   * column C of the view, and keeps the WITHIN DISTINCT key of the query.
   * In the view, the field index of that key is the column C. Each
   * department has 3 jobs, so the result was 3.
   * Result validated with PostgreSQL 14, using COUNT(DISTINCT job). */
  @Test void testMaterializationRollUpWithinDistinctKey() {
    checkScottMaterialization(
        "select deptno, count(*) within distinct (job) as c\n"
            + "from emp group by deptno",
        "select count(*) within distinct (job) as c from emp",
        "C=5\n");
  }

  /** The view groups by DEPTNO, SAL and B. SubstitutionVisitor computes
   * MIN(sal) and COUNT(DISTINCT sal) from the column SAL of the view, and
   * drops the FILTER (gave 800 and 4 for department 20).
   * Result validated with PostgreSQL 14. */
  @Test void testMaterializationRollUpFilter() {
    final String from =
        "from (select deptno, sal, empno > 7700 as b from emp)\n";
    checkScottMaterialization(
        "select deptno, sal, b\n"
            + from + "group by deptno, sal, b",
        "select deptno, min(sal) filter (where b) as m,\n"
            + " count(distinct sal) filter (where b) as c\n"
            + from + "group by deptno order by deptno",
        "DEPTNO=10; M=1300.00; C=3\n"
            + "DEPTNO=20; M=1100.00; C=2\n"
            + "DEPTNO=30; M=950.00; C=2\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7636">[CALCITE-7636]
   * Materialized view union rewriting drops rows when the view filter is not
   * null-rejecting</a>.
   *
   * <p>The view filters on {@code commission > 400}, which is UNKNOWN for the
   * null-commission employee. The union-rewritten result must keep that row and
   * therefore match the result computed without materializations. */
  @Test void testUnionRewritingNullableAggregatePredicate() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL, "m0",
              "select \"deptno\", sum(\"salary\") as s from \"emps\" "
              + "where \"deptno\" > 5 and \"commission\" > 400 group by \"deptno\"")
          .query("select \"deptno\", sum(\"salary\") as s from \"emps\" "
              + "where \"deptno\" > 5 group by \"deptno\"")
          .enableMaterializations(true)
          .explainContains("EnumerableUnion(all=[true])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testViewMaterialization() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      String materialize = "select \"depts\".\"name\"\n"
          + "from \"depts\"\n"
          + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")";
      String query = "select \"depts\".\"name\"\n"
          + "from \"depts\"\n"
          + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")";

      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL, true, "matview", materialize)
          .query(query)
          .enableMaterializations(true)
          .explainMatches(
              "", CalciteAssert.checkResultContains(
              "EnumerableValues(tuples=[[{ 'noname' }]])")).returnsValue("noname");
    }
  }

  @Test void testTableModify() {
    final String m = "select \"deptno\", \"empid\", \"name\""
        + "from \"emps\" where \"deptno\" = 10";
    final String q = "upsert into \"dependents\""
        + "select \"empid\" + 1 as x, \"name\""
        + "from \"emps\" where \"deptno\" = 10";

    final List<List<List<String>>> substitutedNames = new ArrayList<>();
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", m)
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .explainContains("hr, m0");
    } catch (Exception e) {
      // Table "dependents" not modifiable.
    }
    assertThat(substitutedNames, is(list3(new String[][][]{{{"hr", "m0"}}})));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-761">[CALCITE-761]
   * Pre-populated materializations</a>. */
  @Test void testPrePopulated() {
    String q = "select distinct \"deptno\" from \"emps\"";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(
              HR_FKUK_MODEL, builder -> {
                final Map<String, Object> map = builder.map();
                map.put("table", "locations");
                String sql = "select distinct `deptno` as `empid`, '' as `name`\n"
                    + "from `emps`";
                final String sql2 = sql.replace("`", "\"");
                map.put("sql", sql2);
                return ImmutableList.of(map);
              })
          .query(q)
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testViewSchemaPath() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final String m = "select empno, deptno from emp";
      final String q = "select deptno from scott.emp";
      final List<String> path = ImmutableList.of("SCOTT");
      final JsonBuilder builder = new JsonBuilder();
      final String model = "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'hr',\n"
          + "  schemas: [\n"
          + JdbcTest.SCOTT_SCHEMA
          + "  ,\n"
          + "    {\n"
          + "      materializations: [\n"
          + "        {\n"
          + "          table: 'm0',\n"
          + "          view: 'm0v',\n"
          + "          sql: " + builder.toJsonString(m) + ",\n"
          + "          viewSchemaPath: " + builder.toJsonString(path)
          + "        }\n"
          + "      ],\n"
          + "      type: 'custom',\n"
          + "      name: 'hr',\n"
          + "      factory: 'org.apache.calcite.adapter.java.ReflectiveSchema$Factory',\n"
          + "      operand: {\n"
          + "        class: '" + HrSchema.class.getName() + "'\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";
      CalciteAssert.that()
          .withModel(model)
          .query(q)
          .enableMaterializations(true)
          .explainMatches("", CONTAINS_M0)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testMultiMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select \"deptno\", count(*) as c from \"emps\" group by \"deptno\") using (\"deptno\")";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" group by \"deptno\"",
              "m1", "select * from \"emps\" where \"empid\" < 500")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Disabled("Creating mv for depts considering all its column throws exception")
  @Test void testMultiMaterializationOnJoinQuery() {
    final String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" < 300 "
        + "and \"depts\".\"deptno\" > 200";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 500",
              "m1", "select * from \"depts\" where \"deptno\" > 100")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testMaterializationSubstitution() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  @Test void testMaterializationSubstitution2() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "emps"}, {"hr", "m2"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "m2"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "m2"}},
        {{"hr", "m2"}, {"hr", "emps"}},
        {{"hr", "m2"}, {"hr", "m0"}},
        {{"hr", "m2"}, {"hr", "m1"}},
        {{"hr", "m2"}, {"hr", "m2"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600",
              "m2", "select * from \"m1\"")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  private static <E> List<List<List<E>>> list3(E[][][] as) {
    final ImmutableList.Builder<List<List<E>>> builder =
        ImmutableList.builder();
    for (E[][] a : as) {
      builder.add(list2(a));
    }
    return builder.build();
  }

  private static <E> List<List<E>> list2(E[][] as) {
    final ImmutableList.Builder<List<E>> builder = ImmutableList.builder();
    for (E[] a : as) {
      builder.add(ImmutableList.copyOf(a));
    }
    return builder.build();
  }

  /**
   * Implementation of RelVisitor to extract substituted table names.
   */
  private static class TableNameVisitor extends RelVisitor {
    private final List<List<String>> names = new ArrayList<>();

    List<List<String>> run(RelNode input) {
      go(input);
      return names;
    }

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
      if (node instanceof TableScan) {
        RelOptTable table = node.getTable();
        List<String> qName = table.getQualifiedName();
        names.add(qName);
      }
      super.visit(node, ordinal, parent);
    }
  }

  /**
   * Hr schema with FK-UK relationship.
   */
  @ReflectiveSchema.Input
  public static class HrFKUKSchema {
    @Override public String toString() {
      return "HrFKUKSchema";
    }

    public final Employee[] emps = {
        new Employee(100, 10, "Bill", 10000, 1000),
        new Employee(200, 20, "Eric", 8000, 500),
        new Employee(150, 10, "Sebastian", 7000, null),
        new Employee(110, 10, "Theodore", 10000, 250),
    };
    public final Department[] depts = {
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
            new Location(-122, 38)),
        new Department(30, "Marketing", ImmutableList.of(),
            new Location(0, 52)),
        new Department(20, "HR", Collections.singletonList(emps[1]), null),
    };
    public final DepartmentPlus[] depts2 = {
        new DepartmentPlus(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
            new Location(-122, 38), new Timestamp(0)),
        new DepartmentPlus(30, "Marketing", ImmutableList.of(),
            new Location(0, 52), new Timestamp(0)),
        new DepartmentPlus(20, "HR", Collections.singletonList(emps[1]),
            null, new Timestamp(0)),
    };
    public final Dependent[] dependents = {
        new Dependent(10, "Michael"),
        new Dependent(10, "Jane"),
    };
    public final NullableTest[] nullables = {
        new NullableTest(null, null, 1),
    };
    public final Dependent[] locations = {
        new Dependent(10, "San Francisco"),
        new Dependent(20, "San Diego"),
    };
    public final Event[] events = {
        new Event(100, new Timestamp(0)),
        new Event(200, new Timestamp(0)),
        new Event(150, new Timestamp(0)),
        new Event(110, null),
    };

    public final RelReferentialConstraint rcs0 =
        RelReferentialConstraintImpl.of(
            ImmutableList.of("hr", "emps"), ImmutableList.of("hr", "depts"),
            ImmutableList.of(IntPair.of(1, 0)));

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }

    public TranslatableTable matview() {
      return Smalls.strView("noname");
    }
  }
}
