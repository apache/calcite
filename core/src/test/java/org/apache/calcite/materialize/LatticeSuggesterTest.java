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
package org.apache.calcite.materialize;

import org.apache.calcite.materialize.Lattice.Measure;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.statistic.MapSqlStatisticProvider;
import org.apache.calcite.statistic.QuerySqlStatisticProvider;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.FoodMartQuerySet;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableBitSet.Builder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

/**
 * Unit tests for {@link LatticeSuggester}.
 */
class LatticeSuggesterTest {

  /** Some basic query patterns on the Scott schema with "EMP" and "DEPT"
   * tables. */
  @Test void testEmpDept() throws Exception {
    final Tester t = new Tester();
    final String q0 = "select dept.dname, count(*), sum(sal)\n"
        + "from emp\n"
        + "join dept using (deptno)\n"
        + "group by dept.dname";
    assertThat(t.addQuery(q0),
        isGraphs("EMP (DEPT:DEPTNO)", "[COUNT(), SUM(EMP.SAL)]"));

    // Same as above, but using WHERE rather than JOIN
    final String q1 = "select dept.dname, count(*), sum(sal)\n"
        + "from emp, dept\n"
        + "where emp.deptno = dept.deptno\n"
        + "group by dept.dname";
    assertThat(t.addQuery(q1),
        isGraphs("EMP (DEPT:DEPTNO)", "[COUNT(), SUM(EMP.SAL)]"));

    // With HAVING
    final String q2 = "select dept.dname\n"
        + "from emp, dept\n"
        + "where emp.deptno = dept.deptno\n"
        + "group by dept.dname\n"
        + "having count(*) > 10";
    assertThat(t.addQuery(q2),
        isGraphs("EMP (DEPT:DEPTNO)", "[COUNT()]"));

    // No joins, therefore graph has a single node and no edges
    final String q3 = "select distinct dname\n"
        + "from dept";
    assertThat(t.addQuery(q3),
        isGraphs("DEPT", "[]"));

    // Graph is empty because there are no tables
    final String q4 = "select distinct t.c\n"
        + "from (values 1, 2) as t(c)"
        + "join (values 2, 3) as u(c) using (c)\n";
    assertThat(t.addQuery(q4),
        isGraphs());

    // Self-join
    final String q5 = "select *\n"
        + "from emp as e\n"
        + "join emp as m on e.mgr = m.empno";
    assertThat(t.addQuery(q5),
        isGraphs("EMP (EMP:MGR)", "[]"));

    // Self-join, twice
    final String q6 = "select *\n"
        + "from emp as e join emp as m on e.mgr = m.empno\n"
        + "join emp as m2 on m.mgr = m2.empno";
    assertThat(t.addQuery(q6),
        isGraphs("EMP (EMP:MGR (EMP:MGR))", "[]"));

    // No graphs, because cyclic: e -> m, m -> m2, m2 -> e
    final String q7 = "select *\n"
        + "from emp as e\n"
        + "join emp as m on e.mgr = m.empno\n"
        + "join emp as m2 on m.mgr = m2.empno\n"
        + "where m2.mgr = e.empno";
    assertThat(t.addQuery(q7),
        isGraphs());

    // The graph of all tables and hops
    final String expected = "graph("
        + "vertices: [[scott, DEPT],"
        + " [scott, EMP]], "
        + "edges: [Step([scott, EMP], [scott, DEPT], DEPTNO:DEPTNO),"
        + " Step([scott, EMP], [scott, EMP], MGR:EMPNO)])";
    assertThat(t.s.space.g, hasToString(expected));
  }

  @Test void testFoodmart() throws Exception {
    final Tester t = new Tester().foodmart();
    final String q = "select \"t\".\"the_year\" as \"c0\",\n"
        + " \"t\".\"quarter\" as \"c1\",\n"
        + " \"pc\".\"product_family\" as \"c2\",\n"
        + " sum(\"s\".\"unit_sales\") as \"m0\"\n"
        + "from \"time_by_day\" as \"t\",\n"
        + " \"sales_fact_1997\" as \"s\",\n"
        + " \"product_class\" as \"pc\",\n"
        + " \"product\" as \"p\"\n"
        + "where \"s\".\"time_id\" = \"t\".\"time_id\"\n"
        + "and \"t\".\"the_year\" = 1997\n"
        + "and \"s\".\"product_id\" = \"p\".\"product_id\"\n"
        + "and \"p\".\"product_class_id\" = \"pc\".\"product_class_id\"\n"
        + "group by \"t\".\"the_year\",\n"
        + " \"t\".\"quarter\",\n"
        + " \"pc\".\"product_family\"";
    final String g = "sales_fact_1997"
        + " (product:product_id (product_class:product_class_id)"
        + " time_by_day:time_id)";
    assertThat(t.addQuery(q),
        isGraphs(g, "[SUM(sales_fact_1997.unit_sales)]"));

    // The graph of all tables and hops
    final String expected = "graph("
        + "vertices: ["
        + "[foodmart, product], "
        + "[foodmart, product_class], "
        + "[foodmart, sales_fact_1997], "
        + "[foodmart, time_by_day]], "
        + "edges: ["
        + "Step([foodmart, product], [foodmart, product_class],"
        + " product_class_id:product_class_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, product],"
        + " product_id:product_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, time_by_day],"
        + " time_id:time_id)])";
    assertThat(t.s.space.g, hasToString(expected));
  }

  @Test void testAggregateExpression() throws Exception {
    final Tester t = new Tester().foodmart();
    final String q = "select \"t\".\"the_year\" as \"c0\",\n"
        + " \"pc\".\"product_family\" as \"c1\",\n"
        + " sum((case when \"s\".\"promotion_id\" = 0\n"
        + "     then 0 else \"s\".\"store_sales\"\n"
        + "     end)) as \"sum_m0\"\n"
        + "from \"time_by_day\" as \"t\",\n"
        + " \"sales_fact_1997\" as \"s\",\n"
        + " \"product_class\" as \"pc\",\n"
        + " \"product\" as \"p\"\n"
        + "where \"s\".\"time_id\" = \"t\".\"time_id\"\n"
        + " and \"t\".\"the_year\" = 1997\n"
        + " and \"s\".\"product_id\" = \"p\".\"product_id\"\n"
        + " and \"p\".\"product_class_id\" = \"pc\".\"product_class_id\"\n"
        + "group by \"t\".\"the_year\",\n"
        + " \"pc\".\"product_family\"\n";
    final String g = "sales_fact_1997"
        + " (product:product_id (product_class:product_class_id)"
        + " time_by_day:time_id)";
    final String expected = "[SUM(m0)]";
    assertThat(t.addQuery(q),
        allOf(isGraphs(g, expected),
            hasMeasureNames(0, "sum_m0"),
            hasDerivedColumnNames(0, "m0")));
  }

  private Matcher<List<Lattice>> hasMeasureNames(int ordinal,
      String... names) {
    final List<String> nameList = ImmutableList.copyOf(names);
    return new TypeSafeMatcher<List<Lattice>>() {
      public void describeTo(Description description) {
        description.appendValue(names);
      }

      protected boolean matchesSafely(List<Lattice> lattices) {
        final Lattice lattice = lattices.get(ordinal);
        final List<String> actualNameList =
            Util.transform(lattice.defaultMeasures, measure -> measure.name);
        return actualNameList.equals(nameList);
      }
    };
  }

  private Matcher<List<Lattice>> hasDerivedColumnNames(int ordinal,
      String... names) {
    final List<String> nameList = ImmutableList.copyOf(names);
    return new TypeSafeMatcher<List<Lattice>>() {
      public void describeTo(Description description) {
        description.appendValue(names);
      }

      protected boolean matchesSafely(List<Lattice> lattices) {
        final Lattice lattice = lattices.get(ordinal);
        final List<String> actualNameList =
            lattice.columns.stream()
                .filter(c -> c instanceof Lattice.DerivedColumn)
                .map(c -> ((Lattice.DerivedColumn) c).alias)
                .collect(Collectors.toList());
        return actualNameList.equals(nameList);
      }
    };
  }

  @Tag("slow")
  @Test void testSharedSnowflake() throws Exception {
    final Tester t = new Tester().foodmart();
    // foodmart query 5827 (also 5828, 5830, 5832) uses the "region" table
    // twice: once via "store" and once via "customer";
    // TODO: test what happens if FK from "store" to "region" is reversed
    final String q = "select \"s\".\"store_country\" as \"c0\",\n"
        + " \"r\".\"sales_region\" as \"c1\",\n"
        + " \"r1\".\"sales_region\" as \"c2\",\n"
        + " sum(\"f\".\"unit_sales\") as \"m0\"\n"
        + "from \"store\" as \"s\",\n"
        + " \"sales_fact_1997\" as \"f\",\n"
        + " \"region\" as \"r\",\n"
        + " \"region\" as \"r1\",\n"
        + " \"customer\" as \"c\"\n"
        + "where \"f\".\"store_id\" = \"s\".\"store_id\"\n"
        + " and \"s\".\"store_country\" = 'USA'\n"
        + " and \"s\".\"region_id\" = \"r\".\"region_id\"\n"
        + " and \"r\".\"sales_region\" = 'South West'\n"
        + " and \"f\".\"customer_id\" = \"c\".\"customer_id\"\n"
        + " and \"c\".\"customer_region_id\" = \"r1\".\"region_id\"\n"
        + " and \"r1\".\"sales_region\" = 'South West'\n"
        + "group by \"s\".\"store_country\",\n"
        + " \"r\".\"sales_region\",\n"
        + " \"r1\".\"sales_region\"\n";
    final String g = "sales_fact_1997"
        + " (customer:customer_id (region:customer_region_id)"
        + " store:store_id (region:region_id))";
    assertThat(t.addQuery(q),
        isGraphs(g, "[SUM(sales_fact_1997.unit_sales)]"));
  }

  @Test void testExpressionInAggregate() throws Exception {
    final Tester t = new Tester().withEvolve(true).foodmart();
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    for (int id : new int[]{392, 393}) {
      t.addQuery(set.queries.get(id).sql);
    }
  }

  private void checkFoodMartAll(boolean evolve) throws Exception {
    final Tester t = new Tester().sqlToRelConverter(config -> config.withExpand(true))
        .foodmart()
        .withEvolve(evolve);
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    for (FoodMartQuerySet.FoodmartQuery query : set.queries.values()) {
      if (query.sql.contains("\"agg_10_foo_fact\"")
          || query.sql.contains("\"agg_line_class\"")
          || query.sql.contains("\"agg_tenant\"")
          || query.sql.contains("\"line\"")
          || query.sql.contains("\"line_class\"")
          || query.sql.contains("\"tenant\"")
          || query.sql.contains("\"test_lp_xxx_fact\"")
          || query.sql.contains("\"product_csv\"")
          || query.sql.contains("\"product_cat\"")
          || query.sql.contains("\"cat\"")
          || query.sql.contains("\"fact\"")) {
        continue;
      }
      switch (query.id) {
      case 2455: // missing RTRIM function
      case 2456: // missing RTRIM function
      case 2457: // missing RTRIM function
      case 5682: // case sensitivity
      case 5700: // || applied to smallint
        continue;
      default:
        t.addQuery(query.sql);
      }
    }

    // The graph of all tables and hops
    final String expected = "graph("
        + "vertices: ["
        + "[foodmart, agg_c_10_sales_fact_1997], "
        + "[foodmart, agg_c_14_sales_fact_1997], "
        + "[foodmart, agg_c_special_sales_fact_1997], "
        + "[foodmart, agg_g_ms_pcat_sales_fact_1997], "
        + "[foodmart, agg_l_03_sales_fact_1997], "
        + "[foodmart, agg_l_04_sales_fact_1997], "
        + "[foodmart, agg_l_05_sales_fact_1997], "
        + "[foodmart, agg_lc_06_sales_fact_1997], "
        + "[foodmart, agg_lc_100_sales_fact_1997], "
        + "[foodmart, agg_ll_01_sales_fact_1997], "
        + "[foodmart, agg_pl_01_sales_fact_1997], "
        + "[foodmart, customer], "
        + "[foodmart, department], "
        + "[foodmart, employee], "
        + "[foodmart, employee_closure], "
        + "[foodmart, inventory_fact_1997], "
        + "[foodmart, position], "
        + "[foodmart, product], "
        + "[foodmart, product_class], "
        + "[foodmart, promotion], "
        + "[foodmart, region], "
        + "[foodmart, salary], "
        + "[foodmart, sales_fact_1997], "
        + "[foodmart, store], "
        + "[foodmart, store_ragged], "
        + "[foodmart, time_by_day], "
        + "[foodmart, warehouse], "
        + "[foodmart, warehouse_class]], "
        + "edges: ["
        + "Step([foodmart, agg_c_14_sales_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, customer], [foodmart, region], customer_region_id:region_id), "
        + "Step([foodmart, employee], [foodmart, employee], supervisor_id:employee_id), "
        + "Step([foodmart, employee], [foodmart, position], position_id:position_id), "
        + "Step([foodmart, employee], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, employee], product_id:employee_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, employee], time_id:employee_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, product], product_id:product_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, store], warehouse_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, time_by_day], time_id:time_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, warehouse],"
        + " warehouse_id:warehouse_id), "
        + "Step([foodmart, product], [foodmart, product_class],"
        + " product_class_id:product_class_id), "
        + "Step([foodmart, product], [foodmart, store], product_class_id:store_id), "
        + "Step([foodmart, salary], [foodmart, department], department_id:department_id), "
        + "Step([foodmart, salary], [foodmart, employee], employee_id:employee_id), "
        + "Step([foodmart, salary], [foodmart, employee_closure], employee_id:employee_id), "
        + "Step([foodmart, salary], [foodmart, time_by_day], pay_date:the_date), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], customer_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], product_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], store_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, product], product_id:product_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, promotion], promotion_id:promotion_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store], product_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store_ragged], store_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, time_by_day], product_id:time_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, time_by_day], time_id:time_id), "
        + "Step([foodmart, store], [foodmart, customer], store_state:state_province), "
        + "Step([foodmart, store], [foodmart, product_class], region_id:product_class_id), "
        + "Step([foodmart, store], [foodmart, region], region_id:region_id), "
        + "Step([foodmart, time_by_day], [foodmart, agg_c_14_sales_fact_1997], month_of_year:month_of_year), "
        + "Step([foodmart, warehouse], [foodmart, store], stores_id:store_id), "
        + "Step([foodmart, warehouse], [foodmart, warehouse_class],"
        + " warehouse_class_id:warehouse_class_id)])";
    assertThat(t.s.space.g, hasToString(expected));
    if (evolve) {
      // compared to evolve=false, there are a few more nodes (137 vs 119),
      // the same number of paths, and a lot fewer lattices (27 vs 388)
      assertThat(t.s.space.nodeMap, aMapWithSize(137));
      assertThat(t.s.latticeMap, aMapWithSize(27));
      assertThat(t.s.space.pathMap, aMapWithSize(46));
    } else {
      assertThat(t.s.space.nodeMap, aMapWithSize(119));
      assertThat(t.s.latticeMap, aMapWithSize(388));
      assertThat(t.s.space.pathMap, aMapWithSize(46));
    }
  }

  @Tag("slow")
  @Test void testFoodMartAll() throws Exception {
    checkFoodMartAll(false);
  }

  @Tag("slow")
  @Test void testFoodMartAllEvolve() throws Exception {
    checkFoodMartAll(true);
  }

  @Test void testContains() throws Exception {
    final Tester t = new Tester().foodmart();
    final LatticeRootNode fNode = t.node("select *\n"
        + "from \"sales_fact_1997\"");
    final LatticeRootNode fcNode = t.node("select *\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"customer\" using (\"customer_id\")");
    final LatticeRootNode fcpNode = t.node("select *\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"customer\" using (\"customer_id\")\n"
        + "join \"product\" using (\"product_id\")");
    assertThat(fNode.contains(fNode), is(true));
    assertThat(fNode.contains(fcNode), is(false));
    assertThat(fNode.contains(fcpNode), is(false));
    assertThat(fcNode.contains(fNode), is(true));
    assertThat(fcNode.contains(fcNode), is(true));
    assertThat(fcNode.contains(fcpNode), is(false));
    assertThat(fcpNode.contains(fNode), is(true));
    assertThat(fcpNode.contains(fcNode), is(true));
    assertThat(fcpNode.contains(fcpNode), is(true));
  }

  @Test void testEvolve() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);

    final String q0 = "select count(*)\n"
        + "from \"sales_fact_1997\"";
    final String l0 = "sales_fact_1997:[COUNT()]";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l0));

    final String q1 = "select sum(\"unit_sales\")\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"customer\" using (\"customer_id\")\n"
        + "group by \"customer\".\"city\"";
    final String l1 = "sales_fact_1997 (customer:customer_id)"
        + ":[COUNT(), SUM(sales_fact_1997.unit_sales)]";
    t.addQuery(q1);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l1));

    final String q2 = "select count(distinct \"the_day\")\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"time_by_day\" using (\"time_id\")\n"
        + "join \"product\" using (\"product_id\")";
    final String l2 = "sales_fact_1997"
        + " (customer:customer_id product:product_id time_by_day:time_id)"
        + ":[COUNT(), SUM(sales_fact_1997.unit_sales),"
        + " COUNT(DISTINCT time_by_day.the_day)]";
    t.addQuery(q2);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l2));

    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final List<List<String>> tableNames =
        lattice.tables().stream().map(table ->
            table.t.getQualifiedName())
            .sorted(Comparator.comparing(Object::toString))
            .collect(toImmutableList());
    assertThat(tableNames,
        hasToString("[[foodmart, customer],"
            + " [foodmart, product],"
            + " [foodmart, sales_fact_1997],"
            + " [foodmart, time_by_day]]"));

    final String q3 = "select min(\"product\".\"product_id\")\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"product\" using (\"product_id\")\n"
        + "join \"product_class\" as pc using (\"product_class_id\")\n"
        + "group by pc.\"product_department\"";
    final String l3 = "sales_fact_1997"
        + " (customer:customer_id product:product_id"
        + " (product_class:product_class_id) time_by_day:time_id)"
        + ":[COUNT(), SUM(sales_fact_1997.unit_sales),"
        + " MIN(product.product_id), COUNT(DISTINCT time_by_day.the_day)]";
    t.addQuery(q3);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l3));
  }

  @Test void testExpression() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);

    final String q0 = "select\n"
        + "  \"fname\" || ' ' || \"lname\" as \"full_name\",\n"
        + "  count(*) as c,\n"
        + "  avg(\"total_children\" - \"num_children_at_home\")\n"
        + "from \"customer\"\n"
        + "group by \"fname\", \"lname\"";
    final String l0 = "customer:[COUNT(), AVG($f2)]";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l0));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final List<Lattice.DerivedColumn> derivedColumns = lattice.columns.stream()
        .filter(c -> c instanceof Lattice.DerivedColumn)
        .map(c -> (Lattice.DerivedColumn) c)
        .collect(Collectors.toList());
    assertThat(derivedColumns, hasSize(2));
    final List<String> tables = ImmutableList.of("customer");
    checkDerivedColumn(lattice, tables, derivedColumns, 0, "$f2", true);
    checkDerivedColumn(lattice, tables, derivedColumns, 1, "full_name", false);
  }

  /** As {@link #testExpression()} but with multiple queries.
   * Some expressions are measures in one query and dimensions in another. */
  @Test void testExpressionEvolution() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);

    // q0 uses n10 as a measure, n11 as a measure, n12 as a dimension
    final String q0 = "select\n"
        + "  \"num_children_at_home\" + 12 as \"n12\",\n"
        + "  sum(\"num_children_at_home\" + 10) as \"n10\",\n"
        + "  sum(\"num_children_at_home\" + 11) as \"n11\",\n"
        + "  count(*) as c\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 12";
    // q1 uses n10 as a dimension, n12 as a measure
    final String q1 = "select\n"
        + "  \"num_children_at_home\" + 10 as \"n10\",\n"
        + "  \"num_children_at_home\" + 14 as \"n14\",\n"
        + "  sum(\"num_children_at_home\" + 12) as \"n12\",\n"
        + "  sum(\"num_children_at_home\" + 13) as \"n13\"\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 10,"
        + "   \"num_children_at_home\" + 14";
    // n10 = [measure, dimension] -> not always measure
    // n11 = [measure, _] -> always measure
    // n12 = [dimension, measure] -> not always measure
    // n13 = [_, measure] -> always measure
    // n14 = [_, dimension] -> not always measure
    t.addQuery(q0);
    t.addQuery(q1);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    final String l0 =
        "customer:[COUNT(), SUM(n10), SUM(n11), SUM(n12), SUM(n13)]";
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l0));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final List<Lattice.DerivedColumn> derivedColumns = lattice.columns.stream()
        .filter(c -> c instanceof Lattice.DerivedColumn)
        .map(c -> (Lattice.DerivedColumn) c)
        .collect(Collectors.toList());
    assertThat(derivedColumns, hasSize(5));
    final List<String> tables = ImmutableList.of("customer");

    checkDerivedColumn(lattice, tables, derivedColumns, 0, "n10", false);
    checkDerivedColumn(lattice, tables, derivedColumns, 1, "n11", true);
    checkDerivedColumn(lattice, tables, derivedColumns, 2, "n12", false);
    checkDerivedColumn(lattice, tables, derivedColumns, 3, "n13", true);
    checkDerivedColumn(lattice, tables, derivedColumns, 4, "n14", false);
  }

  private void checkDerivedColumn(Lattice lattice, List<String> tables,
      List<Lattice.DerivedColumn> derivedColumns,
      int index, String name, boolean alwaysMeasure) {
    final Lattice.DerivedColumn dc0 = derivedColumns.get(index);
    assertThat(dc0.tables, is(tables));
    assertThat(dc0.alias, is(name));
    assertThat(lattice.isAlwaysMeasure(dc0), is(alwaysMeasure));
  }

  @Test void testExpressionInJoin() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);

    final String q0 = "select\n"
        + "  \"fname\" || ' ' || \"lname\" as \"full_name\",\n"
        + "  count(*) as c,\n"
        + "  avg(\"total_children\" - \"num_children_at_home\")\n"
        + "from \"customer\" join \"sales_fact_1997\" using (\"customer_id\")\n"
        + "group by \"fname\", \"lname\"";
    final String l0 = "sales_fact_1997 (customer:customer_id)"
        + ":[COUNT(), AVG($f2)]";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l0));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final List<Lattice.DerivedColumn> derivedColumns = lattice.columns.stream()
        .filter(c -> c instanceof Lattice.DerivedColumn)
        .map(c -> (Lattice.DerivedColumn) c)
        .collect(Collectors.toList());
    assertThat(derivedColumns, hasSize(2));
    assertThat(derivedColumns.get(0).tables, isListOf("customer"));
    assertThat(derivedColumns.get(1).tables, isListOf("customer"));
  }

  /** Tests a number of features only available in Redshift: the {@code CONCAT}
   * and {@code CONVERT_TIMEZONE} functions. */
  @Test void testRedshiftDialect() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true)
        .withDialect(SqlDialect.DatabaseProduct.REDSHIFT.getDialect())
        .withLibrary(SqlLibrary.REDSHIFT);

    final String q0 = "select\n"
        // CONCAT function in RedShift only accepts two arguments
        + "  CONCAT(\"fname\", \"lname\") as \"full_name\",\n"
        + "  convert_timezone('UTC', 'America/Los_Angeles',\n"
        + "  cast('2019-01-01 01:00:00' as timestamp)),\n"
        + "  left(\"fname\", 1) as \"initial\",\n"
        + "  to_date('2019-01-01', 'YYYY-MM-DD'),\n"
        + "  to_timestamp('2019-01-01 01:00:00', 'YYYY-MM-DD HH:MM:SS'),\n"
        + "  count(*) as c,\n"
        + "  avg(\"total_children\" - \"num_children_at_home\")\n"
        + "from \"customer\" join \"sales_fact_1997\" using (\"customer_id\")\n"
        + "group by \"fname\", \"lname\"";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
  }

  /** Tests a number of features only available in BigQuery: back-ticks;
   * GROUP BY ordinal; case-insensitive unquoted identifiers;
   * the {@code COUNTIF} aggregate function. */
  @Test void testBigQueryDialect() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true)
        .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
        .withLibrary(SqlLibrary.BIG_QUERY);

    final String q0 = "select `product_id`,\n"
        + "  countif(unit_sales > 1000) as num_over_thousand,\n"
        + "  SUM(unit_sales)\n"
        + "from\n"
        + "  `sales_fact_1997`"
        + "group by 1";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
  }

  /** A tricky case involving a CTE (WITH), a join condition that references an
   * expression, a complex WHERE clause, and some other queries. */
  @Test void testJoinUsingExpression() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);

    final String q0 = "with c as (select\n"
        + "    \"customer_id\" + 1 as \"customer_id\",\n"
        + "    \"fname\"\n"
        + "  from \"customer\")\n"
        + "select\n"
        + "  COUNT(distinct c.\"customer_id\") as \"customer.count\"\n"
        + "from c\n"
        + "left join \"sales_fact_1997\" using (\"customer_id\")\n"
        + "where case\n"
        + "  when lower(substring(\"fname\", 11, 1)) in (0, 1)\n"
        + "    then 'Amy Adams'\n"
        + "  when lower(substring(\"fname\", 11, 1)) in (2, 3)\n"
        + "    then 'Barry Manilow'\n"
        + "  when lower(substring(\"fname\", 11, 1)) in ('y', 'z')\n"
        + "   then 'Yvonne Zane'\n"
        + "  end = 'Barry Manilow'\n"
        + "LIMIT 500";
    final String q1 = "select * from \"customer\"";
    final String q2 = "select sum(\"product_id\") from \"product\"";
    // similar to q0, but "c" is a sub-select rather than CTE
    final String q4 = "select\n"
        + "  COUNT(distinct c.\"customer_id\") as \"customer.count\"\n"
        + "from (select \"customer_id\" + 1 as \"customer_id\", \"fname\"\n"
        + "  from \"customer\") as c\n"
        + "left join \"sales_fact_1997\" using (\"customer_id\")\n";
    t.addQuery(q1);
    t.addQuery(q0);
    t.addQuery(q1);
    t.addQuery(q4);
    t.addQuery(q2);
    assertThat(t.s.latticeMap, aMapWithSize(3));
  }

  @Test void testDerivedColRef() throws Exception {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(Tester.schemaFrom(CalciteAssert.SchemaSpec.SCOTT))
        .statisticProvider(QuerySqlStatisticProvider.SILENT_CACHING_INSTANCE)
        .build();
    final Tester t = new Tester(config).foodmart().withEvolve(true);

    final String q0 = "select\n"
        + "  min(c.\"fname\") as \"customer.count\"\n"
        + "from \"customer\" as c\n"
        + "left join \"sales_fact_1997\" as s\n"
        + "on c.\"customer_id\" + 1 = s.\"customer_id\" + 2";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(t.s.latticeMap.keySet().iterator().next(),
        is("sales_fact_1997 (customer:+($2, 2)):[MIN(customer.fname)]"));
    assertThat(t.s.space.g,
        hasToString("graph(vertices: [[foodmart, customer],"
            + " [foodmart, sales_fact_1997]], "
            + "edges: [Step([foodmart, sales_fact_1997],"
            + " [foodmart, customer], +($2, 2):+($0, 1))])"));
  }

  /** Tests that we can run the suggester against non-JDBC schemas.
   *
   * <p>{@link org.apache.calcite.test.CalciteAssert.SchemaSpec#FAKE_FOODMART}
   * is not based on {@link org.apache.calcite.adapter.jdbc.JdbcSchema} or
   * {@link org.apache.calcite.adapter.jdbc.JdbcTable} but can provide a
   * {@link javax.sql.DataSource}
   * and {@link SqlDialect} for executing statistics queries.
   *
   * <p>The query has a join, and so we have to execute statistics queries
   * to deduce the direction of the foreign key.
   */
  @Test void testFoodmartSimpleJoin() throws Exception {
    checkFoodmartSimpleJoin(CalciteAssert.SchemaSpec.JDBC_FOODMART);
    checkFoodmartSimpleJoin(CalciteAssert.SchemaSpec.FAKE_FOODMART);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6374">[CALCITE-6374]
   * LatticeSuggester throw NullPointerException when agg call covered with cast </a>. */
  @Test void testCastAggrNameExpression() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);
    final String q0 = "select\n"
        + "  \"num_children_at_home\" + 12 as \"n12\",\n"
        + "  sum(\"num_children_at_home\" + 10) as \"n10\",\n"
        + "  cast(sum(\"num_children_at_home\" + 11) as double) as \"n11\",\n"
        + "  count(*) as c\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 12";
    final String l0 = "customer:[COUNT(), SUM(n10), SUM($f2)]";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()),
        is(l0));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final List<Lattice.DerivedColumn> derivedColumns = lattice.columns.stream()
        .filter(c -> c instanceof Lattice.DerivedColumn)
        .map(c -> (Lattice.DerivedColumn) c)
        .collect(Collectors.toList());
    assertThat(derivedColumns, hasSize(4));
    final List<String> tables = ImmutableList.of("customer");
    checkDerivedColumn(lattice, tables, derivedColumns, 0, "n10", true);
    checkDerivedColumn(lattice, tables, derivedColumns, 1, "$f2", true);
    checkDerivedColumn(lattice, tables, derivedColumns, 2, "n12", false);
    checkDerivedColumn(lattice, tables, derivedColumns, 3, "n11", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6605">[CALCITE-6605]
   * Lattice SQL supports complex column expressions </a>. */
  @Test void testExpressionLatticeSql() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);
    final String q0 = "select\n"
        + "  \"num_children_at_home\" + 12 as \"n12\",\n"
        + "  sum(\"num_children_at_home\") as \"n10\",\n"
        + "  count(*) as c\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 12";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final String l0 = "customer:[COUNT(), SUM(customer.num_children_at_home)]";
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()), is(l0));
    ImmutableList<Measure> measures = lattice.defaultMeasures;
    assert measures.size() == 2;
    Builder groupSetBuilder = ImmutableBitSet.builder();
    measures.forEach(measure -> groupSetBuilder.addAll(measure.argBitSet()));
    ImmutableBitSet groupSet = groupSetBuilder.build();
    String sql = "SELECT \"customer\".\"num_children_at_home\", COUNT(*) AS \"m0\", "
        + "SUM(\"customer\".\"num_children_at_home\") AS \"m1\"\n"
        + "FROM \"foodmart\".\"customer\" AS \"customer\"\n"
        + "GROUP BY \"customer\".\"num_children_at_home\"";
    assertThat(lattice.sql(groupSet, true, measures),
        is(sql));
  }

  /** Test case for measure field involving a complex column operation,
   * for example sum("num_children_at_home" + 10). */
  @Test void testExpressionLatticeSql2() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);
    final String q0 = "select\n"
        + "  \"num_children_at_home\" + 12 as \"n12\",\n"
        + "  sum(\"num_children_at_home\" + 10) as \"n10\",\n"
        + "  sum(\"num_children_at_home\" + 11) as \"n11\",\n"
        + "  count(*) as c\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 12";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final String l0 = "customer:[COUNT(), SUM(n10), SUM(n11)]";
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()), is(l0));
    ImmutableList<Measure> measures = lattice.defaultMeasures;
    assert measures.size() == 3;
    Builder groupSetBuilder = ImmutableBitSet.builder();
    measures.forEach(measure -> groupSetBuilder.addAll(measure.argBitSet()));
    ImmutableBitSet groupSet = groupSetBuilder.build();
    String sql = "SELECT \"num_children_at_home\" + 10 AS \"n10\", "
        + "\"num_children_at_home\" + 11 AS \"n11\", COUNT(*) AS \"m0\", "
        + "SUM(\"num_children_at_home\" + 10) AS \"m1\", "
        + "SUM(\"num_children_at_home\" + 11) AS \"m2\"\n"
        + "FROM \"foodmart\".\"customer\" AS \"customer\"\n"
        + "GROUP BY \"num_children_at_home\" + 10, \"num_children_at_home\" + 11";
    assertThat(lattice.sql(groupSet, true, measures),
        is(sql));
  }

  /** Test case for measure field involving a complex column operation with functions,
   * for example sum(cast("num_children_at_home" as double) + 11). */
  @Test void testExpressionLatticeSql3() throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);
    final String q0 = "select\n"
        + "  \"num_children_at_home\" + 12 as \"n12\",\n"
        + "  sum(\"num_children_at_home\" + 10) as \"n10\",\n"
        + "  sum(cast(\"num_children_at_home\" as double) + 11) as \"n11\",\n"
        + "  count(*) as c\n"
        + "from \"customer\"\n"
        + "group by \"num_children_at_home\" + 12";
    t.addQuery(q0);
    assertThat(t.s.latticeMap, aMapWithSize(1));
    final Lattice lattice = Iterables.getOnlyElement(t.s.latticeMap.values());
    final String l0 = "customer:[COUNT(), SUM(n10), SUM(n11)]";
    assertThat(Iterables.getOnlyElement(t.s.latticeMap.keySet()), is(l0));
    ImmutableList<Measure> measures = lattice.defaultMeasures;
    assert measures.size() == 3;
    Builder groupSetBuilder = ImmutableBitSet.builder();
    measures.forEach(measure -> groupSetBuilder.addAll(measure.argBitSet()));
    ImmutableBitSet groupSet = groupSetBuilder.build();
    String sql = "SELECT \"num_children_at_home\" + 10 AS \"n10\", "
        + "CAST(\"num_children_at_home\" AS DOUBLE) + 11 AS \"n11\", "
        + "COUNT(*) AS \"m0\", SUM(\"num_children_at_home\" + 10) AS \"m1\", "
        + "SUM(CAST(\"num_children_at_home\" AS DOUBLE) + 11) AS \"m2\"\n"
        + "FROM \"foodmart\".\"customer\" AS \"customer\"\n"
        + "GROUP BY \"num_children_at_home\" + 10, CAST(\"num_children_at_home\" AS DOUBLE) + 11";
    assertThat(lattice.sql(groupSet, true, measures),
        is(sql));
  }

  private void checkFoodmartSimpleJoin(CalciteAssert.SchemaSpec schemaSpec)
      throws Exception {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(Tester.schemaFrom(schemaSpec))
        .statisticProvider(QuerySqlStatisticProvider.SILENT_CACHING_INSTANCE)
        .build();
    final Tester t = new Tester(config);
    final String q = "select *\n"
        + "from \"time_by_day\" as \"t\",\n"
        + " \"sales_fact_1997\" as \"s\"\n"
        + "where \"s\".\"time_id\" = \"t\".\"time_id\"\n";
    final String g = "sales_fact_1997 (time_by_day:time_id)";
    assertThat(t.addQuery(q), isGraphs(g, "[]"));
  }

  @Test void testUnion() throws Exception {
    checkUnion("union");
    checkUnion("union all");
    checkUnion("intersect");
    checkUnion("except");
  }

  private void checkUnion(String setOp) throws Exception {
    final Tester t = new Tester().foodmart().withEvolve(true);
    final String q = "select \"t\".\"time_id\"\n"
        + "from \"time_by_day\" as \"t\",\n"
        + " \"sales_fact_1997\" as \"s\"\n"
        + "where \"s\".\"time_id\" = \"t\".\"time_id\"\n"
        + setOp + "\n"
        + "select min(\"unit_sales\")\n"
        + "from \"sales_fact_1997\" as \"s\" join \"product\" as \"p\"\n"
        + " using (\"product_id\")\n"
        + "group by \"s\".\"customer_id\"";

    // Adding a query generates two lattices
    final List<Lattice> latticeList = t.addQuery(q);
    assertThat(latticeList, hasSize(2));

    // But because of 'evolve' flag, the lattices are merged into a single
    // lattice
    final String g = "sales_fact_1997 (product:product_id time_by_day:time_id)";
    final String measures = "[MIN(sales_fact_1997.unit_sales)]";
    assertThat(t.s.getLatticeSet(), isGraphs(g, measures));
  }

  /** Creates a matcher that matches query graphs to strings. */
  private BaseMatcher<Collection<Lattice>> isGraphs(
      String... strings) {
    final List<String> expectedList = Arrays.asList(strings);
    return new BaseMatcher<Collection<Lattice>>() {
      public boolean matches(Object item) {
        //noinspection unchecked
        return item instanceof Collection
            && ((Collection<Object>) item).size() * 2 == expectedList.size()
            && allEqual(ImmutableList.copyOf((Collection) item), expectedList);
      }

      private boolean allEqual(List<Lattice> items,
          List<String> expects) {
        for (int i = 0; i < items.size(); i++) {
          final Lattice lattice = items.get(i);
          final String expectedNode = expects.get(2 * i);
          if (!lattice.rootNode.digest.equals(expectedNode)) {
            return false;
          }
          final String expectedMeasures = expects.get(2 * i + 1);
          if (!lattice.defaultMeasures.toString().equals(expectedMeasures)) {
            return false;
          }
        }
        return true;
      }

      public void describeTo(Description description) {
        description.appendValue(expectedList);
      }
    };
  }

  /** Test helper. */
  private static class Tester {
    final LatticeSuggester s;
    private final FrameworkConfig config;

    Tester() {
      this(
          Frameworks.newConfigBuilder()
              .defaultSchema(schemaFrom(CalciteAssert.SchemaSpec.SCOTT))
              .statisticProvider(MapSqlStatisticProvider.INSTANCE)
              .build());
    }

    private Tester(FrameworkConfig config) {
      this.config = config;
      s = new LatticeSuggester(config);
    }

    Tester withConfig(FrameworkConfig config) {
      return new Tester(config);
    }

    Tester sqlToRelConverter(UnaryOperator<SqlToRelConverter.Config> sqlToRelConverterConfig) {
      return withConfig(
          builder().sqlToRelConverterConfig(sqlToRelConverterConfig
              .apply(config.getSqlToRelConverterConfig())).build());
    }

    Tester foodmart() {
      return schema(CalciteAssert.SchemaSpec.JDBC_FOODMART);
    }

    private Tester schema(CalciteAssert.SchemaSpec schemaSpec) {
      return withConfig(builder()
          .defaultSchema(schemaFrom(schemaSpec))
          .build());
    }

    private Frameworks.ConfigBuilder builder() {
      return Frameworks.newConfigBuilder(config);
    }

    List<Lattice> addQuery(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final Planner planner = new PlannerImpl(config);
      final SqlNode node = planner.parse(q);
      final SqlNode node2 = planner.validate(node);
      final RelRoot root = planner.rel(node2);
      return s.addQuery(root.project());
    }

    /** Parses a query returns its graph. */
    LatticeRootNode node(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final List<Lattice> list = addQuery(q);
      assertThat(list, hasSize(1));
      return list.get(0).rootNode;
    }

    private static SchemaPlus schemaFrom(CalciteAssert.SchemaSpec spec) {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      return CalciteAssert.addSchema(rootSchema, spec);
    }

    Tester withEvolve(boolean evolve) {
      return withConfig(builder().evolveLattice(evolve).build());
    }

    private Tester withParser(UnaryOperator<SqlParser.Config> transform) {
      return withConfig(
          builder()
              .parserConfig(transform.apply(config.getParserConfig()))
              .build());
    }

    Tester withDialect(SqlDialect dialect) {
      return withParser(dialect::configureParser);
    }

    Tester withLibrary(SqlLibrary library) {
      SqlOperatorTable opTab = SqlLibraryOperatorTableFactory.INSTANCE
          .getOperatorTable(EnumSet.of(SqlLibrary.STANDARD, library));
      return withConfig(builder().operatorTable(opTab).build());
    }
  }
}
