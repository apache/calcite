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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.runtime.Hook;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.util.TestUtil;
import org.eigenbase.util.Util;

import com.google.common.base.Function;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit test for lattices.
 */
public class LatticeTest {
  private OptiqAssert.AssertThat modelWithLattice(String name, String sql) {
    return modelWithLattices(
        "{ name: '" + name + "', sql: " + TestUtil.escapeString(sql) + "}");
  }

  private OptiqAssert.AssertThat modelWithLattices(String... lattices) {
    final Class<JdbcTest.EmpDeptTableFactory> clazz =
        JdbcTest.EmpDeptTableFactory.class;
    return OptiqAssert.that().withModel(""
        + "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + JdbcTest.FOODMART_SCHEMA
        + ",\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + clazz.getName()
        + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       lattices: "
        + Arrays.toString(lattices)
        + "     }\n"
        + "   ]\n"
        + "}").withSchema("adhoc");
  }

  /** Tests that it's OK for a lattice to have the same name as a table in the
   * schema. */
  @Test public void testLatticeWithSameNameAsTable() {
    modelWithLattice("EMPLOYEES", "select * from \"foodmart\".\"days\"")
        .query("select count(*) from EMPLOYEES")
        .returnsValue("4");
  }

  /** Tests that it's an error to have two lattices with the same name in a
   * schema. */
  @Test public void testTwoLatticesWithSameNameFails() {
    modelWithLattices(
        "{name: 'Lattice1', sql: 'select * from \"foodmart\".\"days\"'}",
        "{name: 'Lattice1', sql: 'select * from \"foodmart\".\"time_by_day\"'}")
        .connectThrows("Duplicate lattice 'Lattice1'");
  }

  /** Tests a lattice whose SQL is invalid. */
  @Test public void testLatticeInvalidSqlFails() {
    modelWithLattice("star", "select foo from nonexistent")
        .connectThrows("Error instantiating JsonLattice(name=star, ")
        .connectThrows("Table 'NONEXISTENT' not found");
  }

  /** Tests a lattice whose SQL is invalid because it contains a GROUP BY. */
  @Test public void testLatticeSqlWithGroupByFails() {
    modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s group by \"product_id\"")
        .connectThrows("Invalid node type AggregateRel in lattice query");
  }

  /** Tests a lattice whose SQL is invalid because it contains a ORDER BY. */
  @Test public void testLatticeSqlWithOrderByFails() {
    modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s order by \"product_id\"")
        .connectThrows("Invalid node type SortRel in lattice query");
  }

  /** Tests a lattice whose SQL is invalid because it contains a UNION ALL. */
  @Test public void testLatticeSqlWithUnionFails() {
    modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
        + "union all\n"
        + "select 1 from \"foodmart\".\"sales_fact_1997\" as s")
        .connectThrows("Invalid node type UnionRel in lattice query");
  }

  /** Tests a lattice with valid join SQL. */
  @Test public void testLatticeSqlWithJoin() {
    foodmartModel()
        .query("values 1")
        .returnsValue("1");
  }

  /** Tests a lattice with invalid SQL (for a lattice). */
  @Test public void testLatticeInvalidSql() {
    modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
            + "join \"foodmart\".\"time_by_day\" as t on s.\"product_id\" = 100")
        .connectThrows("only equi-join of columns allowed: 100");
  }

  /** Left join is invalid in a lattice. */
  @Test public void testLatticeInvalidSql2() {
    modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
        + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
        + "left join \"foodmart\".\"time_by_day\" as t on s.\"product_id\" = p.\"product_id\"")
        .connectThrows("only inner join allowed, but got LEFT");
  }

  /** When a lattice is registered, there is a table with the same name.
   * It can be used for explain, but not for queries. */
  @Test public void testLatticeStarTable() {
    final AtomicInteger counter = new AtomicInteger();
    try {
      foodmartModel()
          .query("select count(*) from \"adhoc\".\"star\"")
          .convertMatches(
              OptiqAssert.checkRel(
                  "AggregateRel(group=[{}], EXPR$0=[COUNT()])\n"
                  + "  ProjectRel(DUMMY=[0])\n"
                  + "    StarTableScan(table=[[adhoc, star]])\n",
                  counter));
    } catch (RuntimeException e) {
      assertThat(Util.getStackTrace(e), containsString("CannotPlanException"));
    }
    assertThat(counter.get(), equalTo(1));
  }

  /** Tests that a 2-way join query can be mapped 4-way join lattice. */
  @Test public void testLatticeRecognizeJoin() {
    final AtomicInteger counter = new AtomicInteger();
    foodmartModel()
        .query(
            "select s.\"unit_sales\", p.\"brand_name\"\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"product\" as p using (\"product_id\")\n")
        .enableMaterializations(true)
        .substitutionMatches(
            OptiqAssert.checkRel(
                "ProjectRel(unit_sales=[$7], brand_name=[$10])\n"
                + "  ProjectRel($f0=[$0], $f1=[$1], $f2=[$2], $f3=[$3], $f4=[$4], $f5=[$5], $f6=[$6], $f7=[$7], $f8=[$8], $f9=[$9], $f10=[$10], $f11=[$11], $f12=[$12], $f13=[$13], $f14=[$14], $f15=[$15], $f16=[$16], $f17=[$17], $f18=[$18], $f19=[$19], $f20=[$20], $f21=[$21], $f22=[$22])\n"
                + "    TableAccessRel(table=[[adhoc, star]])\n",
                counter));
    assertThat(counter.intValue(), equalTo(1));
  }

  /** Tests an aggregate on a 2-way join query can use an aggregate table. */
  @Test public void testLatticeRecognizeGroupJoin() {
    final AtomicInteger counter = new AtomicInteger();
    OptiqAssert.AssertQuery that = foodmartModel()
        .query(
            "select distinct p.\"brand_name\", s.\"customer_id\"\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"product\" as p using (\"product_id\")\n")
        .enableMaterializations(true)
        .substitutionMatches(
            new Function<RelNode, Void>() {
              public Void apply(RelNode relNode) {
                counter.incrementAndGet();
                String s = Util.toLinux(RelOptUtil.toString(relNode));
                assertThat(s,
                    anyOf(
                        containsString(
                            "AggregateRel(group=[{0, 1}])\n"
                            + "  ProjectRel(brand_name=[$10], customer_id=[$2])\n"
                            + "    ProjectRel($f0=[$0], $f1=[$1], $f2=[$2], $f3=[$3], $f4=[$4], $f5=[$5], $f6=[$6], $f7=[$7], $f8=[$8], $f9=[$9], $f10=[$10], $f11=[$11], $f12=[$12], $f13=[$13], $f14=[$14], $f15=[$15], $f16=[$16], $f17=[$17], $f18=[$18], $f19=[$19], $f20=[$20], $f21=[$21], $f22=[$22])\n"
                            + "      TableAccessRel(table=[[adhoc, star]])\n"),
                        containsString(
                            "AggregateRel(group=[{0, 1}])\n"
                            + "  ProjectRel(customer_id=[$2], brand_name=[$10])\n"
                            + "    ProjectRel($f0=[$0], $f1=[$1], $f2=[$2], $f3=[$3], $f4=[$4], $f5=[$5], $f6=[$6], $f7=[$7], $f8=[$8], $f9=[$9], $f10=[$10], $f11=[$11], $f12=[$12], $f13=[$13], $f14=[$14], $f15=[$15], $f16=[$16], $f17=[$17], $f18=[$18], $f19=[$19], $f20=[$20], $f21=[$21], $f22=[$22])\n"
                            + "      TableAccessRel(table=[[adhoc, star]])\n")));
                return null;
              }
            });
    assertThat(counter.intValue(), equalTo(2));
    that.explainContains(
        "EnumerableCalcRel(expr#0..1=[{inputs}], $f0=[$t1], $f1=[$t0])\n"
        + "  EnumerableTableAccessRel(table=[[adhoc, m{2, 10}]])")
        .returnsCount(69203);

    // Run the same query again and see whether it uses the same
    // materialization.
    that.withHook(
        Hook.CREATE_MATERIALIZATION,
        new Function<String, Void>() {
          public Void apply(String materializationName) {
            counter.incrementAndGet();
            return null;
          }
        })
        .returnsCount(69203);

    // Ideally the counter would stay at 2. It increments to 3 because
    // OptiqAssert.AssertQuery creates a new schema for every request,
    // and therefore cannot re-use lattices or materializations from the
    // previous request.
    assertThat(counter.intValue(), equalTo(3));
  }

  private OptiqAssert.AssertThat foodmartModel() {
    return modelWithLattice("star",
        "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
        + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
        + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n"
        + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"");
  }
}

// End LatticeTest.java
