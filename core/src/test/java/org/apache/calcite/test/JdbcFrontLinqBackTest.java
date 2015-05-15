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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.test.CalciteAssert.hr;
import static org.apache.calcite.test.CalciteAssert.that;

/**
 * Tests for a JDBC front-end (with some quite complex SQL) and Linq4j back-end
 * (based on in-memory collections).
 */
public class JdbcFrontLinqBackTest {
  /**
   * Runs a simple query that reads from a table in an in-memory schema.
   */
  @Test public void testSelect() {
    hr()
        .query("select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where s.\"cust_id\" = 100")
        .returns("cust_id=100; prod_id=10\n");
  }

  /**
   * Runs a simple query that joins between two in-memory schemas.
   */
  @Test public void testJoin() {
    hr()
        .query("select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"hr\".\"emps\" as e\n"
            + "on e.\"empid\" = s.\"cust_id\"")
        .returnsUnordered(
            "cust_id=100; prod_id=10; empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
            "cust_id=150; prod_id=20; empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null");
  }

  /**
   * Simple GROUP BY.
   */
  @Test public void testGroupBy() {
    hr()
        .query("select \"deptno\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"hr\".\"emps\" as e\n"
            + "group by \"deptno\"")
        .returns("deptno=20; S=200; C=1\n"
            + "deptno=10; S=360; C=3\n");
  }

  /**
   * Simple ORDER BY.
   */
  @Test public void testOrderBy() {
    hr()
        .query("select upper(\"name\") as un, \"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "order by \"deptno\", \"name\" desc")
        .explainContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[UPPER($t1)], UN=[$t2], deptno=[$t0])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], deptno=[$t1], name=[$t2])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])")
        .returns("UN=THEODORE; deptno=10\n"
            + "UN=SEBASTIAN; deptno=10\n"
            + "UN=BILL; deptno=10\n"
            + "UN=ERIC; deptno=20\n");
  }

  /**
   * Simple UNION, plus ORDER BY.
   *
   * <p>Also tests a query that returns a single column. We optimize this case
   * internally, using non-array representations for rows.</p>
   */
  @Test public void testUnionAllOrderBy() {
    hr()
        .query("select \"name\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "union all\n"
            + "select \"name\"\n"
            + "from \"hr\".\"depts\"\n"
            + "order by 1 desc")
        .returns("name=Theodore\n"
            + "name=Sebastian\n"
            + "name=Sales\n"
            + "name=Marketing\n"
            + "name=HR\n"
            + "name=Eric\n"
            + "name=Bill\n");
  }

  /**
   * Tests UNION.
   */
  @Test public void testUnion() {
    hr()
        .query("select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "union\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"")
        .returnsUnordered(
            "X=T",
            "X=E",
            "X=S",
            "X=B",
            "X=M",
            "X=H");
  }

  /**
   * Tests INTERSECT.
   */
  @Ignore
  @Test public void testIntersect() {
    hr()
        .query("select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "intersect\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"")
        .returns("X=S\n");
  }

  /**
   * Tests EXCEPT.
   */
  @Ignore
  @Test public void testExcept() {
    hr()
        .query("select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "except\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"")
        .returnsUnordered(
            "X=T",
            "X=E",
            "X=B");
  }

  @Test public void testWhereBad() {
    hr()
        .query("select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where empid > 120")
        .throws_("Column 'EMPID' not found in any table");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-9">CALCITE-9</a>,
   * "RexToLixTranslator not incrementing local variable name counter". */
  @Test public void testWhereOr() {
    hr()
        .query("select * from \"hr\".\"emps\"\n"
            + "where (\"empid\" = 100 or \"empid\" = 200)\n"
            + "and \"deptno\" = 10")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  @Test public void testWhereLike() {
    hr()
        .query("select *\n"
            + "from \"hr\".\"emps\" as e\n"
            + "where e.\"empid\" < 120 or e.\"name\" like 'S%'")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
  }

  @Test public void testInsert() {
    final List<JdbcTest.Employee> employees =
        new ArrayList<JdbcTest.Employee>();
    CalciteAssert.AssertThat with = mutable(employees);
    with.query("select * from \"foo\".\"bar\"")
        .returns(
            "empid=0; deptno=0; name=first; salary=0.0; commission=null\n");
    with.query("insert into \"foo\".\"bar\" select * from \"hr\".\"emps\"")
        .returns("ROWCOUNT=4\n");
    with.query("select count(*) as c from \"foo\".\"bar\"")
        .returns("C=5\n");
    with.query("insert into \"foo\".\"bar\" "
        + "select * from \"hr\".\"emps\" where \"deptno\" = 10")
        .returns("ROWCOUNT=3\n");
    with.query("select \"name\", count(*) as c from \"foo\".\"bar\" "
        + "group by \"name\"")
        .returnsUnordered(
            "name=Bill; C=2",
            "name=Eric; C=1",
            "name=Theodore; C=2",
            "name=first; C=1",
            "name=Sebastian; C=2");
  }

  private CalciteAssert.AssertThat mutable(
      final List<JdbcTest.Employee> employees) {
    employees.add(new JdbcTest.Employee(0, 0, "first", 0f, null));
    return that()
        .with(CalciteAssert.Config.REGULAR)
        .with(
            new CalciteAssert.ConnectionPostProcessor() {
              public Connection apply(final Connection connection)
                  throws SQLException {
                CalciteConnection calciteConnection =
                    connection.unwrap(CalciteConnection.class);
                SchemaPlus rootSchema =
                    calciteConnection.getRootSchema();
                SchemaPlus mapSchema =
                    rootSchema.add("foo", new AbstractSchema());
                final String tableName = "bar";
                final JdbcTest.AbstractModifiableTable table =
                    mutable(tableName, employees);
                mapSchema.add(tableName, table);
                return calciteConnection;
              }
            });
  }

  static JdbcTest.AbstractModifiableTable mutable(String tableName,
      final List<JdbcTest.Employee> employees) {
    return new JdbcTest.AbstractModifiableTable(tableName) {
      public RelDataType getRowType(
          RelDataTypeFactory typeFactory) {
        return ((JavaTypeFactory) typeFactory)
            .createType(JdbcTest.Employee.class);
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this,
            tableName) {
          public Enumerator<T> enumerator() {
            //noinspection unchecked
            return (Enumerator<T>) Linq4j.enumerator(employees);
          }
        };
      }

      public Type getElementType() {
        return JdbcTest.Employee.class;
      }

      public Expression getExpression(SchemaPlus schema, String tableName,
          Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName,
            clazz);
      }

      public Collection getModifiableCollection() {
        return employees;
      }
    };
  }

  @Test public void testInsert2() {
    final List<JdbcTest.Employee> employees =
        new ArrayList<JdbcTest.Employee>();
    CalciteAssert.AssertThat with = mutable(employees);
    with.query("insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)")
        .returns("ROWCOUNT=1\n");
    with.query("insert into \"foo\".\"bar\"\n"
        + "values (1, 3, 'third', 0, 3), (1, 4, 'fourth', 0, 4), (1, 5, 'fifth ', 0, 3)")
        .returns("ROWCOUNT=3\n");
    with.query("select count(*) as c from \"foo\".\"bar\"")
        .returns("C=5\n");
    with.query("insert into \"foo\".\"bar\" values (1, 6, null, 0, null)")
        .returns("ROWCOUNT=1\n");
    with.query("select count(*) as c from \"foo\".\"bar\"")
        .returns("C=6\n");
  }

  /** Some of the rows have the wrong number of columns. */
  @Test public void testInsertMultipleRowMismatch() {
    final List<JdbcTest.Employee> employees =
        new ArrayList<JdbcTest.Employee>();
    CalciteAssert.AssertThat with = mutable(employees);
    with.query("insert into \"foo\".\"bar\" values\n"
        + " (1, 3, 'third'),\n"
        + " (1, 4, 'fourth'),\n"
        + " (1, 5, 'fifth ', 3)")
        .throws_("Incompatible types");
  }
}

// End JdbcFrontLinqBackTest.java
