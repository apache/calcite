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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for utility {@link RelToSqlConverter}
 */
public class RelToSqlConverterTest {
  /** Initiates a test case with a given SQL query. */
  private Sql sql(String sql) {
    return new Sql(CalciteAssert.SchemaSpec.JDBC_FOODMART, sql,
        SqlDialect.CALCITE);
  }

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, CalciteAssert.SchemaSpec schemaSpec,
      Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, schemaSpec))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  @Test public void testSimpleSelectStarFromProductTable() {
    String query = "select * from \"product\"";
    sql(query).ok("SELECT *\nFROM \"foodmart\".\"product\"");
  }

  @Test public void testSimpleSelectQueryFromProductTable() {
    String query = "select \"product_id\", \"product_class_id\" from \"product\"";
    final String expected = "SELECT \"product_id\", \"product_class_id\"\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  //TODO: add test for query -> select * from product

  @Test public void testSelectQueryWithWhereClauseOfLessThan() {
    String query =
        "select \"product_id\", \"shelf_width\"  from \"product\" where \"product_id\" < 10";
    final String expected = "SELECT \"product_id\", \"shelf_width\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" < 10";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithWhereClauseOfBasicOperators() {
    String query = "select * from \"product\" "
        + "where (\"product_id\" = 10 OR \"product_id\" <= 5) "
        + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE (\"product_id\" = 10 OR \"product_id\" <= 5) "
        + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)";
    sql(query).ok(expected);
  }


  @Test public void testSelectQueryWithGroupBy() {
    String query = "select count(*) from \"product\" group by \"product_class_id\", \"product_id\"";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithMinAggregateFunction() {
    String query = "select min(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT MIN(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithMinAggregateFunction1() {
    String query = "select \"product_class_id\", min(\"net_weight\") from"
        + " \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", MIN(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithSumAggregateFunction() {
    String query =
        "select sum(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT SUM(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithMultipleAggregateFunction() {
    String query = "select sum(\"net_weight\"), min(\"low_fat\"), count(*)"
        + " from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT SUM(\"net_weight\"), MIN(\"low_fat\"),"
        + " COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithMultipleAggregateFunction1() {
    String query = "select \"product_class_id\","
        + " sum(\"net_weight\"), min(\"low_fat\"), count(*)"
        + " from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT \"product_class_id\","
        + " SUM(\"net_weight\"), MIN(\"low_fat\"), COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithGroupByAndProjectList() {
    String query = "select \"product_class_id\", \"product_id\", count(*) "
        + "from \"product\" group by \"product_class_id\", \"product_id\"  ";
    final String expected = "SELECT \"product_class_id\", \"product_id\","
        + " COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithGroupByAndProjectList1() {
    String query =
        "select count(*)  from \"product\" group by \"product_class_id\", \"product_id\"";

    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithGroupByHaving() {
    String query = "select count(*) from \"product\" group by \"product_class_id\","
        + " \"product_id\"  having \"product_id\"  > 10";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM (SELECT \"product_class_id\", \"product_id\", COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\") AS \"t0\"\n"
        + "WHERE \"product_id\" > 10";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithOrderByClause() {
    String query = "select \"product_id\"  from \"product\" order by \"net_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithOrderByClause1() {
    String query =
        "select \"product_id\", \"net_weight\" from \"product\" order by \"net_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithTwoOrderByClause() {
    String query =
        "select \"product_id\"  from \"product\" order by \"net_weight\", \"gross_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\","
        + " \"gross_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithAscDescOrderByClause() {
    String query = "select \"product_id\" from \"product\" "
        + "order by \"net_weight\" asc, \"gross_weight\" desc, \"low_fat\"";
    final String expected = "SELECT"
        + " \"product_id\", \"net_weight\", \"gross_weight\", \"low_fat\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\" DESC, \"low_fat\"";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithLimitClause() {
    String query = "select \"product_id\"  from \"product\" limit 100 offset 10";
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "LIMIT 100\nOFFSET 10";
    sql(query).dialect(SqlDialect.DatabaseProduct.HIVE.getDialect())
        .ok(expected);
  }

  @Test public void testSelectQueryWithLimitClauseWithoutOrder() {
    String query = "select \"product_id\"  from \"product\" limit 100 offset 10";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithLimitOffsetClause() {
    String query = "select \"product_id\"  from \"product\" order by \"net_weight\" asc"
        + " limit 100 offset 10";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithFetchOffsetClause() {
    String query = "select \"product_id\"  from \"product\" order by \"product_id\""
        + " offset 10 rows fetch next 100 rows only";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryComplex() {
    String query =
        "select count(*), \"units_per_case\" from \"product\" where \"cases_per_pallet\" > 100 "
            + "group by \"product_id\", \"units_per_case\" order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), \"units_per_case\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"cases_per_pallet\" > 100\n"
        + "GROUP BY \"product_id\", \"units_per_case\"\n"
        + "ORDER BY \"units_per_case\" DESC";
    sql(query).ok(expected);
  }

  @Test public void testSelectQueryWithGroup() {
    String query = "select"
        + " count(*), sum(\"employee_id\") from \"reserve_employee\" "
        + "where \"hire_date\" > '2015-01-01' "
        + "and (\"position_title\" = 'SDE' or \"position_title\" = 'SDM') "
        + "group by \"store_id\", \"position_title\"";
    final String expected = "SELECT COUNT(*), SUM(\"employee_id\")\n"
        + "FROM \"foodmart\".\"reserve_employee\"\n"
        + "WHERE \"hire_date\" > '2015-01-01' "
        + "AND (\"position_title\" = 'SDE' OR \"position_title\" = 'SDM')\n"
        + "GROUP BY \"store_id\", \"position_title\"";
    sql(query).ok(expected);
  }

  @Test public void testSimpleJoin() {
    String query = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "  join \"product_class\" as pc using (\"product_class_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and pc.\"product_department\" = 'Snacks'\n";
    final String expected = "SELECT *\nFROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\""
        + ".\"customer_id\"\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "INNER JOIN \"foodmart\".\"product_class\" "
        + "ON \"product\".\"product_class_id\" = \"product_class\""
        + ".\"product_class_id\"\n"
        + "WHERE \"customer\".\"city\" = 'San Francisco' AND "
        + "\"product_class\".\"product_department\" = 'Snacks'";
    sql(query).ok(expected);
  }

  @Test public void testSimpleIn() {
    String query = "select * from \"department\" where \"department_id\" in (\n"
        + "  select \"department_id\" from \"employee\"\n"
        + "  where \"store_id\" < 150)";
    final String expected = "SELECT "
        + "\"department\".\"department_id\", \"department\""
        + ".\"department_description\"\n"
        + "FROM \"foodmart\".\"department\"\nINNER JOIN "
        + "(SELECT \"department_id\"\nFROM \"foodmart\".\"employee\"\n"
        + "WHERE \"store_id\" < 150\nGROUP BY \"department_id\") AS \"t1\" "
        + "ON \"department\".\"department_id\" = \"t1\".\"department_id\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1332">[CALCITE-1332]
   * DB2 should always use aliases for tables: x.y.z AS z</a>. */
  @Test public void testDb2DialectJoinStar() {
    String query = "select * "
        + "from \"foodmart\".\"employee\" A "
        + "join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectSelfJoinStar() {
    String query = "select * "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectJoin() {
    String query = "select A.\"employee_id\", B.\"department_id\" "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT"
        + " employee.employee_id, department.department_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectSelfJoin() {
    String query = "select A.\"employee_id\", B.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT"
        + " employee.employee_id, employee0.employee_id AS employee_id0\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectWhere() {
    String query = "select A.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A where A.\"department_id\" < 1000";
    final String expected = "SELECT employee.employee_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE employee.department_id < 1000";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectJoinWhere() {
    String query = "select A.\"employee_id\", B.\"department_id\" "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\" "
        + "where A.\"employee_id\" < 1000";
    final String expected = "SELECT"
        + " employee.employee_id, department.department_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id\n"
        + "WHERE employee.employee_id < 1000";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectSelfJoinWhere() {
    String query = "select A.\"employee_id\", B.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\" "
        + "where B.\"employee_id\" < 2000";
    final String expected = "SELECT "
        + "employee.employee_id, employee0.employee_id AS employee_id0\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id\n"
        + "WHERE employee0.employee_id < 2000";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectCast() {
    String query = "select \"hire_date\", cast(\"hire_date\" as varchar(10)) "
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT reserve_employee.hire_date, "
        + "CAST(reserve_employee.hire_date AS VARCHAR(10))\n"
        + "FROM foodmart.reserve_employee AS reserve_employee";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectSelectQueryWithGroupByHaving() {
    String query = "select count(*) from \"product\" "
        + "group by \"product_class_id\", \"product_id\" "
        + "having \"product_id\"  > 10";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM (SELECT product.product_class_id, product.product_id, COUNT"
        + "(*)\n"
        + "FROM foodmart.product AS product\n"
        + "GROUP BY product.product_class_id, product.product_id) AS t0\n"
        + "WHERE t0.product_id > 10";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }


  @Test public void testDb2DialectSelectQueryComplex() {
    String query = "select count(*), \"units_per_case\" "
        + "from \"product\" where \"cases_per_pallet\" > 100 "
        + "group by \"product_id\", \"units_per_case\" "
        + "order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), product.units_per_case\n"
        + "FROM foodmart.product AS product\n"
        + "WHERE product.cases_per_pallet > 100\n"
        + "GROUP BY product.product_id, product.units_per_case\n"
        + "ORDER BY product.units_per_case DESC";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  @Test public void testDb2DialectSelectQueryWithGroup() {
    String query = "select count(*), sum(\"employee_id\") "
        + "from \"reserve_employee\" "
        + "where \"hire_date\" > '2015-01-01' "
        + "and (\"position_title\" = 'SDE' or \"position_title\" = 'SDM') "
        + "group by \"store_id\", \"position_title\"";
    final String expected = "SELECT"
        + " COUNT(*), SUM(reserve_employee.employee_id)\n"
        + "FROM foodmart.reserve_employee AS reserve_employee\n"
        + "WHERE reserve_employee.hire_date > '2015-01-01' "
        + "AND (reserve_employee.position_title = 'SDE' OR "
        + "reserve_employee.position_title = 'SDM')\n"
        + "GROUP BY reserve_employee.store_id, reserve_employee.position_title";
    sql(query).dialect(DatabaseProduct.DB2.getDialect()).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1372">[CALCITE-1372]
   * JDBC adapter generates SQL with wrong field names</a>. */
  @Test public void testJoinPlan2() {
    final String sql = "SELECT v1.deptno, v2.deptno\n"
        + "FROM dept v1 LEFT JOIN emp v2 ON v1.deptno = v2.deptno\n"
        + "WHERE v2.job LIKE 'PRESIDENT'";
    final String expected = "SELECT \"DEPT\".\"DEPTNO\","
        + " \"EMP\".\"DEPTNO\" AS \"DEPTNO0\"\n"
        + "FROM \"JDBC_SCOTT\".\"DEPT\"\n"
        + "LEFT JOIN \"JDBC_SCOTT\".\"EMP\""
        + " ON \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\"\n"
        + "WHERE \"EMP\".\"JOB\" LIKE 'PRESIDENT'";
    final String expected2 = "SELECT DEPT.DEPTNO, EMP.DEPTNO AS DEPTNO0\n"
        + "FROM JDBC_SCOTT.DEPT AS DEPT\n"
        + "LEFT JOIN JDBC_SCOTT.EMP AS EMP ON DEPT.DEPTNO = EMP.DEPTNO\n"
        + "WHERE EMP.JOB LIKE 'PRESIDENT'";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected)
        .dialect(DatabaseProduct.DB2.getDialect())
        .ok(expected2);
  }

  /** Fluid interface to run tests. */
  private static class Sql {
    private CalciteAssert.SchemaSpec schemaSpec;
    private final String sql;
    private final SqlDialect dialect;

    Sql(CalciteAssert.SchemaSpec schemaSpec, String sql, SqlDialect dialect) {
      this.schemaSpec = schemaSpec;
      this.sql = sql;
      this.dialect = dialect;
    }

    Sql dialect(SqlDialect dialect) {
      return new Sql(schemaSpec, sql, dialect);
    }

    Sql planner(Planner planner) {
      return new Sql(schemaSpec, sql, dialect);
    }

    Sql ok(String expectedQuery) {
      final Planner planner =
          getPlanner(null, SqlParser.Config.DEFAULT, schemaSpec);
      try {
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode rel = planner.rel(validate).rel;
        final RelToSqlConverter converter =
            new RelToSqlConverter(dialect);
        final SqlNode sqlNode = converter.visitChild(0, rel).asQuery();
        assertThat(Util.toLinux(sqlNode.toSqlString(dialect).getSql()),
            is(expectedQuery));
      } catch (Exception e) {
        assertTrue("Parsing failed throwing error: " + e.getMessage(), false);
      }
      return this;
    }

    public Sql schema(CalciteAssert.SchemaSpec schemaSpec) {
      return new Sql(schemaSpec, sql, dialect);
    }
  }
}

// End RelToSqlConverterTest.java
