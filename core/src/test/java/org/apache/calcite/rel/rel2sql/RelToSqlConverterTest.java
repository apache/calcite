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
  private Planner logicalPlanner = getPlanner(null);

  private void checkRel2Sql(Planner planner, String query, String expectedQuery,
      SqlDialect dialect) {
    try {
      SqlNode parse = planner.parse(query);
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
  }

  private void checkRel2Sql(Planner planner, String query, String expectedQuery) {
    checkRel2Sql(planner, query, expectedQuery, SqlDialect.CALCITE);
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs, Program... programs) {
    return getPlanner(traitDefs, SqlParser.Config.DEFAULT, programs);
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.JDBC_FOODMART))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  @Test
  public void testSimpleSelectStarFromProductTable() {
    String query = "select * from \"product\"";
    checkRel2Sql(this.logicalPlanner,
        query,
         "SELECT *\nFROM \"foodmart\".\"product\"");
  }

  @Test
  public void testSimpleSelectQueryFromProductTable() {
    String query = "select \"product_id\", \"product_class_id\" from \"product\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"product_class_id\"\n"
            + "FROM \"foodmart\".\"product\"");
  }

  //TODO: add test for query -> select * from product

  @Test
  public void testSelectQueryWithWhereClauseOfLessThan() {
    String query =
        "select \"product_id\", \"shelf_width\"  from \"product\" where \"product_id\" < 10";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"shelf_width\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "WHERE \"product_id\" < 10");
  }

  @Test
  public void testSelectQueryWithWhereClauseOfBasicOperators() {
    String query = "select * from \"product\" "
        + "where (\"product_id\" = 10 OR \"product_id\" <= 5) "
        + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT *\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "WHERE (\"product_id\" = 10 OR \"product_id\" <= 5) "
            + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)");
  }


  @Test
  public void testSelectQueryWithGroupBy() {
    String query = "select count(*) from \"product\" group by \"product_class_id\", \"product_id\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\", \"product_id\"");
  }

  @Test
  public void testSelectQueryWithMinAggregateFunction() {
    String query = "select min(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT MIN(\"net_weight\")\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\"");
  }

  @Test
  public void testSelectQueryWithMinAggregateFunction1() {
    String query = "select \"product_class_id\", min(\"net_weight\") from"
        + " \"product\" group by \"product_class_id\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_class_id\", MIN(\"net_weight\")\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\"");
  }

  @Test
  public void testSelectQueryWithSumAggregateFunction() {
    String query =
        "select sum(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT SUM(\"net_weight\")\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\"");
  }

  @Test
  public void testSelectQueryWithMultipleAggregateFunction() {
    String query =
        "select sum(\"net_weight\"), min(\"low_fat\"), count(*)"
            + " from \"product\" group by \"product_class_id\" ";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT SUM(\"net_weight\"), MIN(\"low_fat\"), COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\"");
  }

  @Test
  public void testSelectQueryWithMultipleAggregateFunction1() {
    String query =
        "select \"product_class_id\", sum(\"net_weight\"), min(\"low_fat\"), count(*)"
            + " from \"product\" group by \"product_class_id\" ";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_class_id\", SUM(\"net_weight\"), MIN(\"low_fat\"), COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\""
    );
  }

  @Test
  public void testSelectQueryWithGroupByAndProjectList() {
    String query =
        "select \"product_class_id\", \"product_id\", count(*) from \"product\" group "
            + "by \"product_class_id\", \"product_id\"  ";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_class_id\", \"product_id\", COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\", \"product_id\"");
  }

  @Test
  public void testSelectQueryWithGroupByAndProjectList1() {
    String query =
        "select count(*)  from \"product\" group by \"product_class_id\", \"product_id\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\", \"product_id\"");
  }

  @Test
  public void testSelectQueryWithGroupByHaving() {
    String query = "select count(*) from \"product\" group by \"product_class_id\","
        + " \"product_id\"  having \"product_id\"  > 10";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT COUNT(*)\n"
            + "FROM (SELECT \"product_class_id\", \"product_id\", COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "GROUP BY \"product_class_id\", \"product_id\") AS \"t0\"\n"
            + "WHERE \"product_id\" > 10");
  }

  @Test
  public void testSelectQueryWithOrderByClause() {
    String query = "select \"product_id\"  from \"product\" order by \"net_weight\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"net_weight\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\"");
  }

  @Test
  public void testSelectQueryWithOrderByClause1() {
    String query =
        "select \"product_id\", \"net_weight\" from \"product\" order by \"net_weight\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"net_weight\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\"");
  }

  @Test
  public void testSelectQueryWithTwoOrderByClause() {
    String query =
        "select \"product_id\"  from \"product\" order by \"net_weight\", \"gross_weight\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"net_weight\", \"gross_weight\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\", \"gross_weight\"");
  }

  @Test
  public void testSelectQueryWithAscDescOrderByClause() {
    String query =
        "select \"product_id\" from \"product\" order by \"net_weight\" asc, "
            + "\"gross_weight\" desc, \"low_fat\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"net_weight\", \"gross_weight\", \"low_fat\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\", \"gross_weight\" DESC, \"low_fat\"");
  }

  @Test
  public void testSelectQueryWithLimitClause() {
    String query = "select \"product_id\"  from \"product\" limit 100 offset 10";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT product_id\n"
            + "FROM foodmart.product\n"
            + "LIMIT 100\nOFFSET 10",
        SqlDialect.DatabaseProduct.HIVE.getDialect());
  }

  @Test
  public void testSelectQueryWithLimitClauseWithoutOrder() {
    String query = "select \"product_id\"  from \"product\" limit 100 offset 10";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "OFFSET 10 ROWS\n"
            + "FETCH NEXT 100 ROWS ONLY");
  }

  @Test
  public void testSelectQueryWithLimitOffsetClause() {
    String query = "select \"product_id\"  from \"product\" order by \"net_weight\" asc"
        + " limit 100 offset 10";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\", \"net_weight\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\"\n"
            + "OFFSET 10 ROWS\n"
            + "FETCH NEXT 100 ROWS ONLY");
  }

  @Test
  public void testSelectQueryWithFetchOffsetClause() {
    String query = "select \"product_id\"  from \"product\" order by \"product_id\""
        + " offset 10 rows fetch next 100 rows only";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"product_id\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"product_id\"\n"
            + "OFFSET 10 ROWS\n"
            + "FETCH NEXT 100 ROWS ONLY");
  }

  @Test
  public void testSelectQueryComplex() {
    String query =
        "select count(*), \"units_per_case\" from \"product\" where \"cases_per_pallet\" > 100 "
            + "group by \"product_id\", \"units_per_case\" order by \"units_per_case\" desc";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT COUNT(*), \"units_per_case\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "WHERE \"cases_per_pallet\" > 100\n"
            + "GROUP BY \"product_id\", \"units_per_case\"\n"
            + "ORDER BY \"units_per_case\" DESC");
  }

  @Test
  public void testSelectQueryWithGroup() {
    String query =
        "select count(*), sum(\"employee_id\") from \"reserve_employee\" "
            + "where \"hire_date\" > '2015-01-01' "
            + "and (\"position_title\" = 'SDE' or \"position_title\" = 'SDM') "
            + "group by \"store_id\", \"position_title\"";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT COUNT(*), SUM(\"employee_id\")\n"
            + "FROM \"foodmart\".\"reserve_employee\"\n"
            + "WHERE \"hire_date\" > '2015-01-01' "
            + "AND (\"position_title\" = 'SDE' OR \"position_title\" = 'SDM')\n"
            + "GROUP BY \"store_id\", \"position_title\"");
  }

  @Test
  public void testSimpleJoin() {
    String query = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "  join \"product_class\" as pc using (\"product_class_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and pc.\"product_department\" = 'Snacks'\n";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT *\nFROM \"foodmart\".\"sales_fact_1997\"\n"
            + "INNER JOIN \"foodmart\".\"customer\" "
            + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "INNER JOIN \"foodmart\".\"product\" "
            + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "INNER JOIN \"foodmart\".\"product_class\" "
            + "ON \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "WHERE \"customer\".\"city\" = 'San Francisco' AND "
            + "\"product_class\".\"product_department\" = 'Snacks'"
    );
  }

  @Test public void testSimpleIn() {
    String query = "select * from \"department\" where \"department_id\" in (\n"
        + "  select \"department_id\" from \"employee\"\n"
        + "  where \"store_id\" < 150)";
    checkRel2Sql(this.logicalPlanner,
        query,
        "SELECT \"department\".\"department_id\", \"department\".\"department_description\"\n"
            + "FROM \"foodmart\".\"department\"\nINNER JOIN "
            + "(SELECT \"department_id\"\nFROM \"foodmart\".\"employee\"\n"
            + "WHERE \"store_id\" < 150\nGROUP BY \"department_id\") AS \"t1\" "
            + "ON \"department\".\"department_id\" = \"t1\".\"department_id\"");
  }
}

// End RelToSqlConverterTest.java
