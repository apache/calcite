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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ScannableTableTest;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for {@link MaterializedViewSubstitutionVisitor}.
 */
public class MaterializedViewSubstitutionVisitorTest extends SqlToRelTestBase {

  // Before
  private SchemaPlus rootSchema;
  private Planner planner;
  private RelBuilder relBuilder;

  @Before
  public void setUp() {
    rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .executor(RexUtil.EXECUTOR)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .build();
    planner = Frameworks.getPlanner(config);
    relBuilder = RelBuilder.create(config);
  }

  @Test
  public void testEquivalents() throws Exception {
    String mv = "SELECT \"empid\", \"deptno\", SUM(\"salary\") FROM \"emps\" "
        + "GROUP BY \"empid\", \"deptno\"";
    String query = "SELECT \"empid\", SUM(\"salary\") FROM \"emps\" GROUP BY \"empid\"";

    RelNode mvRelNode = convertSqlToRel(mv);
    RelNode queryRelNode = convertSqlToRel(query);

    SchemaPlus hr = rootSchema.getSubSchema("hr");
    hr.add("mv", new ScannableTableTest.SimpleTable());
    RelNode tableScan = relBuilder.scan("hr", "mv").build();

    List<RelNode> relNodes = new MaterializedViewSubstitutionVisitor(mvRelNode, queryRelNode)
        .go(tableScan);

    String relStr = "LogicalAggregate(group=[{0}], EXPR$1=[SUM($2)])\n"
        + "  LogicalTableScan(table=[[hr, mv]])\n";

    Assert.assertEquals(relNodes.size(), 1);
    Assert.assertEquals(RelOptUtil.toString(relNodes.get(0)), relStr);
  }

  private RelNode convertSqlToRel(String sql)
      throws SqlParseException, ValidationException, RelConversionException {
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    planner.close();
    return convert;
  }

  @After
  public void tearDown() {
    rootSchema = null;
    planner = null;
  }

}

// End MaterializedViewSubstitutionVisitorTest.java
