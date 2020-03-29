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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.rel.CascadesTestDistributionEnforcer;
import org.apache.calcite.plan.cascades.rel.CascadesTestSortEnforcer;
import org.apache.calcite.plan.cascades.rel.TestTable;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.plan.cascades.CascadesTestUtils.CASCADES_TEST_CONVENTION;
import static org.apache.calcite.plan.cascades.CascadesTestUtils.LOGICAL_RULES;
import static org.apache.calcite.plan.cascades.CascadesTestUtils.PHYSICAL_RULES;
import static org.apache.calcite.tools.Frameworks.createRootSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class CascadesPlannerTest {
  static {
    System.setProperty("calcite.enable.join.commute", "true");
  }

  @Test public void select() throws Exception {
    String sql = "select * from emps";
    String logicalPlan = ""
        + "LogicalProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "  LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void selectWhere() throws Exception {
    String sql = "select * from emps where \"name\" = 'Leonhard Euler'";
    String logicalPlan = ""
        + "LogicalProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "  LogicalFilter(condition=[=($2, 'Leonhard Euler')])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    CascadesTestFilter(condition=[=($2, 'Leonhard Euler')])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void selectOrderByIndexedColumn1() throws Exception {
    String sql = "select * from emps order by \"depId\"";
    String logicalPlan = ""
        + "LogicalSort(sort0=[$3], dir0=[ASC])\n"
        + "  LogicalProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[3]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void selectOrderByIndexedColumn2() throws Exception {
    String sql = "select * from emps order by \"id\"";
    String logicalPlan = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void selectFilterOrderByIndexedColumn2() throws Exception {
    String sql = "select * from emps where \"name\" = 'Leonhard Euler' order by \"id\" ";
    String logicalPlan = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    LogicalFilter(condition=[=($2, 'Leonhard Euler')])\n"
        + "      LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(id=[$0], age=[$1], name=[$2], depId=[$3], projectId=[$4])\n"
        + "    CascadesTestFilter(condition=[=($2, 'Leonhard Euler')])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void groupByCollocatedIndexedColumn() throws Exception {
    String sql = "select \"id\", sum(\"depId\") from emps group by \"id\"";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(id=[$0], depId=[$3])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestStreamAggregate(group=[{0}], EXPR$1=[$SUM0($3)])\n"
        + "    CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void groupByNonCollocatedNonIndexedColumn() throws Exception {
    String sql = "select \"name\", sum(\"id\") from emps group by \"name\"";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(name=[$2], id=[$0])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestHashAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n"
        + "    CascadesTestExchange(distribution=[hash[0]])\n"
        + "      CascadesTestProject(name=[$2], id=[$0])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void groupByNonCollocatedIndexedColumn() throws Exception {
    String sql = "select \"depId\", sum(\"id\") from emps group by \"depId\"";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(depId=[$3], id=[$0])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestStreamAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n"
        + "    CascadesTestExchange(distribution=[hash[0]])\n"
        + "      CascadesTestProject(depId=[$3], id=[$0])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[3]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void joinOnSortedNonCollocatedKeys() throws Exception {
    String sql = "select e.\"name\" as ename, e.\"age\" as eage, d.\"name\" as dname "
        + "from emps e inner join deps d "
        + "on e.\"depId\" = d.\"id\"";
    String logicalPlan = ""
        + "LogicalProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "  LogicalJoin(condition=[=($3, $5)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "    LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "    CascadesTestMergeJoin(condition=[=($3, $5)], joinType=[inner])\n"
        + "      CascadesTestExchange(distribution=[hash[3]])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[3]])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void joinOnSortedCollocatedKeys() throws Exception {
    String sql = "select e.\"name\" as ename, e.\"age\" as eage, d.\"name\" as dname "
        + "from emps e inner join deps d "
        + "on e.\"id\" = d.\"id\"";
    String logicalPlan = ""
        + "LogicalProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "    LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "    CascadesTestMergeJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void joinOnNonSortedNonCollocatedKeys() throws Exception {
    String sql = "select e.\"name\" as ename, e.\"age\" as eage, d.\"name\" as dname "
        + "from emps e inner join deps d "
        + "on e.\"projectId\" = d.\"projectId\"";
    String logicalPlan = ""
        + "LogicalProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "  LogicalJoin(condition=[=($4, $7)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "    LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestProject(ENAME=[$2], EAGE=[$1], DNAME=[$6])\n"
        + "    CascadesTestHashJoin(condition=[=($4, $7)], joinType=[inner])\n"
        + "      CascadesTestExchange(distribution=[hash[4]])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[]])\n"
        + "      CascadesTestExchange(distribution=[hash[2]])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void aggOverJoinOnSortedCollocatedKeys() throws Exception {
    String sql = "select e.\"id\", SUM(e.\"age\") "
        + "from emps e inner join deps d "
        + "on e.\"id\" = d.\"id\" "
        + "group by e.\"id\" ";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(id=[$0], age=[$1])\n"
        + "    LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "      LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestStreamAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n"
        + "    CascadesTestMergeJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n"
        + "      CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void aggOnNonSortedNonCollocatedOverJoinOnSortedCollocatedKeys() throws Exception {
    String sql = "select e.\"depId\", SUM(e.\"age\") "
        + "from emps e inner join deps d "
        + "on e.\"id\" = d.\"id\" "
        + "group by e.\"depId\" ";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(depId=[$3], age=[$1])\n"
        + "    LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "      LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestHashAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n"
        + "    CascadesTestExchange(distribution=[hash[0]])\n"
        + "      CascadesTestProject(depId=[$3], age=[$1])\n"
        + "        CascadesTestMergeJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "          CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[0]])\n"
        + "          CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[0]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  @Test public void aggOnNonSortedNoCollocatedOverJoinOnNonSortedNonCollocatedKeys()
      throws Exception {
    String sql = "select e.\"projectId\", SUM(e.\"age\") "
        + "from emps e inner join deps d "
        + "on e.\"projectId\" = d.\"projectId\" "
        + "group by e.\"projectId\" ";
    String logicalPlan = ""
        + "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
        + "  LogicalProject(projectId=[$4], age=[$1])\n"
        + "    LogicalJoin(condition=[=($4, $7)], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[PUBLIC, EMPS]])\n"
        + "      LogicalTableScan(table=[[PUBLIC, DEPS]])\n";
    String physicalPlan = ""
        + "CascadesTestExchange(distribution=[single])\n"
        + "  CascadesTestHashAggregate(group=[{4}], EXPR$1=[$SUM0($1)])\n"
        + "    CascadesTestHashJoin(condition=[=($4, $7)], joinType=[inner])\n"
        + "      CascadesTestExchange(distribution=[hash[4]])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, EMPS]], sort=[[]])\n"
        + "      CascadesTestExchange(distribution=[hash[2]])\n"
        + "        CascadesTestTableScan(table=[[PUBLIC, DEPS]], sort=[[]])\n";

    checkPlan(sql, logicalPlan, physicalPlan);
  }

  private Planner createPlanner() {
    JavaTypeFactoryImpl f = new JavaTypeFactoryImpl(new RelDataTypeSystemImpl() { });

    final Table empsTbl = new TestTable(new RelDataTypeFactory.Builder(f)
        .add("id", SqlTypeName.INTEGER)
        .add("age", SqlTypeName.TINYINT)
        .add("name", SqlTypeName.VARCHAR)
        .add("depId", SqlTypeName.INTEGER)
        .add("projectId", SqlTypeName.INTEGER)
        .build(),
        5000d,
        Arrays.asList(RelCollations.of(0), RelCollations.of(3)),
        RelDistributions.hash(Arrays.asList(0)));

    final Table depsTbl = new TestTable(new RelDataTypeFactory.Builder(f)
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR)
        .add("projectId", SqlTypeName.INTEGER)
        .build(),
        500d,
        Arrays.asList(RelCollations.of(0)),
        RelDistributions.hash(Arrays.asList(0)));

    Map<String, Table> tableMap = new HashMap<>();
    tableMap.put("EMPS", empsTbl);
    tableMap.put("DEPS", depsTbl);
    CascadesTableSchema schema = new CascadesTableSchema(tableMap);

    SchemaPlus rootSchema = createRootSchema(false)
        .add("PUBLIC", schema);

    List<RelOptRule> rules = new ArrayList<>();
    rules.addAll(PHYSICAL_RULES);
    rules.addAll(LOGICAL_RULES);

    List<Enforcer> enforcers = new ArrayList<>();
    enforcers.add(CascadesTestSortEnforcer.INSTANCE);
    enforcers.add(CascadesTestDistributionEnforcer.INSTANCE);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .useCascadesPlanner(true)
        .defaultSchema(rootSchema)
        .enforcers(enforcers)
        .programs(Programs.ofRules(rules))
        .traitDefs(ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            RelDistributionTraitDef.INSTANCE)
        .build();

    return Frameworks.getPlanner(config);
  }

  private void checkPlan(String sql, String logicalPlan, String physicalPlan) throws Exception {
    Planner planner = createPlanner();
    SqlNode sqlNode = planner.parse(sql);
    sqlNode = planner.validate(sqlNode);
    RelRoot root =  planner.rel(sqlNode);

    assertEquals(logicalPlan, RelOptUtil.toString(root.rel));

    RelTraitSet desiredTraits = planner.getEmptyTraitSet()
        .plus(CASCADES_TEST_CONVENTION)
        .plus(root.collation)
        .plus(RelDistributions.SINGLETON);

    RelNode physicalRel = planner.transform(0,  desiredTraits, root.rel);

    assertEquals(physicalPlan, RelOptUtil.toString(physicalRel));
  }

  private static class CascadesTableSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    CascadesTableSchema(Map<String, Table> tableMap) {
      this.tableMap = Objects.requireNonNull(tableMap);
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }
  }
}
