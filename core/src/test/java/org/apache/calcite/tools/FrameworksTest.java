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
package org.apache.calcite.tools;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for methods in {@link Frameworks}.
 */
public class FrameworksTest {
  @Test public void testOptimize() {
    RelNode x =
        Frameworks.withPlanner(new Frameworks.PlannerAction<RelNode>() {
          public RelNode apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            final Table table = new AbstractTable() {
              public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                final RelDataType stringType =
                    typeFactory.createJavaType(String.class);
                final RelDataType integerType =
                    typeFactory.createJavaType(Integer.class);
                return typeFactory.builder()
                    .add("s", stringType)
                    .add("i", integerType)
                    .build();
              }
            };

            // "SELECT * FROM myTable"
            final RelOptAbstractTable relOptTable = new RelOptAbstractTable(
                relOptSchema,
                "myTable",
                table.getRowType(typeFactory)) {
            };
            final EnumerableTableScan tableRel =
                EnumerableTableScan.create(cluster, relOptTable);

            // "WHERE i > 1"
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final RexNode condition =
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                    rexBuilder.makeFieldAccess(
                        rexBuilder.makeRangeReference(tableRel), "i", true),
                    rexBuilder.makeExactLiteral(BigDecimal.ONE));
            final LogicalFilter filter =
                LogicalFilter.create(tableRel, condition);

            // Specify that the result should be in Enumerable convention.
            final RelNode rootRel = filter;
            final RelOptPlanner planner = cluster.getPlanner();
            RelTraitSet desiredTraits =
                cluster.traitSet().replace(EnumerableConvention.INSTANCE);
            final RelNode rootRel2 = planner.changeTraits(rootRel,
                desiredTraits);
            planner.setRoot(rootRel2);

            // Now, plan.
            return planner.findBestExp();
          }
        });
    String s =
        RelOptUtil.dumpPlan("", x, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertThat(Util.toLinux(s),
        equalTo("EnumerableFilter(condition=[>($1, 1)])\n"
            + "  EnumerableTableScan(table=[[myTable]])\n"));
  }

  /** Unit test to test create root schema which has no "metadata" schema. */
  @Test public void testCreateRootSchemaWithNoMetadataSchema() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    assertThat(rootSchema.getSubSchemaNames().size(), equalTo(0));
  }

  /** Tests that validation (specifically, inferring the result of adding
   * two DECIMAL(19, 0) values together) happens differently with a type system
   * that allows a larger maximum precision for decimals.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-413">CALCITE-413</a>,
   * "Add RelDataTypeSystem plugin, allowing different max precision of a
   * DECIMAL".
   *
   * <p>Also tests the plugin system, by specifying implementations of a
   * plugin interface with public and private constructors. */
  @Test public void testTypeSystem() {
    checkTypeSystem(19, Frameworks.newConfigBuilder().build());
    checkTypeSystem(25, Frameworks.newConfigBuilder()
        .typeSystem(HiveLikeTypeSystem.INSTANCE).build());
    checkTypeSystem(31, Frameworks.newConfigBuilder()
        .typeSystem(new HiveLikeTypeSystem2()).build());
  }

  private void checkTypeSystem(final int expected, FrameworkConfig config) {
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>(config) {
          @Override public Void apply(RelOptCluster cluster,
              RelOptSchema relOptSchema, SchemaPlus rootSchema,
              CalciteServerStatement statement) {
            final RelDataType type =
                cluster.getTypeFactory()
                    .createSqlType(SqlTypeName.DECIMAL, 30, 2);
            final RexLiteral literal =
                cluster.getRexBuilder().makeExactLiteral(BigDecimal.ONE, type);
            final RexNode call =
                cluster.getRexBuilder().makeCall(SqlStdOperatorTable.PLUS,
                    literal,
                    literal);
            assertEquals(expected, call.getType().getPrecision());
            return null;
          }
        });
  }

  /** Tests that the validator expands identifiers by default.
   *
   * <p>Test case for
   * [<a href="https://issues.apache.org/jira/browse/CALCITE-593">CALCITE-593</a>]
   * "Validator in Frameworks should expand identifiers".
   */
  @Test public void testFrameworksValidatorWithIdentifierExpansion()
      throws Exception {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse("select * from \"emps\" ");
    SqlNode val = planner.validate(parse);

    String valStr =
        val.toSqlString(SqlDialect.DUMMY, false).getSql();

    String expandedStr =
        "SELECT `emps`.`empid`, `emps`.`deptno`, `emps`.`name`, `emps`.`salary`, `emps`.`commission`\n"
            + "FROM `hr`.`emps` AS `emps`";
    assertThat(Util.toLinux(valStr), equalTo(expandedStr));
  }

  /** Dummy type system, similar to Hive's, accessed via an INSTANCE member. */
  public static class HiveLikeTypeSystem extends RelDataTypeSystemImpl {
    public static final RelDataTypeSystem INSTANCE = new HiveLikeTypeSystem();

    private HiveLikeTypeSystem() {}

    @Override public int getMaxNumericPrecision() {
      assert super.getMaxNumericPrecision() == 19;
      return 25;
    }
  }

  /** Dummy type system, similar to Hive's, accessed via a public default
   * constructor. */
  public static class HiveLikeTypeSystem2 extends RelDataTypeSystemImpl {
    public HiveLikeTypeSystem2() {}

    @Override public int getMaxNumericPrecision() {
      assert super.getMaxNumericPrecision() == 19;
      return 38;
    }
  }
}

// End FrameworksTest.java
