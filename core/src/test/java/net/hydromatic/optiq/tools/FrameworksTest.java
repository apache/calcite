/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractTable;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Util;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
            final JavaRules.EnumerableTableAccessRel tableRel =
                new JavaRules.EnumerableTableAccessRel(
                    cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE),
                    relOptTable, Object[].class);

            // "WHERE i > 1"
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final RexNode condition =
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                    rexBuilder.makeFieldAccess(
                        rexBuilder.makeRangeReference(tableRel), "i", true),
                    rexBuilder.makeExactLiteral(BigDecimal.ONE));
            final FilterRel filterRel =
                new FilterRel(cluster, tableRel, condition);

            // Specify that the result should be in Enumerable convention.
            final RelNode rootRel = filterRel;
            final RelOptPlanner planner = cluster.getPlanner();
            RelTraitSet desiredTraits = rootRel.getTraitSet().replace(
                EnumerableConvention.INSTANCE);
            final RelNode rootRel2 = planner.changeTraits(rootRel,
                desiredTraits);
            planner.setRoot(rootRel2);

            // Now, plan.
            return planner.findBestExp();
          }
        });
    String s =
        RelOptUtil.dumpPlan("", x, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertThat(Util.toLinux(s), equalTo(
        "EnumerableFilterRel(condition=[>($1, 1)])\n"
        + "  EnumerableTableAccessRel(table=[[myTable]])\n"));
  }

  /** Unit test to test create root schema which has no "metadata" schema. */
  @Test public void testCreateRootSchemaWithNoMetadataSchema() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    assertThat(rootSchema.getSubSchemaNames().size(), equalTo(0));
  }
}

// End FrameworksTest.java
