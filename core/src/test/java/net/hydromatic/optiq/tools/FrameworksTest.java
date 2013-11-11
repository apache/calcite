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

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.optiq.Schema;
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
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Pair;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for methods in {@link Frameworks}.
 */
public class FrameworksTest {
  @Test public void testOptimize() {
    final List<Object[]> strings =
        Arrays.asList(
            new Object[]{"x", 1},
            new Object[]{"y", 2},
            new Object[]{"z", 3});
    RelNode x =
        Frameworks.withPlanner(new Frameworks.PlannerAction<RelNode>() {
          public RelNode apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              Schema schema) {
            final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            final RelDataType stringType =
                typeFactory.createJavaType(String.class);
            final RelDataType integerType =
                typeFactory.createJavaType(Integer.class);
            final RelDataType rowType =
                typeFactory.createStructType(
                    Arrays.asList(
                        Pair.of("s", stringType),
                        Pair.of("i", integerType)));
            final Table table = new AbstractTable(schema,
                String.class,
                rowType,
                "myTable") {
              public Enumerator enumerator() {
                return Linq4j.enumerator(strings);
              }
            };

            // "SELECT * FROM myTable"
            final RelOptAbstractTable relOptTable = new RelOptAbstractTable(
                relOptSchema,
                "myTable",
                table.getRowType()) {
            };
            final JavaRules.EnumerableTableAccessRel tableRel =
                new JavaRules.EnumerableTableAccessRel(
                    cluster,
                    cluster.traitSetOf(EnumerableConvention.INSTANCE),
                    relOptTable,
                    schema.getExpression(),
                    Object[].class);

            // "WHERE i > 1"
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final RexNode condition =
                rexBuilder.makeCall(SqlStdOperatorTable.greaterThanOperator,
                    rexBuilder.makeFieldAccess(
                        rexBuilder.makeRangeReference(table.getRowType()),
                        "i"),
                    rexBuilder.makeExactLiteral(BigDecimal.ONE));
            final FilterRel filterRel = new FilterRel(cluster,
                tableRel,
                condition);

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
    assertEquals("EnumerableCalcRel#12", x.toString());
  }
}

// End FrameworksTest.java
