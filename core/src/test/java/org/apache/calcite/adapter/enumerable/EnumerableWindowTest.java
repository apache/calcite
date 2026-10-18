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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockRelOptPlanner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test for {@link org.apache.calcite.adapter.enumerable.EnumerableWindow}.
 */
public class EnumerableWindowTest {
  @ParameterizedTest
  @MethodSource("rangeNullableOrderKeyCases")
  void testRangeNullableOrderKey(String type, String nulls, String frame,
      String sum1, String sum2, String sum4, String sumNull) {
    CalciteAssert.that()
        .query("select k, sum(v) over (order by k nulls " + nulls
            + " range between " + frame + ") as s\n"
            + "from (values (cast(1 as " + type + "), 10), (2, 20), (4, 40),\n"
            + "  (cast(null as " + type + "), 100), (null, 200)) as t(k, v)")
        .explainContains("EnumerableWindow")
        .returnsUnordered("K=1; S=" + sum1,
            "K=2; S=" + sum2,
            "K=4; S=" + sum4,
            "K=null; S=" + sumNull,
            "K=null; S=" + sumNull);
  }

  private static List<Arguments> rangeNullableOrderKeyCases() {
    // Expected results for all 28 cases were verified on PostgreSQL 14.24
    // by executing the same SQL and comparing all five result rows,
    // including duplicate null-key rows, without regard to row order.
    final List<Arguments> cases = new ArrayList<>();
    for (String type : new String[] {"integer", "bigint"}) {
      for (String nulls : new String[] {"first", "last"}) {
        // Cover PRECEDING and FOLLOWING for both the lower and upper bounds.
        cases.add(
            Arguments.of(type, nulls, "1 preceding and current row",
            "10", "30", "40", "300"));
        cases.add(
            Arguments.of(type, nulls, "current row and 1 following",
            "30", "20", "40", "300"));
        cases.add(
            Arguments.of(type, nulls, "1 preceding and 1 following",
            "30", "30", "40", "300"));
        cases.add(
            Arguments.of(type, nulls, "2 preceding and 1 preceding",
            "null", "10", "20", "300"));
        cases.add(
            Arguments.of(type, nulls, "1 following and 2 following",
            "20", "40", "null", "300"));
        // An unbounded end can include non-null keys in a null key's frame.
        if (nulls.equals("first")) {
          cases.add(
              Arguments.of(type, nulls, "unbounded preceding and 1 following",
              "330", "330", "370", "300"));
          cases.add(
              Arguments.of(type, nulls, "1 preceding and unbounded following",
              "70", "70", "40", "370"));
        } else {
          cases.add(
              Arguments.of(type, nulls, "unbounded preceding and 1 following",
              "30", "30", "70", "370"));
          cases.add(
              Arguments.of(type, nulls, "1 preceding and unbounded following",
              "370", "370", "340", "300"));
        }
      }
    }
    return cases;
  }

  @Test void testCopyWithConstants() {
    final MockRelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    final RelTraitSet traitSet = RelTraitSet.createEmpty();
    final RelNode relNode = new AbstractRelNode(cluster, traitSet) {
    };
    final RelDataTypeSystem dataTypeSystem = new RelDataTypeSystemImpl() {
    };

    final RelDataType dataType = new BasicSqlType(dataTypeSystem, SqlTypeName.BOOLEAN);
    final List<RexLiteral> constants =
        Collections.singletonList(
            RexLiteral.fromJdbcString(dataType,
                SqlTypeName.BOOLEAN,
                "TRUE"));
    final RelDataType rowDataType = new BasicSqlType(dataTypeSystem, SqlTypeName.ROW);
    final List<Window.Group> groups = Collections.emptyList();

    final EnumerableWindow original =
        new EnumerableWindow(cluster, traitSet, relNode, constants, rowDataType, groups);
    final List<RexLiteral> newConstants =
        Collections.singletonList(
            RexLiteral.fromJdbcString(dataType,
                SqlTypeName.BOOLEAN,
                "FALSE"));
    final Window updated = original.copy(newConstants);

    assertNotSame(original, updated);
    assertThat(original.getConstants(), hasSize(1));
    assertSame(constants.get(0), original.getConstants().get(0));
    assertThat(updated.getConstants(), hasSize(1));
    assertSame(newConstants.get(0), updated.getConstants().get(0));
  }
}
