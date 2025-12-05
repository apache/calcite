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
package org.apache.calcite.rel.logical;

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
import org.apache.calcite.test.MockRelOptPlanner;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.rel.core.Window.Group;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test for {@link org.apache.calcite.rel.logical.LogicalWindow}.
 */
public class LogicalWindowTest {
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
    final List<Group> groups = Collections.emptyList();

    final LogicalWindow original =
        new LogicalWindow(cluster, traitSet, relNode, constants, rowDataType, groups);
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
