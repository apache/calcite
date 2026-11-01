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
package org.apache.calcite.rex;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.SmallStack;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests for {@link RexUtil}. */
class RexUtilTest {
  @Test void testSubQueryFinderByKind() {
    final RelBuilder builder =
        RelBuilder.create(Frameworks.newConfigBuilder().build());
    final RelNode rel = builder.values(new String[] {"i"}, 1).build();
    final RexSubQuery arrayQuery = RexSubQuery.array(rel);
    final RexSubQuery scalarQuery = RexSubQuery.scalar(rel);
    final RexNode expression = rel.getCluster().getRexBuilder()
        .makeCall(SqlStdOperatorTable.ROW, arrayQuery, scalarQuery);

    assertSame(arrayQuery, RexUtil.SubQueryFinder.find(expression));
    assertSame(scalarQuery,
        RexUtil.SubQueryFinder.find(expression, SqlKind.SCALAR_QUERY));
    assertNull(RexUtil.SubQueryFinder.find(expression, SqlKind.EXISTS));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RexUtil#flattenAnd} computes a deeply nested
   * binary AND tree on a small-stack thread; the previous recursive
   * implementation overflows. */
  @Test void testFlattenAndDeeplyNested() {
    final RelBuilder builder =
        RelBuilder.create(Frameworks.newConfigBuilder().build());
    final RexBuilder rexBuilder = builder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        builder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    // Build AND(AND(AND(..., $1), $2), $3, ...) as a left-deep binary tree,
    // iteratively. Hash codes are pre-warmed bottom-up so that the
    // de-duplication set inside flattenAnd does not itself recurse deeply.
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    node.hashCode();
    for (int i = 1; i < depth; i++) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.AND, node,
              rexBuilder.makeInputRef(booleanType, i));
      node.hashCode();
    }
    final RexNode deepTree = node;
    final List<RexNode> flat =
        SmallStack.run(() -> RexUtil.flattenAnd(ImmutableList.of(deepTree)));
    assertThat(flat, hasSize(depth));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RexUtil#flatten} computes a deeply nested binary
   * AND tree on a small-stack thread; the previous recursive implementation
   * overflows. */
  @Test void testFlattenDeeplyNested() {
    final RelBuilder builder =
        RelBuilder.create(Frameworks.newConfigBuilder().build());
    final RexBuilder rexBuilder = builder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        builder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    for (int i = 1; i < depth; i++) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.AND, node,
              rexBuilder.makeInputRef(booleanType, i));
    }
    final RexNode deepTree = node;
    final List<RexNode> flat =
        SmallStack.run(
            () -> RexUtil.flatten(ImmutableList.of(deepTree),
            SqlStdOperatorTable.AND));
    assertThat(flat, hasSize(depth));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RexUtil#isFlat(RexNode)} computes a deeply nested
   * tree of a non-associative operator (NOT) on a small-stack thread, where
   * short-circuiting at the top level is not possible; the previous recursive
   * implementation overflows. */
  @Test void testIsFlatDeeplyNested() {
    final RelBuilder builder =
        RelBuilder.create(Frameworks.newConfigBuilder().build());
    final RexBuilder rexBuilder = builder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        builder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    for (int i = 0; i < depth; i++) {
      node = rexBuilder.makeCall(SqlStdOperatorTable.NOT, node);
    }
    final RexNode deepTree = node;
    assertThat(SmallStack.run(() -> RexUtil.isFlat(deepTree)), is(true));
  }
}
