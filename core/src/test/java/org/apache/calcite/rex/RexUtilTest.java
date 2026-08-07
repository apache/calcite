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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

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
}
