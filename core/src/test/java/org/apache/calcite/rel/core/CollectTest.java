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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for <a href="https://issues.apache.org/jira/browse/CALCITE-7717">[CALCITE-7717]
 * Add a Collect.isValid method to check type invariants</a>.
 */
class CollectTest {
  @Test void testIsValid() {
    final RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode oneColumn = b.values(new String[] {"i"}, 1, 2).build();
    final RelDataTypeFactory typeFactory = oneColumn.getCluster().getTypeFactory();

    // Element type is the input row type.  'x' is the name of the result field
    final Collect collect0 = Collect.create(oneColumn, SqlKind.ARRAY_QUERY_CONSTRUCTOR, "x");
    assertThat(collect0.isValid(Litmus.IGNORE, null), is(true));

    // Element type is the type of the sole input column.
    final Collect collect1 =
        new Collect(oneColumn.getCluster(),
            oneColumn.getCluster().traitSetOf(Convention.NONE), oneColumn,
            Collect.deriveRowType(typeFactory, SqlTypeName.ARRAY, "x",
                oneColumn.getRowType().getFieldList().get(0).getType()));
    assertThat(collect1.isValid(Litmus.IGNORE, null), is(true));

    // Array over two columns is invalid
    final RelNode twoColumns = b.values(new String[] {"k", "v"}, 1, "a").build();
    final Collect mismatched =
            new Collect(oneColumn.getCluster(),
            oneColumn.getCluster().traitSetOf(Convention.NONE), oneColumn,
            Collect.deriveRowType(typeFactory, SqlTypeName.ARRAY, "x",
                twoColumns.getRowType()));
    assertThat(mismatched.isValid(Litmus.IGNORE, null), is(false));

    final RelDataType mapRowType =
        Collect.deriveRowType(typeFactory, SqlTypeName.MAP, "x", twoColumns.getRowType());

   // A MAP(subquery) over an input that does not have exactly two columns is invalid
    final Collect mapOverOneColumn =
        new Collect(oneColumn.getCluster(),
            oneColumn.getCluster().traitSetOf(Convention.NONE), oneColumn, mapRowType);
    assertThat(mapOverOneColumn.isValid(Litmus.IGNORE, null), is(false));

    // The same MAP row type over the two-column input is valid.
    final Collect mapOverTwoColumns =
        new Collect(twoColumns.getCluster(),
            twoColumns.getCluster().traitSetOf(Convention.NONE), twoColumns, mapRowType);
    assertThat(mapOverTwoColumns.isValid(Litmus.IGNORE, null), is(true));
  }
}
