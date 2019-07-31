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
package org.apache.calcite.test;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link RexShuttle}
 */
public class RexShuttleTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3165">[CALCITE-3165]
   * Project#accept(RexShuttle shuttle) does not update rowType</a>. */
  @Test
  public void testProjectUpdatesRowType() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // Equivalent SQL: SELECT deptno, sal FROM emp
    final RelNode root =
        builder
            .scan("EMP")
            .project(
                builder.field("DEPTNO"),
                builder.field("SAL"))
            .build();

    // Equivalent SQL: SELECT CAST(deptno AS VARCHAR), CAST(sal AS VARCHAR) FROM emp
    final RelNode rootWithCast =
        builder
            .scan("EMP")
            .project(
                builder.cast(builder.field("DEPTNO"), SqlTypeName.VARCHAR),
                builder.cast(builder.field("SAL"), SqlTypeName.VARCHAR))
            .build();
    final RelDataType type = rootWithCast.getRowType();

    // Transform the first expression into the second one, by using a RexShuttle
    // that converts every RexInputRef into a 'CAST(RexInputRef AS VARCHAR)'
    final RelNode rootWithCastViaRexShuttle = root.accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        return  builder.cast(inputRef, SqlTypeName.VARCHAR);
      }
    });
    final RelDataType type2 = rootWithCastViaRexShuttle.getRowType();

    assertThat(type, is(type2));
  }
}

// End RexShuttleTest.java
