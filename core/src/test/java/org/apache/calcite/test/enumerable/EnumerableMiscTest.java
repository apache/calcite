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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.jupiter.api.Test;

/**
 * Unit test for miscellaneous operators.
 */
class EnumerableMiscTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4415">[CALCITE-4415]
   * SqlStdOperatorTable.NOT_LIKE has a wrong implementor</a>. */
  @Test void testNotLike() {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new JdbcTest.HrSchema()))
        .query("?")
        .withRel(
            builder -> builder
                .scan("s", "emps")
                .filter(
                    builder.call(
                        SqlStdOperatorTable.NOT_LIKE,
                        builder.field("name"),
                        builder.literal("%r%c")))
                .project(
                    builder.field("empid"),
                    builder.field("name"))
                .build())
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }
}
