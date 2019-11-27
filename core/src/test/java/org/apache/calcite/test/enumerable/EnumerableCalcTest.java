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
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.Test;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableCalc}
 */
public class EnumerableCalcTest {

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3536">[CALCITE-3536]
   * Wrong semantics in CoalesceImplementor</a>.
   */
  @Test public void testCoalesceWithoutWriting() {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new JdbcTest.HrSchema()))
        .query("?")
        .withRel(
            builder -> builder
                .scan("s", "emps")
                .project(builder.field(4))
                .filter(
                    builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        builder.call(SqlStdOperatorTable.COALESCE,
                            builder.field(0), builder.literal(0)),
                        builder.literal(500)))
                .build()
        )
        .planContains(""
            + "            public boolean moveNext() {\n"
            + "              while (inputEnumerator.moveNext()) {\n"
            + "                final Integer inp4_ = (Integer) ((Object[]) inputEnumerator.current())[4];\n"
            + "                if ((inp4_ != null ? inp4_.intValue() : 0) >= 500) {\n"
            + "                  return true;\n"
            + "                }\n"
            + "              }\n"
            + "              return false;\n"
            + "            }")
        .returnsUnordered(
            "commission=500",
            "commission=1000");
  }
}

// End EnumerableCalcTest.java
