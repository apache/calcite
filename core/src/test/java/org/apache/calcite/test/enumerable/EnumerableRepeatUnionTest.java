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

import org.apache.calcite.adapter.enumerable.EnumerableRepeatUnion;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;

import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link EnumerableRepeatUnion}.
 *
 * <p>Added in
 * <a href="https://issues.apache.org/jira/browse/CALCITE-2812">[CALCITE-2812]
 * Add algebraic operators to allow expressing recursive queries</a>.
 */
public class EnumerableRepeatUnionTest {

  @Test public void testGenerateNumbers() {
    CalciteAssert.that()
        .query("?")
        .withRel(
            //   WITH RECURSIVE delta(n) AS (
            //     VALUES (1)
            //     UNION ALL
            //     SELECT n+1 FROM delta WHERE n < 10
            //   )
            //   SELECT * FROM delta
            builder -> builder
                .values(new String[] { "i" }, 1)
                .transientScan("DELTA")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(0),
                        builder.literal(10)))
                .project(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field(0),
                        builder.literal(1)))
                .repeatUnion("DELTA", true)
                .build())
        .returnsOrdered("i=1", "i=2", "i=3", "i=4", "i=5", "i=6", "i=7", "i=8", "i=9", "i=10");
  }

  @Test public void testFactorial() {
    CalciteAssert.that()
        .query("?")
        .withRel(
            //   WITH RECURSIVE d(n, fact) AS (
            //     VALUES (0, 1)
            //     UNION ALL
            //     SELECT n+1, (n+1)*fact FROM d WHERE n < 7
            //   )
            //   SELECT * FROM delta
            builder -> builder
                .values(new String[] { "n", "fact" }, 0, 1)
                .transientScan("D")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field("n"),
                        builder.literal(7)))
                .project(
                    Arrays.asList(
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field("n"),
                            builder.literal(1)),
                        builder.call(SqlStdOperatorTable.MULTIPLY,
                            builder.call(SqlStdOperatorTable.PLUS,
                                builder.field("n"),
                                builder.literal(1)),
                            builder.field("fact"))),
                    Arrays.asList("n", "fact"))
                .repeatUnion("D", true)
                .build())
        .returnsOrdered("n=0; fact=1",
            "n=1; fact=1",
            "n=2; fact=2",
            "n=3; fact=6",
            "n=4; fact=24",
            "n=5; fact=120",
            "n=6; fact=720",
            "n=7; fact=5040");
  }

  @Test public void testGenerateNumbersNestedRecursion() {
    CalciteAssert.that()
        .query("?")
        .withRel(
            //   WITH RECURSIVE t_out(n) AS (
            //     WITH RECURSIVE t_in(n) AS (
            //       VALUES (1)
            //       UNION ALL
            //       SELECT n+1 FROM t_in WHERE n < 9
            //     )
            //     SELECT n FROM t_in
            //     UNION ALL
            //     SELECT n*10 FROM t_out WHERE n < 100
            //   )
            //   SELECT n FROM t_out
            builder -> builder
                .values(new String[] { "n" }, 1)
                .transientScan("T_IN")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field("n"),
                        builder.literal(9)))
                .project(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("n"),
                        builder.literal(1)))
                .repeatUnion("T_IN", true)

                .transientScan("T_OUT")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field("n"),
                        builder.literal(100)))
                .project(
                    builder.call(SqlStdOperatorTable.MULTIPLY,
                        builder.field("n"),
                        builder.literal(10)))
                .repeatUnion("T_OUT", true)
                .build())
        .returnsOrdered(
            "n=1",   "n=2",   "n=3",   "n=4",   "n=5",   "n=6",   "n=7",   "n=8",   "n=9",
            "n=10",  "n=20",  "n=30",  "n=40",  "n=50",  "n=60",  "n=70",  "n=80",  "n=90",
            "n=100", "n=200", "n=300", "n=400", "n=500", "n=600", "n=700", "n=800", "n=900");
  }

}

// End EnumerableRepeatUnionTest.java
