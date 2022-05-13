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
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinToCorrelateRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HierarchySchema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Unit tests for {@link EnumerableRepeatUnion}.
 *
 * <p>Added in
 * <a href="https://issues.apache.org/jira/browse/CALCITE-2812">[CALCITE-2812]
 * Add algebraic operators to allow expressing recursive queries</a>.
 */
class EnumerableRepeatUnionTest {

  @Test void testGenerateNumbers() {
    CalciteAssert.that()
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

  @Test void testGenerateNumbers2() {
    CalciteAssert.that()
        .withRel(
            //   WITH RECURSIVE aux(i) AS (
            //     VALUES (0)
            //     UNION -- (ALL would generate an infinite loop!)
            //     SELECT (i+1)%10 FROM aux WHERE i < 10
            //   )
            //   SELECT * FROM aux
            builder -> builder
                .values(new String[] { "i" }, 0)
                .transientScan("AUX")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(0),
                        builder.literal(10)))
                .project(
                    builder.call(SqlStdOperatorTable.MOD,
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field(0),
                            builder.literal(1)),
                        builder.literal(10)))
                .repeatUnion("AUX", false)
                .build())
        .returnsOrdered("i=0", "i=1", "i=2", "i=3", "i=4", "i=5", "i=6", "i=7", "i=8", "i=9");
  }

  @Test void testGenerateNumbers3() {
    CalciteAssert.that()
        .withRel(
            //   WITH RECURSIVE aux(i, j) AS (
            //     VALUES (0, 0)
            //     UNION -- (ALL would generate an infinite loop!)
            //     SELECT (i+1)%10, j FROM aux WHERE i < 10
            //   )
            //   SELECT * FROM aux
            builder -> builder
                .values(new String[] { "i", "j" }, 0, 0)
                .transientScan("AUX")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(0),
                        builder.literal(10)))
                .project(
                    builder.call(SqlStdOperatorTable.MOD,
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field(0),
                            builder.literal(1)),
                        builder.literal(10)),
                    builder.field(1))
                .repeatUnion("AUX", false)
                .build())
        .returnsOrdered("i=0; j=0",
            "i=1; j=0",
            "i=2; j=0",
            "i=3; j=0",
            "i=4; j=0",
            "i=5; j=0",
            "i=6; j=0",
            "i=7; j=0",
            "i=8; j=0",
            "i=9; j=0");
  }

  @Test void testFactorial() {
    CalciteAssert.that()
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

  @Test void testGenerateNumbersNestedRecursion() {
    CalciteAssert.that()
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4139">[CALCITE-4139]
   * Prevent NPE in ListTransientTable</a>. */
  @Test void testGenerateNumbersWithNull() {
    CalciteAssert.that()
        .withRel(
            builder -> builder
                .values(new String[] { "i" }, 1, 2, null, 3)
                .transientScan("DELTA")
                .filter(
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(0),
                        builder.literal(3)))
                .project(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field(0),
                        builder.literal(1)))
                .repeatUnion("DELTA", true)
                .build())
        .returnsOrdered("i=1", "i=2", "i=null", "i=3", "i=2", "i=3", "i=3");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4054">[CALCITE-4054]
   * RepeatUnion containing a Correlate with a transientScan on its RHS causes NPE</a>. */
  @Test void testRepeatUnionWithCorrelateWithTransientScanOnItsRight() {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new HierarchySchema()))
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(JoinToCorrelateRule.Config.DEFAULT.toRule());
          planner.removeRule(JoinCommuteRule.Config.DEFAULT.toRule());
          planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(builder -> {
          builder
              //   WITH RECURSIVE delta(empid, name) as (
              //     SELECT empid, name FROM emps WHERE empid = 2
              //     UNION ALL
              //     SELECT e.empid, e.name FROM delta d
              //                            JOIN hierarchies h ON d.empid = h.managerid
              //                            JOIN emps e        ON h.subordinateid = e.empid
              //   )
              //   SELECT empid, name FROM delta
              .scan("s", "emps")
              .filter(
                  builder.equals(
                      builder.field("empid"),
                      builder.literal(2)))
              .project(
                  builder.field("emps", "empid"),
                  builder.field("emps", "name"))

              .transientScan("#DELTA#");
          RelNode transientScan = builder.build(); // pop the transientScan to use it later

          builder
              .scan("s", "hierarchies")
              .push(transientScan) // use the transientScan as right input of the join
              .join(
                  JoinRelType.INNER,
                  builder.equals(
                      builder.field(2, "#DELTA#", "empid"),
                      builder.field(2, "hierarchies", "managerid")))

              .scan("s", "emps")
              .join(
                  JoinRelType.INNER,
                  builder.equals(
                      builder.field(2, "hierarchies", "subordinateid"),
                      builder.field(2, "emps", "empid")))
              .project(
                  builder.field("emps", "empid"),
                  builder.field("emps", "name"))
              .repeatUnion("#DELTA#", true);
          return builder.build();
        })
        .explainHookMatches(""
            + "EnumerableRepeatUnion(all=[true])\n"
            + "  EnumerableTableSpool(readType=[LAZY], writeType=[LAZY], table=[[#DELTA#]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[2], expr#6=[=($t0, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableTableSpool(readType=[LAZY], writeType=[LAZY], table=[[#DELTA#]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t3], name=[$t4])\n"
            + "      EnumerableCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])\n"
            // It is important to have EnumerableCorrelate + #DELTA# table scan on its right
            // to reproduce the issue CALCITE-4054
            + "        EnumerableCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
            + "          EnumerableTableScan(table=[[s, hierarchies]])\n"
            + "          EnumerableCalc(expr#0..1=[{inputs}], expr#2=[$cor0], expr#3=[$t2.managerid], expr#4=[=($t0, $t3)], empid=[$t0], $condition=[$t4])\n"
            + "            EnumerableInterpreter\n"
            + "              BindableTableScan(table=[[#DELTA#]])\n"
            + "        EnumerableCalc(expr#0..4=[{inputs}], expr#5=[$cor1], expr#6=[$t5.subordinateid], expr#7=[=($t6, $t0)], empid=[$t0], name=[$t2], $condition=[$t7])\n"
            + "          EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(""
            + "empid=2; name=Emp2\n"
            + "empid=3; name=Emp3\n"
            + "empid=5; name=Emp5");
  }
}
