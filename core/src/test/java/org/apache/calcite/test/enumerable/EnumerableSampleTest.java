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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

public class EnumerableSampleTest
{
  @Test
  void sortedSample() {
    tester(false, new HrSchema())
        .query("select deptno from emps tablesample bernoulli(50)")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(EnumerableRules.ENUMERABLE_SAMPLE_RULE);
        })
        .explainContains(
            "EnumerableCalc(expr#0..4=[{inputs}], deptno=[$t1])\n" +
                "  EnumerableSample\n" +
                "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "deptno=10; max_salary=11500.0; num_employee=3",
            "deptno=20; max_salary=8000.0; num_employee=1");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
