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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;

import org.junit.Test;

/** Test case for CALCITE-2894 */
public class RelMdPercentageOriginalRowsTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2894">[CALCITE-2894]
   * NullPointerException thrown by RelMdPercentageOriginalRows when explaining
   * plan with all attributes</a>. */
  @Test public void testExplainAllAttributesSemiJoinUnionCorrelate() {
    CalciteAssert.that()
            .with(CalciteConnectionProperty.LEX, Lex.JAVA)
            .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
            .withSchema("s", new ReflectiveSchema(new JdbcTest.HrSchema()))
            .query(
                    "select deptno, name from depts where deptno in (\n"
                            + " select e.deptno from emps e where exists (select 1 from depts d where d.deptno=e.deptno)\n"
                            + " union select e.deptno from emps e where e.salary > 10000) ")
            .explainMatches("including all attributes ",
                    CalciteAssert.checkResultContains("EnumerableCorrelate"));
  }
}

// End RelMdPercentageOriginalRowsTest.java
