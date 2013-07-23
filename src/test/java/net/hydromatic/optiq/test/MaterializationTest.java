/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.materialize.MaterializationService;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
public class MaterializationTest {
  @Test public void testFilter() {
    try {
      OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .withMaterializations(
              JdbcTest.HR_MODEL,
              "m0",
              "select * from \"emps\" where \"deptno\" = 10")
          .query(
              "select \"empid\" + 1 from \"emps\" where \"deptno\" = 10")
          .enableMaterializations(true)
          .explainContains(
              "EnumerableTableAccessRel(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    } finally {
      MaterializationService.INSTANCE.clear();
    }
  }

  @Ignore
  @Test public void testSwapJoin() {
    String q1 =
        "select count(*) as c from \"foodmart\".\"sales_fact_1997\" as s join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\"";
    String q2 =
        "select count(*) as c from \"foodmart\".\"time_by_day\" as t join \"foodmart\".\"sales_fact_1997\" as s on t.\"time_id\" = s.\"time_id\"";
  }

  @Ignore
  @Test public void testDifferentColumnNames() {}

  @Ignore
  @Test public void testDifferentType() {}

  @Ignore
  @Test public void testPartialUnion() {}

  @Ignore
  @Test public void testNonDisjointUnion() {}

  @Ignore
  @Test public void testMaterializationReferencesTableInOtherSchema() {}
}

// End MaterializationTest.java
