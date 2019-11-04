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
package org.apache.calcite.plan;

import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.util.Litmus;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Test to verify {@link SubstitutionVisitor}.
 */
public class SubstitutionVisitorTest extends SqlToRelTestBase {

  @Test public void testRowTypesEquivalent0() {
    final SqlToRelTestBase.Tester tester = createTester();
    final MutableRel rel0 = MutableRels.toMutable(
        tester.convertSqlToRel("SELECT \"EMPNO\", \"DEPTNO\" FROM \"EMP_R\"").rel);
    final MutableRel rel1 = MutableRels.toMutable(
        tester.convertSqlToRel("SELECT \"DEPTNO\", \"SLACKINGMIN\" FROM \"DEPT_R\"").rel);

    assertThat(rel0.rowType.toString(),
        equalTo("RecordType(INTEGER EMPNO, INTEGER DEPTNO)"));
    assertThat(rel1.rowType.toString(),
        equalTo("RecordType(INTEGER DEPTNO, INTEGER SLACKINGMIN)"));

    // RowTypes between rel0 and rel1 are equal.
    assertThat(SubstitutionVisitor.rowTypesAreEquivalent(rel0, rel1, Litmus.IGNORE),
        equalTo(true));
  }

  @Test public void testRowTypesEquivalent1() {
    final SqlToRelTestBase.Tester tester = createTester();
    final MutableRel rel0 = MutableRels.toMutable(
        tester.convertSqlToRel("SELECT \"EMPNO\", \"SLACKER\" FROM \"EMP_R\"").rel);
    final MutableRel rel1 = MutableRels.toMutable(
        tester.convertSqlToRel("SELECT \"DEPTNO\", \"SLACKINGMIN\" FROM \"DEPT_R\"").rel);

    assertThat(rel0.rowType.toString(),
        equalTo("RecordType(INTEGER EMPNO, BOOLEAN SLACKER)"));
    assertThat(rel1.rowType.toString(),
        equalTo("RecordType(INTEGER DEPTNO, INTEGER SLACKINGMIN)"));

    // RowTypes between rel0 and rel1 are not equal.
    assertThat(SubstitutionVisitor.rowTypesAreEquivalent(rel0, rel1, Litmus.IGNORE),
        equalTo(false));
  }

}

// End SubstitutionVisitorTest.java
