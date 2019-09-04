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

import org.apache.calcite.sql.validate.implicit.TypeCoercion;

import org.junit.Test;

/**
 * Test cases for implicit type coercion converter. see {@link TypeCoercion} doc
 * or <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">CalciteImplicitCasts</a>
 * for conversion details.
 */
public class TypeCoercionConverterTest extends SqlToRelTestBase {

  @Override protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(TypeCoercionConverterTest.class);
  }

  @Override protected Tester createTester() {
    return super.createTester().withCatalogReaderFactory(new TypeCoercionTest()
        .getCatalogReaderFactory());
  }

  /** Test case for {@link TypeCoercion#commonTypeForBinaryComparison}. */
  @Test public void testBinaryComparable() {
    // for constant cast, there is reduce rule
    checkPlanEquals("select 1<'1' from (values true)");
  }

  @Test public void testBinaryComparable1() {
    checkPlanEquals("select 1<='1' from (values true)");
  }

  @Test public void testBinaryComparable2() {
    checkPlanEquals("select 1>'1' from (values true)");
  }

  @Test public void testBinaryComparable3() {
    checkPlanEquals("select 1>='1' from (values true)");
  }

  @Test public void testBinaryComparable4() {
    checkPlanEquals("select 1='1' from (values true)");
  }

  @Test public void testBinaryComparable5() {
    checkPlanEquals("select t1_date > t1_timestamp from t1");
  }

  @Test public void testBinaryComparable6() {
    checkPlanEquals("select '2' is not distinct from 2 from (values true)");
  }

  /** Test cases for {@link TypeCoercion#inOperationCoercion}. */
  @Test public void testInOperation() {
    checkPlanEquals("select 1 in ('1', '2', '3') from (values true)");
  }

  @Test public void testInOperation1() {
    checkPlanEquals("select (1, 2) in (select '1', '2' "
        + "from (values (true, true))) from (values true)");
  }

  @Test public void testInOperation2() {
    checkPlanEquals("select (1, 2) in (('1', '2'), ('3', '4')) from (values true)");
  }

  /** Test cases for
   * {@link org.apache.calcite.sql.validate.implicit.TypeCoercionImpl#booleanEquality}. */
  @Test public void testBooleanEquality() {
    // REVIEW Danny 2018-05-16: Now we do not support cast between numeric <-> boolean for
    // Calcite execution runtime, but we still add cast in the plan so other systems
    // using Calcite can rewrite Cast operator implementation.
    // for this case, we replace the boolean literal with numeric 1.
    checkPlanEquals("select 1=true from (values true)");
  }

  @Test public void testBooleanEquality1() {
    checkPlanEquals("select 1.0=true from (values true)");
  }

  @Test public void testBooleanEquality2() {
    checkPlanEquals("select 0.0=true from (values true)");
  }

  @Test public void testBooleanEquality3() {
    checkPlanEquals("select 1.23=t1_boolean from t1");
  }

  @Test public void testBooleanEquality4() {
    // int boolean
    checkPlanEquals("select t1_smallint=t1_boolean from t1");
  }

  @Test public void testBooleanEquality5() {
    checkPlanEquals("select 10000000000=true from (values true)");
  }

  @Test public void testCaseWhen() {
    checkPlanEquals("select case when 1 > 0 then t2_bigint else t2_decimal end from t2");
  }

  @Test  public void testBuiltinFunctionCoercion() {
    checkPlanEquals("select 1||'a' from (values true)");
  }

  @Test public void testStarImplicitTypeCoercion() {
    checkPlanEquals("select * from (values(1, '3')) union select * from (values('2', 4))");
  }

  @Test public void testSetOperations() {
    // int decimal smallint double
    // char decimal float bigint
    // char decimal float double
    // char decimal smallint double
    final String sql = "select t1_int, t1_decimal, t1_smallint, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_float, t2_bigint from t2 "
        + "union select t1_varchar20, t1_decimal, t1_float, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_smallint, t2_double from t2";
    checkPlanEquals(sql);
  }

  private void checkPlanEquals(String sql) {
    tester.assertConvertsTo(sql, "${plan}");
  }
}

// End TypeCoercionConverterTest.java
