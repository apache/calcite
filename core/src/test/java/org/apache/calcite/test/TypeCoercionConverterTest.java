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
  @Test public void testBinaryComparison() {
    // for constant cast, there is reduce rule
    checkPlanEquals("select\n"
        + "1<'1' as f0,\n"
        + "1<='1' as f1,\n"
        + "1>'1' as f2,\n"
        + "1>='1' as f3,\n"
        + "1='1' as f4,\n"
        + "t1_date > t1_timestamp as f5,\n"
        + "'2' is not distinct from 2 as f6,\n"
        + "'2019-09-23' between t1_date and t1_timestamp as f7,\n"
        + "cast('2019-09-23' as date) between t1_date and t1_timestamp as f8\n"
        + "from t1");
  }

  /** Test cases for {@link TypeCoercion#inOperationCoercion}. */
  @Test public void testInOperation() {
    checkPlanEquals("select\n"
        + "1 in ('1', '2', '3') as f0,\n"
        + "(1, 2) in (('1', '2')) as f1,\n"
        + "(1, 2) in (('1', '2'), ('3', '4')) as f2\n"
        + "from (values (true, true, true))");
  }

  /** Test cases for
   * {@link org.apache.calcite.sql.validate.implicit.TypeCoercionImpl#booleanEquality}. */
  @Test public void testBooleanEquality() {
    // REVIEW Danny 2018-05-16: Now we do not support cast between numeric <-> boolean for
    // Calcite execution runtime, but we still add cast in the plan so other systems
    // using Calcite can rewrite Cast operator implementation.
    // for this case, we replace the boolean literal with numeric 1.
    checkPlanEquals("select\n"
        + "1=true as f0,\n"
        + "1.0=true as f1,\n"
        + "0.0=true=true as f2,\n"
        + "1.23=t1_boolean as f3,\n"
        + "t1_smallint=t1_boolean as f4,\n"
        + "10000000000=true as f5\n"
        + "from t1");
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

  @Test public void testSetOperation() {
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
