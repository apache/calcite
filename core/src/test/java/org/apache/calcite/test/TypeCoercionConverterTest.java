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

import org.junit.jupiter.api.Test;

/**
 * Test cases for implicit type coercion converter. see {@link TypeCoercion} doc
 * or <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">CalciteImplicitCasts</a>
 * for conversion details.
 */
class TypeCoercionConverterTest extends SqlToRelTestBase {

  protected static final SqlToRelFixture FIXTURE =
      SqlToRelFixture.DEFAULT
          .withDiffRepos(DiffRepository.lookup(TypeCoercionConverterTest.class))
          .withFactory(f -> f.withCatalogReader(TCatalogReader::create))
          .withDecorrelate(false);

  @Override public SqlToRelFixture fixture() {
    return FIXTURE;
  }

  /** Test case for {@link TypeCoercion#commonTypeForBinaryComparison}. */
  @Test void testBinaryComparison() {
    // for constant cast, there is reduce rule
    sql("select\n"
        + "1<'1' as f0,\n"
        + "1<='1' as f1,\n"
        + "1>'1' as f2,\n"
        + "1>='1' as f3,\n"
        + "1='1' as f4,\n"
        + "t1_date > t1_timestamp as f5,\n"
        + "'2' is not distinct from 2 as f6,\n"
        + "'2019-09-23' between t1_date and t1_timestamp as f7,\n"
        + "cast('2019-09-23' as date) between t1_date and t1_timestamp as f8\n"
        + "from t1").ok();
  }

  /** Test cases for {@link TypeCoercion#inOperationCoercion}. */
  @Test void testInOperation() {
    sql("select\n"
        + "1 in ('1', '2', '3') as f0,\n"
        + "(1, 2) in (('1', '2')) as f1,\n"
        + "(1, 2) in (('1', '2'), ('3', '4')) as f2\n"
        + "from (values (true, true, true))").ok();
  }

  @Test void testNotInOperation() {
    sql("select\n"
        + "1 not in ('1', '2', '3') as f0,\n"
        + "(1, 2) not in (('1', '2')) as f1,\n"
        + "(1, 2) not in (('1', '2'), ('3', '4')) as f2\n"
        + "from (values (false, false, false))").ok();
  }

  /** Test cases for {@link TypeCoercion#inOperationCoercion}. */
  @Test void testInDateTimestamp() {
    sql("select (t1_timestamp, t1_date)\n"
        + "in ((DATE '2020-04-16', TIMESTAMP '2020-04-16 11:40:53'))\n"
        + "from t1").ok();
  }

  /** Test case for
   * {@link org.apache.calcite.sql.validate.implicit.TypeCoercionImpl}.{@code booleanEquality}. */
  @Test void testBooleanEquality() {
    // REVIEW Danny 2018-05-16: Now we do not support cast between numeric <-> boolean for
    // Calcite execution runtime, but we still add cast in the plan so other systems
    // using Calcite can rewrite Cast operator implementation.
    // for this case, we replace the boolean literal with numeric 1.
    sql("select\n"
        + "1=true as f0,\n"
        + "1.0=true as f1,\n"
        + "0.0=true=true as f2,\n"
        + "1.23=t1_boolean as f3,\n"
        + "t1_smallint=t1_boolean as f4,\n"
        + "10000000000=true as f5\n"
        + "from t1").ok();
  }

  @Test void testCaseWhen() {
    sql("select case when 1 > 0 then t2_bigint else t2_decimal end from t2")
        .ok();
  }

  @Test void testBuiltinFunctionCoercion() {
    sql("select 1||'a' from (values true)").ok();
  }

  @Test void testStarImplicitTypeCoercion() {
    sql("select * from (values(1, '3')) union select * from (values('2', 4))")
        .ok();
  }

  @Test void testSetOperation() {
    // int decimal smallint double
    // char decimal float bigint
    // char decimal float double
    // char decimal smallint double
    final String sql = "select t1_int, t1_decimal, t1_smallint, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_float, t2_bigint from t2 "
        + "union select t1_varchar20, t1_decimal, t1_float, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_smallint, t2_double from t2";
    sql(sql).ok();
  }

  @Test void testInsertQuerySourceCoercion() {
    final String sql = "insert into t1 select t2_smallint, t2_int, t2_bigint, t2_float,\n"
        + "t2_double, t2_decimal, t2_int, t2_date, t2_timestamp, t2_varchar20, t2_int from t2";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4897">[CALCITE-4897]
   * Set operation in DML, implicit type conversion is not complete</a>. */
  @Test void testInsertUnionQuerySourceCoercion() {
    final String sql = "insert into t1 "
        + "select 'a', 1, 1.0,"
        + " 0, 0, 0, 0, TIMESTAMP '2021-11-28 00:00:00', date '2021-11-28', x'0A', false union "
        + "select 'b', 2, 2,"
        + " 0, 0, 0, 0, TIMESTAMP '2021-11-28 00:00:00', date '2021-11-28', x'0A', false union "
        + "select 'c', CAST(3 AS SMALLINT), 3.0,"
        + " 0, 0, 0, 0, TIMESTAMP '2021-11-28 00:00:00', date '2021-11-28', x'0A', false union "
        + "select 'd', 4, 4.0,"
        + " 0, 0, 0, 0, TIMESTAMP '2021-11-28 00:00:00', date '2021-11-28', x'0A', false union "
        + "select 'e', 5, 5.0,"
        + " 0, 0, 0, 0, TIMESTAMP '2021-11-28 00:00:00', date '2021-11-28', x'0A', false";
    sql(sql).ok();
  }

  @Test void testUpdateQuerySourceCoercion() {
    final String sql = "update t1 set t1_varchar20=123, "
        + "t1_date=TIMESTAMP '2020-01-03 10:14:34', t1_int=12.3";
    sql(sql).ok();
  }

}
