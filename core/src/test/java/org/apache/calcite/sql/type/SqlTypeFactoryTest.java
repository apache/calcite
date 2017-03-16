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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;

import com.google.common.collect.Lists;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link SqlTypeFactoryImpl}.
 */
public class SqlTypeFactoryTest {

  @Test public void testLeastRestrictiveWithAny() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
  }

  @Test public void testLeastRestrictiveWithNumbers() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.BIGINT));
  }

  @Test public void testLeastRestrictiveWithNullability() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlVarcharNullable, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test public void testLeastRestrictiveWithNull() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlNull, f.sqlNull));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.NULL));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  /** Unit test for {@link SqlTypeUtil#comparePrecision(int, int)}
   * and  {@link SqlTypeUtil#maxPrecision(int, int)}. */
  @Test public void testMaxPrecision() {
    final int un = RelDataType.PRECISION_NOT_SPECIFIED;
    checkPrecision(1, 1, 1, 0);
    checkPrecision(2, 1, 2, 1);
    checkPrecision(2, 100, 100, -1);
    checkPrecision(2, un, un, -1);
    checkPrecision(un, 2, un, 1);
    checkPrecision(un, un, un, 0);
  }

  private void checkPrecision(int p0, int p1, int expectedMax,
      int expectedComparison) {
    assertThat(SqlTypeUtil.maxPrecision(p0, p1), is(expectedMax));
    assertThat(SqlTypeUtil.maxPrecision(p1, p0), is(expectedMax));
    assertThat(SqlTypeUtil.maxPrecision(p0, p0), is(p0));
    assertThat(SqlTypeUtil.maxPrecision(p1, p1), is(p1));
    assertThat(SqlTypeUtil.comparePrecision(p0, p1), is(expectedComparison));
    assertThat(SqlTypeUtil.comparePrecision(p0, p0), is(0));
    assertThat(SqlTypeUtil.comparePrecision(p1, p1), is(0));
  }

  /** Sets up data needed by a test. */
  private static class Fixture {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType sqlBigInt = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    final RelDataType sqlInt = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    final RelDataType sqlVarcharNullable = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    final RelDataType sqlNull = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.NULL), false);
    final RelDataType sqlAny = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.ANY), false);
  }

}

// End SqlTypeFactoryTest.java
