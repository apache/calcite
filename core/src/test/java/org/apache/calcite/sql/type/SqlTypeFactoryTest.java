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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl.UnknownSqlType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for {@link SqlTypeFactoryImpl}.
 */
class SqlTypeFactoryTest {

  @Test void testLeastRestrictiveWithAny() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
  }

  @Test void testLeastRestrictiveWithNumbers() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.BIGINT));
  }

  @Test void testLeastRestrictiveWithNullability() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlVarcharNullable, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2994">[CALCITE-2994]
   * Least restrictive type among structs does not consider nullability</a>. */
  @Test void testLeastRestrictiveWithNullableStruct() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(ImmutableList.of(f.structOfIntNullable, f.structOfInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ROW));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test void testLeastRestrictiveWithNull() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlNull, f.sqlNull));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.NULL));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test void testLeastRestrictiveForImpossibleWithArray() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.arraySqlChar10, f.sqlChar));
    assertNull(leastRestrictive);
  }

  @Test void testLeastRestrictiveForArrays() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.arraySqlChar10, f.arraySqlChar1));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ARRAY));
    assertThat(leastRestrictive.isNullable(), is(false));
    assertThat(leastRestrictive.getComponentType().getPrecision(), is(10));
  }

  @Test void testLeastRestrictiveForMultisets() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.multisetSqlChar10Nullable, f.multisetSqlChar1));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.MULTISET));
    assertThat(leastRestrictive.isNullable(), is(true));
    assertThat(leastRestrictive.getComponentType().getPrecision(), is(10));
  }

  @Test void testLeastRestrictiveForMultisetsAndArrays() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.multisetSqlChar10Nullable, f.arraySqlChar1));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.MULTISET));
    assertThat(leastRestrictive.isNullable(), is(true));
    assertThat(leastRestrictive.getComponentType().getPrecision(), is(10));
  }

  @Test void testLeastRestrictiveForImpossibleWithMultisets() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.multisetSqlChar10Nullable, f.mapSqlChar1));
    assertNull(leastRestrictive);
  }

  @Test void testLeastRestrictiveForMaps() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.mapSqlChar10Nullable, f.mapSqlChar1));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.MAP));
    assertThat(leastRestrictive.isNullable(), is(true));
    assertThat(leastRestrictive.getKeyType().getPrecision(), is(10));
    assertThat(leastRestrictive.getValueType().getPrecision(), is(10));
  }

  @Test void testLeastRestrictiveForImpossibleWithMaps() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.mapSqlChar10Nullable, f.arraySqlChar1));
    assertNull(leastRestrictive);
  }

  /** Unit test for {@link SqlTypeUtil#comparePrecision(int, int)}
   * and  {@link SqlTypeUtil#maxPrecision(int, int)}. */
  @Test void testMaxPrecision() {
    final int un = RelDataType.PRECISION_NOT_SPECIFIED;
    checkPrecision(1, 1, 1, 0);
    checkPrecision(2, 1, 2, 1);
    checkPrecision(2, 100, 100, -1);
    checkPrecision(2, un, un, -1);
    checkPrecision(un, 2, un, 1);
    checkPrecision(un, un, un, 0);
  }

  /** Unit test for {@link ArraySqlType#getPrecedenceList()}. */
  @Test void testArrayPrecedenceList() {
    SqlTypeFixture f = new SqlTypeFixture();
    assertThat(checkPrecendenceList(f.arrayBigInt, f.arrayBigInt, f.arrayFloat),
        is(3));
    assertThat(
        checkPrecendenceList(f.arrayOfArrayBigInt, f.arrayOfArrayBigInt,
            f.arrayOfArrayFloat), is(3));
    assertThat(checkPrecendenceList(f.sqlBigInt, f.sqlBigInt, f.sqlFloat),
        is(3));
    assertThat(
        checkPrecendenceList(f.multisetBigInt, f.multisetBigInt,
            f.multisetFloat), is(3));
    assertThat(
        checkPrecendenceList(f.arrayBigInt, f.arrayBigInt,
            f.arrayBigIntNullable), is(0));
    try {
      int i = checkPrecendenceList(f.arrayBigInt, f.sqlBigInt, f.sqlInt);
      fail("Expected assert, got " + i);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("must contain type: BIGINT"));
    }
  }

  private int checkPrecendenceList(RelDataType t, RelDataType type1, RelDataType type2) {
    return t.getPrecedenceList().compareTypePrecedence(type1, type2);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2464">[CALCITE-2464]
   * Allow to set nullability for columns of structured types</a>. */
  @Test void createStructTypeWithNullability() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataTypeFactory typeFactory = f.typeFactory;
    List<RelDataTypeField> fields = new ArrayList<>();
    RelDataTypeField field0 = new RelDataTypeFieldImpl(
            "i", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 = new RelDataTypeFieldImpl(
            "s", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);
    final RelDataType recordType = new RelRecordType(fields); // nullable false by default
    final RelDataType copyRecordType = typeFactory.createTypeWithNullability(recordType, true);
    assertFalse(recordType.isNullable());
    assertTrue(copyRecordType.isNullable());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3429">[CALCITE-3429]
   * AssertionError thrown for user-defined table function with map argument</a>. */
  @Test void testCreateTypeWithJavaMapType() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType relDataType = f.typeFactory.createJavaType(Map.class);
    assertThat(relDataType.getSqlTypeName(), is(SqlTypeName.MAP));
    assertThat(relDataType.getKeyType().getSqlTypeName(), is(SqlTypeName.ANY));

    try {
      f.typeFactory.createSqlType(SqlTypeName.MAP);
      fail();
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("use createMapType() instead"));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3924">[CALCITE-3924]
   * Fix flakey test to handle TIMESTAMP and TIMESTAMP(0) correctly</a>. */
  @Test void testCreateSqlTypeWithPrecision() {
    SqlTypeFixture f = new SqlTypeFixture();
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIME);
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIMESTAMP);
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  }

  private void checkCreateSqlTypeWithPrecision(
      RelDataTypeFactory typeFactory, SqlTypeName sqlTypeName) {
    RelDataType ts = typeFactory.createSqlType(sqlTypeName);
    RelDataType tsWithoutPrecision = typeFactory.createSqlType(sqlTypeName, -1);
    RelDataType tsWithPrecision0 = typeFactory.createSqlType(sqlTypeName, 0);
    RelDataType tsWithPrecision1 = typeFactory.createSqlType(sqlTypeName, 1);
    RelDataType tsWithPrecision2 = typeFactory.createSqlType(sqlTypeName, 2);
    RelDataType tsWithPrecision3 = typeFactory.createSqlType(sqlTypeName, 3);
    // for instance, 8 exceeds max precision for timestamp which is 3
    RelDataType tsWithPrecision8 = typeFactory.createSqlType(sqlTypeName, 8);

    assertThat(ts.toString(), is(sqlTypeName.getName() + "(0)"));
    assertThat(ts.getFullTypeString(), is(sqlTypeName.getName() + "(0) NOT NULL"));
    assertThat(tsWithoutPrecision.toString(), is(sqlTypeName.getName()));
    assertThat(tsWithoutPrecision.getFullTypeString(), is(sqlTypeName.getName() + " NOT NULL"));
    assertThat(tsWithPrecision0.toString(), is(sqlTypeName.getName() + "(0)"));
    assertThat(tsWithPrecision0.getFullTypeString(), is(sqlTypeName.getName() + "(0) NOT NULL"));
    assertThat(tsWithPrecision1.toString(), is(sqlTypeName.getName() + "(1)"));
    assertThat(tsWithPrecision1.getFullTypeString(), is(sqlTypeName.getName() + "(1) NOT NULL"));
    assertThat(tsWithPrecision2.toString(), is(sqlTypeName.getName() + "(2)"));
    assertThat(tsWithPrecision2.getFullTypeString(), is(sqlTypeName.getName() + "(2) NOT NULL"));
    assertThat(tsWithPrecision3.toString(), is(sqlTypeName.getName() + "(3)"));
    assertThat(tsWithPrecision3.getFullTypeString(), is(sqlTypeName.getName() + "(3) NOT NULL"));
    assertThat(tsWithPrecision8.toString(), is(sqlTypeName.getName() + "(3)"));
    assertThat(tsWithPrecision8.getFullTypeString(), is(sqlTypeName.getName() + "(3) NOT NULL"));

    assertThat(ts != tsWithoutPrecision, is(true));
    assertThat(ts == tsWithPrecision0, is(true));
    assertThat(tsWithPrecision3 == tsWithPrecision8, is(true));
  }

  /** Test that the {code UNKNOWN} type does not does not change class when nullified. */
  @Test void testUnknownCreateWithNullabiliityTypeConsistency() {
    SqlTypeFixture f = new SqlTypeFixture();

    RelDataType unknownType  = f.typeFactory.createUnknownType();
    assertThat(unknownType, isA(UnknownSqlType.class));
    assertThat(unknownType.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
    assertFalse(unknownType.isNullable());

    RelDataType nullableRelDataType = f.typeFactory.createTypeWithNullability(unknownType, true);
    assertThat(nullableRelDataType, isA(UnknownSqlType.class));
    assertThat(nullableRelDataType.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
    assertTrue(nullableRelDataType.isNullable());
  }
}
