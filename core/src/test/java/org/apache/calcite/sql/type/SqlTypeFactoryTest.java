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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for {@link SqlTypeFactoryImpl}.
 */
class SqlTypeFactoryTest {

  @Test void testGeographyTypeRegistration() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType geography = factory.createSqlType(SqlTypeName.GEOGRAPHY);
    RelDataType geometry = factory.createSqlType(SqlTypeName.GEOMETRY);
    RelDataType uuid = factory.createSqlType(SqlTypeName.UUID);

    assertSame(SqlTypeName.GEOGRAPHY, SqlTypeName.get("GEOGRAPHY"));
    assertSame(SqlTypeName.GEOGRAPHY, geography.getSqlTypeName());
    assertSame(SqlTypeFamily.GEOGRAPHY, geography.getFamily());
    assertEquals(Types.OTHER, geography.getSqlTypeName().getJdbcOrdinal());
    assertNull(SqlTypeName.getNameForJdbcType(Types.OTHER));
    assertNull(SqlTypeFamily.getFamilyForJdbcType(Types.OTHER));
    assertFalse(SqlTypeName.GEOGRAPHY.allowsPrec());
    assertFalse(SqlTypeName.GEOGRAPHY.allowsScale());
    assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, geography.getPrecision());
    assertEquals(RelDataType.SCALE_NOT_SPECIFIED, geography.getScale());
    assertTrue(SqlTypeFamily.GEOGRAPHY.contains(geography));
    assertFalse(SqlTypeFamily.GEOGRAPHY.contains(geometry));
    assertFalse(SqlTypeFamily.GEO.contains(geography));
    assertSame(geography, SqlTypeFamily.GEOGRAPHY.getDefaultConcreteType(factory));
    assertNotEquals(geometry, geography);
    assertNotEquals(geometry.hashCode(), geography.hashCode());
    assertNotSame(geometry, geography);
    assertEquals(Types.OTHER, uuid.getSqlTypeName().getJdbcOrdinal());
    assertNotEquals(uuid, geography);
    assertNotSame(uuid, geography);
  }

  @Test void testGeographyTypeCopiesAndNullability() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory otherFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType geography = factory.createSqlType(SqlTypeName.GEOGRAPHY);
    RelDataType nullable = factory.createTypeWithNullability(geography, true);

    assertFalse(geography.isNullable());
    assertTrue(nullable.isNullable());
    assertSame(SqlTypeName.GEOGRAPHY, nullable.getSqlTypeName());
    assertSame(SqlTypeFamily.GEOGRAPHY, nullable.getFamily());
    assertSame(geography, factory.createTypeWithNullability(nullable, false));
    assertNotEquals(geography, nullable);
    assertEquals("GEOGRAPHY", geography.toString());
    assertEquals("GEOGRAPHY NOT NULL", geography.getFullTypeString());
    assertEquals("GEOGRAPHY", nullable.getFullTypeString());
    assertEquals("GEOGRAPHY NOT NULL",
        SqlTypeUtil.asSqlType(geography, SqlTypeUtil.NullabilityDisplay.DisplayNonNullability));

    for (RelDataType type : ImmutableList.of(geography, nullable)) {
      assertSame(type, factory.copyType(type));
      RelDataType copy = otherFactory.copyType(type);
      assertSame(SqlTypeName.GEOGRAPHY, copy.getSqlTypeName());
      assertSame(SqlTypeFamily.GEOGRAPHY, copy.getFamily());
      assertEquals(type.isNullable(), copy.isNullable());
      assertEquals(type, copy);
      assertEquals(type.hashCode(), copy.hashCode());
      assertEquals(type.getFullTypeString(), copy.getFullTypeString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNestedGeographyTypeCopies(boolean nullable) {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory otherFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType geography =
        factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.GEOGRAPHY), nullable);
    RelDataType array = factory.createArrayType(geography, -1);
    RelDataType multiset = factory.createMultisetType(geography, -1);
    RelDataType map = factory.createMapType(factory.createSqlType(SqlTypeName.INTEGER), geography);
    RelDataType row = factory.builder().add("g", geography).build();

    assertEquals(geography, array.getComponentType());
    assertEquals(geography, multiset.getComponentType());
    assertEquals(geography, map.getValueType());
    assertEquals(geography, row.getFieldList().get(0).getType());
    assertEquals(geography, otherFactory.copyType(array).getComponentType());
    assertEquals(geography, otherFactory.copyType(multiset).getComponentType());
    assertEquals(geography, otherFactory.copyType(map).getValueType());
    assertEquals(geography, otherFactory.copyType(row).getFieldList().get(0).getType());
  }

  @Test void testGeographyRejectsPrecisionAndScale() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    assertSame(factory.createSqlType(SqlTypeName.GEOGRAPHY),
        factory.createSqlType(SqlTypeName.GEOGRAPHY, RelDataType.PRECISION_NOT_SPECIFIED));
    assertThrows(AssertionError.class, () -> factory.createSqlType(SqlTypeName.GEOGRAPHY, 1));
    assertThrows(AssertionError.class, () -> factory.createSqlType(SqlTypeName.GEOGRAPHY, 1, 0));
    assertThrows(AssertionError.class,
        () -> factory.createSqlType(SqlTypeName.GEOGRAPHY,
            RelDataType.PRECISION_NOT_SPECIFIED, 0));
  }

  @Test void testGeographyCommonTypes() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType geography = factory.createSqlType(SqlTypeName.GEOGRAPHY);
    RelDataType nullableGeography = factory.createTypeWithNullability(geography, true);
    RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
    RelDataType date = factory.createSqlType(SqlTypeName.DATE);
    RelDataType nullType = factory.createSqlType(SqlTypeName.NULL);
    assertEquals(nullableGeography,
        factory.leastRestrictive(ImmutableList.of(geography, nullType)));
    assertNull(factory.leastRestrictive(ImmutableList.of(integer, date, geography)));

    RelDataType geometry = factory.createSqlType(SqlTypeName.GEOMETRY);
    for (Function<RelDataType, RelDataType> container
        : ImmutableList.<Function<RelDataType, RelDataType>>of(
            leaf -> factory.createArrayType(leaf, -1),
            leaf -> factory.createMapType(integer, leaf),
            leaf -> factory.builder().add("G", leaf).build(),
            leaf -> factory.createMultisetType(leaf, -1))) {
      RelDataType notNullLeaf = container.apply(geography);
      RelDataType nullableLeaf = container.apply(nullableGeography);
      RelDataType geometryLeaf = container.apply(geometry);
      assertEquals(nullableLeaf,
          factory.leastRestrictive(ImmutableList.of(notNullLeaf, nullableLeaf)));
      assertNull(factory.leastRestrictive(ImmutableList.of(notNullLeaf, geometryLeaf)));
      assertNull(factory.leastRestrictive(ImmutableList.of(geometryLeaf, notNullLeaf)));
      assertTrue(SqlTypeUtil.canAssignFrom(notNullLeaf, nullableLeaf));
      assertFalse(SqlTypeUtil.canAssignFrom(notNullLeaf, geometryLeaf));
      assertFalse(SqlTypeUtil.canAssignFrom(geometryLeaf, notNullLeaf));
    }

    RelDataType record = factory.builder().add("G", geography).build();
    RelDataType nullableField = factory.builder().add("G", nullableGeography).build();
    RelDataType common =
        factory.leastRestrictive(ImmutableList.of(nullType, nullableField, record));
    assertEquals(factory.createTypeWithNullability(nullableField, true), common);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGeographyGenericCastEligibility(boolean coerce) {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType geography = factory.createSqlType(SqlTypeName.GEOGRAPHY);
    for (RelDataType generic : ImmutableList.of(factory.createSqlType(SqlTypeName.ANY),
        factory.createUnknownType(), factory.createSqlType(SqlTypeName.VARIANT))) {
      assertTrue(SqlTypeUtil.canCastFrom(geography, generic, coerce));
      assertTrue(SqlTypeUtil.canCastFrom(generic, geography, coerce));
    }
  }

  @Test void testGeographyCustomCastRule() {
    final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType geography = factory.createSqlType(SqlTypeName.GEOGRAPHY);
    final RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
    final ImmutableMap.Builder<SqlTypeName, ImmutableSet<SqlTypeName>> rules =
        ImmutableMap.builder();
    for (SqlTypeName name : SqlTypeName.values()) {
      rules.put(name, ImmutableSet.copyOf(SqlTypeName.values()));
    }
    final SqlTypeMappingRule permissive = () -> rules.build();
    for (Function<RelDataType, RelDataType> container
        : ImmutableList.<Function<RelDataType, RelDataType>>of(
            leaf -> leaf,
            leaf -> factory.createArrayType(leaf, -1),
            leaf -> factory.createMapType(integer, leaf),
            leaf -> factory.builder().add("V", leaf).build(),
            leaf -> factory.createMultisetType(leaf, -1))) {
      final RelDataType target = container.apply(geography);
      assertTrue(SqlTypeUtil.canCastFrom(target, target, permissive));
      assertTrue(
          SqlTypeUtil.canCastFrom(target, factory.createSqlType(SqlTypeName.NULL),
          permissive));
      for (SqlTypeName name
          : ImmutableList.of(SqlTypeName.GEOMETRY, SqlTypeName.VARCHAR, SqlTypeName.VARBINARY)) {
        final RelDataType other = container.apply(factory.createSqlType(name));
        assertFalse(SqlTypeUtil.canCastFrom(target, other, permissive));
        assertFalse(SqlTypeUtil.canCastFrom(other, target, permissive));
      }
    }
  }

  @Test void testOrdinaryRowCommonTypeWithNull() {
    final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType row = factory.builder().add("I", SqlTypeName.INTEGER)
        .add("S", SqlTypeName.VARCHAR, 20).nullable(true).build();
    final RelDataType nullType = factory.createSqlType(SqlTypeName.NULL);
    final RelDataType expected = factory.createTypeWithNullability(row, true);
    assertFalse(row.isNullable());
    assertFalse(row.getFieldList().get(0).getType().isNullable());
    assertTrue(row.getFieldList().get(1).getType().isNullable());
    for (List<RelDataType> inputs : ImmutableList.of(ImmutableList.of(row, nullType),
        ImmutableList.of(nullType, row))) {
      final RelDataType result = factory.leastRestrictive(inputs);
      assertEquals(expected, result);
      assertTrue(result.isNullable());
      assertTrue(result.getFieldList().get(0).getType().isNullable());
      assertTrue(result.getFieldList().get(1).getType().isNullable());
    }
    final RelDataType incompatible = factory.builder().add("I", SqlTypeName.INTEGER).build();
    assertNull(factory.leastRestrictive(ImmutableList.of(row, nullType, incompatible)));
    assertNull(factory.leastRestrictive(ImmutableList.of(incompatible, nullType, row)));
  }

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

  /** UNKNOWN types in leastRestrictive() affect only the result nullability. */
  @Test void testLeastRestrictiveWithUnknown() {
    SqlTypeFixture f = new SqlTypeFixture();
    // UNKNOWN never constrains the result, no matter its position
    checkUnknownWithType(f, f.sqlBigInt);
    checkUnknownWithType(f, f.structOfInt);
    checkUnknownWithType(f, f.arrayBigInt);
    checkUnknownWithType(f, f.mapOfInt);
    // A nullable UNKNOWN makes the result nullable
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.structOfInt,
                f.typeFactory.createTypeWithNullability(f.sqlUnknown, true)));
    assertThat(leastRestrictive, notNullValue());
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ROW));
    assertThat(leastRestrictive.isNullable(), is(true));
    // A list of UNKNOWN unifies to UNKNOWN
    leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlUnknown, f.sqlUnknown));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
  }

  /** Checks that leastRestictive({@code type}, UNKNOWN) yields {@code type},
   * in either order. */
  private void checkUnknownWithType(SqlTypeFixture f, RelDataType type) {
    RelDataType r1 =
        f.typeFactory.leastRestrictive(Lists.newArrayList(type, f.sqlUnknown));
    RelDataType r2 =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlUnknown, type));
    assertThat(r1, notNullValue());
    assertThat(r2, notNullValue());
    assertThat(r1.getFullTypeString(), is(type.getFullTypeString()));
    assertThat(r2.getFullTypeString(), is(type.getFullTypeString()));
  }

  @Test void testLeastRestrictiveStructWithNull() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlNull, f.structOfInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ROW));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test void testLeastRestrictiveForImpossibleWithArray() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.arraySqlChar10, f.sqlChar));
    // Some SQL dialects, like Postgres, allow casts between strings and arrays
    assertThat(leastRestrictive, is(f.arraySqlChar10));
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6091">[CALCITE-6091]
   * CASE WHEN with ARRAY/MAP types and NULL ELSE branch should not truncate
   * element type</a>. */
  @Test void testLeastRestrictiveForArraysWithNull() {
    SqlTypeFixture f = new SqlTypeFixture();
    // Simulates: CASE WHEN true THEN ARRAY['abc'] WHEN false THEN ARRAY['d'] ELSE NULL END
    // Types: ARRAY<CHAR(3)>, ARRAY<CHAR(1)>, NULL
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.arraySqlChar10, f.arraySqlChar1, f.sqlNull));
    assertThat(leastRestrictive, notNullValue());
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ARRAY));
    assertThat(leastRestrictive.isNullable(), is(true));
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6091">[CALCITE-6091]
   * CASE WHEN with MAP types and NULL ELSE branch should not fail type inference</a>. */
  @Test void testLeastRestrictiveForMapsWithNull() {
    SqlTypeFixture f = new SqlTypeFixture();
    // Simulates: CASE WHEN ... THEN MAP[...] WHEN ... THEN MAP[...] ELSE NULL END
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.mapSqlChar10Nullable, f.mapSqlChar1, f.sqlNull));
    assertThat(leastRestrictive, notNullValue());
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.MAP));
    assertThat(leastRestrictive.isNullable(), is(true));
    assertThat(leastRestrictive.getKeyType().getPrecision(), is(10));
    assertThat(leastRestrictive.getValueType().getPrecision(), is(10));
  }

  @Test void testLeastRestrictiveForTimestamps() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestampPrec0, f.sqlTimestampPrec3));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
    assertThat(leastRestrictive.isNullable(), is(false));
    assertThat(leastRestrictive.getPrecision(), is(3));
  }

  @Test void testLeastRestrictiveForTimestamps2() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestampPrec3, f.sqlTimestampPrec0));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
    assertThat(leastRestrictive.isNullable(), is(false));
    assertThat(leastRestrictive.getPrecision(), is(3));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7567">
   * LeastRetrictiveSqlType for TIMESTAMP, TIMESTAMP_LTZ might ignore precision</a>. */
  @Test void testLeastRestrictiveForTimestampAndTimestampLtz() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType ltz0 =
        f.typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0);
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(ltz0, f.sqlTimestampPrec3));
    assertThat(leastRestrictive, is(notNullValue()));
    assertThat(leastRestrictive.getPrecision(), is(3));
  }

  @Test void testLeastRestrictiveForTimestampLtzAndTimestamp() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType ltz0 =
        f.typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0);
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlTimestampPrec3, ltz0));
    assertThat(leastRestrictive, is(notNullValue()));
    assertThat(leastRestrictive.getPrecision(), is(3));
  }

  @Test void testLeastRestrictiveForTimestampAndDate() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestampPrec3, f.sqlDate));
    assertNull(leastRestrictive);
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
    RelDataTypeField field0 =
        new RelDataTypeFieldImpl("i", 0,
            typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 =
        new RelDataTypeFieldImpl("s", 1,
            typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);
    final RelDataType recordType = new RelRecordType(fields); // nullable false by default
    final RelDataType copyRecordType =
        typeFactory.createTypeWithNullability(recordType, true);
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
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIME_TZ);
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    checkCreateSqlTypeWithPrecision(f.typeFactory, SqlTypeName.TIMESTAMP_TZ);
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

    assertThat(ts, hasToString(sqlTypeName.getName() + "(0)"));
    assertThat(ts.getFullTypeString(), is(sqlTypeName.getName() + "(0) NOT NULL"));
    assertThat(tsWithoutPrecision, hasToString(sqlTypeName.getName()));
    assertThat(tsWithoutPrecision.getFullTypeString(), is(sqlTypeName.getName() + " NOT NULL"));
    assertThat(tsWithPrecision0, hasToString(sqlTypeName.getName() + "(0)"));
    assertThat(tsWithPrecision0.getFullTypeString(), is(sqlTypeName.getName() + "(0) NOT NULL"));
    assertThat(tsWithPrecision1, hasToString(sqlTypeName.getName() + "(1)"));
    assertThat(tsWithPrecision1.getFullTypeString(), is(sqlTypeName.getName() + "(1) NOT NULL"));
    assertThat(tsWithPrecision2, hasToString(sqlTypeName.getName() + "(2)"));
    assertThat(tsWithPrecision2.getFullTypeString(), is(sqlTypeName.getName() + "(2) NOT NULL"));
    assertThat(tsWithPrecision3, hasToString(sqlTypeName.getName() + "(3)"));
    assertThat(tsWithPrecision3.getFullTypeString(), is(sqlTypeName.getName() + "(3) NOT NULL"));
    assertThat(tsWithPrecision8, hasToString(sqlTypeName.getName() + "(3)"));
    assertThat(tsWithPrecision8.getFullTypeString(), is(sqlTypeName.getName() + "(3) NOT NULL"));

    assertThat(ts != tsWithoutPrecision, is(true));
    assertThat(ts == tsWithPrecision0, is(true));
    assertThat(tsWithPrecision3 == tsWithPrecision8, is(true));
  }

  /** Test that the {@code UNKNOWN} type is a {@link BasicSqlType} and remains
   * so when nullified. */
  @Test void testUnknownCreateWithNullabilityTypeConsistency() {
    final SqlTypeFixture f = new SqlTypeFixture();

    final RelDataType unknownType = f.typeFactory.createUnknownType();
    assertThat(unknownType, instanceOf(BasicSqlType.class));
    assertThat(unknownType.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
    assertFalse(unknownType.isNullable());
    assertThat(unknownType.getFullTypeString(), is("UNKNOWN NOT NULL"));

    final RelDataType nullableType =
        f.typeFactory.createTypeWithNullability(unknownType, true);
    assertThat(nullableType, instanceOf(BasicSqlType.class));
    assertThat(nullableType.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
    assertTrue(nullableType.isNullable());
    assertThat(nullableType.getFullTypeString(), is("UNKNOWN"));

    final RelDataType unknownType2 =
        f.typeFactory.createTypeWithNullability(nullableType, false);
    assertThat(unknownType2, is(unknownType));
    assertThat(unknownType2, instanceOf(BasicSqlType.class));
    assertThat(unknownType2.getSqlTypeName(), is(SqlTypeName.UNKNOWN));
    assertFalse(unknownType2.isNullable());
    assertThat(unknownType2.getFullTypeString(), is("UNKNOWN NOT NULL"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5199">[CALCITE-5199]
   * The leastRestrictiveStructuredType method should reserve the StructKind instead of
   * override it to FULLY_QUALIFIED</a>. */
  @Test void testLeastRestrictiveStructurePreservesStructKind() {
    SqlTypeFixture f = new SqlTypeFixture();
    RelDataTypeFactory typeFactory = f.typeFactory;

    // Test with PEEK_FIELDS
    testStructKindPreservation(typeFactory, f, StructKind.PEEK_FIELDS);
    // Test with PEEK_FIELDS_DEFAULT
    testStructKindPreservation(typeFactory, f, StructKind.PEEK_FIELDS_DEFAULT);
  }

  private void testStructKindPreservation(
      RelDataTypeFactory typeFactory, SqlTypeFixture f, StructKind kind) {
    // Create two struct types with the specified kind
    RelDataType struct1 =
        typeFactory.createStructType(kind,
            ImmutableList.of(f.sqlInt, f.sqlInt),
            ImmutableList.of("i", "j"));
    RelDataType struct2 =
        typeFactory.createStructType(kind,
            ImmutableList.of(f.sqlBigInt, f.sqlBigInt),
            ImmutableList.of("i", "j"));

    // Compute least restrictive type
    RelDataType leastRestrictive =
        typeFactory.leastRestrictive(ImmutableList.of(struct1, struct2));

    // Verify the result is a struct type
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ROW));

    // Verify the struct kind is preserved, not overridden to FULLY_QUALIFIED
    assertThat(leastRestrictive.getStructKind(), is(kind));
  }
}
