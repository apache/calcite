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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.util.TryThreadLocal;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeUtil.areSameFamily;
import static org.apache.calcite.sql.type.SqlTypeUtil.convertTypeToSpec;
import static org.apache.calcite.sql.type.SqlTypeUtil.equalAsCollectionSansNullability;
import static org.apache.calcite.sql.type.SqlTypeUtil.equalAsMapSansNullability;
import static org.apache.calcite.sql.type.SqlTypeUtil.integerBound;
import static org.apache.calcite.sql.type.SqlTypeUtil.integerRangeContains;
import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test of {@link org.apache.calcite.sql.type.SqlTypeUtil}.
 */
class SqlTypeUtilTest {

  private final SqlTypeFixture f = new SqlTypeFixture();

  @Test void testTypesIsSameFamilyWithNumberTypes() {
    assertThat(areSameFamily(ImmutableList.of(f.sqlBigInt, f.sqlBigInt)), is(true));
    assertThat(areSameFamily(ImmutableList.of(f.sqlInt, f.sqlBigInt)), is(true));
    assertThat(areSameFamily(ImmutableList.of(f.sqlFloat, f.sqlBigInt)), is(true));
    assertThat(areSameFamily(ImmutableList.of(f.sqlInt, f.sqlBigIntNullable)),
        is(true));
  }

  @Test void testTypesIsSameFamilyWithCharTypes() {
    assertThat(areSameFamily(ImmutableList.of(f.sqlVarchar, f.sqlVarchar)), is(true));
    assertThat(areSameFamily(ImmutableList.of(f.sqlVarchar, f.sqlChar)), is(true));
    assertThat(areSameFamily(ImmutableList.of(f.sqlVarchar, f.sqlVarcharNullable)),
        is(true));
  }

  @Test void testTypesIsSameFamilyWithInconvertibleTypes() {
    assertThat(areSameFamily(ImmutableList.of(f.sqlBoolean, f.sqlBigInt)), is(false));
    assertThat(areSameFamily(ImmutableList.of(f.sqlFloat, f.sqlBoolean)), is(false));
    assertThat(areSameFamily(ImmutableList.of(f.sqlInt, f.sqlDate)), is(false));
  }

  @Test void testTypesIsSameFamilyWithNumberStructTypes() {
    final RelDataType bigIntAndFloat = struct(f.sqlBigInt, f.sqlFloat);
    final RelDataType floatAndBigInt = struct(f.sqlFloat, f.sqlBigInt);

    assertThat(areSameFamily(ImmutableList.of(bigIntAndFloat, floatAndBigInt)),
        is(true));
    assertThat(areSameFamily(ImmutableList.of(bigIntAndFloat, bigIntAndFloat)),
        is(true));
    assertThat(areSameFamily(ImmutableList.of(floatAndBigInt, bigIntAndFloat)),
        is(true));
    assertThat(areSameFamily(ImmutableList.of(floatAndBigInt, floatAndBigInt)),
        is(true));
  }

  @Test void testTypesIsSameFamilyWithCharStructTypes() {
    final RelDataType varCharStruct = struct(f.sqlVarchar);
    final RelDataType charStruct = struct(f.sqlChar);

    assertThat(areSameFamily(ImmutableList.of(varCharStruct, charStruct)), is(true));
    assertThat(areSameFamily(ImmutableList.of(charStruct, varCharStruct)), is(true));
    assertThat(areSameFamily(ImmutableList.of(varCharStruct, varCharStruct)), is(true));
    assertThat(areSameFamily(ImmutableList.of(charStruct, charStruct)), is(true));
  }

  @Test void testTypesIsSameFamilyWithInconvertibleStructTypes() {
    final RelDataType dateStruct = struct(f.sqlDate);
    final RelDataType boolStruct = struct(f.sqlBoolean);
    assertThat(areSameFamily(ImmutableList.of(dateStruct, boolStruct)), is(false));

    final RelDataType charIntStruct = struct(f.sqlChar, f.sqlInt);
    final RelDataType charDateStruct = struct(f.sqlChar, f.sqlDate);
    assertThat(areSameFamily(ImmutableList.of(charIntStruct, charDateStruct)),
        is(false));

    final RelDataType boolDateStruct = struct(f.sqlBoolean, f.sqlDate);
    final RelDataType floatIntStruct = struct(f.sqlInt, f.sqlFloat);
    assertThat(areSameFamily(ImmutableList.of(boolDateStruct, floatIntStruct)),
        is(false));
  }

  private RelDataType struct(RelDataType...relDataTypes) {
    final RelDataTypeFactory.Builder builder = f.typeFactory.builder();
    for (int i = 0; i < relDataTypes.length; i++) {
      builder.add("field" + i, relDataTypes[i]);
    }
    return builder.build();
  }

  @Test void testModifyTypeCoercionMappings() {
    SqlTypeMappingRules.Builder builder = SqlTypeMappingRules.builder();
    final SqlTypeCoercionRule defaultRules = SqlTypeCoercionRule.instance();
    builder.addAll(defaultRules.getTypeMapping());
    // Do the tweak, for example, if we want to add a rule to allow
    // coercion of BOOLEAN to TIMESTAMP.
    builder.add(SqlTypeName.TIMESTAMP,
        builder.copyValues(SqlTypeName.TIMESTAMP)
            .add(SqlTypeName.BOOLEAN).build());

    // Try converting with both default rules and the new rule set.
    checkConvert(defaultRules, is(false));
    final SqlTypeCoercionRule typeCoercionRules =
        SqlTypeCoercionRule.instance(builder.map);
    try (TryThreadLocal.Memo ignored =
             SqlTypeCoercionRule.THREAD_PROVIDERS.push(typeCoercionRules)) {
      checkConvert(typeCoercionRules, is(true));
    }
  }

  private void checkConvert(SqlTypeCoercionRule rules,
      Matcher<Boolean> matcher) {
    assertThat(
        SqlTypeUtil.canCastFrom(f.sqlTimestampPrec3, f.sqlBoolean, true),
        matcher);
    assertThat(
        SqlTypeUtil.canCastFrom(f.sqlTimestampPrec3, f.sqlBoolean, rules),
        matcher);
  }

  @Test void testEqualAsCollectionSansNullability() {
    // case array
    assertThat(
        equalAsCollectionSansNullability(f.typeFactory, f.arrayBigInt, f.arrayBigIntNullable),
        is(true));

    // case multiset
    assertThat(
        equalAsCollectionSansNullability(f.typeFactory, f.multisetBigInt, f.multisetBigIntNullable),
        is(true));

    // multiset and array are not equal.
    assertThat(
        equalAsCollectionSansNullability(f.typeFactory, f.arrayBigInt, f.multisetBigInt),
        is(false));
  }

  @Test void testEqualAsMapSansNullability() {
    assertThat(
        equalAsMapSansNullability(f.typeFactory, f.mapOfInt, f.mapOfIntNullable), is(true));
  }

  @Test void testConvertTypeToSpec() {
    SqlBasicTypeNameSpec nullSpec =
        (SqlBasicTypeNameSpec) convertTypeToSpec(f.sqlNull).getTypeNameSpec();
    assertThat(nullSpec.getTypeName().getSimple(), is("NULL"));

    SqlBasicTypeNameSpec unknownSpec =
        (SqlBasicTypeNameSpec) convertTypeToSpec(f.sqlUnknown).getTypeNameSpec();
    assertThat(unknownSpec.getTypeName().getSimple(), is("UNKNOWN"));

    SqlBasicTypeNameSpec basicSpec =
        (SqlBasicTypeNameSpec) convertTypeToSpec(f.sqlBigInt).getTypeNameSpec();
    assertThat(basicSpec.getTypeName().getSimple(), is("BIGINT"));

    SqlCollectionTypeNameSpec arraySpec =
        (SqlCollectionTypeNameSpec) convertTypeToSpec(f.arrayBigInt).getTypeNameSpec();
    assertThat(arraySpec.getTypeName().getSimple(), is("ARRAY"));
    assertThat(arraySpec.getElementTypeName().getTypeName().getSimple(), is("BIGINT"));

    SqlCollectionTypeNameSpec multisetSpec =
        (SqlCollectionTypeNameSpec) convertTypeToSpec(f.multisetBigInt).getTypeNameSpec();
    assertThat(multisetSpec.getTypeName().getSimple(), is("MULTISET"));
    assertThat(multisetSpec.getElementTypeName().getTypeName().getSimple(), is("BIGINT"));

    SqlRowTypeNameSpec rowSpec =
        (SqlRowTypeNameSpec) convertTypeToSpec(f.structOfInt).getTypeNameSpec();
    List<String> fieldNames =
        SqlIdentifier.simpleNames(rowSpec.getFieldNames());
    List<String> fieldTypeNames = rowSpec.getFieldTypes()
        .stream()
        .map(f -> f.getTypeName().getSimple())
        .collect(Collectors.toList());
    assertThat(rowSpec.getTypeName().getSimple(), is("ROW"));
    assertThat(fieldNames, isListOf("i", "j"));
    assertThat(fieldTypeNames, isListOf("INTEGER", "INTEGER"));
  }

  /**
   * Test for <a href="https://issues.apache.org/jira/browse/CALCITE-7186">[CALCITE-7186]</a>
   * Add mapping from Character[] to VARCHAR in Java UDF.
   */
  @Test void testJavaToSqlCharacterArrayAndByteArrayMapping() {
    SqlTypeName sqlTypeName = JavaToSqlTypeConversionRules.instance().lookup(Character[].class);
    assertThat("Character[].class should map to SqlTypeName.VARCHAR",
        sqlTypeName, is(SqlTypeName.VARCHAR));
  }

  @Test void testIntegerRangeContainsForSignedAndUnsigned() {
    RelDataType uTinyInt = f.typeFactory.createSqlType(SqlTypeName.UTINYINT);
    RelDataType tinyInt = f.typeFactory.createSqlType(SqlTypeName.TINYINT);
    RelDataType smallInt = f.typeFactory.createSqlType(SqlTypeName.SMALLINT);
    RelDataType intType = f.typeFactory.createSqlType(SqlTypeName.INTEGER);

    assertThat(integerRangeContains(intType, smallInt), is(true));
    assertThat(integerRangeContains(intType, tinyInt), is(true));
    assertThat(integerRangeContains(intType, uTinyInt), is(true));
    assertThat(integerRangeContains(tinyInt, uTinyInt), is(false));
    assertThrows(IllegalArgumentException.class,
        () -> integerRangeContains(f.sqlVarchar, intType));
  }

  @Test void testIntegerRangeContainsWithDecimal() {
    RelDataType intType = f.typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType dec9 = f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 9, 0);
    RelDataType dec10 = f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    RelDataType dec10Scale1 = f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 1);

    assertThat(integerRangeContains(intType, dec9), is(true));
    assertThat(integerRangeContains(intType, dec10), is(false));
    assertThat(integerRangeContains(intType, dec10Scale1), is(false));
  }

  @Test void testIntegerBound() {
    RelDataType intType = f.typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType uIntType = f.typeFactory.createSqlType(SqlTypeName.UINTEGER);
    RelDataType dec10 = f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    RelDataType dec10Scale1 = f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 1);

    assertThat(integerBound(intType, false), is(BigInteger.valueOf(Integer.MIN_VALUE)));
    assertThat(integerBound(intType, true), is(BigInteger.valueOf(Integer.MAX_VALUE)));
    assertThat(integerBound(uIntType, true), is(BigInteger.valueOf((1L << 32) - 1)));
    assertThat(integerBound(dec10, true), is(BigInteger.valueOf(9_999_999_999L)));
    assertThat(integerBound(dec10Scale1, true), is((BigInteger) null));
  }

  @Test void testGetMaxPrecisionScaleDecimal() {
    RelDataType decimal = SqlTypeUtil.getMaxPrecisionScaleDecimal(f.typeFactory);
    assertThat(decimal, is(f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 9)));
  }

  private void compareTypesIgnoringNullability(
      String comment, RelDataType type1, RelDataType type2, boolean expectedResult) {
    String typeString1 = type1.getFullTypeString();
    String typeString2 = type2.getFullTypeString();

    assertThat(
        "The result of SqlTypeUtil.equalSansNullability"
            + "(typeFactory, " + typeString1 + ", " + typeString2 + ") is incorrect: " + comment,
        SqlTypeUtil.equalSansNullability(f.typeFactory, type1, type2), is(expectedResult));
    assertThat("The result of SqlTypeUtil.equalSansNullability"
            + "(" + typeString1 + ", " + typeString2 + ") is incorrect: " + comment,
        SqlTypeUtil.equalSansNullability(type1, type2), is(expectedResult));
  }

  @Test void testEqualSansNullability() {
    RelDataType bigIntType = f.sqlBigInt;
    RelDataType nullableBigIntType = f.sqlBigIntNullable;
    RelDataType varCharType = f.sqlVarchar;
    RelDataType bigIntType1 =
        f.typeFactory.createTypeWithNullability(nullableBigIntType, false);

    compareTypesIgnoringNullability("different types should return false. ",
        bigIntType, varCharType, false);

    compareTypesIgnoringNullability("types differing only in nullability should return true.",
        bigIntType, nullableBigIntType, true);

    compareTypesIgnoringNullability("identical types should return true.",
        bigIntType, bigIntType1, true);
  }

  @Test void testCanAlwaysCastToUnknownFromBasic() {
    RelDataType unknownType = f.typeFactory.createUnknownType();
    RelDataType nullableUnknownType = f.typeFactory.createTypeWithNullability(unknownType, true);

    for (SqlTypeName fromTypeName : SqlTypeName.values()) {
      BasicSqlType fromType;
      try {
        // This only works for basic types. Ignore the rest.
        fromType = (BasicSqlType) f.typeFactory.createSqlType(fromTypeName);
      } catch (AssertionError e) {
        continue;
      }
      BasicSqlType nullableFromType = fromType.createWithNullability(!fromType.isNullable);

      assertCanCast(unknownType, fromType);
      assertCanCast(unknownType, nullableFromType);
      assertCanCast(nullableUnknownType, fromType);
      assertCanCast(nullableUnknownType, nullableFromType);
    }
  }

  /** Tests that casting BOOLEAN to INTEGER is not allowed for the default
   * {@link SqlTypeCoercionRule}, but is allowed in lenient mode. */
  @Test void testCastBooleanToInteger() {
    RelDataType booleanType = f.sqlBoolean;
    RelDataType intType = f.sqlInt;
    final SqlTypeCoercionRule rule = SqlTypeCoercionRule.instance();
    final SqlTypeCoercionRule lenientRule =
        SqlTypeCoercionRule.lenientInstance();
    assertThat(SqlTypeUtil.canCastFrom(intType, booleanType, rule),
        is(false));
    assertThat(SqlTypeUtil.canCastFrom(intType, booleanType, lenientRule),
        is(true));
  }

  private static void assertCanCast(RelDataType toType, RelDataType fromType) {
    final SqlTypeCoercionRule defaultRules = SqlTypeCoercionRule.instance();
    assertThat(
        String.format(Locale.ROOT,
            "Expected to be able to cast from %s to %s without coercion.", fromType, toType),
        SqlTypeUtil.canCastFrom(toType, fromType, /* coerce= */ false), is(true));
    assertThat(
        String.format(Locale.ROOT,
            "Expected to be able to cast from %s to %s with coercion.", fromType, toType),
        SqlTypeUtil.canCastFrom(toType, fromType, /* coerce= */ true), is(true));
    assertThat(
        String.format(Locale.ROOT,
            "Expected to be able to cast from %s to %s without coercion.", fromType, toType),
        SqlTypeUtil.canCastFrom(toType, fromType, /* coerce= */ defaultRules), is(true));
  }
}
