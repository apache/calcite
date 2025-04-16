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
import org.apache.calcite.rel.type.RelDataTypeSystem;

import com.google.common.collect.Sets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test to ensure that {@link SqlTypeFactoryImpl} creates canonical types as per its contract.
 * Two (or more) calls to the same method with the same arguments should always give back the same
 * {@code RelDataType} object.
 */
class SqlTypeFactoryCanonicalTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("typesProvider")
  void testLeastRestrictive(List<RelDataType> types) {
    RelDataTypeFactory f = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType r1 = f.leastRestrictive(types);
    RelDataType r2 = f.leastRestrictive(types);
    assertSame(r1, r2);
  }

  static Collection<List<RelDataType>> typesProvider() {
    RelDataTypeFactory f = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Set<RelDataType> numbers = generateComplexTypes(f, NUMERIC_TYPES);
    Set<RelDataType> chars = generateComplexTypes(f, CHAR_TYPES);
    Set<RelDataType> datetimes = generateComplexTypes(f, DATETIME_TYPES);
    Set<RelDataType> bools = generateComplexTypes(f, BOOLEAN_TYPES);
    Set<List<RelDataType>> products = new HashSet<>();
    products.addAll(Sets.cartesianProduct(numbers, numbers));
    products.addAll(Sets.cartesianProduct(chars, chars));
    products.addAll(Sets.cartesianProduct(datetimes, datetimes));
    products.addAll(Sets.cartesianProduct(bools, bools));
    return products;
  }

  private static Set<RelDataType> generateComplexTypes(RelDataTypeFactory f,
      List<SqlTypeName> typeName) {
    Set<RelDataType> types = new HashSet<>();
    for (SqlTypeName t : typeName) {
      RelDataType basic = f.createSqlType(t);
      types.add(basic);
      types.add(f.createArrayType(basic, -1));
      types.add(f.createMapType(basic, basic));
      types.add(f.createMultisetType(basic, -1));
      types.add(f.createStructType(Arrays.asList(basic, basic), Arrays.asList("f1", "f2")));
    }
    return types;
  }

}
