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

import com.google.common.collect.ImmutableList;

/**
 * Reusable {@link RelDataType} fixtures for tests.
 */
class SqlTypeFixture {
  SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  final RelDataType sqlBoolean = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
  final RelDataType sqlBigInt = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  final RelDataType sqlBigIntNullable = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.BIGINT), true);
  final RelDataType sqlInt = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.INTEGER), false);
  final RelDataType sqlDate = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.DATE), false);
  final RelDataType sqlVarchar = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
  final RelDataType sqlChar = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.CHAR), false);
  final RelDataType sqlVarcharNullable = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
  final RelDataType sqlNull = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.NULL), false);
  final RelDataType sqlUnknown = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.UNKNOWN), false);
  final RelDataType sqlAny = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.ANY), false);
  final RelDataType sqlFloat = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.FLOAT), false);
  final RelDataType sqlTimestamp = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), false);
  final RelDataType sqlGeometry = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.GEOMETRY), false);
  final RelDataType arrayFloat = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(sqlFloat, -1), false);
  final RelDataType arrayBigInt = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(sqlBigIntNullable, -1), false);
  final RelDataType multisetFloat = typeFactory.createTypeWithNullability(
      typeFactory.createMultisetType(sqlFloat, -1), false);
  final RelDataType multisetBigInt = typeFactory.createTypeWithNullability(
      typeFactory.createMultisetType(sqlBigIntNullable, -1), false);
  final RelDataType multisetBigIntNullable = typeFactory.createTypeWithNullability(
      typeFactory.createMultisetType(sqlBigIntNullable, -1), true);
  final RelDataType arrayBigIntNullable = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(sqlBigIntNullable, -1), true);
  final RelDataType arrayOfArrayBigInt = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(arrayBigInt, -1), false);
  final RelDataType arrayOfArrayFloat = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(arrayFloat, -1), false);
  final RelDataType structOfInt = typeFactory.createTypeWithNullability(
      typeFactory.createStructType(
          ImmutableList.of(sqlInt, sqlInt),
          ImmutableList.of("i", "j")), false);
  final RelDataType structOfIntNullable = typeFactory.createTypeWithNullability(
      typeFactory.createStructType(
          ImmutableList.of(sqlInt, sqlInt),
          ImmutableList.of("i", "j")), true);
  final RelDataType mapOfInt = typeFactory.createTypeWithNullability(
      typeFactory.createMapType(sqlInt, sqlInt), false);
  final RelDataType mapOfIntNullable = typeFactory.createTypeWithNullability(
      typeFactory.createMapType(sqlInt, sqlInt), true);
  final RelDataType sqlChar1 = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.CHAR, 1), false);
  final RelDataType sqlChar10 = typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.CHAR, 10), false);
  final RelDataType arraySqlChar10 = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(sqlChar10, -1), false);
  final RelDataType arraySqlChar1 = typeFactory.createTypeWithNullability(
      typeFactory.createArrayType(sqlChar1, -1), false);
  final RelDataType multisetSqlChar10Nullable = typeFactory.createTypeWithNullability(
      typeFactory.createMultisetType(sqlChar10, -1), true);
  final RelDataType multisetSqlChar1 = typeFactory.createTypeWithNullability(
      typeFactory.createMultisetType(sqlChar1, -1), false);
  final RelDataType mapSqlChar10Nullable = typeFactory.createTypeWithNullability(
      typeFactory.createMapType(sqlChar10, sqlChar10), true);
  final RelDataType mapSqlChar1 = typeFactory.createTypeWithNullability(
      typeFactory.createMapType(sqlChar1, sqlChar1), false);
}
