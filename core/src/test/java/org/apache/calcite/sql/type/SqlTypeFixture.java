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
  final SqlTypeFactoryImpl typeFactory =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  final RelDataType sqlBoolean = type(SqlTypeName.BOOLEAN, false);
  final RelDataType sqlBigInt = type(SqlTypeName.BIGINT, false);
  final RelDataType sqlBigIntNullable = type(SqlTypeName.BIGINT, true);
  final RelDataType sqlInt = type(SqlTypeName.INTEGER, false);
  final RelDataType sqlDate = type(SqlTypeName.DATE, false);
  final RelDataType sqlVarchar = type(SqlTypeName.VARCHAR, false);
  final RelDataType sqlChar = type(SqlTypeName.CHAR, false);
  final RelDataType sqlVarcharNullable = type(SqlTypeName.VARCHAR, true);
  final RelDataType sqlNull = type(SqlTypeName.NULL, false);
  final RelDataType sqlUnknown = type(SqlTypeName.UNKNOWN, false);
  final RelDataType sqlInterval = type(SqlTypeName.INTERVAL, false);
  final RelDataType sqlAny = type(SqlTypeName.ANY, false);
  final RelDataType sqlFloat = type(SqlTypeName.FLOAT, false);
  final RelDataType sqlTimestampPrec0 = type(SqlTypeName.TIMESTAMP, 0);
  final RelDataType sqlTimestampPrec3 = type(SqlTypeName.TIMESTAMP, 3);
  final RelDataType sqlGeometry = type(SqlTypeName.GEOMETRY, false);
  final RelDataType sqlGeography = type(SqlTypeName.GEOGRAPHY, false);
  final RelDataType arrayFloat =
      notNullable(typeFactory.createArrayType(sqlFloat, -1));
  final RelDataType arrayBigInt =
      notNullable(typeFactory.createArrayType(sqlBigIntNullable, -1));
  final RelDataType multisetFloat =
      notNullable(typeFactory.createMultisetType(sqlFloat, -1));
  final RelDataType multisetBigInt =
      notNullable(typeFactory.createMultisetType(sqlBigIntNullable, -1));
  final RelDataType multisetBigIntNullable =
      nullable(typeFactory.createMultisetType(sqlBigIntNullable, -1));
  final RelDataType arrayBigIntNullable =
      nullable(typeFactory.createArrayType(sqlBigIntNullable, -1));
  final RelDataType arrayOfArrayBigInt =
      notNullable(typeFactory.createArrayType(arrayBigInt, -1));
  final RelDataType arrayOfArrayFloat =
      notNullable(typeFactory.createArrayType(arrayFloat, -1));
  final RelDataType structOfInt =
      notNullable(
          typeFactory.createStructType(
              ImmutableList.of(sqlInt, sqlInt), ImmutableList.of("i", "j")));
  final RelDataType structOfIntNullable =
      nullable(
          typeFactory.createStructType(
              ImmutableList.of(sqlInt, sqlInt), ImmutableList.of("i", "j")));
  final RelDataType mapOfInt =
      notNullable(typeFactory.createMapType(sqlInt, sqlInt));
  final RelDataType mapOfIntNullable =
      nullable(typeFactory.createMapType(sqlInt, sqlInt));
  final RelDataType sqlChar1 = type(SqlTypeName.CHAR, 1);
  final RelDataType sqlChar10 = type(SqlTypeName.CHAR, 10);
  final RelDataType arraySqlChar10 =
      notNullable(typeFactory.createArrayType(sqlChar10, -1));
  final RelDataType arraySqlChar1 =
      notNullable(typeFactory.createArrayType(sqlChar1, -1));
  final RelDataType multisetSqlChar10Nullable =
      nullable(typeFactory.createMultisetType(sqlChar10, -1));
  final RelDataType multisetSqlChar1 =
      notNullable(typeFactory.createMultisetType(sqlChar1, -1));
  final RelDataType mapSqlChar10Nullable =
      nullable(typeFactory.createMapType(sqlChar10, sqlChar10));
  final RelDataType mapSqlChar1 =
      notNullable(typeFactory.createMapType(sqlChar1, sqlChar1));

  private RelDataType type(SqlTypeName typeName, int precision) {
    return notNullable(typeFactory.createSqlType(typeName, precision));
  }

  private RelDataType type(SqlTypeName typeName, boolean nullable) {
    RelDataType type = typeFactory.createSqlType(typeName);
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private RelDataType notNullable(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, false);
  }

  private RelDataType nullable(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, true);
  }
}
