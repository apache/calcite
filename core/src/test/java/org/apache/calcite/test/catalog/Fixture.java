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
package org.apache.calcite.test.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Types used during initialization. */
final class Fixture {
  final RelDataType intType;
  final RelDataType intTypeNull;
  final RelDataType bigintType;
  final RelDataType bigintTypeNull;
  final RelDataType decimalType;
  final RelDataType varcharType;
  final RelDataType varchar5Type;
  final RelDataType varchar10Type;
  final RelDataType varchar10TypeNull;
  final RelDataType varchar20Type;
  final RelDataType varchar20TypeNull;
  final RelDataType timestampType;
  final RelDataType timestampTypeNull;
  final RelDataType dateType;
  final RelDataType booleanType;
  final RelDataType booleanTypeNull;
  final RelDataType rectilinearCoordType;
  final RelDataType rectilinearPeekCoordType;
  final RelDataType rectilinearPeekNoExpandCoordType;
  final RelDataType abRecordType;
  final RelDataType skillRecordType;
  final RelDataType empRecordType;
  final RelDataType empListType;
  final ObjectSqlType addressType;
  // Row(f0 int, f1 varchar)
  final RelDataType recordType1;
  // Row(f0 int not null, f1 varchar null)
  final RelDataType recordType2;
  // Row(f0 Row(ff0 int not null, ff1 varchar null) null, f1 timestamp not null)
  final RelDataType recordType3;
  // Row(f0 bigint not null, f1 decimal null) array
  final RelDataType recordType4;
  // Row(f0 varchar not null, f1 timestamp null) multiset
  final RelDataType recordType5;
  final RelDataType intArrayType;
  final RelDataType varchar5ArrayType;
  final RelDataType intArrayArrayType;
  final RelDataType varchar5ArrayArrayType;
  final RelDataType intMultisetType;
  final RelDataType varchar5MultisetType;
  final RelDataType intMultisetArrayType;
  final RelDataType varchar5MultisetArrayType;
  final RelDataType intArrayMultisetType;
  // Row(f0 int array multiset, f1 varchar(5) array) array multiset
  final RelDataType rowArrayMultisetType;

  Fixture(RelDataTypeFactory typeFactory) {
    intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    intTypeNull = typeFactory.createTypeWithNullability(intType, true);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    bigintTypeNull = typeFactory.createTypeWithNullability(bigintType, true);
    decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    varchar5Type = typeFactory.createSqlType(SqlTypeName.VARCHAR, 5);
    varchar10Type = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    varchar10TypeNull = typeFactory.createTypeWithNullability(varchar10Type, true);
    varchar20Type = typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    varchar20TypeNull = typeFactory.createTypeWithNullability(varchar20Type, true);
    timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    timestampTypeNull =
        typeFactory.createTypeWithNullability(timestampType, true);
    dateType =
        typeFactory.createSqlType(SqlTypeName.DATE);
    booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    booleanTypeNull =
        typeFactory.createTypeWithNullability(booleanType, true);
    rectilinearCoordType =
        typeFactory.builder()
            .add("X", intType)
            .add("Y", intType)
            .build();
    rectilinearPeekCoordType =
        typeFactory.builder()
            .add("X", intType)
            .add("Y", intType)
            .add("unit", varchar20Type)
            .kind(StructKind.PEEK_FIELDS)
            .build();
    rectilinearPeekNoExpandCoordType =
        typeFactory.builder()
            .add("M", intType)
            .add("SUB",
                typeFactory.builder()
                    .add("A", intType)
                    .add("B", intType)
                    .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
                    .build())
            .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
            .build();
    abRecordType =
        typeFactory.builder()
            .add("A", varchar10Type)
            .add("B", varchar10Type)
            .build();
    skillRecordType =
        typeFactory.builder()
            .add("TYPE", varchar10Type)
            .add("DESC", varchar20Type)
            .add("OTHERS", abRecordType)
            .build();
    empRecordType =
        typeFactory.builder()
            .add("EMPNO", intType)
            .add("ENAME", varchar10Type)
            .add("DETAIL",
                typeFactory.builder().add("SKILLS",
                    typeFactory.createArrayType(skillRecordType, -1)).build())
            .build();
    empListType =
        typeFactory.createArrayType(empRecordType, -1);
    addressType =
        new ObjectSqlType(SqlTypeName.STRUCTURED,
            new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
            false,
            Arrays.asList(
                new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
                new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
                new RelDataTypeFieldImpl("ZIP", 2, intType),
                new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
            RelDataTypeComparability.NONE);
    // Row(f0 int, f1 varchar)
    recordType1 = typeFactory.createStructType(
        Arrays.asList(intType, varcharType),
        Arrays.asList("f0", "f1"));
    // Row(f0 int not null, f1 varchar null)
    recordType2 = typeFactory.createStructType(
        Arrays.asList(intType,
            nullable(typeFactory, varcharType)),
        Arrays.asList("f0", "f1"));
    // Row(f0 Row(ff0 int not null, ff1 varchar null) null, f1 timestamp not null)
    recordType3 = typeFactory.createStructType(
        Arrays.asList(
            nullable(typeFactory,
                typeFactory.createStructType(
                    recordType2.getFieldList().stream()
                        .map(RelDataTypeField::getType).collect(Collectors.toList()),
                    Arrays.asList("ff0", "ff1"))), timestampType),
        Arrays.asList("f0", "f1"));
    // Row(f0 bigint not null, f1 decimal null) array
    recordType4 = typeFactory.createArrayType(
        typeFactory.createStructType(
            Arrays.asList(bigintType, nullable(typeFactory, decimalType)),
            Arrays.asList("f0", "f1")),
        -1);
    // Row(f0 varchar not null, f1 timestamp null) multiset
    recordType5 = typeFactory.createMultisetType(
        typeFactory.createStructType(
            Arrays.asList(varcharType, nullable(typeFactory, timestampType)),
            Arrays.asList("f0", "f1")),
        -1);
    intArrayType = typeFactory.createArrayType(intType, -1);
    varchar5ArrayType = typeFactory.createArrayType(varchar5Type, -1);
    intArrayArrayType = typeFactory.createArrayType(intArrayType, -1);
    varchar5ArrayArrayType = typeFactory.createArrayType(varchar5ArrayType, -1);
    intMultisetType = typeFactory.createMultisetType(intType, -1);
    varchar5MultisetType = typeFactory.createMultisetType(varchar5Type, -1);
    intMultisetArrayType = typeFactory.createArrayType(intMultisetType, -1);
    varchar5MultisetArrayType = typeFactory.createArrayType(varchar5MultisetType, -1);
    intArrayMultisetType = typeFactory.createMultisetType(intArrayType, -1);
    // Row(f0 int array multiset, f1 varchar(5) array) array multiset
    rowArrayMultisetType = typeFactory.createMultisetType(
        typeFactory.createArrayType(
            typeFactory.createStructType(
                Arrays.asList(intArrayMultisetType, varchar5ArrayType),
                Arrays.asList("f0", "f1")),
            -1),
        -1);
  }

  private static RelDataType nullable(RelDataTypeFactory typeFactory, RelDataType type) {
    return typeFactory.createTypeWithNullability(type, true);
  }
}

// End Fixture.java
