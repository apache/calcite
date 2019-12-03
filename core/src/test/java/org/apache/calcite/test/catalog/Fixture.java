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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

/** Types used during initialization. */
final class Fixture extends AbstractFixture {
  final RelDataType intType = sqlType(SqlTypeName.INTEGER);
  final RelDataType intTypeNull = nullable(intType);
  final RelDataType bigintType = sqlType(SqlTypeName.BIGINT);
  final RelDataType decimalType = sqlType(SqlTypeName.DECIMAL);
  final RelDataType varcharType = sqlType(SqlTypeName.VARCHAR);
  final RelDataType varcharTypeNull = sqlType(SqlTypeName.VARCHAR);
  final RelDataType varchar5Type = sqlType(SqlTypeName.VARCHAR, 5);
  final RelDataType varchar10Type = sqlType(SqlTypeName.VARCHAR, 10);
  final RelDataType varchar10TypeNull = nullable(varchar10Type);
  final RelDataType varchar20Type = sqlType(SqlTypeName.VARCHAR, 20);
  final RelDataType varchar20TypeNull = nullable(varchar20Type);
  final RelDataType timestampType = sqlType(SqlTypeName.TIMESTAMP);
  final RelDataType timestampTypeNull = nullable(timestampType);
  final RelDataType dateType = sqlType(SqlTypeName.DATE);
  final RelDataType booleanType = sqlType(SqlTypeName.BOOLEAN);
  final RelDataType booleanTypeNull = nullable(booleanType);
  final RelDataType rectilinearCoordType = typeFactory.builder()
      .add("X", intType)
      .add("Y", intType)
      .build();
  final RelDataType rectilinearPeekCoordType = typeFactory.builder()
      .add("X", intType)
      .add("Y", intType)
      .add("unit", varchar20Type)
      .kind(StructKind.PEEK_FIELDS)
      .build();
  final RelDataType rectilinearPeekNoExpandCoordType = typeFactory.builder()
      .add("M", intType)
      .add("SUB",
          typeFactory.builder()
              .add("A", intType)
              .add("B", intType)
              .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
              .build())
      .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
      .build();
  final RelDataType abRecordType = typeFactory.builder()
      .add("A", varchar10Type)
      .add("B", varchar10Type)
      .build();;
  final RelDataType skillRecordType = typeFactory.builder()
      .add("TYPE", varchar10Type)
      .add("DESC", varchar20Type)
      .add("OTHERS", abRecordType)
      .build();
  final RelDataType empRecordType = typeFactory.builder()
      .add("EMPNO", intType)
      .add("ENAME", varchar10Type)
      .add("DETAIL", typeFactory.builder()
          .add("SKILLS", array(skillRecordType)).build())
      .build();
  final RelDataType empListType = array(empRecordType);
  final ObjectSqlType addressType = new ObjectSqlType(SqlTypeName.STRUCTURED,
      new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
      false,
      Arrays.asList(
          new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
          new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
          new RelDataTypeFieldImpl("ZIP", 2, intType),
          new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
      RelDataTypeComparability.NONE);
  // Row(f0 int, f1 varchar)
  final RelDataType recordType1 = typeFactory.createStructType(
      Arrays.asList(intType, varcharType),
      Arrays.asList("f0", "f1"));
  // Row(f0 int not null, f1 varchar null)
  final RelDataType recordType2 = typeFactory.createStructType(
      Arrays.asList(intType, nullable(varcharType)),
      Arrays.asList("f0", "f1"));
  // Row(f0 Row(ff0 int not null, ff1 varchar null) null, f1 timestamp not null)
  final RelDataType recordType3 = typeFactory.createStructType(
      Arrays.asList(
          nullable(
              typeFactory.createStructType(Arrays.asList(intType, varcharTypeNull),
          Arrays.asList("ff0", "ff1"))), timestampType), Arrays.asList("f0", "f1"));
  // Row(f0 bigint not null, f1 decimal null) array
  final RelDataType recordType4 = array(
      typeFactory.createStructType(
      Arrays.asList(bigintType, nullable(decimalType)),
      Arrays.asList("f0", "f1")));
  // Row(f0 varchar not null, f1 timestamp null) multiset
  final RelDataType recordType5 = typeFactory.createMultisetType(
      typeFactory.createStructType(
          Arrays.asList(varcharType, timestampTypeNull),
          Arrays.asList("f0", "f1")),
      -1);
  final RelDataType intArrayType = array(intType);
  final RelDataType varchar5ArrayType = array(varchar5Type);
  final RelDataType intArrayArrayType = array(intArrayType);
  final RelDataType varchar5ArrayArrayType = array(varchar5ArrayType);
  final RelDataType intMultisetType = typeFactory.createMultisetType(intType, -1);
  final RelDataType varchar5MultisetType = typeFactory.createMultisetType(varchar5Type, -1);
  final RelDataType intMultisetArrayType = array(intMultisetType);
  final RelDataType varchar5MultisetArrayType = array(varchar5MultisetType);
  final RelDataType intArrayMultisetType = typeFactory.createMultisetType(intArrayType, -1);
  // Row(f0 int array multiset, f1 varchar(5) array) array multiset
  final RelDataType rowArrayMultisetType = typeFactory.createMultisetType(
      array(
          typeFactory.createStructType(
          Arrays.asList(intArrayMultisetType, varchar5ArrayType),
          Arrays.asList("f0", "f1"))),
      -1);

  Fixture(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  private RelDataType nullable(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, true);
  }

  private RelDataType sqlType(SqlTypeName typeName, int... args) {
    assert args.length < 3 : "unknown size of additional int args";
    return args.length == 2 ? typeFactory.createSqlType(typeName, args[0], args[1])
        : args.length == 1 ? typeFactory.createSqlType(typeName, args[0])
        : typeFactory.createSqlType(typeName);
  }

  private RelDataType array(RelDataType type) {
    return typeFactory.createArrayType(type, -1);
  }
}

/**
 * Just a little trick to store factory ref before field init in fixture
 */
abstract class AbstractFixture {
  final RelDataTypeFactory typeFactory;

  AbstractFixture(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }
}

// End Fixture.java
