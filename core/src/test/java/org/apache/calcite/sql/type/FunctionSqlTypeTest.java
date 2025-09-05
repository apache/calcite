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

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FunctionSqlType}.
 */
public class FunctionSqlTypeTest {
  final RelDataTypeFactory sqlTypeFactory =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  final RelDataType parameterTypes =
      sqlTypeFactory.createStructType(
          ImmutableList.of(sqlTypeFactory.createSqlType(SqlTypeName.BOOLEAN)),
          ImmutableList.of("field1"));
  final RelDataType nonStructParameterTypes = sqlTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
  final RelDataType returnType = sqlTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
  final FunctionSqlType functionSqlType =
      new FunctionSqlType(parameterTypes, returnType);

  @Test void testFailsOnNullParameterType() {
    assertThrows(NullPointerException.class, () -> {
      new FunctionSqlType(null, returnType);
    }, "parameterType");
  }

  @Test void testFailsOnNonStructParameterTypes() {
    assertThrows(IllegalArgumentException.class, () -> {
      new FunctionSqlType(nonStructParameterTypes, returnType);
    }, "ParameterType must be a struct");
  }

  @Test void testFailsOnNullReturnType() {
    assertThrows(NullPointerException.class, () -> {
      new FunctionSqlType(parameterTypes, null);
    });
  }

  @Test void testGetParameterTypes() {
    assertEquals(parameterTypes, functionSqlType.getParameterTypes());
  }

  @Test void testGetReturnType() {
    assertEquals(returnType, functionSqlType.getReturnType());
  }

}
