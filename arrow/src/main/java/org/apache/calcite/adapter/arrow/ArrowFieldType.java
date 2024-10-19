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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Arrow field type.
 */
enum ArrowFieldType {
  INT(Primitive.INT),
  BOOLEAN(Primitive.BOOLEAN),
  STRING(String.class),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(Date.class),
  LIST(List.class),
  DECIMAL(BigDecimal.class),
  LONG(Primitive.LONG),
  BYTE(Primitive.BYTE),
  SHORT(Primitive.SHORT);

  private final Class<?> clazz;

  ArrowFieldType(Primitive primitive) {
    this(requireNonNull(primitive.boxClass, "boxClass"));
  }

  ArrowFieldType(Class<?> clazz) {
    this.clazz = clazz;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static ArrowFieldType of(ArrowType arrowType) {
    switch (arrowType.getTypeID()) {
    case Int:
      int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
      switch (bitWidth) {
      case 64:
        return LONG;
      case 32:
        return INT;
      case 16:
        return SHORT;
      case 8:
        return BYTE;
      default:
        throw new IllegalArgumentException("Unsupported Int bit width: " + bitWidth);
      }
    case Bool:
      return BOOLEAN;
    case Utf8:
      return STRING;
    case FloatingPoint:
      FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
      switch (precision) {
      case SINGLE:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      default:
        throw new IllegalArgumentException("Unsupported Floating point precision: " + precision);
      }
    case Date:
      return DATE;
    case Decimal:
      return DECIMAL;
    default:
      throw new IllegalArgumentException("Unsupported type: " + arrowType);
    }
  }
}
