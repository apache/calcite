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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

enum ArrowFieldType {
  INT(Primitive.LONG),
  BOOLEAN(Primitive.BOOLEAN),
  STRING(String.class, null),
  FLOAT(Primitive.DOUBLE);

  private final Class clazz;
  private final Primitive primitive;

  private static final Map<Class<? extends ArrowType>, ArrowFieldType> MAP = new HashMap<>();

  static {
    MAP.put(ArrowType.Int.class, INT);
    MAP.put(ArrowType.Bool.class, BOOLEAN);
    MAP.put(ArrowType.Utf8.class, STRING);
    MAP.put(ArrowType.FloatingPoint.class, FLOAT);
  }

  ArrowFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive);
  }

  ArrowFieldType(Class clazz, Primitive primitive) {
    this.clazz = clazz;
    this.primitive = primitive;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static ArrowFieldType of(ArrowType arrowType) {
    return MAP.get(arrowType.getClass());
  }
}
