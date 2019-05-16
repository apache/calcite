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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Type of a field in a Web (HTML) table.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the fields
 * makes it easier to write SQL.
 *
 * <p>Trivially modified from CsvFieldType.
 */
enum FileFieldType {
  STRING(null, String.class),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(null, java.sql.Date.class),
  TIME(null, java.sql.Time.class),
  TIMESTAMP(null, java.sql.Timestamp.class);

  private final Primitive primitive;
  private final Class clazz;

  private static final Map<String, FileFieldType> MAP;

  static {
    ImmutableMap.Builder<String, FileFieldType> builder =
        ImmutableMap.builder();
    for (FileFieldType value : values()) {
      builder.put(value.clazz.getSimpleName(), value);

      if (value.primitive != null) {
        builder.put(value.primitive.primitiveName, value);
      }
    }
    MAP = builder.build();
  }

  FileFieldType(Primitive primitive) {
    this(primitive, primitive.boxClass);
  }

  FileFieldType(Primitive primitive, Class clazz) {
    this.primitive = primitive;
    this.clazz = clazz;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static FileFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}

// End FileFieldType.java
