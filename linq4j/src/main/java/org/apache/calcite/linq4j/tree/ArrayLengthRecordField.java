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
package org.apache.calcite.linq4j.tree;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Length field of a RecordType.
 */
@SuppressWarnings("rawtypes")
public class ArrayLengthRecordField implements Types.RecordField {
  private final String fieldName;
  private final Class clazz;

  public ArrayLengthRecordField(String fieldName, Class clazz) {
    this.fieldName = requireNonNull(fieldName, "fieldName");
    this.clazz = requireNonNull(clazz, "clazz");
  }

  @Override public boolean nullable() {
    return false;
  }

  @Override public String getName() {
    return fieldName;
  }

  @Override public Type getType() {
    return int.class;
  }

  @Override public int getModifiers() {
    return 0;
  }

  @Override public Object get(@Nullable Object o) {
    return Array.getLength(requireNonNull(o, "o"));
  }

  @Override public Type getDeclaringClass() {
    return clazz;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayLengthRecordField that = (ArrayLengthRecordField) o;
    return clazz.equals(that.clazz)
        && fieldName.equals(that.fieldName);
  }

  @Override public int hashCode() {
    return Objects.hash(fieldName, clazz);
  }
}
