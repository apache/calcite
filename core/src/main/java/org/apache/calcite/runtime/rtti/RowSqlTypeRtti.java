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
package org.apache.calcite.runtime.rtti;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Map;

/** Runtime type information for a ROW type. */
public class RowSqlTypeRtti extends RuntimeTypeInformation {
  private final Map.Entry<String, RuntimeTypeInformation>[] fields;

  @SafeVarargs
  public RowSqlTypeRtti(Map.Entry<String, RuntimeTypeInformation>... fields) {
    super(RuntimeSqlTypeName.ROW);
    this.fields = fields;
  }

  @Override public String getTypeString()  {
    return "ROW";
  }

  // This method is used to serialize the type in Java code implementations,
  // so it should produce a computation that reconstructs the type at runtime
  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("new RowSqlTypeRtti(");
    boolean first = true;
    for (Map.Entry<String, RuntimeTypeInformation> arg : this.fields) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(arg.toString());
    }
    builder.append(")");
    return builder.toString();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RowSqlTypeRtti that = (RowSqlTypeRtti) o;
    return Arrays.equals(fields, that.fields);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(fields);
  }

  /** Get the field with the specified index. */
  public Map.Entry<String, RuntimeTypeInformation> getField(int index) {
    return this.fields[index];
  }

  public int size() {
    return this.fields.length;
  }

  /** Return the runtime type information of the associated field,
   * or null if no such field exists.
   *
   * @param index Field index, starting from 0
   */
  public @Nullable RuntimeTypeInformation getFieldType(Object index) {
    if (index instanceof Integer) {
      int intIndex = (Integer) index;
      if (intIndex < 0 || intIndex >= this.fields.length) {
        return null;
      }
      return this.fields[intIndex].getValue();
    } else if (index instanceof String) {
      String stringIndex = (String) index;
      for (Map.Entry<String, RuntimeTypeInformation> field : this.fields) {
        if (field.getKey().equalsIgnoreCase(stringIndex)) {
          return field.getValue();
        }
      }
    }
    return null;
  }
}
