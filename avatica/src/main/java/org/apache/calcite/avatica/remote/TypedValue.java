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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/** Value and type. */
public class TypedValue {
  /** Type of the value. */
  public final ColumnMetaData.Rep type;

  /** Value.
   *
   * <p>Always in a form that can be serialized to JSON by Jackson.
   * For example, byte arrays are represented as String. */
  public final Object value;

  private TypedValue(ColumnMetaData.Rep rep, Object value) {
    this.type = rep;
    this.value = serialize(rep, value);
  }

  @JsonCreator
  public static TypedValue create(@JsonProperty("type") String type,
      @JsonProperty("value") Object value) {
    ColumnMetaData.Rep rep = ColumnMetaData.Rep.valueOf(type);
    return new TypedValue(rep, deserialize(rep, value));
  }

  /** Converts the value into the preferred representation.
   *
   * <p>For example, a byte string is represented as a {@link ByteString};
   * a long is represented as a {@link Long} (not just some {@link Number}).
   */
  public Object deserialize() {
    return deserialize(type, value);
  }

  /** Converts a value to the exact type required for the given
   * representation. */
  private static Object deserialize(ColumnMetaData.Rep rep, Object value) {
    if (value == null) {
      return null;
    }
    if (value.getClass() == rep.clazz) {
      return value;
    }
    switch (rep) {
    case BYTE:
      return ((Number) value).byteValue();
    case SHORT:
      return ((Number) value).shortValue();
    case INTEGER:
      return ((Number) value).intValue();
    case LONG:
      return ((Number) value).longValue();
    case FLOAT:
      return ((Number) value).floatValue();
    case DOUBLE:
      return ((Number) value).doubleValue();
    case BYTE_STRING:
      return ByteString.ofBase64((String) value);
    case JAVA_UTIL_DATE:
      return new java.util.Date((Long) value);
    case JAVA_SQL_DATE:
      return new java.sql.Date((long) (Integer) value
          * DateTimeUtils.MILLIS_PER_DAY);
    case JAVA_SQL_TIME:
      return new java.sql.Time((Integer) value);
    case JAVA_SQL_TIMESTAMP:
      return new java.sql.Timestamp((Long) value);
    default:
      throw new IllegalArgumentException("cannot convert " + value + " ("
          + value.getClass() + ") to " + rep);
    }
  }

  /** Converts a value to a type that can be serialized as JSON. */
  private static Object serialize(ColumnMetaData.Rep rep, Object value) {
    switch (rep) {
    case BYTE_STRING:
      return ((ByteString) value).toBase64String();
    case JAVA_UTIL_DATE:
    case JAVA_SQL_TIMESTAMP:
      return ((java.util.Date) value).getTime();
    case JAVA_SQL_DATE:
      return (int) DateTimeUtils.floorDiv(((java.sql.Date) value).getTime(),
          DateTimeUtils.MILLIS_PER_DAY);
    case JAVA_SQL_TIME:
      return (int) DateTimeUtils.floorMod(((java.sql.Time) value).getTime(),
          DateTimeUtils.MILLIS_PER_DAY);
    default:
      return value;
    }
  }

  /** Converts a list of {@code TypedValue} to a list of values. */
  public static List<Object> values(List<TypedValue> typedValues) {
    final List<Object> list = new ArrayList<>();
    for (TypedValue typedValue : typedValues) {
      list.add(typedValue.deserialize());
    }
    return list;
  }

  /** Converts a list of values to a list of {@code TypedValue}. */
  public static List<TypedValue> list(List<Object> values) {
    final List<TypedValue> list = new ArrayList<>();
    for (Object value : values) {
      list.add(create(value));
    }
    return list;
  }

  private static TypedValue create(Object value) {
    if (value == null) {
      return new TypedValue(ColumnMetaData.Rep.OBJECT, null);
    }
    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(value.getClass());
    return new TypedValue(rep, value);
  }
}

// End TypedValue.java
