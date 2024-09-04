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
package org.apache.calcite.util;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.util.rtti.BasicSqlTypeRtti;
import org.apache.calcite.util.rtti.RuntimeTypeInformation;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** This class is the runtime support for values of the VARIANT SQL type. */
public class Variant {
  /** Actual value. */
  final @Nullable Object value;
  /** Type of the value. */
  final RuntimeTypeInformation runtimeType;
  /** The VARIANT type has its own notion of null, which is
   * different from the SQL NULL value.  For example, two variant nulls are equal
   * with each other.  This flag is 'true' if this value represents a variant null. */
  final boolean isVariantNull;

  private Variant(@Nullable Object value, RuntimeTypeInformation runtimeType,
      boolean isVariantNull) {
    this.value = value;
    this.runtimeType = runtimeType;
    this.isVariantNull = isVariantNull;
  }

  public Variant(@Nullable Object value, RuntimeTypeInformation runtimeType) {
    this(value, runtimeType, false);
  }

  /** Create a variant object with a null value. */
  public Variant() {
    this(null,
        new BasicSqlTypeRtti(RuntimeTypeInformation.RuntimeSqlTypeName.VARIANT,
            BasicSqlType.PRECISION_NOT_SPECIFIED, BasicSqlType.SCALE_NOT_SPECIFIED),
        true);
  }

  public String getType() {
    return this.runtimeType.getTypeString();
  }

  public boolean isVariantNull() {
    return this.isVariantNull;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Variant variant = (Variant) o;
    return isVariantNull == variant.isVariantNull
        && Objects.equals(value, variant.value)
        && runtimeType.equals(variant.runtimeType);
  }

  @Override public int hashCode() {
    int result = Objects.hashCode(value);
    result = 31 * result + runtimeType.hashCode();
    result = 31 * result + Boolean.hashCode(isVariantNull);
    return result;
  }

  /** Cast this value to the specified type.  Currently, the rule is:
   * if the value has the specified type, the value field is returned, otherwise a SQL
   * NULL is returned. */
  @Nullable Object cast(RuntimeTypeInformation type) {
    if (this.runtimeType.isScalar()) {
      if (this.runtimeType.equals(type)) {
        return this.value;
      } else {
        return null;
      }
    } else {
      if (this.runtimeType.equals(type)) {
        return this.value;
      }
      // TODO: allow casts that change some of the generic arguments only
    }
    return null;
  }

  // This method is invoked from {@link RexToLixTranslator} VARIANT_CAST
  public static @Nullable Object cast(@Nullable Object variant, RuntimeTypeInformation type) {
    if (variant == null) {
      return null;
    }
    if (!(variant instanceof Variant)) {
      throw new RuntimeException("Expected a variant value " + variant);
    }
    return ((Variant) variant).cast(type);
  }

  // Implementation of the array index operator for VARIANT values
  public @Nullable Object item(Object index) {
    if (this.value == null) {
      return null;
    }
    switch (this.runtimeType.getTypeName()) {
    case ROW:
    case ARRAY:
    case MAP:
      return SqlFunctions.item(this.value, index);
    default:
      return null;
    }
  }

  // This method is called by the testing code.
  @Override public String toString() {
    if (value == null) {
      return "NULL";
    }
    if (this.isVariantNull()) {
      return "null";
    }
    if (this.runtimeType.getTypeName() == RuntimeTypeInformation.RuntimeSqlTypeName.ROW) {
      if (value instanceof Object[]) {
        Object[] array = (Object []) value;
        StringBuilder buf = new StringBuilder("{");

        boolean first = true;
        for (Object o : array) {
          if (!first) {
            buf.append(", ");
          }
          first = false;
          buf.append(o.toString());
        }
        buf.append("}");
        return buf.toString();
      }
    }
    String quote = "";
    switch (this.runtimeType.getTypeName()) {
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIME_TZ:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_TZ:
    case INTERVAL_LONG:
    case INTERVAL_SHORT:
    case CHAR:
    case VARCHAR:
    case BINARY:
    case VARBINARY:
      // At least in Snowflake VARIANT values that are strings
      // are printed with double quotes
      // https://docs.snowflake.com/en/sql-reference/data-types-semistructured
      quote = "\"";
      break;
    default:
      break;
    }
    return quote + value + quote;
  }
}
