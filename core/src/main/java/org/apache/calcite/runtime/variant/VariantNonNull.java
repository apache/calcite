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
package org.apache.calcite.runtime.variant;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.rtti.GenericSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RowSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** A VARIANT value that contains a non-null value. */
public class VariantNonNull extends VariantSqlValue {
  /** Actual value - can have any SQL type. */
  final Object value;

  VariantNonNull(Object value, RuntimeTypeInformation runtimeType) {
    super(runtimeType);
    this.value = value;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    VariantNonNull variant = (VariantNonNull) o;
    return Objects.equals(value, variant.value)
        && runtimeType.equals(variant.runtimeType);
  }

  @Override public int hashCode() {
    int result = Objects.hashCode(value);
    result = 31 * result + runtimeType.hashCode();
    return result;
  }

  /** Cast this value to the specified type.  Currently, the rule is:
   * if the value has the specified type, the value field is returned, otherwise a SQL
   * NULL is returned. */
  // This method is invoked from {@link RexToLixTranslator} VARIANT_CAST
  @Override public @Nullable Object cast(RuntimeTypeInformation type) {
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

  // Implementation of the array index operator for VARIANT values
  public @Nullable VariantValue item(Object index) {
    @Nullable RuntimeTypeInformation fieldType;
    boolean isInteger = index instanceof Integer;
    switch (this.runtimeType.getTypeName()) {
    case ROW:
      // The type of the field
      fieldType = ((RowSqlTypeRtti) this.runtimeType).getFieldType(index);
      break;
    case ARRAY:
      if (!isInteger) {
        return null;
      }
      // The type of the elements
      fieldType = ((GenericSqlTypeRtti) this.runtimeType).getTypeArgument(0);
      break;
    case MAP:
      // The type of the values
      fieldType = ((GenericSqlTypeRtti) this.runtimeType).getTypeArgument(1);
      break;
    default:
      return null;
    }
    if (fieldType == null) {
      return null;
    }

    Object result = SqlFunctions.itemOptional(this.value, index);
    if (result == null) {
      return null;
    }
    // If result is a variant, return as is
    if (result instanceof VariantValue) {
      return (VariantValue) result;
    }
    // Otherwise pack the result in a Variant
    return VariantSqlValue.create(result, fieldType);
  }

  // This method is called by the testing code.
  @Override public String toString() {
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
    case VARCHAR:
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
