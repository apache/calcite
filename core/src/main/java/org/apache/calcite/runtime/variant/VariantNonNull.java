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

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.UnsignedType;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.rtti.BasicSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RowSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.joou.UByte;
import org.joou.UInteger;
import org.joou.ULong;
import org.joou.UShort;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName.NAME;

import static java.util.Objects.requireNonNull;

/** A VARIANT value that contains a non-null value. */
public class VariantNonNull extends VariantSqlValue {
  final RoundingMode roundingMode;
  /** Actual value - can have any SQL type. */
  final Object value;

  VariantNonNull(RoundingMode roundingMode, Object value, RuntimeTypeInformation runtimeType) {
    super(runtimeType.getTypeName());
    this.roundingMode = roundingMode;
    // sanity check
    switch (runtimeType.getTypeName()) {
    case UUID:
      assert value instanceof UUID;
      this.value = value;
      break;
    case NAME:
      assert value instanceof String;
      this.value = value;
      break;
    case BOOLEAN:
      assert value instanceof Boolean;
      this.value = value;
      break;
    case TINYINT:
      assert value instanceof Byte;
      this.value = value;
      break;
    case UTINYINT:
      assert value instanceof UByte;
      this.value = value;
      break;
    case SMALLINT:
      assert value instanceof Short;
      this.value = value;
      break;
    case USMALLINT:
      assert value instanceof UShort;
      this.value = value;
      break;
    case INTEGER:
      assert value instanceof Integer;
      this.value = value;
      break;
    case UINTEGER:
      assert value instanceof UInteger;
      this.value = value;
      break;
    case BIGINT:
      assert value instanceof Long;
      this.value = value;
      break;
    case UBIGINT:
      assert value instanceof ULong;
      this.value = value;
      break;
    case DECIMAL:
      assert value instanceof BigDecimal;
      this.value = value;
      break;
    case REAL:
      assert value instanceof Float;
      this.value = value;
      break;
    case DOUBLE:
      assert value instanceof Double;
      this.value = value;
      break;
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIME_TZ:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_TZ:
    case INTERVAL_LONG:
    case INTERVAL_SHORT:
      this.value = value;
      break;
    case VARCHAR:
      this.value = value;
      assert value instanceof String;
      break;
    case NULL:
    default:
      throw new RuntimeException("Unreachable");
    case VARBINARY:
    case GEOMETRY:
    case VARIANT:
      this.value = value;
      break;
    case MAP: {
      RuntimeTypeInformation keyType = runtimeType.asGeneric().getTypeArgument(0);
      RuntimeTypeInformation valueType = runtimeType.asGeneric().getTypeArgument(1);
      assert value instanceof Map<?, ?>;
      Map<?, ?> map = (Map<?, ?>) value;
      LinkedHashMap<VariantValue, VariantValue> converted = new LinkedHashMap<>(map.size());
      for (Map.Entry<?, ?> o : map.entrySet()) {
        VariantValue key = VariantSqlValue.create(roundingMode, o.getKey(), keyType);
        VariantValue val = VariantSqlValue.create(roundingMode, o.getValue(), valueType);
        converted.put(key, val);
      }
      this.value = converted;
      break;
    }
    case ROW: {
      assert value instanceof Object[];
      Object[] a = (Object[]) value;
      assert runtimeType instanceof RowSqlTypeRtti;
      RowSqlTypeRtti rowType = (RowSqlTypeRtti) runtimeType;
      LinkedHashMap<VariantValue, VariantValue> converted = new LinkedHashMap<>(a.length);
      RuntimeTypeInformation name = new BasicSqlTypeRtti(NAME);
      for (int i = 0; i < a.length; i++) {
        Map.Entry<String, RuntimeTypeInformation> fieldType = rowType.getField(i);
        VariantValue key = VariantSqlValue.create(roundingMode, fieldType.getKey(), name);
        VariantValue val = VariantSqlValue.create(roundingMode, a[i], fieldType.getValue());
        converted.put(key, val);
      }
      this.value = converted;
      break;
    }
    case MULTISET:
    case ARRAY: {
      RuntimeTypeInformation elementType = runtimeType.asGeneric().getTypeArgument(0);
      assert value instanceof List<?>;
      List<?> list = (List<?>) value;
      List<VariantValue> converted = new ArrayList<>(list.size());
      for (Object o : list) {
        VariantValue element = VariantSqlValue.create(roundingMode, o, elementType);
        converted.add(element);
      }
      this.value = converted;
      break;
    }
    }
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
        && runtimeType == variant.runtimeType;
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
      if (this.runtimeType == type.getTypeName()) {
        return this.value;
      } else {
        // Convert numeric values
        @Nullable Primitive target = type.asPrimitive();
        switch (this.runtimeType) {
        case TINYINT: {
          byte b = (byte) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(b, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(b);
          case UTINYINT:
            return UnsignedType.toUByte(b);
          case USMALLINT:
            return UnsignedType.toUShort(b);
          case UINTEGER:
            return UnsignedType.toUInteger(b);
          case UBIGINT:
            return UnsignedType.toULong(b);
          default:
            break;
          }
          break;
        }
        case SMALLINT: {
          short s = (short) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(s, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(s);
          case UTINYINT:
            return UnsignedType.toUByte(s);
          case USMALLINT:
            return UnsignedType.toUShort(s);
          case UINTEGER:
            return UnsignedType.toUInteger(s);
          case UBIGINT:
            return UnsignedType.toULong(s);
          default:
            break;
          }
          break;
        }
        case INTEGER: {
          int i = (int) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(i, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(i);
          case UTINYINT:
            return UnsignedType.toUByte(i);
          case USMALLINT:
            return UnsignedType.toUShort(i);
          case UINTEGER:
            return UnsignedType.toUInteger(i);
          case UBIGINT:
            return UnsignedType.toULong(i);
          default:
            break;
          }
          break;
        }
        case BIGINT: {
          long l = (int) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(l, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(l);
          case UTINYINT:
            return UnsignedType.toUByte(l);
          case USMALLINT:
            return UnsignedType.toUShort(l);
          case UINTEGER:
            return UnsignedType.toUInteger(l);
          case UBIGINT:
            return UnsignedType.toULong(l);
          default:
            break;
          }
          break;
        }
        case DECIMAL: {
          BigDecimal d = (BigDecimal) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(d, roundingMode);
          case DECIMAL:
            return d;
          case UTINYINT:
            return UnsignedType.toUByte(d.longValueExact());
          case USMALLINT:
            return UnsignedType.toUShort(d.longValueExact());
          case UINTEGER:
            return UnsignedType.toUInteger(d.longValueExact());
          case UBIGINT:
            return UnsignedType.toULong(d.longValueExact());
          default:
            break;
          }
          break;
        }
        case REAL: {
          float f = (float) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(f, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(f);
          case UTINYINT:
            return UnsignedType.toUByte(f);
          case USMALLINT:
            return UnsignedType.toUShort(f);
          case UINTEGER:
            return UnsignedType.toUInteger(f);
          case UBIGINT:
            return UnsignedType.toULong(f);
          default:
            break;
          }
          break;
        }
        case DOUBLE: {
          double d = (double) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(d, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(d);
          case UTINYINT:
            return UnsignedType.toUByte(d);
          case USMALLINT:
            return UnsignedType.toUShort(d);
          case UINTEGER:
            return UnsignedType.toUInteger(d);
          case UBIGINT:
            return UnsignedType.toULong(d);
          default:
            break;
          }
          break;
        }
        case UTINYINT: {
          UByte b = (UByte) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(b, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(b.intValue());
          case UTINYINT:
            return UnsignedType.toUByte(b.intValue());
          case USMALLINT:
            return UnsignedType.toUShort(b.intValue());
          case UINTEGER:
            return UnsignedType.toUInteger(b.intValue());
          case UBIGINT:
            return UnsignedType.toULong(b.intValue());
          default:
            break;
          }
          break;
        }
        case USMALLINT: {
          UShort s = (UShort) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(s, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(s.intValue());
          case UTINYINT:
            return UnsignedType.toUByte(s.intValue());
          case USMALLINT:
            return UnsignedType.toUShort(s.intValue());
          case UINTEGER:
            return UnsignedType.toUInteger(s.intValue());
          case UBIGINT:
            return UnsignedType.toULong(s.intValue());
          default:
            break;
          }
          break;
        }
        case UINTEGER: {
          UInteger b = (UInteger) value;
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(b, roundingMode);
          case DECIMAL:
            return BigDecimal.valueOf(b.longValue());
          case UTINYINT:
            return UnsignedType.toUByte(b.longValue());
          case USMALLINT:
            return UnsignedType.toUShort(b.longValue());
          case UINTEGER:
            return UnsignedType.toUInteger(b.longValue());
          case UBIGINT:
            return UnsignedType.toULong(b.longValue());
          default:
            break;
          }
          break;
        }
        case UBIGINT: {
          BigInteger i = UnsignedType.toBigInteger((ULong) value);
          switch (type.getTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case REAL:
          case DOUBLE:
            return requireNonNull(target, "target").numberValue(i, roundingMode);
          case DECIMAL:
            return new BigDecimal(i);
          case UTINYINT:
            return UnsignedType.toUByte(i.longValueExact());
          case USMALLINT:
            return UnsignedType.toUShort(i.longValueExact());
          case UINTEGER:
            return UnsignedType.toUInteger(i.longValueExact());
          case UBIGINT:
            return value;
          default:
            break;
          }
          break;
        }
        default:
          break;
        }
        return null;
      }
    } else {
      switch (this.runtimeType) {
      case ARRAY:
        if (type.getTypeName() == RuntimeTypeInformation.RuntimeSqlTypeName.ARRAY) {
          RuntimeTypeInformation elementType = type.asGeneric().getTypeArgument(0);
          assert value instanceof List;
          List<VariantSqlValue> list = (List<VariantSqlValue>) value;
          List<@Nullable Object> result = new ArrayList<>(list.size());
          for (VariantSqlValue o : list) {
            @Nullable Object converted = o.cast(elementType);
            result.add(converted);
          }
          return result;
        }
        break;
      case MAP:
        assert value instanceof Map;
        Map<VariantSqlValue, VariantSqlValue> map = (Map<VariantSqlValue, VariantSqlValue>) value;
        if (type.getTypeName() == RuntimeTypeInformation.RuntimeSqlTypeName.MAP) {
          // Convert map to map: cast keys and values recursively
          RuntimeTypeInformation keyType = type.asGeneric().getTypeArgument(0);
          RuntimeTypeInformation valueType = type.asGeneric().getTypeArgument(0);
          LinkedHashMap<@Nullable Object, @Nullable Object> result =
              new LinkedHashMap<>(map.size());
          for (Map.Entry<VariantSqlValue, VariantSqlValue> e : map.entrySet()) {
            @Nullable Object key = e.getKey().cast(keyType);
            @Nullable Object value = e.getValue().cast(valueType);
            result.put(key, value);
          }
          return result;
        } else if (type.getTypeName() == RuntimeTypeInformation.RuntimeSqlTypeName.ROW) {
          // Convert map to row: lookup the row's fields in the map
          RowSqlTypeRtti rowType = (RowSqlTypeRtti) type;
          @Nullable Object [] result = new Object[rowType.size()];
          for (int i = 0; i < rowType.size(); i++) {
            Map.Entry<String, RuntimeTypeInformation> field = rowType.getField(i);
            Object fieldValue = null;
            VariantValue v = this.item(field.getKey());
            if (v != null) {
              fieldValue = v.cast(field.getValue());
            }
            result[i] = fieldValue;
          }
          return result;
        }
        break;
      default:
        break;
      }
    }
    return null;
  }

  // Implementation of the array index operator for VARIANT values
  @Override public @Nullable VariantValue item(Object index) {
    boolean isInteger = index instanceof Integer;
    switch (this.runtimeType) {
    case ROW:
      if (index instanceof String) {
        RuntimeTypeInformation string =
            new BasicSqlTypeRtti(RuntimeTypeInformation.RuntimeSqlTypeName.NAME);
        index = VariantSqlValue.create(roundingMode, index, string);
      }
      break;
    case MAP:
      if (index instanceof String) {
        RuntimeTypeInformation string =
            new BasicSqlTypeRtti(RuntimeTypeInformation.RuntimeSqlTypeName.VARCHAR);
        index = VariantSqlValue.create(roundingMode, index, string);
      } else if (isInteger) {
        RuntimeTypeInformation i =
            new BasicSqlTypeRtti(RuntimeTypeInformation.RuntimeSqlTypeName.INTEGER);
        index = VariantSqlValue.create(roundingMode, index, i);
      }
      break;
    case ARRAY:
      if (!isInteger) {
        // Arrays only support integer indexes
        return null;
      }
      break;
    default:
      return null;
    }

    // If index is VARIANT, leave it unchanged
    Object result = SqlFunctions.itemOptional(this.value, index);
    if (result == null) {
      return null;
    }
    // If result is a variant, return as is
    if (result instanceof VariantValue) {
      return (VariantValue) result;
    }
    return null;
  }

  // This method is called by the testing code.
  @Override public String toString() {
    if (this.runtimeType == RuntimeTypeInformation.RuntimeSqlTypeName.ROW) {
      if (value instanceof Map<?, ?>) {
        // Do not print field names, only their values
        Map<?, ?> map = (Map<?, ?>) value;
        StringBuilder buf = new StringBuilder("{");

        boolean first = true;
        for (Map.Entry<?, ?> o : map.entrySet()) {
          if (!first) {
            buf.append(", ");
          }
          first = false;
          if (o.getValue() != null) {
            // This should always be true
            buf.append(o.getValue());
          }
        }
        buf.append("}");
        return buf.toString();
      }
    }
    String quote = "";
    switch (this.runtimeType) {
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
