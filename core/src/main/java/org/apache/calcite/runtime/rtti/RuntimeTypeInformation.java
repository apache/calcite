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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractMap;

import static java.util.Objects.requireNonNull;

/**
 * The type of a SQL expression at runtime.
 * Normally SQL is a statically-typed language, and there is no need for
 * runtime-type information. However, the VARIANT data type is actually
 * a dynamically-typed value, and needs this kind of information.
 * We cannot use the very similar RelDataType type since it carries extra
 * baggage, like the type system, which is not available at runtime. */
public abstract class RuntimeTypeInformation {
  /** Names of SQL types as represented at runtime. */
  public enum RuntimeSqlTypeName {
    BOOLEAN(false),
    TINYINT(false),
    SMALLINT(false),
    INTEGER(false),
    BIGINT(false),
    UTINYINT(false),
    USMALLINT(false),
    UINTEGER(false),
    UBIGINT(false),
    DECIMAL(false),
    REAL(false),
    // FLOAT is represented as DOUBLE
    DOUBLE(false),
    DATE(false),
    TIME(false),
    TIME_WITH_LOCAL_TIME_ZONE(false),
    TIME_TZ(false),
    TIMESTAMP(false),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE(false),
    TIMESTAMP_TZ(false),
    INTERVAL_LONG(false),
    INTERVAL_SHORT(false),
    // "Name" is used for structure field names
    NAME(false),
    // CHAR is represented as VARCHAR
    VARCHAR(false),
    // BINARY is represented as VARBINARY
    VARBINARY(false),
    NULL(false),
    MULTISET(true),
    ARRAY(true),
    MAP(true),
    ROW(true),
    GEOMETRY(false),
    UUID(false),
    // used only for VARIANT.null value
    VARIANT(false);

    private final boolean composite;

    RuntimeSqlTypeName(boolean composite) {
      this.composite = composite;
    }

    public boolean isScalar() {
      return !this.composite;
    }

    @Override public String toString() {
      switch (this) {
      case UINTEGER:
        return "INTEGER UNSIGNED";
      case UBIGINT:
        return "BIGINT UNSIGNED";
      case UTINYINT:
        return "TINYINT UNSIGNED";
      case USMALLINT:
        return "SMALLINT UNSIGNED";
      default:
        return this.name();
      }
    }
  }

  final RuntimeSqlTypeName typeName;

  protected RuntimeTypeInformation(RuntimeSqlTypeName typeName) {
    this.typeName = typeName;
  }

  public abstract String getTypeString();

  public RuntimeSqlTypeName getTypeName() {
    return this.typeName;
  }

  public boolean isScalar() {
    return this.typeName.isScalar();
  }

  /** If this type is a Primitive, return it, otherwise return null. */
  public @Nullable Primitive asPrimitive() {
    switch (typeName) {
    case BOOLEAN:
      return Primitive.BOOLEAN;
    case TINYINT:
      return Primitive.BYTE;
    case SMALLINT:
      return Primitive.SHORT;
    case INTEGER:
      return Primitive.INT;
    case BIGINT:
      return Primitive.LONG;
    case REAL:
      return Primitive.FLOAT;
    case DOUBLE:
      return Primitive.DOUBLE;
    default:
      return null;
    }
  }

  public GenericSqlTypeRtti asGeneric() {
    assert this instanceof GenericSqlTypeRtti;
    return (GenericSqlTypeRtti) this;
  }

  /**
   * Creates and returns an expression that creates a runtime type that
   * reflects the information in the statically-known type 'type'.
   *
   * @param type The static type of an expression.
   */
  public static Expression createExpression(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.BOOLEAN));
    case TINYINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TINYINT));
    case SMALLINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.SMALLINT));
    case INTEGER:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.INTEGER));
    case BIGINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.BIGINT));
    case UTINYINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.UTINYINT));
    case USMALLINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.USMALLINT));
    case UINTEGER:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.UINTEGER));
    case UBIGINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.UBIGINT));
    case DECIMAL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DECIMAL));
    case REAL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.REAL));
    case FLOAT:
    case DOUBLE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DOUBLE));
    case DATE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DATE));
    case TIME:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME));
    case TIME_WITH_LOCAL_TIME_ZONE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
    case TIME_TZ:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME_TZ));
    case TIMESTAMP:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP));
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    case TIMESTAMP_TZ:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP_TZ));
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.INTERVAL_LONG));
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.INTERVAL_SHORT));
    case CHAR:
    case VARCHAR:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARCHAR));
    case BINARY:
    case VARBINARY:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARBINARY));
    case NULL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.NULL));
    case MULTISET: {
      Expression comp = createExpression(requireNonNull(type.getComponentType()));
      return Expressions.new_(GenericSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.MULTISET), comp);
    }
    case ARRAY: {
      Expression comp = createExpression(requireNonNull(type.getComponentType()));
      return Expressions.new_(GenericSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.ARRAY), comp);
    }
    case MAP: {
      Expression key = createExpression(requireNonNull(type.getKeyType()));
      Expression value = createExpression(requireNonNull(type.getValueType()));
      return Expressions.new_(GenericSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.MAP), key, value);
    }
    case ROW: {
      Expression[] fields = new Expression[type.getFieldCount()];
      int index = 0;
      for (RelDataTypeField field : type.getFieldList()) {
        String name = field.getName();
        RelDataType fieldType = field.getType();
        Expression fieldTypeExpression = createExpression(fieldType);
        Expression nameExpression = Expressions.constant(name);
        Expression entry =
            Expressions.new_(AbstractMap.SimpleEntry.class, nameExpression, fieldTypeExpression);
        fields[index++] = entry;
      }
      return Expressions.new_(RowSqlTypeRtti.class, fields);
    }
    case GEOMETRY:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.GEOMETRY));
    case VARIANT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARIANT));
    case UUID:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.UUID));
    default:
      throw new RuntimeException("Unexpected type " + type);
    }
  }
}
