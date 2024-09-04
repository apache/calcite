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
package org.apache.calcite.util.rtti;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.AbstractMap;

import static java.util.Objects.requireNonNull;

/**
 * This class represents the type of a SQL expression at runtime.
 * Normally SQL is a statically-typed language, and there is no need for
 * runtime-type information.  However, the VARIANT data type is actually
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
    DECIMAL(false),
    FLOAT(false),
    REAL(false),
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
    CHAR(false),
    VARCHAR(false),
    BINARY(false),
    VARBINARY(false),
    NULL(false),
    MULTISET(true),
    ARRAY(true),
    MAP(true),
    ROW(true),
    GEOMETRY(false),
    // used only for VARIANT.null value
    VARIANT(false);

    private final boolean composite;

    RuntimeSqlTypeName(boolean composite) {
      this.composite = composite;
    }

    public boolean isScalar() {
      return !this.composite;
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

  /**
   * Create and return an expression that creates a runtime type that
   * reflects the information in the statically-known type 'type'.
   *
   * @param type The static type of an expression.
   */
  public static Expression createExpression(RelDataType type) {
    final Expression precision = Expressions.constant(type.getPrecision());
    final Expression scale = Expressions.constant(type.getScale());
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.BOOLEAN), precision, scale);
    case TINYINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TINYINT), precision, scale);
    case SMALLINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.SMALLINT), precision, scale);
    case INTEGER:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.INTEGER), precision, scale);
    case BIGINT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.BIGINT), precision, scale);
    case DECIMAL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DECIMAL), precision, scale);
    case FLOAT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.FLOAT), precision, scale);
    case REAL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.REAL), precision, scale);
    case DOUBLE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DOUBLE), precision, scale);
    case DATE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.DATE), precision, scale);
    case TIME:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME), precision, scale);
    case TIME_WITH_LOCAL_TIME_ZONE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME_WITH_LOCAL_TIME_ZONE),
          precision, scale);
    case TIME_TZ:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIME_TZ), precision, scale);
    case TIMESTAMP:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP), precision, scale);
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
          precision, scale);
    case TIMESTAMP_TZ:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.TIMESTAMP_TZ), precision, scale);
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.INTERVAL_LONG), precision, scale);
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
          Expressions.constant(RuntimeSqlTypeName.INTERVAL_SHORT), precision, scale);
    case CHAR:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.CHAR), precision, scale);
    case VARCHAR:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARCHAR), precision, scale);
    case BINARY:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.BINARY), precision, scale);
    case VARBINARY:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARBINARY), precision, scale);
    case NULL:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.NULL), precision, scale);
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
      Expression key = createExpression(requireNonNull(type.getValueType()));
      Expression value = createExpression(requireNonNull(type.getKeyType()));
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
          Expressions.constant(RuntimeSqlTypeName.GEOMETRY), precision, scale);
    case VARIANT:
      return Expressions.new_(BasicSqlTypeRtti.class,
          Expressions.constant(RuntimeSqlTypeName.VARIANT), precision, scale);
    default:
      throw new RuntimeException("Unexpected type " + type);
    }
  }
}
