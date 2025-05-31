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

import java.util.Objects;

/** Runtime type information about a base (primitive) SQL type. */
public class BasicSqlTypeRtti extends RuntimeTypeInformation {
  public BasicSqlTypeRtti(RuntimeSqlTypeName typeName) {
    super(typeName);
    assert typeName.isScalar() : "Base SQL type must be a scalar type " + typeName;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BasicSqlTypeRtti that = (BasicSqlTypeRtti) o;
    return typeName == that.typeName;
  }

  @Override public int hashCode() {
    return Objects.hashCode(typeName);
  }

  @Override public String getTypeString()  {
    switch (this.typeName) {
    case BOOLEAN:
      return "BOOLEAN";
    case TINYINT:
      return "TINYINT";
    case SMALLINT:
      return "SMALLINT";
    case INTEGER:
      return "INTEGER";
    case BIGINT:
      return "BIGINT";
    case UTINYINT:
      return "TINYINT UNSIGNED";
    case USMALLINT:
      return "SMALLINT UNSIGNED";
    case UINTEGER:
      return "INTEGER UNSIGNED";
    case UBIGINT:
      return "BIGINT UNSIGNED";
    case DECIMAL:
      return "DECIMAL";
    case REAL:
      return "REAL";
    case DOUBLE:
      return "DOUBLE";
    case DATE:
      return "DATE";
    case TIME:
      return "TIME";
    case TIME_WITH_LOCAL_TIME_ZONE:
      return "TIME_WITH_LOCAL_TIME_ZONE";
    case TIME_TZ:
      return "TIME_WITH_TIME_ZONE";
    case TIMESTAMP:
      return "TIMESTAMP";
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return "TIMESTAMP_WITH_LOCAL_TIME_ZONE";
    case TIMESTAMP_TZ:
      return "TIMESTAMP_WITH_TIME_ZONE";
    case INTERVAL_LONG:
    case INTERVAL_SHORT:
      return "INTERVAL";
    case VARCHAR:
      return "VARCHAR";
    case VARBINARY:
      return "VARBINARY";
    case NULL:
      return "NULL";
    case GEOMETRY:
      return "GEOMETRY";
    case VARIANT:
      return "VARIANT";
    default:
      throw new RuntimeException("Unexpected type " + this.typeName);
    }
  }

  // This method is used to serialize the type in Java code implementations,
  // so it should produce a computation that reconstructs the type at runtime
  @Override public String toString() {
    return "new BasicSqlTypeRtti(" + this.getTypeString() + ")";
  }
}
