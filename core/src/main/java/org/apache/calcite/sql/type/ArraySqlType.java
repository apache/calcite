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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;

import static java.util.Objects.requireNonNull;

/**
 * SQL array type.
 */
public class ArraySqlType extends AbstractSqlType {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType elementType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an ArraySqlType. This constructor should only be called
   * from a factory method.
   */
  public ArraySqlType(RelDataType elementType, boolean isNullable) {
    super(SqlTypeName.ARRAY, isNullable, null);
    this.elementType = requireNonNull(elementType, "elementType");
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelDataTypeImpl
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    if (withDetail) {
      sb.append(elementType.getFullTypeString());
    } else {
      sb.append(elementType.toString());
    }
    sb.append(" ARRAY");
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    ArraySqlType that = (ArraySqlType) obj;
    return this.isNullable() == that.isNullable() && elementType.equals(that.elementType);
  }

  @Override public int deepHashCode() {
    return Objects.hash(SqlTypeName.ARRAY.ordinal(), isNullable, elementType.hashCode());
  }

  // implement RelDataType
  @Override public RelDataType getComponentType() {
    return elementType;
  }

  // implement RelDataType
  @Override public RelDataTypeFamily getFamily() {
    return this;
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    return new RelDataTypePrecedenceList() {
      @Override public boolean containsType(RelDataType type) {
        if (type.getSqlTypeName() != getSqlTypeName()) {
          return false;
        }
        RelDataType otherComponentType = type.getComponentType();
        return otherComponentType != null
            && getComponentType().getPrecedenceList().containsType(otherComponentType);
      }

      @Override public int compareTypePrecedence(RelDataType type1, RelDataType type2) {
        if (!containsType(type1)) {
          throw new IllegalArgumentException("must contain type: " + type1);
        }
        if (!containsType(type2)) {
          throw new IllegalArgumentException("must contain type: " + type2);
        }
        return getComponentType().getPrecedenceList()
            .compareTypePrecedence(getComponentTypeOrThrow(type1), getComponentTypeOrThrow(type2));
      }
    };
  }
}
