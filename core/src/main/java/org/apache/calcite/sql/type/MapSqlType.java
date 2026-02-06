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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * SQL map type.
 */
public class MapSqlType extends AbstractSqlType {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType keyType;
  private final RelDataType valueType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MapSqlType. This constructor should only be called
   * from a factory method.
   */
  public MapSqlType(
      RelDataType keyType, RelDataType valueType, boolean isNullable) {
    super(SqlTypeName.MAP, isNullable, null);
    this.keyType = requireNonNull(keyType, "keyType");
    this.valueType = requireNonNull(valueType, "valueType");
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType getValueType() {
    return valueType;
  }

  @Override public RelDataType getKeyType() {
    return keyType;
  }

  // implement RelDataTypeImpl
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(")
        .append(
            withDetail
                ? keyType.getFullTypeString()
                : keyType.toString())
        .append(", ")
        .append(
            withDetail
                ? valueType.getFullTypeString()
                : valueType.toString())
        .append(") MAP");
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    MapSqlType that = (MapSqlType) obj;
    return this.isNullable() == that.isNullable() && keyType.equals(that.keyType)
        && valueType.equals(that.valueType);
  }

  @Override public int deepHashCode() {
    return Objects.hash(SqlTypeName.MAP.ordinal(), this.isNullable, keyType.hashCode(),
        valueType.hashCode());
  }

  // implement RelDataType
  @Override public RelDataTypeFamily getFamily() {
    return this;
  }
}
