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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * SQL map type.
 */
public class MapSqlType extends ApplySqlType {
  //~ Instance fields --------------------------------------------------------
  private static final int KEY_TYPE_INDEX = 0;
  private static final int VALUE_TYPE_INDEX = 1;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MapSqlType. This constructor should only be called
   * from a factory method.
   */
  public MapSqlType(
      RelDataType keyType, RelDataType valueType, boolean isNullable) {
    super(SqlTypeName.MAP, isNullable,
        Arrays.asList(requireNonNull(keyType, "keyType"),
            requireNonNull(valueType, "valueType")));
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType getValueType() {
    return types.get(VALUE_TYPE_INDEX);
  }

  @Override public RelDataType getKeyType() {
    return types.get(KEY_TYPE_INDEX);
  }

  // implement RelDataTypeImpl
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    final RelDataType keyType = getKeyType();
    final RelDataType valueType = getValueType();
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

  // implement RelDataType
  @Override public RelDataTypeFamily getFamily() {
    return this;
  }
}
