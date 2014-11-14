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
    assert keyType != null;
    assert valueType != null;
    this.keyType = keyType;
    this.valueType = valueType;
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
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
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
  public RelDataTypeFamily getFamily() {
    return this;
  }
}

// End MapSqlType.java
