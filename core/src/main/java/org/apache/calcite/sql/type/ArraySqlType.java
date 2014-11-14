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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFamily;

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
    assert elementType != null;
    this.elementType = elementType;
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelDataTypeImpl
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    if (withDetail) {
      sb.append(elementType.getFullTypeString());
    } else {
      sb.append(elementType.toString());
    }
    sb.append(" ARRAY");
  }

  // implement RelDataType
  public RelDataType getComponentType() {
    return elementType;
  }

  // implement RelDataType
  public RelDataTypeFamily getFamily() {
    return this;
  }
}

// End ArraySqlType.java
