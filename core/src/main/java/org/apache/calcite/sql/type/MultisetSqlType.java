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

/**
 * MultisetSqlType represents a standard SQL2003 multiset type.
 */
public class MultisetSqlType extends AbstractSqlType {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType elementType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a new MultisetSqlType. This constructor should only be called
   * from a factory method.
   */
  public MultisetSqlType(RelDataType elementType, boolean isNullable) {
    super(SqlTypeName.MULTISET, isNullable, null);
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
    sb.append(" MULTISET");
  }

  // implement RelDataType
  public RelDataType getComponentType() {
    return elementType;
  }

  // implement RelDataType
  public RelDataTypeFamily getFamily() {
    // TODO jvs 2-Dec-2004:  This gives each multiset type its
    // own family.  But that's not quite correct; the family should
    // be based on the element type for proper comparability
    // semantics (per 4.10.4 of SQL/2003).  So either this should
    // make up canonical families dynamically, or the
    // comparison type-checking should not rely on this.  I
    // think the same goes for ROW types.
    return this;
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    return new RelDataTypePrecedenceList() {
      public boolean containsType(RelDataType type) {
        return type.getSqlTypeName() == getSqlTypeName()
            && type.getComponentType() != null
            && getComponentType().getPrecedenceList().containsType(
                type.getComponentType());
      }

      public int compareTypePrecedence(RelDataType type1, RelDataType type2) {
        if (!containsType(type1)) {
          throw new IllegalArgumentException("must contain type: " + type1);
        }
        if (!containsType(type2)) {
          throw new IllegalArgumentException("must contain type: " + type2);
        }
        return getComponentType().getPrecedenceList()
            .compareTypePrecedence(type1.getComponentType(), type2.getComponentType());
      }
    };
  }
}

// End MultisetSqlType.java
