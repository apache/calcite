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
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeName;

import java.io.Serializable;

/**
 * Default implementation of {@link RelDataTypeField}.
 */
public class RelDataTypeFieldImpl implements RelDataTypeField, Serializable {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType type;
  private final String name;
  private final int index;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelDataTypeFieldImpl.
   */
  public RelDataTypeFieldImpl(
      String name,
      int index,
      RelDataType type) {
    assert name != null;
    assert type != null;
    this.name = name;
    this.index = index;
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public int hashCode() {
    return index
        ^ name.hashCode()
        ^ type.hashCode();
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RelDataTypeFieldImpl)) {
      return false;
    }
    RelDataTypeFieldImpl that = (RelDataTypeFieldImpl) obj;
    return this.index == that.index
        && this.name.equals(that.name)
        && this.type.equals(that.type);
  }

  // implement RelDataTypeField
  public String getName() {
    return name;
  }

  // implement RelDataTypeField
  public int getIndex() {
    return index;
  }

  // implement RelDataTypeField
  public RelDataType getType() {
    return type;
  }

  // implement Map.Entry
  public final String getKey() {
    return getName();
  }

  // implement Map.Entry
  public final RelDataType getValue() {
    return getType();
  }

  // implement Map.Entry
  public RelDataType setValue(RelDataType value) {
    throw new UnsupportedOperationException();
  }

  // for debugging
  public String toString() {
    return "#" + index + ": " + name + " " + type;
  }

  public boolean isDynamicStar() {
    return type.getSqlTypeName() == SqlTypeName.DYNAMIC_STAR;
  }

}

// End RelDataTypeFieldImpl.java
