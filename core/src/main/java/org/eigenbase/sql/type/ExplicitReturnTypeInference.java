/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

/**
 * A {@link SqlReturnTypeInference} which always returns the same SQL type.
 */
public class ExplicitReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int argCount;
  private final SqlTypeName typeName;
  private final int length;
  private final int scale;
  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an inference rule which always returns the same type object.
   *
   * <p>If the requesting type factory is different, returns a copy of the
   * type object made using {@link RelDataTypeFactory#copyType(RelDataType)}
   * within the requesting type factory.
   *
   * <p>REVIEW jvs 6-Aug-2006: Under what circumstances is a copy of the type
   * required?
   *
   * @param type Type object
   */
  public ExplicitReturnTypeInference(RelDataType type) {
    this.type = type;
    this.typeName = null;
    this.length = -1;
    this.scale = -1;
    this.argCount = 0;
  }

  /**
   * Creates an inference rule which always returns a given SQL type with zero
   * parameters (such as <code>DATE</code>).
   *
   * @param typeName Name of the type
   */
  public ExplicitReturnTypeInference(SqlTypeName typeName) {
    this.argCount = 1;
    this.typeName = typeName;
    this.length = -1;
    this.scale = -1;
    this.type = null;
  }

  /**
   * Creates an inference rule which always returns a given SQL type with a
   * precision/length parameter (such as <code>VARCHAR(10)</code> and <code>
   * NUMBER(5)</code>).
   *
   * @param typeName Name of the type
   * @param length   Length or precision of the type
   */
  public ExplicitReturnTypeInference(SqlTypeName typeName, int length) {
    this.argCount = 2;
    this.typeName = typeName;
    this.length = length;
    this.scale = -1;
    this.type = null;
  }

  /**
   * Creates an inference rule which always returns a given SQL type with a
   * precision and scale parameters (such as <code>DECIMAL(8, 3)</code>).
   *
   * @param typeName Name of the type
   * @param length   Precision of the type
   */
  public ExplicitReturnTypeInference(
      SqlTypeName typeName,
      int length,
      int scale) {
    this.argCount = 3;
    this.typeName = typeName;
    this.length = length;
    this.scale = scale;
    this.type = null;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    if (type != null) {
      return opBinding.getTypeFactory().copyType(type);
    }
    return createType(opBinding.getTypeFactory());
  }

  protected RelDataType getExplicitType() {
    return type;
  }

  private RelDataType createType(RelDataTypeFactory typeFactory) {
    switch (argCount) {
    case 1:
      return typeFactory.createSqlType(typeName);
    case 2:
      return typeFactory.createSqlType(typeName, length);
    case 3:
      return typeFactory.createSqlType(typeName, length, scale);
    default:
      throw Util.newInternal("unexpected argCount " + argCount);
    }
  }
}

// End ExplicitReturnTypeInference.java
