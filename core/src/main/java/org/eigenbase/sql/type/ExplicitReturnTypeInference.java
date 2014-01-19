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

/**
 * A {@link SqlReturnTypeInference} which always returns the same SQL type.
 */
public class ExplicitReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  protected final RelProtoDataType protoType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an inference rule which always returns the same type object.
   *
   * <p>If the requesting type factory is different, returns a copy of the
   * type object made using {@link RelDataTypeFactory#copyType(RelDataType)}
   * within the requesting type factory.
   *
   * <p>A copy of the type is required because each statement is prepared using
   * a different type factory; each type factory maintains its own cache of
   * canonical instances of each type.
   *
   * @param protoType Type object
   */
  public ExplicitReturnTypeInference(RelProtoDataType protoType) {
    assert protoType != null;
    this.protoType = protoType;
  }

  /**
   * Creates an inference rule which returns a copy of a given data type.
   */
  public static ExplicitReturnTypeInference of(RelDataType type) {
    return new ExplicitReturnTypeInference(RelDataTypeImpl.proto(type));
  }

  /**
   * Creates an inference rule which returns a type with no precision or scale,
   * such as {@code DATE}.
   */
  public static ExplicitReturnTypeInference of(SqlTypeName typeName) {
    return new ExplicitReturnTypeInference(RelDataTypeImpl.proto(typeName));
  }

  /**
   * Creates an inference rule which returns a type with precision but no scale,
   * such as {@code VARCHAR(100)}.
   */
  public static ExplicitReturnTypeInference of(SqlTypeName typeName,
      int precision) {
    return new ExplicitReturnTypeInference(
        RelDataTypeImpl.proto(typeName, precision));
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return protoType.apply(opBinding.getTypeFactory());
  }
}

// End ExplicitReturnTypeInference.java
