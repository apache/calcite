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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;

import static java.util.Objects.requireNonNull;

/**
 * Function type.
 *
 * <p>The type of lambda expression can be represented by a function type.
 */
public class FunctionSqlType extends AbstractSqlType {
  private final RelDataType parameterTypes;
  private final RelDataType returnType;

  /**
   * Constructs a new function SQL type. This should only be called from a factory method.
   *
   * @param parameterTypes a struct type wrapping function's input parameter types.
   * @param returnType function's return type.
   */
  public FunctionSqlType(
      RelDataType parameterTypes, RelDataType returnType) {
    super(SqlTypeName.FUNCTION, true, null);
    this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes");
    if (!parameterTypes.isStruct()) {
      throw new IllegalArgumentException("ParameterType must be a struct");
    }
    this.returnType = requireNonNull(returnType, "returnType");
    computeDigest();
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("Function");
    sb.append("(");
    for (Ord<RelDataTypeField> ord : Ord.zip(parameterTypes.getFieldList())) {
      if (ord.i > 0) {
        sb.append(", ");
      }
      RelDataTypeField field = ord.e;
      sb.append(withDetail ? field.getType().getFullTypeString() : field.getType().toString());
    }
    sb.append(")");
    sb.append(" -> ");
    sb.append(withDetail ? returnType.getFullTypeString() : returnType.toString());
  }

  @Override public RelDataTypeFamily getFamily() {
    return this;
  }

  /**
   * Returns the parameter types of the function.
   *
   * @return a struct wrapping function's parameter types.
   */
  public RelDataType getParameterTypes() {
    return parameterTypes;
  }

  /**
   * Returns the return type of the function.
   *
   * @return the function's return type.
   */
  public RelDataType getReturnType() {
    return returnType;
  }
}
