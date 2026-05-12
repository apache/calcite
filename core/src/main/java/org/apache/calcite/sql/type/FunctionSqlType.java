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
import org.apache.calcite.util.Comment;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Function type.
 * The type of lambda expression can be represented by a function type.
 */
public class FunctionSqlType extends AbstractSqlType {
  private final RelDataType parameterType;
  private final RelDataType returnType;

  public FunctionSqlType(
      RelDataType parameterType, RelDataType returnType) {
    this(parameterType, returnType, new HashSet<>());
  }

  public FunctionSqlType(
      RelDataType parameterType, RelDataType returnType, Set<Comment> comments) {
    super(SqlTypeName.FUNCTION, true, null, comments);
    this.parameterType = Objects.requireNonNull(parameterType, "parameterType");
    this.returnType = Objects.requireNonNull(returnType, "returnType");
    computeDigest();
  }

  @Override public FunctionSqlType copy(Set<Comment> comments) {
    return new FunctionSqlType(parameterType, returnType, comments);
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("Function");
    sb.append("(");
    for (Ord<RelDataTypeField> ord : Ord.zip(parameterType.getFieldList())) {
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

  public RelDataType getReturnType() {
    return returnType;
  }
}
