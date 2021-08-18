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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Dynamic parameter reference in a row-expression.
 */
public class RexNamedParam extends RexVariable {
  //~ Instance fields --------------------------------------------------------

  private final String name;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a named parameter.
   *
   * @param type  type of parameter
   * @param name Name of the Python variable
   */
  public RexNamedParam(
      RelDataType type,
      String name) {
    super("?" + name, type);
    this.name = name;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.NAMED_PARAM;
  }

  public String getName() {
    return name;
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitNamedParam(this);
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitNamedParam(this, arg);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof RexNamedParam
        && type.equals(((RexNamedParam) obj).type)
        && name.equals(((RexNamedParam) obj).name);
  }

  @Override public int hashCode() {
    return Objects.hash(type, name);
  }
}
