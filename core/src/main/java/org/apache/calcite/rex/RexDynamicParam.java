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

import java.util.Objects;

/**
 * Dynamic parameter reference in a row-expression.
 */
public class RexDynamicParam extends RexVariable {
  //~ Instance fields --------------------------------------------------------

  private final int index;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a dynamic parameter.
   *
   * @param type  inferred type of parameter
   * @param index 0-based index of dynamic parameter in statement
   */
  public RexDynamicParam(
      RelDataType type,
      int index) {
    super("?" + index, type);
    this.index = index;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlKind getKind() {
    return SqlKind.DYNAMIC_PARAM;
  }

  public int getIndex() {
    return index;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitDynamicParam(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitDynamicParam(this, arg);
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RexDynamicParam
        && digest.equals(((RexDynamicParam) obj).digest)
        && type.equals(((RexDynamicParam) obj).type)
        && index == ((RexDynamicParam) obj).index;
  }

  @Override public int hashCode() {
    return Objects.hash(digest, type, index);
  }
}

// End RexDynamicParam.java
