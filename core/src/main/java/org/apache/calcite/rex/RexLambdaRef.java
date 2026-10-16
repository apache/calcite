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

import org.jspecify.annotations.Nullable;

import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Variable that references a field of a lambda expression.
 */
public class RexLambdaRef extends RexSlot {

  public RexLambdaRef(int index, String name, RelDataType type) {
    super(name, index, type);
  }

  @Override public SqlKind getKind() {
    return SqlKind.LAMBDA_REF;
  }

  @Override public <R extends @Nullable Object> R accept(RexVisitor<R> visitor) {
    return visitor.visitLambdaRef(this);
  }

  @Override public <R extends @Nullable Object, P extends @Nullable Object> R accept(
      RexBiVisitor<R, P> visitor, P arg) {
    // a lambda reference has no payload to hand a bi-visitor
    return castNonNull(null);
  }

  @Override public boolean equals(final @Nullable Object obj) {
    return this == obj
        || obj instanceof RexLambdaRef
        && index == ((RexLambdaRef) obj).index
        && type.equals(((RexLambdaRef) obj).type);
  }

  @Override public int hashCode() {
    return Objects.hash(type, index);
  }
}
