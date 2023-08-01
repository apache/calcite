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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The inferred foreign and unique table and column.
 * Within the same level of {@link org.apache.calcite.rel.RelNode},
 * confirmed field is marked as ture when they are confirmed to be
 * foreign key and unique key constraint.
 * The propagation and confirmation are performed from bottom to top.
 *
 * <p>InferredConstraintKey#isNull() is true when the inferredConstraintKey
 * is at the source side in RelOptForeignKey#constraints as it is inferred unique key.
 *
 * @see org.apache.calcite.plan.RelOptForeignKey
 * @see org.apache.calcite.plan.RelOptForeignKey.ShiftSide
 */
public class InferredConstraintKey {

  private final @Nullable List<String> qualifiedName;
  private final @Nullable Integer index;
  /**
   * After join, this confirmed field become true when the constraints in
   * {@link org.apache.calcite.plan.RelOptForeignKey} contains the corresponding
   * foreign InferredConstraintKey and unique InferredConstraintKey.
   */
  private final boolean confirmed;
  private final String digest;

  private InferredConstraintKey(@Nullable List<String> qualifiedName,
      @Nullable Integer index,
      boolean confirmed) {
    this.qualifiedName = qualifiedName;
    this.index = index;
    this.confirmed = confirmed;
    this.digest = qualifiedName + ".$" + index + ".#" + confirmed;
  }

  public @Nullable List<String> getQualifiedName() {
    return this.qualifiedName;
  }

  public @Nullable Integer getIndex() {
    return this.index;
  }

  public boolean isConfirmed() {
    return confirmed;
  }

  public static InferredConstraintKey of() {
    return new InferredConstraintKey(null, null, false);
  }

  public static InferredConstraintKey of(@Nullable List<String> tableName,
      @Nullable Integer index, boolean confirmed) {
    return new InferredConstraintKey(tableName, index, confirmed);
  }

  public boolean isNull() {
    return qualifiedName == null && index == null;
  }

  public InferredConstraintKey copy(boolean confirmed) {
    return InferredConstraintKey.of(
        this.qualifiedName == null ? null : new ArrayList<>(this.qualifiedName),
        this.index,
        confirmed);
  }

  public InferredConstraintKey copy() {
    return InferredConstraintKey.of(
        this.qualifiedName == null ? null : new ArrayList<>(qualifiedName),
        this.index,
        this.confirmed);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    InferredConstraintKey that = (InferredConstraintKey) obj;
    return Objects.equals(qualifiedName, that.qualifiedName)
        && Objects.equals(index, that.index)
        && this.confirmed == that.confirmed;
  }

  @Override public int hashCode() {
    return Objects.hash(digest);
  }

  @Override public String toString() {
    return digest;
  }
}
