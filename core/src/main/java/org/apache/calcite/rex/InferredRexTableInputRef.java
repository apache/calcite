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
 * The inferred foreign key and unique key relationships.
 * Within the same level of {@link org.apache.calcite.rel.RelNode},
 * confirmed field is marked as ture when they are confirmed to be
 * foreign key and unique key relationships.
 * The propagation and confirmation are performed from bottom to top.
 *
 * @see org.apache.calcite.plan.RelOptForeignKey
 */
public class InferredRexTableInputRef {

  private final @Nullable List<String> qualifiedName;
  private final @Nullable Integer index;
  private final boolean confirmed;
  private final String digest;

  private InferredRexTableInputRef(@Nullable List<String> qualifiedName,
      @Nullable Integer index,
      boolean confirmed) {
    this.qualifiedName = qualifiedName;
    this.index = index;
    this.confirmed = confirmed;
    this.digest = qualifiedName + ".$" + index;
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

  public static InferredRexTableInputRef of() {
    return new InferredRexTableInputRef(null, null, false);
  }

  public static InferredRexTableInputRef of(@Nullable List<String> tableName,
      @Nullable Integer index, boolean confirmed) {
    return new InferredRexTableInputRef(tableName, index, confirmed);
  }

  public boolean isNull() {
    return qualifiedName == null && index == null;
  }

  public InferredRexTableInputRef copy(boolean confirmed) {
    return InferredRexTableInputRef.of(
        this.qualifiedName == null ? null : new ArrayList<>(this.qualifiedName),
        this.index,
        confirmed);
  }

  public InferredRexTableInputRef copy() {
    return InferredRexTableInputRef.of(
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
    InferredRexTableInputRef that = (InferredRexTableInputRef) obj;
    return Objects.equals(qualifiedName, that.qualifiedName) && Objects.equals(index, that.index);
  }

  @Override public int hashCode() {
    return Objects.hash(digest);
  }

  @Override public String toString() {
    return digest;
  }
}
