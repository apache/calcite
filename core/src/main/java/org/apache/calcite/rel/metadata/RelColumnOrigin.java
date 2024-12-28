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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.CorrelationId;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * RelColumnOrigin is a data structure describing one of the origins of an
 * output column produced by a relational expression.
 */
public class RelColumnOrigin {
  //~ Instance fields --------------------------------------------------------

  private final @Nullable RelOptTable originTable;

  private final @Nullable CorrelationId correlationId;

  private final int iOriginColumn;

  private final boolean isCorVar;

  private final boolean isDerived;

  //~ Constructors -----------------------------------------------------------

  public RelColumnOrigin(
      RelOptTable originTable,
      int iOriginColumn,
      boolean isDerived) {
    this(originTable, null, iOriginColumn, isDerived, false);
  }

  public RelColumnOrigin(
      CorrelationId correlationId,
      int iOriginColumn,
      boolean isDerived) {
    this(null, correlationId, iOriginColumn, isDerived, true);
  }

  private RelColumnOrigin(@Nullable RelOptTable originTable,
      @Nullable CorrelationId correlationId,
      int iOriginColumn,
      boolean isDerived,
      boolean isCorVar) {
    this.originTable = originTable;
    this.correlationId = correlationId;
    this.iOriginColumn = iOriginColumn;
    this.isDerived = isDerived;
    this.isCorVar = isCorVar;
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns table of origin and null only if isCorVar is true. */
  public @Nullable RelOptTable getOriginTable() {
    return originTable;
  }

  /** Returns correlateId of origin and null only if isCorVar is true. */
  public @Nullable CorrelationId getCorrelationId() {
    return correlationId;
  }

  /** Returns the 0-based index of column in origin table; whether this ordinal
   * is flattened or unflattened depends on whether UDT flattening has already
   * been performed on the relational expression which produced this
   * description. */
  public int getOriginColumnOrdinal() {
    return iOriginColumn;
  }

  /**
   * Consider the query <code>select a+b as c, d as e from t</code>. The
   * output column c has two origins (a and b), both of them derived. The
   * output column e has one origin (d), which is not derived.
   *
   * @return false if value taken directly from column in origin table; true
   * otherwise
   */
  public boolean isDerived() {
    return isDerived;
  }

  /** Returns whether this columnOrigin is from an external Correlate field. */
  public boolean isCorVar() {
    return isCorVar;
  }

  public RelColumnOrigin copyWith(boolean isDerived) {
    if (isCorVar) {
      return new RelColumnOrigin(
          requireNonNull(correlationId, "correlationId"), iOriginColumn, isDerived);
    }
    return new RelColumnOrigin(
        requireNonNull(originTable, "originTable"), iOriginColumn, isDerived);
  }

  // override Object
  @Override public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof RelColumnOrigin)) {
      return false;
    }
    RelColumnOrigin other = (RelColumnOrigin) obj;

    if (isCorVar != other.isCorVar
        || iOriginColumn != other.iOriginColumn
        || isDerived != other.isDerived) {
      return false;
    }

    if (isCorVar) {
      return requireNonNull(correlationId, "correlationId")
          .equals(requireNonNull(other.getCorrelationId(), "other correlationId"));
    }
    return requireNonNull(originTable, "originTable").getQualifiedName()
        .equals(requireNonNull(other.getOriginTable(), "originTable").getQualifiedName());
  }

  // override Object
  @Override public int hashCode() {
    if (isCorVar) {
      return requireNonNull(correlationId, "correlationId").hashCode()
          + iOriginColumn + (isDerived ? 313 : 0);
    }
    return requireNonNull(originTable, "originTable").getQualifiedName().hashCode()
        + iOriginColumn + (isDerived ? 313 : 0);
  }
}
