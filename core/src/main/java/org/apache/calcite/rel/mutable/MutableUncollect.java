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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Uncollect}. */
public class MutableUncollect extends MutableSingleRel {
  public final boolean withOrdinality;
  public final ImmutableBitSet passthroughFieldIndices;
  public final ImmutableBitSet collectionFieldIndices;
  public final boolean isOuter;
  public final boolean expandStructFields;

  private MutableUncollect(RelDataType rowType,
      MutableRel input, boolean withOrdinality,
      ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean isOuter,
      boolean expandStructFields) {
    super(MutableRelType.UNCOLLECT, rowType, input);
    this.withOrdinality = withOrdinality;
    this.passthroughFieldIndices = passthroughFieldIndices;
    this.collectionFieldIndices = collectionFieldIndices;
    this.isOuter = isOuter;
    this.expandStructFields = expandStructFields;
  }

  /**
   * Creates a MutableUncollect that unnests every input field.
   *
   * @param rowType         Row type
   * @param input           Input relational expression
   * @param withOrdinality  Whether the output contains an extra
   *                        {@code ORDINALITY} column
   */
  public static MutableUncollect of(RelDataType rowType,
      MutableRel input, boolean withOrdinality) {
    return of(rowType, input, withOrdinality, ImmutableBitSet.of(),
        ImmutableBitSet.range(input.rowType.getFieldCount()), false, true);
  }

  /**
   * Creates a MutableUncollect.
   *
   * @param rowType                 Row type
   * @param input                   Input relational expression
   * @param withOrdinality          Whether the output contains an extra
   *                                {@code ORDINALITY} column
   * @param passthroughFieldIndices 0-based indices of the input fields to pass
   *                                through unchanged
   * @param collectionFieldIndices  0-based indices of the input fields whose
   *                                values are collections to unnest
   * @param isOuter                 If true, preserves input rows with
   *                                null/empty collections (LEFT JOIN); if
   *                                false, drops them (INNER)
   * @param expandStructFields      If true, a collection whose element type
   *                                is a struct produces one output column per
   *                                struct field; if false, a single column
   *                                typed as the whole element
   */
  public static MutableUncollect of(RelDataType rowType,
      MutableRel input, boolean withOrdinality,
      ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean isOuter,
      boolean expandStructFields) {
    return new MutableUncollect(rowType, input, withOrdinality,
        passthroughFieldIndices, collectionFieldIndices, isOuter,
        expandStructFields);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof MutableUncollect
        && withOrdinality == ((MutableUncollect) obj).withOrdinality
        && passthroughFieldIndices.equals(
            ((MutableUncollect) obj).passthroughFieldIndices)
        && collectionFieldIndices.equals(
            ((MutableUncollect) obj).collectionFieldIndices)
        && isOuter == ((MutableUncollect) obj).isOuter
        && expandStructFields == ((MutableUncollect) obj).expandStructFields
        && input.equals(((MutableUncollect) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, withOrdinality, passthroughFieldIndices,
        collectionFieldIndices, isOuter, expandStructFields);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Uncollect(withOrdinality: ").append(withOrdinality)
        .append(", passthrough: ").append(passthroughFieldIndices)
        .append(", collectionFields: ").append(collectionFieldIndices)
        .append(", isOuter: ").append(isOuter)
        .append(", expandStructFields: ").append(expandStructFields)
        .append(")");
  }

  @Override public MutableRel clone() {
    return MutableUncollect.of(rowType, input.clone(), withOrdinality,
        passthroughFieldIndices, collectionFieldIndices, isOuter,
        expandStructFields);
  }
}
