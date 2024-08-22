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

import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Abstracts "XX PRECEDING/FOLLOWING" and "CURRENT ROW" bounds for windowed
 * aggregates.
 *
 * @see RexWindowBounds
 */
public abstract class RexWindowBound {

  /** Use {@link RexWindowBounds#create(SqlNode, RexNode)}. */
  @Deprecated // to be removed before 2.0
  public static RexWindowBound create(SqlNode node, RexNode rexNode) {
    return RexWindowBounds.create(node, rexNode);
  }

  /**
   * Returns if the bound is unbounded.
   *
   * @return if the bound is unbounded
   */
  @Pure
  @EnsuresNonNullIf(expression = "getOffset()", result = false)
  @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
  public boolean isUnbounded() {
    return false;
  }

  /** Returns whether the bound is {@code UNBOUNDED PRECEDING}. */
  public final boolean isUnboundedPreceding() {
    return isUnbounded() && isPreceding();
  }

  /** Returns whether the bound is {@code UNBOUNDED FOLLOWING}. */
  public final boolean isUnboundedFollowing() {
    return isUnbounded() && isFollowing();
  }

  /**
   * Returns if the bound is PRECEDING.
   *
   * @return if the bound is PRECEDING
   */
  public boolean isPreceding() {
    return false;
  }

  /**
   * Returns if the bound is FOLLOWING.
   *
   * @return if the bound is FOLLOWING
   */
  public boolean isFollowing() {
    return false;
  }

  /**
   * Returns if the bound is CURRENT ROW.
   *
   * @return if the bound is CURRENT ROW
   */
  @Pure
  @EnsuresNonNullIf(expression = "getOffset()", result = false)
  @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
  public boolean isCurrentRow() {
    return false;
  }

  /**
   * Returns offset from XX PRECEDING/FOLLOWING.
   *
   * @return offset from XX PRECEDING/FOLLOWING
   */
  @Pure
  public @Nullable RexNode getOffset() {
    return null;
  }

  /**
   * Returns relative sort offset when known at compile time.
   * For instance, UNBOUNDED PRECEDING is less than CURRENT ROW.
   *
   * @return relative order or -1 when order is not known
   */
  public int getOrderKey() {
    return -1;
  }

  /**
   * Transforms the bound via {@link org.apache.calcite.rex.RexVisitor}.
   *
   * @param visitor visitor to accept
   * @param <R> return type of the visitor
   * @return transformed bound
   */
  public <R> RexWindowBound accept(RexVisitor<R> visitor) {
    return this;
  }

  /**
   * Transforms the bound via {@link org.apache.calcite.rex.RexBiVisitor}.
   *
   * @param visitor visitor to accept
   * @param arg Payload
   * @param <R> return type of the visitor
   * @return transformed bound
   */
  public <R, P> RexWindowBound accept(RexBiVisitor<R, P> visitor, P arg) {
    return this;
  }

  /**
   * Returns the number of nodes in this bound.
   *
   * @see RexNode#nodeCount()
   */
  public int nodeCount() {
    return 1;
  }
}
