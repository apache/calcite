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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Helpers for {@link RexWindowBound}.
 */
public final class RexWindowBounds {
  /** UNBOUNDED PRECEDING. */
  public static final RexWindowBound UNBOUNDED_PRECEDING =
      new RexUnboundedWindowBound(true);

  /** UNBOUNDED FOLLOWING. */
  public static final RexWindowBound UNBOUNDED_FOLLOWING =
      new RexUnboundedWindowBound(false);

  /** CURRENT ROW. */
  public static final RexWindowBound CURRENT_ROW =
      new RexCurrentRowWindowBound();

  private RexWindowBounds() {
  }

  /**
   * Creates a window bound from a {@link SqlNode}.
   *
   * @param node SqlNode of the bound
   * @param rexNode offset value when bound is not UNBOUNDED/CURRENT ROW
   * @return window bound
   */
  public static RexWindowBound create(SqlNode node, @Nullable RexNode rexNode) {
    if (SqlWindow.isUnboundedPreceding(node)) {
      return UNBOUNDED_PRECEDING;
    }
    if (SqlWindow.isUnboundedFollowing(node)) {
      return UNBOUNDED_FOLLOWING;
    }
    if (SqlWindow.isCurrentRow(node)) {
      return CURRENT_ROW;
    }
    requireNonNull(rexNode, "offset value cannot be null for bounded window");
    return new RexBoundedWindowBound((RexCall) rexNode);
  }

  public static RexWindowBound following(RexNode offset) {
    return new RexBoundedWindowBound(
        new RexCall(offset.getType(),
            SqlWindow.FOLLOWING_OPERATOR, ImmutableList.of(offset)));
  }

  public static RexWindowBound preceding(RexNode offset) {
    return new RexBoundedWindowBound(
        new RexCall(offset.getType(),
            SqlWindow.PRECEDING_OPERATOR, ImmutableList.of(offset)));
  }

  /**
   * Implements UNBOUNDED PRECEDING/FOLLOWING bound.
   */
  private static class RexUnboundedWindowBound extends RexWindowBound {
    private final boolean preceding;

    RexUnboundedWindowBound(boolean preceding) {
      this.preceding = preceding;
    }

    @Override public boolean isUnbounded() {
      return true;
    }

    @Override public boolean isPreceding() {
      return preceding;
    }

    @Override public boolean isFollowing() {
      return !preceding;
    }

    @Override public String toString() {
      return preceding ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
    }

    @Override public int getOrderKey() {
      return preceding ? 0 : 2;
    }

    @Override public boolean equals(@Nullable Object o) {
      return this == o
          || o instanceof RexUnboundedWindowBound
          && preceding == ((RexUnboundedWindowBound) o).preceding;
    }

    @Override public int hashCode() {
      return preceding ? 1357 : 1358;
    }
  }

  /**
   * Implements CURRENT ROW bound.
   */
  private static class RexCurrentRowWindowBound extends RexWindowBound {
    @Override public boolean isCurrentRow() {
      return true;
    }

    @Override public String toString() {
      return "CURRENT ROW";
    }

    @Override public int getOrderKey() {
      return 1;
    }

    @Override public boolean equals(@Nullable Object o) {
      return o instanceof RexCurrentRowWindowBound;
    }

    @Override public int hashCode() {
      return 123;
    }
  }

  /**
   * Implements XX PRECEDING/FOLLOWING bound where XX is not UNBOUNDED.
   */
  private static class RexBoundedWindowBound extends RexWindowBound {
    private final SqlKind sqlKind;
    private final RexNode offset;

    RexBoundedWindowBound(RexCall node) {
      this(node.getKind(), node.operands.get(0));
    }

    private RexBoundedWindowBound(SqlKind sqlKind, RexNode offset) {
      this.sqlKind = requireNonNull(sqlKind, "sqlKind");
      this.offset = requireNonNull(offset, "offset");
    }

    @Override public boolean isPreceding() {
      return sqlKind == SqlKind.PRECEDING;
    }

    @Override public boolean isFollowing() {
      return sqlKind == SqlKind.FOLLOWING;
    }

    @Override public RexNode getOffset() {
      return offset;
    }

    @Override public int nodeCount() {
      return super.nodeCount() + offset.nodeCount();
    }

    @Override public <R> RexWindowBound accept(RexVisitor<R> visitor) {
      R r = offset.accept(visitor);
      if (r instanceof RexNode && r != offset) {
        return new RexBoundedWindowBound(sqlKind, (RexNode) r);
      }
      return this;
    }

    @Override public String toString() {
      return offset + " " + sqlKind;
    }

    @Override public boolean equals(@Nullable Object o) {
      return this == o
          || o instanceof RexBoundedWindowBound
          && offset.equals(((RexBoundedWindowBound) o).offset)
          && sqlKind == ((RexBoundedWindowBound) o).sqlKind;
    }

    @Override public int hashCode() {
      return Objects.hash(sqlKind, offset);
    }
  }
}
