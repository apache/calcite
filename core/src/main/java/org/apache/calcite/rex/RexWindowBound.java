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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;

/**
 * Abstracts "XX PRECEDING/FOLLOWING" and "CURRENT ROW" bounds for windowed
 * aggregates.
 */
public abstract class RexWindowBound {

  /**
   * Creates window bound.
   * @param node SqlNode of the bound
   * @param rexNode offset value when bound is not UNBOUNDED/CURRENT ROW
   * @return window bound
   */
  public static RexWindowBound create(SqlNode node, RexNode rexNode) {
    if (SqlWindow.isUnboundedPreceding(node)
        || SqlWindow.isUnboundedFollowing(node)) {
      return new RexWindowBoundUnbounded(node);
    }
    if (SqlWindow.isCurrentRow(node)) {
      return new RexWindowBoundCurrentRow();
    }
    return new RexWindowBoundBounded(rexNode);
  }

  /**
   * Returns if the bound is unbounded.
   * @return if the bound is unbounded
   */
  public boolean isUnbounded() {
    return false;
  }

  /**
   * Returns if the bound is PRECEDING.
   * @return if the bound is PRECEDING
   */
  public boolean isPreceding() {
    return false;
  }

  /**
   * Returns if the bound is FOLLOWING.
   * @return if the bound is FOLLOWING
   */
  public boolean isFollowing() {
    return false;
  }

  /**
   * Returns if the bound is CURRENT ROW.
   * @return if the bound is CURRENT ROW
   */
  public boolean isCurrentRow() {
    return false;
  }

  /**
   * Returns offset from XX PRECEDING/FOLLOWING.
   *
   * @return offset from XX PRECEDING/FOLLOWING
   */
  public RexNode getOffset() {
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
   * @param visitor visitor to accept
   * @param <R> return type of the visitor
   * @return transformed bound
   */
  public <R> RexWindowBound accept(RexVisitor<R> visitor) {
    return this;
  }

  /**
   * Implements UNBOUNDED PRECEDING/FOLLOWING bound.
   */
  private static class RexWindowBoundUnbounded extends RexWindowBound {
    private final SqlNode node;

    RexWindowBoundUnbounded(SqlNode node) {
      this.node = node;
    }

    @Override public boolean isUnbounded() {
      return true;
    }

    @Override public boolean isPreceding() {
      return SqlWindow.isUnboundedPreceding(node);
    }

    @Override public boolean isFollowing() {
      return SqlWindow.isUnboundedFollowing(node);
    }

    @Override public String toString() {
      return ((SqlLiteral) node).getValue().toString();
    }

    @Override public int getOrderKey() {
      return isPreceding() ? 0 : 2;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RexWindowBoundUnbounded that = (RexWindowBoundUnbounded) o;

      if (!node.equals(that.node)) {
        return false;
      }

      return true;
    }

    @Override public int hashCode() {
      return node.hashCode();
    }
  }

  /**
   * Implements CURRENT ROW bound.
   */
  private static class RexWindowBoundCurrentRow extends RexWindowBound {
    @Override public boolean isCurrentRow() {
      return true;
    }

    @Override public String toString() {
      return "CURRENT ROW";
    }

    @Override public int getOrderKey() {
      return 1;
    }

    @Override public boolean equals(Object obj) {
      return getClass() == obj.getClass();
    }

    @Override public int hashCode() {
      return 123;
    }
  }

  /**
   * Implements XX PRECEDING/FOLLOWING bound where XX is not UNBOUNDED.
   */
  private static class RexWindowBoundBounded extends RexWindowBound {
    private final SqlKind sqlKind;
    private final RexNode offset;

    RexWindowBoundBounded(RexNode node) {
      assert node instanceof RexCall
          : "RexWindowBoundBounded window bound should be either 'X preceding'"
          + " or 'X following' call. Actual type is " + node;
      RexCall call = (RexCall) node;
      this.offset = call.getOperands().get(0);
      this.sqlKind = call.getKind();
      assert this.offset != null
          : "RexWindowBoundBounded offset should not be null";
    }

    private RexWindowBoundBounded(SqlKind sqlKind, RexNode offset) {
      this.sqlKind = sqlKind;
      this.offset = offset;
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

    @Override public <R> RexWindowBound accept(RexVisitor<R> visitor) {
      R r = offset.accept(visitor);
      if (r instanceof RexNode && r != offset) {
        return new RexWindowBoundBounded(sqlKind, (RexNode) r);
      }
      return this;
    }

    @Override public String toString() {
      return offset.toString() + " " + sqlKind.toString();
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RexWindowBoundBounded that = (RexWindowBoundBounded) o;

      if (!offset.equals(that.offset)) {
        return false;
      }
      if (sqlKind != that.sqlKind) {
        return false;
      }

      return true;
    }

    @Override public int hashCode() {
      int result = sqlKind.hashCode();
      result = 31 * result + offset.hashCode();
      return result;
    }
  }
}

// End RexWindowBound.java
