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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import org.apiguardian.api.API;

import java.util.Collection;

/**
 * Row expression.
 *
 * <p>Every row-expression has a type.
 * (Compare with {@link org.apache.calcite.sql.SqlNode}, which is created before
 * validation, and therefore types may not be available.)
 *
 * <p>Some common row-expressions are: {@link RexLiteral} (constant value),
 * {@link RexVariable} (variable), {@link RexCall} (call to operator with
 * operands). Expressions are generally created using a {@link RexBuilder}
 * factory.</p>
 *
 * <p>All sub-classes of RexNode are immutable.</p>
 */
public abstract class RexNode {

  /**
   * The digest of {@code RexNode} is normalized by default, however, sometimes a non-normalized
   * representation is required.
   * This method enables subclasses to identify if normalization is required.
   * @return true if the digest needs to be normalized
   */
  @API(since = "1.22", status = API.Status.EXPERIMENTAL)
  protected static boolean needNormalize() {
    return CalciteSystemProperty.ENABLE_REX_DIGEST_NORMALIZE.value();
  }

  //~ Instance fields --------------------------------------------------------

  // Effectively final. Set in each sub-class constructor, and never re-set.
  protected String digest;

  /**
   * Cache of hash code.
   */
  protected int hash = 0;

  //~ Methods ----------------------------------------------------------------

  public abstract RelDataType getType();

  /**
   * Returns whether this expression always returns true. (Such as if this
   * expression is equal to the literal <code>TRUE</code>.)
   */
  public boolean isAlwaysTrue() {
    return false;
  }

  /**
   * Returns whether this expression always returns false. (Such as if this
   * expression is equal to the literal <code>FALSE</code>.)
   */
  public boolean isAlwaysFalse() {
    return false;
  }

  public boolean isA(SqlKind kind) {
    return getKind() == kind;
  }

  public boolean isA(Collection<SqlKind> kinds) {
    return getKind().belongsTo(kinds);
  }

  /**
   * Returns the kind of node this is.
   *
   * @return Node kind, never null
   */
  public SqlKind getKind() {
    return SqlKind.OTHER;
  }

  public String toString() {
    return digest;
  }

  /** Returns the number of nodes in this expression.
   *
   * <p>Leaf nodes, such as {@link RexInputRef} or {@link RexLiteral}, have
   * a count of 1. Calls have a count of 1 plus the sum of their operands.
   *
   * <p>Node count is a measure of expression complexity that is used by some
   * planner rules to prevent deeply nested expressions.
   */
  public int nodeCount() {
    return 1;
  }

  /**
   * Accepts a visitor, dispatching to the right overloaded
   * {@link RexVisitor#visitInputRef visitXxx} method.
   *
   * <p>Also see {@link RexUtil#apply(RexVisitor, java.util.List, RexNode)},
   * which applies a visitor to several expressions simultaneously.
   */
  public abstract <R> R accept(RexVisitor<R> visitor);

  /**
   * Accepts a visitor with a payload, dispatching to the right overloaded
   * {@link RexBiVisitor#visitInputRef(RexInputRef, Object)} visitXxx} method.
   */
  public abstract <R, P> R accept(RexBiVisitor<R, P> visitor, P arg);

  /** {@inheritDoc}
   *
   * <p>Every node must implement {@link #equals} based on its content
   */
  @Override public abstract boolean equals(Object obj);

  /** {@inheritDoc}
   *
   * <p>Every node must implement {@link #hashCode} consistent with
   * {@link #equals}
   */
  @Override public abstract int hashCode();
}
