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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Call to an aggregate function over a window.
 */
public class RexOver extends RexCall {
  private static final Finder FINDER = new Finder();

  //~ Instance fields --------------------------------------------------------

  private final RexWindow window;
  private final boolean distinct;
  private final boolean ignoreNulls;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RexOver.
   *
   * <p>For example, "SUM(DISTINCT x) OVER (ROWS 3 PRECEDING)" is represented
   * as:
   *
   * <ul>
   * <li>type = Integer,
   * <li>op = {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#SUM},
   * <li>operands = { {@link RexFieldAccess}("x") }
   * <li>window = {@link SqlWindow}(ROWS 3 PRECEDING)
   * </ul>
   *
   * @param type     Result type
   * @param op       Aggregate operator
   * @param operands Operands list
   * @param window   Window specification
   * @param distinct Aggregate operator is applied on distinct elements
   */
  RexOver(
      RelDataType type,
      SqlAggFunction op,
      List<RexNode> operands,
      RexWindow window,
      boolean distinct,
      boolean ignoreNulls) {
    super(type, op, operands);
    Preconditions.checkArgument(op.isAggregator());
    this.window = Objects.requireNonNull(window);
    this.distinct = distinct;
    this.ignoreNulls = ignoreNulls;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the aggregate operator for this expression.
   */
  public SqlAggFunction getAggOperator() {
    return (SqlAggFunction) getOperator();
  }

  public RexWindow getWindow() {
    return window;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public boolean ignoreNulls() {
    return ignoreNulls;
  }

  @Override protected @Nonnull String computeDigest(boolean withType) {
    final StringBuilder sb = new StringBuilder(op.getName());
    sb.append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    appendOperands(sb);
    sb.append(")");
    if (ignoreNulls) {
      sb.append(" IGNORE NULLS");
    }
    if (withType) {
      sb.append(":");
      sb.append(type.getFullTypeString());
    }
    sb.append(" OVER (")
        .append(window)
        .append(")");
    return sb.toString();
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitOver(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitOver(this, arg);
  }

  /**
   * Returns whether an expression contains an OVER clause.
   */
  public static boolean containsOver(RexNode expr) {
    try {
      expr.accept(FINDER);
      return false;
    } catch (OverFound e) {
      Util.swallow(e, null);
      return true;
    }
  }

  /**
   * Returns whether a program contains an OVER clause.
   */
  public static boolean containsOver(RexProgram program) {
    try {
      RexUtil.apply(FINDER, program.getExprList(), null);
      return false;
    } catch (OverFound e) {
      Util.swallow(e, null);
      return true;
    }
  }

  /**
   * Returns whether an expression list contains an OVER clause.
   */
  public static boolean containsOver(List<RexNode> exprs, RexNode condition) {
    try {
      RexUtil.apply(FINDER, exprs, condition);
      return false;
    } catch (OverFound e) {
      Util.swallow(e, null);
      return true;
    }
  }

  @Override public RexCall clone(RelDataType type, List<RexNode> operands) {
    throw new UnsupportedOperationException();
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Exception thrown when an OVER is found. */
  private static class OverFound extends ControlFlowException {
    public static final OverFound INSTANCE = new OverFound();
  }

  /**
   * Visitor which detects a {@link RexOver} inside a {@link RexNode}
   * expression.
   *
   * <p>It is re-entrant (two threads can use an instance at the same time)
   * and it can be re-used for multiple visits.
   */
  private static class Finder extends RexVisitorImpl<Void> {
    Finder() {
      super(true);
    }

    public Void visitOver(RexOver over) {
      throw OverFound.INSTANCE;
    }
  }
}

// End RexOver.java
