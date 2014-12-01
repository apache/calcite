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

import java.util.List;

/**
 * Call to an aggregate function over a window.
 */
public class RexOver extends RexCall {
  private static final Finder FINDER = new Finder();

  //~ Instance fields --------------------------------------------------------

  private final RexWindow window;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RexOver.
   *
   * <p>For example, "SUM(x) OVER (ROWS 3 PRECEDING)" is represented as:
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
   * @pre op.isAggregator()
   * @pre window != null
   * @pre window.getRefName() == null
   */
  RexOver(
      RelDataType type,
      SqlAggFunction op,
      List<RexNode> operands,
      RexWindow window) {
    super(type, op, operands);
    assert op.isAggregator() : "precondition: op.isAggregator()";
    assert window != null : "precondition: window != null";
    this.window = window;
    this.digest = computeDigest(true);
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

  protected String computeDigest(boolean withType) {
    return super.computeDigest(withType) + " OVER (" + window + ")";
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitOver(this);
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
      RexProgram.apply(FINDER, program.getExprList(), null);
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
      RexProgram.apply(FINDER, exprs, condition);
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
    public Finder() {
      super(true);
    }

    public Void visitOver(RexOver over) {
      throw OverFound.INSTANCE;
    }
  }
}

// End RexOver.java
