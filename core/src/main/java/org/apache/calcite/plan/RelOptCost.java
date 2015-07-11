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
package org.apache.calcite.plan;

/**
 * RelOptCost defines an interface for optimizer cost in terms of number of rows
 * processed, CPU cost, and I/O cost. Optimizer implementations may use all of
 * this information, or selectively ignore portions of it. The specific units
 * for all of these quantities are rather vague; most relational expressions
 * provide a default cost calculation, but optimizers can override this by
 * plugging in their own cost models with well-defined meanings for each unit.
 * Optimizers which supply their own cost models may also extend this interface
 * with additional cost metrics such as memory usage.
 */
public interface RelOptCost {
  //~ Methods ----------------------------------------------------------------

  /**
   * @return number of rows processed; this should not be confused with the
   * row count produced by a relational expression
   * ({@link org.apache.calcite.rel.RelNode#estimateRowCount})
   */
  double getRows();

  /**
   * @return usage of CPU resources
   */
  double getCpu();

  /**
   * @return usage of I/O resources
   */
  double getIo();

  /**
   * @return true iff this cost represents an expression that hasn't actually
   * been implemented (e.g. a pure relational algebra expression) or can't
   * actually be implemented, e.g. a transfer of data between two disconnected
   * sites
   */
  boolean isInfinite();

  // REVIEW jvs 3-Apr-2006:  we should standardize this
  // to Comparator/equals/hashCode

  /**
   * Compares this to another cost.
   *
   * @param cost another cost
   * @return true iff this is exactly equal to other cost
   */
  boolean equals(RelOptCost cost);

  /**
   * Compares this to another cost, allowing for slight roundoff errors.
   *
   * @param cost another cost
   * @return true iff this is the same as the other cost within a roundoff
   * margin of error
   */
  boolean isEqWithEpsilon(RelOptCost cost);

  /**
   * Compares this to another cost.
   *
   * @param cost another cost
   * @return true iff this is less than or equal to other cost
   */
  boolean isLe(RelOptCost cost);

  /**
   * Compares this to another cost.
   *
   * @param cost another cost
   * @return true iff this is strictly less than other cost
   */
  boolean isLt(RelOptCost cost);

  /**
   * Adds another cost to this.
   *
   * @param cost another cost
   * @return sum of this and other cost
   */
  RelOptCost plus(RelOptCost cost);

  /**
   * Subtracts another cost from this.
   *
   * @param cost another cost
   * @return difference between this and other cost
   */
  RelOptCost minus(RelOptCost cost);

  /**
   * Multiplies this cost by a scalar factor.
   *
   * @param factor scalar factor
   * @return scalar product of this and factor
   */
  RelOptCost multiplyBy(double factor);

  /**
   * Computes the ratio between this cost and another cost.
   *
   * <p>divideBy is the inverse of {@link #multiplyBy(double)}. For any
   * finite, non-zero cost and factor f, <code>
   * cost.divideBy(cost.multiplyBy(f))</code> yields <code>1 / f</code>.
   *
   * @param cost Other cost
   * @return Ratio between costs
   */
  double divideBy(RelOptCost cost);

  /**
   * Forces implementations to override {@link Object#toString} and provide a
   * good cost rendering to use during tracing.
   */
  String toString();
}

// End RelOptCost.java
