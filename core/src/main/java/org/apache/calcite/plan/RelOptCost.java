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

  /**
   * This is used to represent the result of comparing two {@link RelOptCost} objects.
   * Please note that a set of {@link RelOptCost} objects forms a partial order,
   * so there can be objects that cannot be compared. We use UD to represent
   * the results of such comparisons.
   */
  enum ComparisonResult {
    /**
     * Less than.
     */
    LT,

    /**
     * Equal to.
     */
    EQ,

    /**
     * Greater than.
     */
    GT,

    /**
     * Undefined.
     */
    UD;

    /**
     * Converts the result of a signum function to an enum.
     */
    public static ComparisonResult signumToEnum(int signum) {
      if (signum > 0) {
        return GT;
      } else if (signum < 0) {
        return LT;
      } else {
        // signum == 0
        return EQ;
      }
    }
  }

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
   * Compare two {@link RelOptCost} objects.
   * @param cost the other cost to compare.
   * @return the comparison result.
   */
  default ComparisonResult compareCost(RelOptCost cost) {
    return ComparisonResult.UD;
  }

  /**
   * Compares this to another cost.
   * This is based on the implementation of  method {@link #compareCost(RelOptCost)},
   * and is not intended to be overridden.
   *
   * @param cost another cost
   * @return true iff this is exactly equal to other cost
   */
  default boolean equals(RelOptCost cost) {
    return compareCost(cost) == ComparisonResult.EQ;
  }

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
   * This is based on the implementation of method {@link #compareCost(RelOptCost)},
   *  and is not intended to be overridden.
   *
   * @param cost another cost
   * @return true iff this is less than or equal to other cost
   */
  default boolean isLe(RelOptCost cost) {
    ComparisonResult result = compareCost(cost);
    return result == ComparisonResult.LT || result == ComparisonResult.EQ;
  }

  /**
   * Compares this to another cost.
   * This is based on the implementation of method {@link #compareCost(RelOptCost)},
   * and is not intended to be overridden.
   *
   * @param cost another cost
   * @return true iff this is strictly less than other cost
   */
  default boolean isLt(RelOptCost cost) {
    return compareCost(cost) == ComparisonResult.LT;
  }

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
