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
 * The super class for concrete {@link RelOptCost} classes.
 * Please note that it concentrates the comparison logic into a single method
 * to avoid the problems of duplicate/contradicting logic.
 * So client code is supposed to extend this class, rather than directly implementing
 * the {@link RelOptCost} interface.
 */
public abstract class AbstractRelOptCost implements RelOptCost {

  /**
   * This is used to represent the result of comparing two {@link RelOptCost} objects.
   * Please note that a set of {@link RelOptCost} objects forms a partial order,
   * so there can be objects that cannot be compared. We use UD to represent
   * the results of such comparisons.
   */
  public enum ComparisonResult {
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

  /**
   * Compare two {@link RelOptCost} objects.
   * @param cost the other cost to compare.
   * @return the comparison result.
   */
  public abstract ComparisonResult compareCost(RelOptCost cost);

  /**
   * Compares this to another cost.
   * This is based on the implementation of method {@link #compareCost(RelOptCost)},
   *  and is not intended to be overridden.
   *
   * @param cost another cost
   * @return true iff this is less than or equal to other cost
   */
  @Override public final boolean isLe(RelOptCost cost) {
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
  @Override public final boolean isLt(RelOptCost cost) {
    return compareCost(cost) == ComparisonResult.LT;
  }

  /**
   * Compares this to another cost.
   * This is based on the implementation of  method {@link #compareCost(RelOptCost)},
   * and is not intended to be overridden.
   *
   * @param cost another cost
   * @return true iff this is exactly equal to other cost
   */
  @Override public final boolean equals(RelOptCost cost) {
    return compareCost(cost) == ComparisonResult.EQ;
  }
}
