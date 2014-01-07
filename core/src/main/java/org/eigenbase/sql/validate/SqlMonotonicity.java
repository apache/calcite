/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.validate;

/**
 * Enumeration of types of monotonicity.
 */
public enum SqlMonotonicity {
  StrictlyIncreasing,
  Increasing,
  StrictlyDecreasing,
  Decreasing,
  Constant,
  /**
   * Catch-all value for expressions that have some monotonic properties.
   * Maybe it isn't known whether the expression is increasing or decreasing;
   * or maybe the value is neither increasing nor decreasing but the value
   * never repeats.
   */
  Monotonic,
  NotMonotonic;

  /**
   * If this is a strict monotonicity (StrictlyIncreasing, StrictlyDecreasing)
   * returns the non-strict equivalent (Increasing, Decreasing).
   *
   * @return non-strict equivalent monotonicity
   */
  public SqlMonotonicity unstrict() {
    switch (this) {
    case StrictlyIncreasing:
      return Increasing;
    case StrictlyDecreasing:
      return Decreasing;
    default:
      return this;
    }
  }

  /**
   * Returns the reverse monotonicity.
   *
   * @return reverse monotonicity
   */
  public SqlMonotonicity reverse() {
    switch (this) {
    case StrictlyIncreasing:
      return StrictlyDecreasing;
    case Increasing:
      return Decreasing;
    case StrictlyDecreasing:
      return StrictlyIncreasing;
    case Decreasing:
      return Increasing;
    default:
      return this;
    }
  }

  /**
   * Whether values of this monotonicity are decreasing. That is, if a value
   * at a given point in a sequence is X, no point later in the sequence will
   * have a value greater than X.
   *
   * @return whether values are decreasing
   */
  public boolean isDecreasing() {
    switch (this) {
    case StrictlyDecreasing:
    case Decreasing:
      return true;
    default:
      return false;
    }
  }

  /**
   * Returns whether values of this monotonicity may ever repeat after moving
   * to another value: true for {@link #NotMonotonic} and {@link #Constant},
   * false otherwise.
   *
   * <p>If a column is known not to repeat, a sort on that column can make
   * progress before all of the input has been seen.
   *
   * @return whether values repeat
   */
  public boolean mayRepeat() {
    switch (this) {
    case NotMonotonic:
    case Constant:
      return true;
    default:
      return false;
    }
  }
}

// End SqlMonotonicity.java
