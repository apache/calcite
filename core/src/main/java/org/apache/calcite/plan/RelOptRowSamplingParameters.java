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
 * RelOptRowSamplingParameters represents the parameters necessary to
 * produce a fixed-size row sample of a relation.
 *
 * <p>Its parameters are derived from the SQL 2003 TABLESAMPLE clause, with
 * additional support for sampling a fixed number of rows.
 */
public class RelOptRowSamplingParameters {
  //~ Instance fields --------------------------------------------------------

  private final boolean isBernoulli;
  private final int numberOfRows;
  private final boolean isRepeatable;
  private final int repeatableSeed;

  //~ Constructors -----------------------------------------------------------

  public RelOptRowSamplingParameters(
      boolean isBernoulli,
      int numberOfRows,
      boolean isRepeatable,
      int repeatableSeed) {
    this.isBernoulli = isBernoulli;
    this.numberOfRows = numberOfRows;
    this.isRepeatable = isRepeatable;
    this.repeatableSeed = repeatableSeed;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Indicates whether Bernoulli or system sampling should be performed.
   * Bernoulli sampling requires the decision whether to include each row in
   * the the sample to be independent across rows. System sampling allows
   * implementation-dependent behavior.
   *
   * @return true if Bernoulli sampling is configured, false for system
   * sampling
   */
  public boolean isBernoulli() {
    return isBernoulli;
  }

  /**
   * Returns the number of rows that will appear in the output.
   *
   * @return the number of rows between 0 and 1000000, inclusive
   */
  public int getNumberOfRows() {
    return numberOfRows;
  }

  /**
   * Indicates whether the sample results should be repeatable. Sample results
   * are only required to repeat if no changes have been made to the
   * relation's content or structure. If the sample is configured to be
   * repeatable, then a user-specified seed value can be obtained via
   * {@link #getRepeatableSeed()}.
   *
   * @return true if the sample results should be repeatable
   */
  public boolean isRepeatable() {
    return isRepeatable;
  }

  /**
   * If {@link #isRepeatable()} returns <code>true</code>, this method returns a
   * user-specified seed value. Samples of the same, unmodified relation
   * should be identical if the sampling mode, sampling percentage and
   * repeatable seed are the same.
   *
   * @return seed value for repeatable samples
   */
  public int getRepeatableSeed() {
    return repeatableSeed;
  }
}
