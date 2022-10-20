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
 * RelOptSamplingParameters represents the parameters necessary to produce a
 * sample of a relation.
 *
 * <p>It's parameters are derived from the SQL 2003 TABLESAMPLE clause.
 */
public class RelOptSamplingParameters {
  //~ Instance fields --------------------------------------------------------

  private final boolean isBernoulli;
  private final float samplingPercentage;
  private final boolean isRepeatable;
  private final int repeatableSeed;

  //~ Constructors -----------------------------------------------------------

  public RelOptSamplingParameters(
      boolean isBernoulli,
      float samplingPercentage,
      boolean isRepeatable,
      int repeatableSeed) {
    this.isBernoulli = isBernoulli;
    this.samplingPercentage = samplingPercentage;
    this.isRepeatable = isRepeatable;
    this.repeatableSeed = repeatableSeed;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Indicates whether Bernoulli or system sampling should be performed.
   * Bernoulli sampling requires the decision whether to include each row in
   * the sample to be independent across rows. System sampling allows
   * implementation-dependent behavior.
   *
   * @return true if Bernoulli sampling is configured, false for system
   * sampling
   */
  public boolean isBernoulli() {
    return isBernoulli;
  }

  /**
   * Returns the sampling percentage. For Bernoulli sampling, the sampling
   * percentage is the likelihood that any given row will be included in the
   * sample. For system sampling, the sampling percentage indicates (roughly)
   * what percentage of the rows will appear in the sample.
   *
   * @return the sampling percentage between 0.0 and 1.0, exclusive
   */
  public float getSamplingPercentage() {
    return samplingPercentage;
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
