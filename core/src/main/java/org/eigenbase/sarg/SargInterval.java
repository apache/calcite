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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;

/**
 * SargInterval represents a single contiguous search interval over a scalar
 * domain of a given datatype (including null values). It consists of two
 * endpoints: a lower bound and an upper bound, which may be the same for the
 * case of a single point. The endpoints are represented via instances of {@link
 * SargEndpoint}. An empty interval is represented by setting both bounds to be
 * open with the same value (the null value, but it doesn't really matter).
 *
 * <p>Instances of SargInterval are immutable after construction.
 *
 * <p>For string representation, we use the standard mathematical bracketed
 * bounds pair notation, with round brackets for open bounds and square brackets
 * for closed bounds, e.g.
 *
 * <ul>
 * <li>[3,5] represents all values between 3 and 5 inclusive
 * <li>(3,5] represents all values greater than 3 and less than or equal to 5
 * <li>[3,5) represents all values greatern than or equal to 3 and less than 5
 * <li>(3,5) represents all values between 3 and 5 exclusive
 * <li>(3,+infinity) represents all values greater than 3
 * <li>(-infinity,5] represents all values less than or equal to 5
 * <li>[5,5] represents the single point with coordinate 5
 * </ul>
 *
 * <p>Null values are ordered lower than any non-null value but higher than
 * -infinity. So the interval [null,7) would include the null value and any
 * non-null value less than 7.
 */
public class SargInterval extends SargIntervalBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SargInterval.
   */
  SargInterval(
      SargFactory factory,
      RelDataType dataType) {
    super(factory, dataType);
  }

  //~ Methods ----------------------------------------------------------------

  void copyFrom(SargIntervalBase other) {
    assert getDataType() == other.getDataType();
    lowerBound.copyFrom(other.getLowerBound());
    upperBound.copyFrom(other.getUpperBound());
  }

  boolean contains(SargInterval other) {
    assert getDataType() == other.getDataType();
    if (getLowerBound().compareTo(other.getLowerBound()) > 0) {
      return false;
    }
    if (getUpperBound().compareTo(other.getUpperBound()) < 0) {
      return false;
    }
    return true;
  }
}

// End SargInterval.java
