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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/** Utilities for Guava {@link com.google.common.collect.RangeSet}. */
@SuppressWarnings({"BetaApi", "UnstableApiUsage"})
public class RangeSets {
  private RangeSets() {}

  private static final ImmutableRangeSet ALL =
      ImmutableRangeSet.of().complement();

  /** Subtracts a range from a range set. */
  public static <C extends Comparable<C>> RangeSet<C> minus(RangeSet<C> rangeSet,
      Range<C> range) {
    final TreeRangeSet<C> mutableRangeSet = TreeRangeSet.create(rangeSet);
    mutableRangeSet.remove(range);
    return mutableRangeSet.equals(rangeSet) ? rangeSet
        : ImmutableRangeSet.copyOf(mutableRangeSet);
  }

  /** Returns the unrestricted range set. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <C extends Comparable<C>> RangeSet<C> rangeSetAll() {
    return (RangeSet) ALL;
  }
}
