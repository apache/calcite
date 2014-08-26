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

import java.util.*;

import com.google.common.collect.ImmutableList;

/**
 * SargIntervalSequence represents the union of a set of disjoint {@link
 * SargInterval} instances. (If any adjacent intervals weren't disjoint, they
 * would have been combined into one bigger one before creation of the
 * sequence.) Intervals are maintained in coordinate order.
 */
public class SargIntervalSequence {
  //~ Instance fields --------------------------------------------------------

  final List<SargInterval> list;

  //~ Constructors -----------------------------------------------------------

  SargIntervalSequence() {
    list = new ArrayList<SargInterval>();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return true if this sequence represents a point range.
   */
  public boolean isPoint() {
    return (list.size() == 1) && list.get(0).isPoint();
  }

  /**
   * @return true if this sequence represents an empty range.
   */
  public boolean isEmpty() {
    return (list.size() == 1) && list.get(0).isEmpty();
  }

  /**
   * @return true if this sequence represents a non-point, non-empty range.
   */
  public boolean isRange() {
    return list.size() > 1
        || (list.size() == 1 && list.get(0).isRange());
  }

  /**
   * @return unmodifiable list representing this sequence
   */
  public List<SargInterval> getList() {
    return ImmutableList.copyOf(list);
  }

  void addInterval(SargInterval interval) {
    list.add(interval);
  }

  // override Object
  public String toString() {
    // Special case:  empty sequence implies empty set.
    if (list.isEmpty()) {
      return "()";
    }

    // Special case:  don't return UNION of a single interval.
    if (list.size() == 1) {
      return list.get(0).toString();
    }

    StringBuilder sb = new StringBuilder();

    sb.append(SargSetOperator.UNION);
    sb.append("(");

    for (SargInterval interval : list) {
      sb.append(" ");
      sb.append(interval);
    }

    sb.append(" )");
    return sb.toString();
  }
}

// End SargIntervalSequence.java
