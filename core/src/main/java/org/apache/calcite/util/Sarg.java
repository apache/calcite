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

import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;

/** Set of values (or ranges) that are the target of a search.
 *
 * <p>The name is derived from <b>S</b>earch <b>arg</b>ument, an ancient
 * concept in database implementation; see Access Path Selection in a Relational
 * Database Management System &mdash; Selinger et al. 1979 or the
 * "<a href="https://blog.acolyer.org/2016/01/04/access-path-selection/">morning
 * paper summary</a>.
 *
 * @see SqlStdOperatorTable#SEARCH
 */
public class Sarg<C extends Comparable<C>> implements Comparable<Sarg<C>> {
  public final RangeSet<C> rangeSet;

  private Sarg(RangeSet<C> rangeSet) {
    this.rangeSet = rangeSet;
  }

  /** Creates a search argument. */
  public static <C extends Comparable<C>> Sarg<C> of(RangeSet<C> rangeSet) {
    return new Sarg<>(ImmutableRangeSet.copyOf(rangeSet));
  }

  @Override public String toString() {
    return "Sarg(" + rangeSet + ")";
  }

  @Override public int compareTo(Sarg<C> o) {
    return RangeSets.compare(rangeSet, o.rangeSet);
  }

  @Override public int hashCode() {
    return RangeSets.hashCode(rangeSet);
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof Sarg
        && rangeSet.equals(((Sarg) o).rangeSet);
  }
}
