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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.rex.RexNode;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Filter}. */
public class MutableFilter extends MutableSingleRel {
  public final RexNode condition;

  private MutableFilter(MutableRel input, RexNode condition) {
    super(MutableRelType.FILTER, input.rowType, input);
    this.condition = condition;
  }

  /**
   * Creates a MutableFilter.
   *
   * @param input     Input relational expression
   * @param condition Boolean expression which determines whether a row is
   *                  allowed to pass
   */
  public static MutableFilter of(MutableRel input, RexNode condition) {
    return new MutableFilter(input, condition);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableFilter
        && condition.equals(((MutableFilter) obj).condition)
        && input.equals(((MutableFilter) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, condition);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Filter(condition: ").append(condition).append(")");
  }

  @Override public MutableRel clone() {
    return MutableFilter.of(input.clone(), condition);
  }
}

// End MutableFilter.java
