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

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Snapshot}. */
public class MutableSnapshot extends MutableSingleRel {

  public final RexNode period;

  private MutableSnapshot(MutableRel input, RexNode period) {
    super(MutableRelType.SNAPSHOT, input.rowType, input);
    this.period = period;
  }

  /**
   * Creates a MutableSnapshot.
   *
   * @param input     Input relational expression
   * @param period    Timestamp expression which as the table was at the given
   *                  time in the past
   */
  public static MutableSnapshot of(
      MutableRel input, RexNode period) {
    return new MutableSnapshot(input, period);
  }

  @Override public MutableRel clone() {
    return MutableSnapshot.of(input.clone(), period);
  }

  @Override public int hashCode() {
    return Objects.hash(input, period);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableSnapshot
        && period.equals(period)
        && input.equals(input);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Snapshot(period: ").append(period).append(")");
  }

}
