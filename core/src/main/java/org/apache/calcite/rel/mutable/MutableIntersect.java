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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Intersect}. */
public class MutableIntersect extends MutableSetOp {
  private MutableIntersect(RelOptCluster cluster, RelDataType rowType,
      List<MutableRel> inputs, boolean all) {
    super(cluster, rowType, MutableRelType.INTERSECT, inputs, all);
  }

  /**
   * Creates a MutableIntersect.
   *
   * @param rowType Row type
   * @param inputs  Input relational expressions
   * @param all     Whether to perform a multiset intersection or a set
   *                intersection
   */
  public static MutableIntersect of(
      RelDataType rowType, List<MutableRel> inputs, boolean all) {
    assert inputs.size() >= 2;
    final MutableRel input0 = inputs.get(0);
    return new MutableIntersect(input0.cluster, rowType, inputs, all);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Intersect(all: ").append(all).append(")");
  }

  @Override public MutableRel clone() {
    return MutableIntersect.of(rowType, cloneChildren(), all);
  }
}

// End MutableIntersect.java
