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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.SemiJoin;

import java.util.ArrayList;
import java.util.List;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.core.SemiJoin} relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
class EnumerableSemiJoinRule extends ConverterRule {
  EnumerableSemiJoinRule() {
    super(SemiJoin.class, Convention.NONE, EnumerableConvention.INSTANCE,
        "EnumerableSemiJoinRule");
  }

  @Override public RelNode convert(RelNode rel) {
    final SemiJoin semiJoin = (SemiJoin) rel;
    final List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : semiJoin.getInputs()) {
      if (!(input.getConvention() instanceof EnumerableConvention)) {
        input =
            convert(input,
                input.getTraitSet().replace(EnumerableConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    return EnumerableSemiJoin.create(newInputs.get(0), newInputs.get(1),
        semiJoin.getCondition(), semiJoin.leftKeys, semiJoin.rightKeys);
  }
}

// End EnumerableSemiJoinRule.java
