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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;

/**
 * Rule to convert a {@link LogicalRepeatUnion} into an {@link EnumerableRepeatUnion}.
 */
public class EnumerableRepeatUnionRule extends ConverterRule {

  EnumerableRepeatUnionRule() {
    super(
      LogicalRepeatUnion.class,
      Convention.NONE,
      EnumerableConvention.INSTANCE,
      "EnumerableRepeatUnionRule");

  }

  @Override public RelNode convert(RelNode rel) {
    LogicalRepeatUnion union = (LogicalRepeatUnion) rel;
    EnumerableConvention out = EnumerableConvention.INSTANCE;
    RelTraitSet traitSet = union.getTraitSet().replace(out);
    RelNode seedRel = union.getSeedRel();
    RelNode iterativeRel = union.getIterativeRel();

    return new EnumerableRepeatUnion(
        rel.getCluster(),
        traitSet,
        convert(seedRel, seedRel.getTraitSet().replace(out)),
        convert(iterativeRel, iterativeRel.getTraitSet().replace(out)),
        union.all,
        union.iterationLimit);
  }
}

// End EnumerableRepeatUnionRule.java
