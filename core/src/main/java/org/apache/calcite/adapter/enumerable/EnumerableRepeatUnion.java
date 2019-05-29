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

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;


/**
 * Implementation of {@link RepeatUnion} in
 * {@link EnumerableConvention enumerable calling convention}.
 *
 * <p>NOTE: The current API is experimental and subject to change without notice.</p>
 */
@Experimental
public class EnumerableRepeatUnion extends RepeatUnion implements EnumerableRel {

  /**
   * Creates an EnumerableRepeatUnion
   */
  EnumerableRepeatUnion(
          RelOptCluster cluster,
          RelTraitSet traits,
          RelNode seed,
          RelNode iterative,
          boolean all,
          int maxRep) {
    super(cluster, traits, seed, iterative, all, maxRep);
  }

  @Override public EnumerableRepeatUnion copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return new EnumerableRepeatUnion(getCluster(), traitSet,
        inputs.get(0), inputs.get(1), all, maxRep);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // TODO only UNION ALL is supported for the moment
    if (!all) {
      throw new UnsupportedOperationException(
          "Only EnumerableRepeatUnion ALL is supported for the moment");
    }

    // return repeatUnionAll(<seedExp>, <iterativeExp>, maxRep);

    BlockBuilder builder = new BlockBuilder();
    RelNode seed = getSeedRel();
    RelNode iteration = getIterativeRel();

    Result seedResult = implementor.visitChild(this, 0, (EnumerableRel) seed, pref);
    Result iterationResult = implementor.visitChild(this, 1, (EnumerableRel) iteration, pref);

    Expression seedExp = builder.append("seed", seedResult.block);
    Expression iterativeExp = builder.append("iteration", iterationResult.block);

    Expression unionExp = Expressions.call(
        BuiltInMethod.REPEAT_UNION_ALL.method,
        seedExp,
        iterativeExp,
        Expressions.constant(maxRep, int.class));
    builder.add(unionExp);

    PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(),
        getRowType(),
        pref.prefer(seedResult.format));
    return implementor.result(physType, builder.toBlock());
  }

}

// End EnumerableRepeatUnion.java
