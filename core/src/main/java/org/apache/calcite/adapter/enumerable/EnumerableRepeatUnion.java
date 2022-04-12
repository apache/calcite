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
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link RepeatUnion} in
 * {@link EnumerableConvention enumerable calling convention}.
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public class EnumerableRepeatUnion extends RepeatUnion implements EnumerableRel {

  /**
   * Creates an EnumerableRepeatUnion.
   */
  EnumerableRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode seed, RelNode iterative, boolean all, int iterationLimit,
      @Nullable RelOptTable transientTable) {
    super(cluster, traitSet, seed, iterative, all, iterationLimit, transientTable);
  }

  @Override public EnumerableRepeatUnion copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return new EnumerableRepeatUnion(getCluster(), traitSet,
        inputs.get(0), inputs.get(1), all, iterationLimit, transientTable);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

    // return repeatUnion(<seedExp>, <iterativeExp>, iterationLimit, all, <comparer>);

    BlockBuilder builder = new BlockBuilder();
    RelNode seed = getSeedRel();
    RelNode iteration = getIterativeRel();

    Expression cleanUpFunctionExp = Expressions.constant(null);
    if (transientTable != null) {
      // root.getRootSchema().add(tableName, table);
      Expression tableExp = implementor.stash(
          Objects.requireNonNull(transientTable.unwrap(TransientTable.class)),
          TransientTable.class);
      String tableName =
          transientTable.getQualifiedName().get(transientTable.getQualifiedName().size() - 1);
      Expression tableNameExp = Expressions.constant(tableName, String.class);
      builder.append(
          Expressions.call(
              Expressions.call(
                  implementor.getRootExpression(),
                  BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
              BuiltInMethod.SCHEMA_PLUS_ADD_TABLE.method,
              tableNameExp,
              tableExp));
      // root.getRootSchema().removeTable(tableName);
      cleanUpFunctionExp = Expressions.lambda(Function0.class,
          Expressions.call(
              Expressions.call(
                  implementor.getRootExpression(),
                  BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
              BuiltInMethod.SCHEMA_PLUS_REMOVE_TABLE.method, tableNameExp));
    }

    Result seedResult = implementor.visitChild(this, 0, (EnumerableRel) seed, pref);
    Result iterationResult = implementor.visitChild(this, 1, (EnumerableRel) iteration, pref);

    Expression seedExp = builder.append("seed", seedResult.block);
    Expression iterativeExp = builder.append("iteration", iterationResult.block);

    PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(),
        getRowType(),
        pref.prefer(seedResult.format));

    Expression unionExp = Expressions.call(
        BuiltInMethod.REPEAT_UNION.method,
        seedExp,
        iterativeExp,
        Expressions.constant(iterationLimit, int.class),
        Expressions.constant(all, boolean.class),
        Util.first(physType.comparer(), Expressions.call(BuiltInMethod.IDENTITY_COMPARER.method)),
        cleanUpFunctionExp);
    builder.add(unionExp);

    return implementor.result(physType, builder.toBlock());
  }

}
