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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

/** Relational expression that executes its children using an interpreter.
 *
 * <p>Although quite a few kinds of {@link org.apache.calcite.rel.RelNode} can
 * be interpreted, this is only created by default for
 * {@link org.apache.calcite.schema.FilterableTable} and
 * {@link org.apache.calcite.schema.ProjectableFilterableTable}.
 */
public class EnumerableInterpreter extends SingleRel
    implements EnumerableRel {
  private final double factor;

  /**
   * Creates an EnumerableInterpreter.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster Cluster
   * @param traitSet Traits
   * @param input Input relation
   * @param factor Cost multiply factor
   */
  public EnumerableInterpreter(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, double factor) {
    super(cluster, traitSet, input);
    assert getConvention() instanceof EnumerableConvention;
    this.factor = factor;
  }

  /**
   * Creates an EnumerableInterpreter.
   *
   * @param input Input relation
   * @param factor Cost multiply factor
   */
  public static EnumerableInterpreter create(RelNode input, double factor) {
    final RelTraitSet traitSet = input.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    return new EnumerableInterpreter(input.getCluster(), traitSet, input,
        factor);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(factor);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableInterpreter(getCluster(), traitSet, sole(inputs),
        factor);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(typeFactory, getRowType(), JavaRowFormat.ARRAY);
    final Expression interpreter_ = builder.append("interpreter",
        Expressions.new_(Interpreter.class,
            implementor.getRootExpression(),
            implementor.stash(getInput(), RelNode.class)));
    final Expression sliced_ =
        getRowType().getFieldCount() == 1
            ? Expressions.call(BuiltInMethod.SLICE0.method, interpreter_)
            : interpreter_;
    builder.add(sliced_);
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableInterpreter.java
