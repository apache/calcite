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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Uncollect} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableUncollect extends Uncollect implements EnumerableRel {
  @Deprecated // to be removed before 2.0
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    this(cluster, traitSet, child, false);
  }

  /** Creates an EnumerableUncollect.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, boolean withOrdinality) {
    super(cluster, traitSet, child, withOrdinality);
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == child.getConvention();
  }

  /**
   * Creates an EnumerableUncollect.
   *
   * <p>Each field of the input relational expression must be an array or
   * multiset.
   *
   * @param traitSet Trait set
   * @param input    Input relational expression
   * @param withOrdinality Whether output should contain an ORDINALITY column
   */
  public static EnumerableUncollect create(RelTraitSet traitSet, RelNode input,
      boolean withOrdinality) {
    final RelOptCluster cluster = input.getCluster();
    return new EnumerableUncollect(cluster, traitSet, input, withOrdinality);
  }

  @Override public EnumerableUncollect copy(RelTraitSet traitSet,
      RelNode newInput) {
    return new EnumerableUncollect(getCluster(), traitSet, newInput,
        withOrdinality);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            JavaRowFormat.LIST);

    // final Enumerable<List<Employee>> child = <<child adapter>>;
    // return child.selectMany(LIST_TO_ENUMERABLE);
    final Expression child_ =
        builder.append(
            "child", result.block);
    final Expression lambda;
    if (withOrdinality) {
      final BlockBuilder builder2 = new BlockBuilder();
      final ParameterExpression o_ = Expressions.parameter(Modifier.FINAL,
          result.physType.getJavaRowType(),
          "o");
      final Expression list_ = builder2.append("list",
          Expressions.new_(ArrayList.class));
      final ParameterExpression i_ = Expressions.parameter(int.class, "i");
      final BlockBuilder builder3 = new BlockBuilder();
      final Expression v_ =
          builder3.append("v",
              Expressions.call(o_, BuiltInMethod.LIST_GET.method, i_));
      final List<Expression> expressions = new ArrayList<>();
      final PhysType componentPhysType = result.physType.component(0);
      final int fieldCount = componentPhysType.getRowType().getFieldCount();
      expressions.addAll(
          componentPhysType.accessors(v_,
              ImmutableIntList.identity(fieldCount)));
      expressions.add(Expressions.add(i_, Expressions.constant(1)));
      builder3.add(
          Expressions.statement(
              Expressions.call(list_, BuiltInMethod.COLLECTION_ADD.method,
                  physType.record(expressions))));
      builder2.add(
          Expressions.for_(
              Expressions.declare(0, i_, Expressions.constant(0)),
              Expressions.lessThan(i_,
                  Expressions.call(o_, BuiltInMethod.COLLECTION_SIZE.method)),
              Expressions.postIncrementAssign(i_),
              builder3.toBlock()));
      builder2.add(
          Expressions.return_(null,
              Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, list_)));
      lambda = Expressions.lambda(builder2.toBlock(), o_);
    } else {
      lambda = Expressions.call(BuiltInMethod.LIST_TO_ENUMERABLE.method);
    }
    builder.add(
        Expressions.return_(null,
            Expressions.call(child_,
                BuiltInMethod.SELECT_MANY.method,
                lambda)));
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableUncollect.java
