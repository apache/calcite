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
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;

/** Implementation of {@link org.apache.calcite.rel.core.Uncollect} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableUncollect extends Uncollect implements EnumerableRel {
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    super(cluster, traitSet, child);
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == child.getConvention();
  }

  @Override public EnumerableUncollect copy(RelTraitSet traitSet,
      RelNode newInput) {
    return new EnumerableUncollect(getCluster(), traitSet, newInput);
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

    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    RelDataType inputRowType = child.getRowType();

    // final Enumerable<List<Employee>> child = <<child adapter>>;
    // return child.selectMany(LIST_TO_ENUMERABLE);
    final Expression child_ =
        builder.append(
            "child", result.block);
    builder.add(
        Expressions.return_(null,
            Expressions.call(child_,
                BuiltInMethod.SELECT_MANY.method,
                Expressions.call(BuiltInMethod.LIST_TO_ENUMERABLE.method))));
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableUncollect.java
