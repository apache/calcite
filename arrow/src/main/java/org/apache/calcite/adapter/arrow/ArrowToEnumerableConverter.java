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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;

import static java.util.Objects.requireNonNull;

class ArrowToEnumerableConverter
    extends ConverterImpl implements EnumerableRel {
  protected ArrowToEnumerableConverter(RelOptCluster cluster,
      RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ArrowToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    return requireNonNull(cost, "cost").multiplyBy(0.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final ArrowRel.Implementor arrowImplementor = new ArrowRel.Implementor();
    arrowImplementor.visitInput(0, getInput());

    final RelOptTable table = requireNonNull(arrowImplementor.table, "table");
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(),
        getRowType(),
        pref.preferArray());

    final BlockBuilder blockBuilder = new BlockBuilder();
    final int fieldCount = table.getRowType().getFieldCount();
    final Expression fields = arrowImplementor.selectFields != null
        ? Expressions.call(
        BuiltInMethod.IMMUTABLE_INT_LIST_COPY_OF.method,
        Expressions.constant(
            Ints.toArray(arrowImplementor.selectFields)))
        : Expressions.call(
            BuiltInMethod.IMMUTABLE_INT_LIST_IDENTITY.method,
            Expressions.constant(fieldCount));
    final Expression filters = Expressions.constant(ImmutableList.copyOf(arrowImplementor.filters));

    Expression tableExpression;
    if (table.unwrap(ArrowTable.class) != null) {
      tableExpression = table.getExpression(ArrowTable.class);
    } else if (table.unwrap(ParquetTable.class) != null) {
      tableExpression = table.getExpression(ParquetTable.class);
    } else {
      throw new IllegalStateException("Unsupported table type: " + table.getClass().getName());
    }

    final Expression enumerable =
        Expressions.call(tableExpression,
        ArrowMethod.ARROW_QUERY.method,
        implementor.getRootExpression(),
        fields,
        filters);

    blockBuilder.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, blockBuilder.toBlock());
  }

  /** Callback for the implementation of a child node. */
  protected void implementChild(EnumerableRelImplementor implementor,
      EnumerableRel child, int ordinal, Prefer prefer) {
    final Result result = implementor.visitChild(this, ordinal, child, prefer);
    // The ARG keyword in the comment tells IntelliJ that this is a formatter
    // directive, and prevents it from re-formatting the following code.
    //noinspection Annotator
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), child.getRowType(), prefer.prefer(result.format));
  }
}
