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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.adapter.enumerable.EnumerableLimit.getExpression;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 * It optimizes sorts that have a limit and an optional offset.
 */
public class EnumerableLimitSort extends Sort implements EnumerableRel {

  /**
   * Creates an EnumerableLimitSort.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public EnumerableLimitSort(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert this.getConvention() instanceof EnumerableConvention;
    assert this.getConvention() == input.getConvention();
  }

  /** Creates an EnumerableLimitSort. */
  public static EnumerableLimitSort create(
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(
        collation);
    return new EnumerableLimitSort(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override public EnumerableLimitSort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new EnumerableLimitSort(
        this.getCluster(),
        traitSet,
        newInput,
        newCollation,
        offset,
        fetch);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) this.getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(),
        this.getRowType(),
        result.format);
    final Expression childExp = builder.append("child", result.block);

    final PhysType inputPhysType = result.physType;
    final Pair<Expression, Expression> pair =
        inputPhysType.generateCollationKey(this.collation.getFieldCollations());

    final Expression fetchVal;
    if (this.fetch == null) {
      fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
    } else {
      fetchVal = getExpression(this.fetch);
    }

    final Expression offsetVal = this.offset == null ? Expressions.constant(Integer.valueOf(0))
        : getExpression(this.offset);

    builder.add(
        Expressions.return_(
            null, Expressions.call(
                BuiltInMethod.ORDER_BY_WITH_FETCH_AND_OFFSET.method, Expressions.list(
                    childExp,
                    builder.append("keySelector", pair.left))
                    .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
                    .appendIfNotNull(
                        builder.appendIfNotNull("offset",
                            Expressions.constant(offsetVal)))
                    .appendIfNotNull(
                        builder.appendIfNotNull("fetch",
                            Expressions.constant(fetchVal)))
            )));
    return implementor.result(physType, builder.toBlock());
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double rowCount = mq.getRowCount(this.input).doubleValue();
    double toSort = getValue(this.fetch, rowCount);
    if (this.offset != null) {
      toSort += getValue(this.offset, rowCount);
    }
    // we need to sort at most rowCount rows
    toSort = Math.min(rowCount, toSort);

    // we need to process rowCount rows, and for every row
    // we search the key in a TreeMap with at most toSort entries
    final double lookup = Math.max(1., Math.log(toSort));
    final double bytesPerRow = this.getRowType().getFieldCount() * 4.;
    final double cpu = (rowCount * lookup) * bytesPerRow;

    RelOptCost cost = planner.getCostFactory().makeCost(rowCount, cpu, 0);
    return cost;
  }

  private double getValue(RexNode r, double defaultValue) {
    if (r == null || r instanceof RexDynamicParam) {
      return defaultValue;
    }
    return RexLiteral.intValue(r);
  }
}
