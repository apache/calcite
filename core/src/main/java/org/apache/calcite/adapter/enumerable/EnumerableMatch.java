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
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.AutomatonBuilder;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Consumer;

import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_EXPRS;

/** Implementation of {@link org.apache.calcite.rel.core.Match} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableMatch extends Match implements EnumerableRel {
  /**
   * Creates an EnumerableMatch.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public EnumerableMatch(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDataType rowType, RexNode pattern,
      boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
      RexNode interval) {
    super(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
        patternDefinitions, measures, after, subsets, allRows, partitionKeys,
        orderKeys, interval);
  }

  /** Creates an EnumerableMatch. */
  public static EnumerableMatch create(RelNode input, RelDataType rowType,
      RexNode pattern, boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
      RexNode interval) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE);
    return new EnumerableMatch(cluster, traitSet, input, rowType, pattern,
        strictStart, strictEnd, patternDefinitions, measures, after, subsets,
        allRows, partitionKeys, orderKeys, interval);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableMatch(getCluster(), traitSet, inputs.get(0), rowType,
        pattern, strictStart, strictEnd, patternDefinitions, measures, after,
        subsets, allRows, partitionKeys, orderKeys, interval);
  }

  public EnumerableRel.Result implement(EnumerableRelImplementor implementor,
      EnumerableRel.Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel input = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, input, pref);
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
            result.format);
    final Expression inputExp =
        builder.append("input", result.block);

    PhysType inputPhysType = result.physType;

    final PhysType keyPhysType =
        inputPhysType.project(partitionKeys.asList(), JavaRowFormat.LIST);
    final ParameterExpression row_ =
        Expressions.parameter(Object.class, "row");
    final Expression keySelector_ =
        builder.append("keySelector",
            inputPhysType.generateSelector(row_,
                partitionKeys.asList(),
                keyPhysType.getFormat()));

    final Expression automaton_ = builder.append("automaton",
        Expressions.call(Expressions.new_(AutomatonBuilder.class),
            BuiltInMethod.AUTOMATON_BUILD.method));
    final Expression matcherBuilder_ = builder.append("matcherBuilder",
        Expressions.call(BuiltInMethod.MATCHER_BUILDER.method, automaton_));
    final Expression matcher_ = builder.append("matcher",
        Expressions.call(matcherBuilder_,
            BuiltInMethod.MATCHER_BUILDER_BUILD.method));

    final ParameterExpression rows_ =
        Expressions.parameter(List.class, "rows");
    final ParameterExpression rowStates_ =
        Expressions.parameter(List.class, "rowStates");
    final ParameterExpression match_ =
        Expressions.parameter(int.class, "match");
    final ParameterExpression consumer_ =
        Expressions.parameter(Consumer.class, "consumer");
    final BlockBuilder builder2 = new BlockBuilder();
    builder2.add(
        Expressions.statement(
            Expressions.call(rows_, BuiltInMethod.ITERABLE_FOR_EACH.method,
                consumer_)));
    final Expression emitter_ =
        Expressions.new_(Types.of(Enumerables.Emitter.class), NO_EXPRS,
            Expressions.list(
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.EMITTER_EMIT.method,
                    ImmutableList.of(rows_, rowStates_, match_, consumer_),
                    builder2.toBlock())));
    builder.add(
        Expressions.return_(null,
            Expressions.call(BuiltInMethod.MATCH.method,
                inputExp, keySelector_, matcher_, emitter_)));
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableMatch.java
