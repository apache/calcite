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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.BiPredicate;
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

    final Expression matcher_ = implementMatcher(builder, row_);
    final Expression emitter_ = implementEmitter(implementor, physType);
    builder.add(
        Expressions.return_(null,
            Expressions.call(BuiltInMethod.MATCH.method,
                inputExp, keySelector_, matcher_, emitter_)));
    return implementor.result(physType, builder.toBlock());
  }

  private Expression implementEmitter(EnumerableRelImplementor implementor,
      PhysType physType, PhysType inputPhysType) {
    final ParameterExpression rows_ =
        Expressions.parameter(List.class, "rows");
    final ParameterExpression rowStates_ =
        Expressions.parameter(List.class, "rowStates");
    final ParameterExpression match_ =
        Expressions.parameter(int.class, "match");
    final ParameterExpression consumer_ =
        Expressions.parameter(Consumer.class, "consumer");

    final ParameterExpression row_ =
        Expressions.parameter(Object.class, "row");
    final BlockBuilder builder2 = new BlockBuilder();
    final List<Expression> arguments =
        RexToLixTranslator.translateProjects(null,
            (JavaTypeFactory) getCluster().getTypeFactory(),
            implementor.getConformance(), builder2, physType,
            implementor.getRootExpression(),
            new RexToLixTranslator.InputGetterImpl(
                Collections.singletonList(
                    Pair.of(row_, inputPhysType))),
            implementor.allCorrelateVariables);
    for (RexNode measure : measures.values()) {
      arguments.add(impl);
    }
    builder2.add(
        Expressions.statement(
            Expressions.call(consumer_, BuiltInMethod.CONSUMER_ACCEPT.method,
                physType.record(arguments))));

    final BlockBuilder builder = new BlockBuilder();
    builder.add(Expressions.forEach(row_, rows_, builder2.toBlock()));

    return Expressions.new_(
        Types.of(Enumerables.Emitter.class), NO_EXPRS,
        Expressions.list(
            EnumUtils.overridingMethodDecl(
                BuiltInMethod.EMITTER_EMIT.method,
                ImmutableList.of(rows_, rowStates_, match_, consumer_),
                builder.toBlock())));
  }

  private Expression implementMatcher(BlockBuilder builder,
      ParameterExpression row_) {
    final Expression patternBuilder_ = builder.append("patternBuilder",
        Expressions.call(BuiltInMethod.PATTERN_BUILDER.method));
    final Expression automaton_ = builder.append("automaton",
        Expressions.call(
            implementPattern(patternBuilder_, pattern),
            BuiltInMethod.PATTERN_TO_AUTOMATON.method));
    Expression matcherBuilder_ = builder.append("matcherBuilder",
        Expressions.call(BuiltInMethod.MATCHER_BUILDER.method, automaton_));
    for (Map.Entry<String, RexNode> entry : patternDefinitions.entrySet()) {
      final Expression predicate_ = implementPredicate(row_);
      matcherBuilder_ = Expressions.call(matcherBuilder_,
          BuiltInMethod.MATCHER_BUILDER_ADD.method,
          Expressions.constant(entry.getKey()), predicate_);
    }
    return builder.append("matcher",
        Expressions.call(matcherBuilder_,
            BuiltInMethod.MATCHER_BUILDER_BUILD.method));
  }

  /** Generates code for a predicate. */
  private Expression implementPredicate(ParameterExpression row_) {
    final ParameterExpression rows_ =
        Expressions.parameter(List.class, "rows"); // "List<E> rows"
    final BlockBuilder builder2 = new BlockBuilder();
    builder2.add(Expressions.return_(null, Expressions.constant(true)));
    final List<MemberDeclaration> memberDeclarations = new ArrayList<>();
    // Add a predicate method:
    //
    //   public boolean test(E row, List<E> rows) {
    //     return ...;
    //   }
    memberDeclarations.add(
        EnumUtils.overridingMethodDecl(
            BuiltInMethod.BI_PREDICATE_TEST.method,
            ImmutableList.of(row_, rows_), builder2.toBlock()));
    if (EnumerableRules.BRIDGE_METHODS) {
      // Add a bridge method:
      //
      //   public boolean test(Object row, Object rows) {
      //     return this.test(row, (List) rows);
      //   }
      final ParameterExpression rowsO_ =
          Expressions.parameter(Object.class, "rows");
      BlockBuilder bridgeBody = new BlockBuilder();
      bridgeBody.add(
          Expressions.return_(null,
              Expressions.call(
                  Expressions.parameter(Comparable.class, "this"),
                  BuiltInMethod.BI_PREDICATE_TEST.method,
                  row_, Expressions.convert_(rowsO_, List.class))));
      memberDeclarations.add(
          EnumUtils.overridingMethodDecl(
              BuiltInMethod.BI_PREDICATE_TEST.method,
              ImmutableList.of(row_, rowsO_), bridgeBody.toBlock()));
    }
    return Expressions.new_(Types.of(BiPredicate.class), NO_EXPRS,
        memberDeclarations);
  }

  /** Generates code for a pattern.
   *
   * <p>For example, for the pattern {@code (A B)}, generates
   * {@code patternBuilder.symbol("A").symbol("B").seq()}. */
  private Expression implementPattern(Expression patternBuilder_,
      RexNode pattern) {
    switch (pattern.getKind()) {
    case LITERAL:
      final String symbol = ((RexLiteral) pattern).getValueAs(String.class);
      return Expressions.call(patternBuilder_,
          BuiltInMethod.PATTERN_BUILDER_SYMBOL.method,
          Expressions.constant(symbol));

    case PATTERN_CONCAT:
      final RexCall concat = (RexCall) pattern;
      for (Ord<RexNode> operand : Ord.zip(concat.operands)) {
        patternBuilder_ = implementPattern(patternBuilder_, operand.e);
        if (operand.i > 0) {
          patternBuilder_ = Expressions.call(patternBuilder_,
              BuiltInMethod.PATTERN_BUILDER_SEQ.method);
        }
      }
      return patternBuilder_;

    default:
      throw new AssertionError("unknown kind: " + pattern);
    }
  }
}

// End EnumerableMatch.java
