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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.MemoryFactory;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        PhysTypeImpl.of(implementor.getTypeFactory(), input.getRowType(),
            result.format);
    final Expression inputExp =
        builder.append("input", result.block);

    PhysType inputPhysType = result.physType;

    final PhysType keyPhysType =
        inputPhysType.project(partitionKeys.asList(), JavaRowFormat.LIST);
    final ParameterExpression row_ =
        Expressions.parameter(inputPhysType.getJavaRowType(), "row_");
    final Expression keySelector_ =
        builder.append("keySelector",
            inputPhysType.generateSelector(row_,
                partitionKeys.asList(),
                keyPhysType.getFormat()));

    // ...
    List<Map.Entry<String, RelDataType>> types = measures.entrySet().stream()
            .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue().getType()
            )).entrySet().stream()
            .collect(Collectors.toList());

    final PhysType emitType = PhysTypeImpl.of(implementor.getTypeFactory(),
            implementor.getTypeFactory().createStructType(types),
            result.format);

    final Expression matcher_ = implementMatcher(implementor, physType, builder, row_);
    final Expression emitter_ = implementEmitter(implementor, emitType, physType);

    final MaxHistoryFutureVisitor visitor = new MaxHistoryFutureVisitor();
    patternDefinitions.values().forEach(pd -> pd.accept(visitor));

    // Fetch
    // Calculate how many steps we need to look back or forward
    int history = visitor.getHistory();
    int future = visitor.getFuture();

    builder.add(
        Expressions.return_(null,
            Expressions.call(BuiltInMethod.MATCH.method,
                inputExp, keySelector_, matcher_, emitter_, Expressions.constant(history), Expressions.constant(future))));
    return implementor.result(emitType, builder.toBlock());
  }

  private Expression implementEmitter(EnumerableRelImplementor implementor,
      PhysType physType, PhysType inputPhysType) {
    final ParameterExpression rows_ =
        Expressions.parameter(Types.of(List.class, inputPhysType.getJavaRowType()), "rows");
    final ParameterExpression rowStates_ =
        Expressions.parameter(List.class, "rowStates");
    final ParameterExpression symbols_ =
        Expressions.parameter(List.class, "symbols");
    final ParameterExpression match_ =
        Expressions.parameter(int.class, "match");
    final ParameterExpression consumer_ =
        Expressions.parameter(Consumer.class, "consumer");

    final ParameterExpression row_ =
        Expressions.parameter(inputPhysType.getJavaRowType(), "row");
    final BlockBuilder builder2 = new BlockBuilder();

    RexBuilder rexBuilder = new RexBuilder(implementor.getTypeFactory());
    RexProgramBuilder rexProgramBuilder = new RexProgramBuilder(inputPhysType.getRowType(), rexBuilder);
    for (Map.Entry<String, RexNode> entry : measures.entrySet()) {
      rexProgramBuilder.addProject(entry.getValue(), entry.getKey());
    }
    final List<Expression> arguments =
        RexToLixTranslator.translateProjects(rexProgramBuilder.getProgram(),
    (JavaTypeFactory) getCluster().getTypeFactory(),
            implementor.getConformance(), builder2, physType,
            implementor.getRootExpression(),
            new RexToLixTranslator.InputGetterImpl(
                Collections.singletonList(
                    Pair.of(row_, inputPhysType))),
            implementor.allCorrelateVariables);

    final ParameterExpression result_ = Expressions.parameter(physType.getJavaRowType());
    // final ParameterExpression result__ = Expressions.parameter(physType.getJavaRowType());

    builder2.add(Expressions.declare(Modifier.FINAL, result_, Expressions.new_(physType.getJavaRowType())));
    for (int i = 0; i < arguments.size(); i++) {
      builder2.add(Expressions.statement(Expressions.assign(physType.fieldReference(result_, i), arguments.get(i))));
    }
    // builder2.add(Expressions.declare(Modifier.PUBLIC, result__, physType.record(arguments)));
    builder2.add(
        Expressions.statement(
            Expressions.call(consumer_, BuiltInMethod.CONSUMER_ACCEPT.method, result_)
        ));

    final BlockBuilder builder = new BlockBuilder();

    builder.add(Expressions.forEach(row_, rows_,
                    builder2.toBlock()
    ));

    return Expressions.new_(
        Types.of(Enumerables.Emitter.class), NO_EXPRS,
        Expressions.list(
            EnumUtils.overridingMethodDecl(
                BuiltInMethod.EMITTER_EMIT.method,
                ImmutableList.of(rows_, rowStates_, symbols_, match_, consumer_),
                builder.toBlock())));
  }

  private Expression implementMatcher(EnumerableRelImplementor implementor, PhysType physType, BlockBuilder builder,
                                      ParameterExpression row_) {
    final Expression patternBuilder_ = builder.append("patternBuilder",
        Expressions.call(BuiltInMethod.PATTERN_BUILDER.method));
    final Expression automaton_ = builder.append("automaton",
        Expressions.call(
            implementPattern(patternBuilder_, pattern),
            BuiltInMethod.PATTERN_TO_AUTOMATON.method));
    Expression matcherBuilder_ = builder.append("matcherBuilder",
        Expressions.call(BuiltInMethod.MATCHER_BUILDER.method, automaton_));
    final BlockBuilder builder2 = new BlockBuilder();


    // Wrap a MemoryEnumerable around


    for (Map.Entry<String, RexNode> entry : patternDefinitions.entrySet()) {
      // Translate REX to Expressions
      RexBuilder rexBuilder = new RexBuilder(implementor.getTypeFactory());
      RexProgramBuilder rexProgramBuilder = new RexProgramBuilder(physType.getRowType(), rexBuilder);

      rexProgramBuilder.addCondition(entry.getValue());

      final RexToLixTranslator.InputGetter inputGetter1 = new PrevInputGetter(row_, physType);


      final Expression condition = RexToLixTranslator.translateCondition(rexProgramBuilder.getProgram(),
          (JavaTypeFactory) getCluster().getTypeFactory(),
          builder2,
          inputGetter1,
          implementor.allCorrelateVariables,
          implementor.getConformance());


      builder2.add(Expressions.return_(null, condition));
      final Expression predicate_ = implementPredicate(physType, row_, builder2.toBlock());

      matcherBuilder_ = Expressions.call(matcherBuilder_,
          BuiltInMethod.MATCHER_BUILDER_ADD.method,
          Expressions.constant(entry.getKey()),
          predicate_);
    }
    return builder.append("matcher",
        Expressions.call(matcherBuilder_,
            BuiltInMethod.MATCHER_BUILDER_BUILD.method));
  }

  /** Generates code for a predicate. */
  private Expression implementPredicate(PhysType physType, ParameterExpression rows_, BlockStatement body) {
    final List<MemberDeclaration> memberDeclarations = new ArrayList<>();
    ParameterExpression row_ = Expressions.parameter(Types.of(MemoryFactory.Memory.class, physType.getJavaRowType()), "row_");
    try {
      Expressions.assign(row_,
        Expressions.call(rows_, MemoryFactory.Memory.class.getMethod("get")));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException();
    }

    // Implement the Predicate here based on the pattern definition


    // Add a predicate method:
    //
    //   public boolean test(E row, List<E> rows) {
    //     return ...;
    //   }
    memberDeclarations.add(
        EnumUtils.overridingMethodDecl(
            BuiltInMethod.PREDICATE_TEST.method,
            ImmutableList.of(row_), body));
    if (EnumerableRules.BRIDGE_METHODS) {
      // Add a bridge method:
      //
      //   public boolean test(Object row, Object rows) {
      //     return this.test(row, (List) rows);
      //   }
      final ParameterExpression row0_ =
              Expressions.parameter(Object.class, "row");
      final ParameterExpression rowsO_ =
          Expressions.parameter(Object.class, "rows");
      BlockBuilder bridgeBody = new BlockBuilder();
      bridgeBody.add(
          Expressions.return_(null,
              Expressions.call(
                  Expressions.parameter(Comparable.class, "this"),
                  BuiltInMethod.PREDICATE_TEST.method,
                  Expressions.convert_(row0_, Types.of(MemoryFactory.Memory.class, physType.getJavaRowType())))));
      memberDeclarations.add(
          EnumUtils.overridingMethodDecl(
              BuiltInMethod.PREDICATE_TEST.method,
              ImmutableList.of(row0_), bridgeBody.toBlock()));
    }
    return Expressions.new_(Types.of(Predicate.class), NO_EXPRS,
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

  private static class MaxHistoryFutureVisitor implements RexVisitor<Integer> {

    private int history = 0;
    private int future = 0;

    public int getHistory() {
      return history;
    }

    public int getFuture() {
      return future;
    }

    @Override public Integer visitInputRef(RexInputRef inputRef) {
      return null;
    }

    @Override public Integer visitLocalRef(RexLocalRef localRef) {
      return null;
    }

    @Override public Integer visitLiteral(RexLiteral literal) {
      return null;
    }

    @Override public Integer visitCall(RexCall call) {
      call.operands.forEach(o -> o.accept(this));
      if (call.op == SqlStdOperatorTable.PREV) {
        final int prev = (int) ((BigDecimal) ((RexLiteral) call.getOperands().get(1)).getValue()).longValue();
        this.history = Math.max(this.history, prev);
      } else if (call.op == SqlStdOperatorTable.NEXT) {
        final int next = (int) ((BigDecimal) ((RexLiteral) call.getOperands().get(1)).getValue()).longValue();
        this.future = Math.max(this.future, next);
      }
      return null;
    }

    @Override public Integer visitOver(RexOver over) {
      return null;
    }

    @Override public Integer visitCorrelVariable(RexCorrelVariable correlVariable) {
      return null;
    }

    @Override public Integer visitDynamicParam(RexDynamicParam dynamicParam) {
      return null;
    }

    @Override public Integer visitRangeRef(RexRangeRef rangeRef) {
      return null;
    }

    @Override public Integer visitFieldAccess(RexFieldAccess fieldAccess) {
      return null;
    }

    @Override public Integer visitSubQuery(RexSubQuery subQuery) {
      return null;
    }

    @Override public Integer visitTableInputRef(RexTableInputRef fieldRef) {
      return null;
    }

    @Override public Integer visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return null;
    }
  }

  public static class PrevInputGetter implements RexToLixTranslator.InputGetter {

    private Expression offset;
    private final ParameterExpression row_;
    private final Function<Expression, RexToLixTranslator.InputGetter> generator;
    private final PhysType physType;

    public PrevInputGetter(ParameterExpression row_, PhysType physType) {
      this.row_ = row_;
      generator = e -> new RexToLixTranslator.InputGetterImpl(
          Collections.singletonList(
              Pair.of(e, physType)));
      this.physType = physType;
    }

    public void setOffset(Expression offset) {
      this.offset = offset;
    }

    @Override public Expression field(BlockBuilder list, int index, Type storageType) {
      try {
        final ParameterExpression row = Expressions.parameter(physType.getJavaRowType());
        final ParameterExpression tmp = Expressions.parameter(Object.class);
        list.add(
            Expressions.declare(0, tmp,
                Expressions.call(row_,
                    MemoryFactory.Memory.class.getMethod("get", int.class),
                    offset)
            ));
        list.add(
            Expressions.declare(0, row,
                Expressions.convert_(tmp, physType.getJavaRowType())
            ));

        // Add return statement if here is a null!
        list.add(
                Expressions.ifThen(
                        Expressions.equal(tmp, Expressions.constant(null)),
                        Expressions.return_(null, Expressions.constant(false))
                )
        );

//        list.add(
//            Expressions.assign(row,
//                ));
        return generator.apply(row).field(list, index, storageType);
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
        return null;
      }
    }

  }
}

// End EnumerableMatch.java
