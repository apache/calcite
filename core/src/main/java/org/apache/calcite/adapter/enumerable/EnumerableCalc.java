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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.BRIDGE_METHODS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_EXPRS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

/** Implementation of {@link org.apache.calcite.rel.core.Calc} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableCalc extends Calc implements EnumerableRel {
  /**
   * Creates an EnumerableCalc.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public EnumerableCalc(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RexProgram program) {
    super(cluster, traitSet, input, program);
    assert getConvention() instanceof EnumerableConvention;
    assert !program.containsAggs();
  }

  @Deprecated // to be removed before 2.0
  public EnumerableCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RexProgram program,
      List<RelCollation> collationList) {
    this(cluster, traitSet, input, program);
    Util.discard(collationList);
  }

  /** Creates an EnumerableCalc. */
  public static EnumerableCalc create(final RelNode input,
      final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(EnumerableConvention.INSTANCE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.calc(mq, input, program))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.calc(mq, input, program));
    return new EnumerableCalc(cluster, traitSet, input, program);
  }

  @Override public EnumerableCalc copy(RelTraitSet traitSet, RelNode child,
      RexProgram program) {
    // we do not need to copy program; it is immutable
    return new EnumerableCalc(getCluster(), traitSet, child, program);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();

    final Result result =
        implementor.visitChild(this, 0, child, pref);

    final PhysType physType =
        PhysTypeImpl.of(
            typeFactory, getRowType(), pref.prefer(result.format));

    // final Enumerable<Employee> inputEnumerable = <<child adapter>>;
    // return new Enumerable<IntString>() {
    //     Enumerator<IntString> enumerator() {
    //         return new Enumerator<IntString>() {
    //             public void reset() {
    // ...
    Type outputJavaType = physType.getJavaRowType();
    final Type enumeratorType =
        Types.of(
            Enumerator.class, outputJavaType);
    Type inputJavaType = result.physType.getJavaRowType();
    ParameterExpression inputEnumerator =
        Expressions.parameter(
            Types.of(
                Enumerator.class, inputJavaType),
            "inputEnumerator");
    Expression input =
        RexToLixTranslator.convert(
            Expressions.call(
                inputEnumerator,
                BuiltInMethod.ENUMERATOR_CURRENT.method),
            inputJavaType);

    final RexBuilder rexBuilder = getCluster().getRexBuilder();
    final RelMetadataQuery mq = getCluster().getMetadataQuery();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(child);
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
    final RexProgram program = this.program.normalize(rexBuilder, simplify);

    BlockStatement moveNextBody;
    if (program.getCondition() == null) {
      moveNextBody =
          Blocks.toFunctionBlock(
              Expressions.call(
                  inputEnumerator,
                  BuiltInMethod.ENUMERATOR_MOVE_NEXT.method));
    } else {
      final BlockBuilder builder2 = new BlockBuilder();
      Expression condition =
          RexToLixTranslator.translateCondition(
              program,
              typeFactory,
              builder2,
              new RexToLixTranslator.InputGetterImpl(
                  Collections.singletonList(
                      Pair.of(input, result.physType))),
              implementor.allCorrelateVariables, implementor.getConformance());
      builder2.add(
          Expressions.ifThen(
              condition,
              Expressions.return_(
                  null, Expressions.constant(true))));
      moveNextBody =
          Expressions.block(
              Expressions.while_(
                  Expressions.call(
                      inputEnumerator,
                      BuiltInMethod.ENUMERATOR_MOVE_NEXT.method),
                  builder2.toBlock()),
              Expressions.return_(
                  null,
                  Expressions.constant(false)));
    }

    final BlockBuilder builder3 = new BlockBuilder();
    final SqlConformance conformance =
        (SqlConformance) implementor.map.getOrDefault("_conformance",
            SqlConformanceEnum.DEFAULT);
    List<Expression> expressions =
        RexToLixTranslator.translateProjects(
            program,
            typeFactory,
            conformance,
            builder3,
            physType,
            DataContext.ROOT,
            new RexToLixTranslator.InputGetterImpl(
                Collections.singletonList(
                    Pair.of(input, result.physType))),
            implementor.allCorrelateVariables);
    builder3.add(
        Expressions.return_(
            null, physType.record(expressions)));
    BlockStatement currentBody =
        builder3.toBlock();

    final Expression inputEnumerable =
        builder.append(
            "inputEnumerable", result.block, false);
    final Expression body =
        Expressions.new_(
            enumeratorType,
            NO_EXPRS,
            Expressions.list(
                Expressions.fieldDecl(
                    Modifier.PUBLIC
                    | Modifier.FINAL,
                    inputEnumerator,
                    Expressions.call(
                        inputEnumerable,
                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method)),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_RESET.method,
                    NO_PARAMS,
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            inputEnumerator,
                            BuiltInMethod.ENUMERATOR_RESET.method))),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                    NO_PARAMS,
                    moveNextBody),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_CLOSE.method,
                    NO_PARAMS,
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            inputEnumerator,
                            BuiltInMethod.ENUMERATOR_CLOSE.method))),
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    BRIDGE_METHODS
                        ? Object.class
                        : outputJavaType,
                    "current",
                    NO_PARAMS,
                    currentBody)));
    builder.add(
        Expressions.return_(
            null,
            Expressions.new_(
                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                // TODO: generics
                //   Collections.singletonList(inputRowType),
                NO_EXPRS,
                ImmutableList.<MemberDeclaration>of(
                    Expressions.methodDecl(
                        Modifier.PUBLIC,
                        enumeratorType,
                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                        NO_PARAMS,
                        Blocks.toFunctionBlock(body))))));
    return implementor.result(physType, builder.toBlock());
  }

  public RexProgram getProgram() {
    return program;
  }
}

// End EnumerableCalc.java
