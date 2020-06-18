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

import org.apache.calcite.adapter.enumerable.impl.AggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/** Sort based physical implementation of {@link Aggregate} in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableSortedAggregate extends EnumerableAggregateBase implements EnumerableRel {
  public EnumerableSortedAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Override public EnumerableSortedAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new EnumerableSortedAggregate(getCluster(), traitSet, input,
        groupSet, groupSets, aggCalls);
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    if (!isSimple(this)) {
      return null;
    }

    RelTraitSet inputTraits = getInput().getTraitSet();
    RelCollation collation = required.getCollation();
    ImmutableBitSet requiredKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
    ImmutableBitSet groupKeys = ImmutableBitSet.range(groupSet.cardinality());

    Mappings.TargetMapping mapping = Mappings.source(groupSet.toList(),
        input.getRowType().getFieldCount());

    if (requiredKeys.equals(groupKeys)) {
      RelCollation inputCollation = RexUtil.apply(mapping, collation);
      return Pair.of(required, ImmutableList.of(inputTraits.replace(inputCollation)));
    } else if (groupKeys.contains(requiredKeys)) {
      // group by a,b,c order by c,b
      List<RelFieldCollation> list = new ArrayList<>(collation.getFieldCollations());
      groupKeys.except(requiredKeys).forEach(k -> list.add(new RelFieldCollation(k)));
      RelCollation aggCollation = RelCollations.of(list);
      RelCollation inputCollation = RexUtil.apply(mapping, aggCollation);
      return Pair.of(traitSet.replace(aggCollation),
          ImmutableList.of(inputTraits.replace(inputCollation)));
    }

    // Group keys doesn't contain all the required keys, e.g.
    // group by a,b order by a,b,c
    // nothing we can do to propagate traits to child nodes.
    return null;
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    if (!Aggregate.isSimple(this)) {
      throw Util.needToImplement("EnumerableSortedAggregate");
    }

    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    Expression childExp =
        builder.append(
            "child",
            result.block);

    final PhysType physType =
        PhysTypeImpl.of(
            typeFactory, getRowType(), pref.preferCustom());

    final PhysType inputPhysType = result.physType;

    ParameterExpression parameter =
        Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

    final PhysType keyPhysType =
        inputPhysType.project(groupSet.asList(), getGroupType() != Group.SIMPLE,
            JavaRowFormat.LIST);
    final int groupCount = getGroupCount();

    final List<AggImpState> aggs = new ArrayList<>(aggCalls.size());
    for (Ord<AggregateCall> call : Ord.zip(aggCalls)) {
      aggs.add(new AggImpState(call.i, call.e, false));
    }

    // Function0<Object[]> accumulatorInitializer =
    //     new Function0<Object[]>() {
    //         public Object[] apply() {
    //             return new Object[] {0, 0};
    //         }
    //     };
    final List<Expression> initExpressions = new ArrayList<>();
    final BlockBuilder initBlock = new BlockBuilder();

    final List<Type> aggStateTypes = createAggStateTypes(
        initExpressions, initBlock, aggs, typeFactory);

    final PhysType accPhysType =
        PhysTypeImpl.of(typeFactory,
            typeFactory.createSyntheticType(aggStateTypes));

    declareParentAccumulator(initExpressions, initBlock, accPhysType);

    final Expression accumulatorInitializer =
        builder.append("accumulatorInitializer",
            Expressions.lambda(
                Function0.class,
                initBlock.toBlock()));

    // Function2<Object[], Employee, Object[]> accumulatorAdder =
    //     new Function2<Object[], Employee, Object[]>() {
    //         public Object[] apply(Object[] acc, Employee in) {
    //              acc[0] = ((Integer) acc[0]) + 1;
    //              acc[1] = ((Integer) acc[1]) + in.salary;
    //             return acc;
    //         }
    //     };
    final ParameterExpression inParameter =
        Expressions.parameter(inputPhysType.getJavaRowType(), "in");
    final ParameterExpression acc_ =
        Expressions.parameter(accPhysType.getJavaRowType(), "acc");

    createAccumulatorAdders(
        inParameter, aggs, accPhysType, acc_, inputPhysType, builder, implementor, typeFactory);

    final ParameterExpression lambdaFactory =
        Expressions.parameter(AggregateLambdaFactory.class,
            builder.newName("lambdaFactory"));

    implementLambdaFactory(builder, inputPhysType, aggs, accumulatorInitializer,
        false, lambdaFactory);

    final BlockBuilder resultBlock = new BlockBuilder();
    final List<Expression> results = Expressions.list();
    final ParameterExpression key_;
    final Type keyType = keyPhysType.getJavaRowType();
    key_ = Expressions.parameter(keyType, "key");
    for (int j = 0; j < groupCount; j++) {
      final Expression ref = keyPhysType.fieldReference(key_, j);
      results.add(ref);
    }

    for (final AggImpState agg : aggs) {
      results.add(
          agg.implementor.implementResult(agg.context,
              new AggResultContextImpl(resultBlock, agg.call, agg.state, key_,
                  keyPhysType)));
    }
    resultBlock.add(physType.record(results));

    final Expression keySelector_ =
        builder.append("keySelector",
            inputPhysType.generateSelector(parameter,
                groupSet.asList(),
                keyPhysType.getFormat()));
    // Generate the appropriate key Comparator. In the case of NULL values
    // in group keys, the comparator must be able to support NULL values by giving a
    // consistent sort ordering.
    final Expression comparator = keyPhysType.generateComparator(getTraitSet().getCollation());

    final Expression resultSelector_ =
        builder.append("resultSelector",
            Expressions.lambda(Function2.class,
                resultBlock.toBlock(),
                key_,
                acc_));

    builder.add(
        Expressions.return_(null,
            Expressions.call(childExp,
                BuiltInMethod.SORTED_GROUP_BY.method,
                Expressions.list(keySelector_,
                    Expressions.call(lambdaFactory,
                        BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method),
                    Expressions.call(lambdaFactory,
                        BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method),
                    Expressions.call(lambdaFactory,
                        BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                        resultSelector_), comparator)
                    )));

    return implementor.result(physType, builder.toBlock());
  }
}
