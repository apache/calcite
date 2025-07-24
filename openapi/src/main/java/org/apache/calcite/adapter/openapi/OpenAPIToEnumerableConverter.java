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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Relational expression representing a scan of an OpenAPI endpoint.
 */
public class OpenAPIToEnumerableConverter extends ConverterImpl implements EnumerableRel {

  OpenAPIToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OpenAPIToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor relImplementor, Prefer prefer) {
    final BlockBuilder block = new BlockBuilder();
    final OpenAPIRel.Implementor implementor = new OpenAPIRel.Implementor();
    implementor.visitChild(0, getInput());

    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(relImplementor.getTypeFactory(),
            rowType, prefer.prefer(JavaRowFormat.ARRAY));

    // Convert filters map to expression
    final Expression filters = block.append("filters",
        Expressions.constant(implementor.filters));

    // Convert projections to field list
    final Expression fields = block.append("fields",
        constantArrayList(
            implementor.projections.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()),
            Pair.class));

    // Convert sorts to list
    final Expression sort = block.append("sort",
        constantArrayList(
            implementor.sorts.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()),
            Pair.class));

    // Get table expression
    final Expression table = block.append("table",
        implementor.table.getExpression(OpenAPITable.OpenAPIQueryable.class));

    // Pagination parameters
    final Expression offset = block.append("offset", Expressions.constant(implementor.offset));
    final Expression fetch = block.append("fetch", Expressions.constant(implementor.fetch));

    // Generate method call to find()
    Expression enumerable = block.append("enumerable",
        Expressions.call(table,
            OpenAPIMethod.OPENAPI_QUERYABLE_FIND.method,
            filters, fields, sort, offset, fetch));

    block.add(Expressions.return_(null, enumerable));
    return relImplementor.result(physType, block.toBlock());
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')".
   */
  private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** Convert list of values to list of constant expressions. */
  private static <T> List<Expression> constantList(List<T> values) {
    return values.stream().map(Expressions::constant).collect(Collectors.toList());
  }
}

/**
 * Rule to convert a relational expression from {@link OpenAPIRel#CONVENTION}
 * to {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}.
 */
class OpenAPIToEnumerableConverterRule extends org.apache.calcite.rel.convert.ConverterRule {

  /** Singleton instance. */
  static final OpenAPIToEnumerableConverterRule INSTANCE = Config.INSTANCE
      .withConversion(RelNode.class, OpenAPIRel.CONVENTION,
          org.apache.calcite.adapter.enumerable.EnumerableConvention.INSTANCE,
          "OpenAPIToEnumerableConverterRule")
      .withRuleFactory(OpenAPIToEnumerableConverterRule::new)
      .toRule(OpenAPIToEnumerableConverterRule.class);

  protected OpenAPIToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode relNode) {
    RelTraitSet newTraitSet = relNode.getTraitSet().replace(getOutConvention());
    return new OpenAPIToEnumerableConverter(relNode.getCluster(), newTraitSet, relNode);
  }
}
