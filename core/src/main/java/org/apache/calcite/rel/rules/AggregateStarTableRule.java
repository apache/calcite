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
package org.apache.calcite.rel.rules;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.TileKey;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.AbstractSourceMapping;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Aggregate} on
 * top of a {@link org.apache.calcite.schema.impl.StarTable.StarTableScan}.
 *
 * <p>This pattern indicates that an aggregate table may exist. The rule asks
 * the star table for an aggregate table at the required level of aggregation.
 *
 * @see AggregateProjectStarTableRule
 * @see CoreRules#AGGREGATE_STAR_TABLE
 * @see CoreRules#AGGREGATE_PROJECT_STAR_TABLE
 */
@Value.Enclosing
public class AggregateStarTableRule
    extends RelRule<AggregateStarTableRule.Config>
    implements TransformationRule {

  /** Creates an AggregateStarTableRule. */
  protected AggregateStarTableRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateStarTableRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final StarTable.StarTableScan scan = call.rel(1);
    apply(call, null, aggregate, scan);
  }

  protected void apply(RelOptRuleCall call, @Nullable Project postProject,
      final Aggregate aggregate, StarTable.StarTableScan scan) {
    final RelOptPlanner planner = call.getPlanner();
    final Optional<CalciteConnectionConfig> config =
        planner.getContext().maybeUnwrap(CalciteConnectionConfig.class);
    if (!(config.isPresent() && config.get().createMaterializations())) {
      // Disable this rule if materializations are disabled - in
      // particular, if we are in a recursive statement that is being used to
      // populate a materialization
      return;
    }
    final RelOptCluster cluster = scan.getCluster();
    final RelOptTable table = scan.getTable();
    final RelOptLattice lattice =
        requireNonNull(planner.getLattice(table),
            () -> "planner.getLattice(table) is null for " + table);
    final List<Lattice.Measure> measures =
        lattice.lattice.toMeasures(aggregate.getAggCallList());
    final Pair<CalciteSchema.TableEntry, TileKey> pair =
        lattice.getAggregate(planner, aggregate.getGroupSet(), measures);
    if (pair == null) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    final CalciteSchema.TableEntry tableEntry = pair.left;
    final TileKey tileKey = pair.right;
    final RelMetadataQuery mq = call.getMetadataQuery();
    final double rowCount = mq.getRowCount(aggregate);
    final Table aggregateTable = tableEntry.getTable();
    final RelDataType aggregateTableRowType =
        aggregateTable.getRowType(cluster.getTypeFactory());
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(
            table.getRelOptSchema(),
            aggregateTableRowType,
            tableEntry,
            rowCount);
    relBuilder.push(aggregateRelOptTable.toRel(ViewExpanders.simpleContext(cluster)));
    if (tileKey == null) {
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + " (exact match)");
      }
    } else if (!tileKey.dimensions.equals(aggregate.getGroupSet())) {
      // Aggregate has finer granularity than we need. Roll up.
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + ", rolling up " + tileKey.dimensions + " to "
            + aggregate.getGroupSet());
      }
      assert tileKey.dimensions.contains(aggregate.getGroupSet());
      final List<AggregateCall> aggCalls = new ArrayList<>();
      ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
      for (int key : aggregate.getGroupSet()) {
        groupSet.set(tileKey.dimensions.indexOf(key));
      }
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        final AggregateCall copy =
            rollUp(groupSet.isEmpty(), relBuilder, aggCall, tileKey);
        if (copy == null) {
          return;
        }
        aggCalls.add(copy);
      }
      relBuilder.push(
          aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
              groupSet.build(), null, aggCalls));
    } else if (!tileKey.measures.equals(measures)) {
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + ", right granularity, but different measures "
            + aggregate.getAggCallList());
      }
      relBuilder.project(
          relBuilder.fields(
              new AbstractSourceMapping(
                  tileKey.dimensions.cardinality() + tileKey.measures.size(),
                  aggregate.getRowType().getFieldCount()) {
                @Override public int getSourceOpt(int source) {
                  if (source < aggregate.getGroupCount()) {
                    int in = tileKey.dimensions.nth(source);
                    return aggregate.getGroupSet().indexOf(in);
                  }
                  Lattice.Measure measure =
                      measures.get(source - aggregate.getGroupCount());
                  int i = tileKey.measures.indexOf(measure);
                  assert i >= 0;
                  return tileKey.dimensions.cardinality() + i;
                }
              } .inverse()));
    }
    if (postProject != null) {
      relBuilder.push(
          postProject.copy(postProject.getTraitSet(),
              ImmutableList.of(relBuilder.peek())));
    }
    call.transformTo(relBuilder.build());
  }

  private static @Nullable AggregateCall rollUp(boolean hasEmptyGroup,
      RelBuilder relBuilder, AggregateCall call, TileKey tileKey) {
    if (call.isDistinct()) {
      return null;
    }
    final SqlAggFunction aggregation = call.getAggregation();
    final Pair<SqlAggFunction, List<Integer>> seek =
        Pair.of(aggregation, call.getArgList());
    final int offset = tileKey.dimensions.cardinality();
    final ImmutableList<Lattice.Measure> measures = tileKey.measures;

    // First, try to satisfy the aggregation by rolling up an aggregate in the
    // materialization.
    final int i = find(measures, seek);
  tryRoll:
    if (i >= 0) {
      final SqlAggFunction roll = aggregation.getRollup();
      if (roll == null) {
        break tryRoll;
      }
      return AggregateCall.create(call.getParserPosition(), roll, false, call.isApproximate(),
          call.ignoreNulls(), call.rexList, ImmutableList.of(offset + i), -1,
          call.distinctKeys, call.collation,
          hasEmptyGroup, relBuilder.peek(), null, call.name);
    }

    // Second, try to satisfy the aggregation based on group set columns.
  tryGroup:
    {
      List<Integer> newArgs = new ArrayList<>();
      for (Integer arg : call.getArgList()) {
        int z = tileKey.dimensions.indexOf(arg);
        if (z < 0) {
          break tryGroup;
        }
        newArgs.add(z);
      }
      return AggregateCall.create(call.getParserPosition(), aggregation, false,
          call.isApproximate(), call.ignoreNulls(), call.rexList,
          newArgs, -1, call.distinctKeys, call.collation,
          hasEmptyGroup, relBuilder.peek(), null, call.name);
    }

    // No roll up possible.
    return null;
  }

  private static int find(ImmutableList<Lattice.Measure> measures,
      Pair<SqlAggFunction, List<Integer>> seek) {
    for (int i = 0; i < measures.size(); i++) {
      Lattice.Measure measure = measures.get(i);
      if (measure.agg.equals(seek.left)
          && measure.argOrdinals().equals(seek.right)) {
        return i;
      }
    }
    return -1;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {

    Config DEFAULT = ImmutableAggregateStarTableRule.Config.of()
        .withOperandFor(Aggregate.class, StarTable.StarTableScan.class);

    @Override default AggregateStarTableRule toRule() {
      return new AggregateStarTableRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<StarTable.StarTableScan> scanClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .predicate(Aggregate::isSimple)
              .oneInput(b1 ->
                  b1.operand(scanClass).noInputs()))
          .as(Config.class);
    }
  }

}
