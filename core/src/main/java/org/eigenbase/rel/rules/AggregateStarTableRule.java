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
package org.eigenbase.rel.rules;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptLattice;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Pair;
import org.eigenbase.util.mapping.AbstractSourceMapping;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.materialize.Lattice;
import net.hydromatic.optiq.materialize.MaterializationService;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.prepare.RelOptTableImpl;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Planner rule that matches an {@link org.eigenbase.rel.AggregateRelBase} on
 * top of a {@link net.hydromatic.optiq.impl.StarTable.StarTableScan}.
 *
 * <p>This pattern indicates that an aggregate table may exist. The rule asks
 * the star table for an aggregate table at the required level of aggregation.
 */
public class AggregateStarTableRule extends RelOptRule {
  public static final AggregateStarTableRule INSTANCE =
      new AggregateStarTableRule(
          operand(AggregateRelBase.class,
              some(operand(StarTable.StarTableScan.class, none()))),
          "AggregateStarTableRule");

  public static final AggregateStarTableRule INSTANCE2 =
      new AggregateStarTableRule(
          operand(AggregateRelBase.class,
              operand(ProjectRelBase.class,
                  operand(StarTable.StarTableScan.class, none()))),
          "AggregateStarTableRule:project") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final AggregateRelBase aggregate = call.rel(0);
          final ProjectRelBase project = call.rel(1);
          final StarTable.StarTableScan scan = call.rel(2);
          final RelNode rel =
              AggregateProjectMergeRule.apply(aggregate, project);
          final AggregateRelBase aggregate2;
          final ProjectRelBase project2;
          if (rel instanceof AggregateRelBase) {
            project2 = null;
            aggregate2 = (AggregateRelBase) rel;
          } else if (rel instanceof ProjectRelBase) {
            project2 = (ProjectRelBase) rel;
            aggregate2 = (AggregateRelBase) project2.getChild();
          } else {
            return;
          }
          apply(call, project2, aggregate2, scan);
        }
      };

  private AggregateStarTableRule(RelOptRuleOperand operand,
      String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final AggregateRelBase aggregate = call.rel(0);
    final StarTable.StarTableScan scan = call.rel(1);
    apply(call, null, aggregate, scan);
  }

  protected void apply(RelOptRuleCall call, ProjectRelBase postProject,
      final AggregateRelBase aggregate, StarTable.StarTableScan scan) {
    final RelOptCluster cluster = scan.getCluster();
    final RelOptTable table = scan.getTable();
    final RelOptLattice lattice = call.getPlanner().getLattice(table);
    final List<Lattice.Measure> measures =
        lattice.lattice.toMeasures(aggregate.getAggCallList());
    Pair<OptiqSchema.TableEntry, MaterializationService.TileKey> pair =
        lattice.getAggregate(call.getPlanner(), aggregate.getGroupSet(),
            measures);
    if (pair == null) {
      return;
    }
    final OptiqSchema.TableEntry tableEntry = pair.left;
    final MaterializationService.TileKey tileKey = pair.right;
    final double rowCount = aggregate.getRows();
    final Table aggregateTable = tableEntry.getTable();
    final RelDataType aggregateTableRowType =
        aggregateTable.getRowType(cluster.getTypeFactory());
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(table.getRelOptSchema(), aggregateTableRowType,
            tableEntry, rowCount);
    RelNode rel = aggregateRelOptTable.toRel(RelOptUtil.getContext(cluster));
    if (tileKey == null) {
      if (OptiqPrepareImpl.DEBUG) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + " (exact match)");
      }
    } else if (!tileKey.dimensions.equals(aggregate.getGroupSet())) {
      // Aggregate has finer granularity than we need. Roll up.
      if (OptiqPrepareImpl.DEBUG) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + ", rolling up " + tileKey.dimensions + " to "
            + aggregate.getGroupSet());
      }
      assert BitSets.contains(tileKey.dimensions, aggregate.getGroupSet());
      final List<AggregateCall> aggCalls = Lists.newArrayList();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        final AggregateCall copy = rollUp(aggCall, tileKey);
        if (copy == null) {
          return;
        }
        aggCalls.add(copy);
      }
      BitSet groupSet = new BitSet();
      for (int key : BitSets.toIter(aggregate.getGroupSet())) {
        groupSet.set(BitSets.toList(tileKey.dimensions).indexOf(key));
      }
      rel = aggregate.copy(aggregate.getTraitSet(), rel, groupSet, aggCalls);
    } else if (!tileKey.measures.equals(measures)) {
      System.out.println("Using materialization "
          + aggregateRelOptTable.getQualifiedName()
          + ", right granularity, but different measures "
          + aggregate.getAggCallList());
      rel = RelOptUtil.project(rel,
          new AbstractSourceMapping(
              tileKey.dimensions.cardinality() + tileKey.measures.size(),
              aggregate.getRowType().getFieldCount()) {
            public int getSourceOpt(int source) {
              if (source < aggregate.getGroupCount()) {
                int in = BitSets.toList(tileKey.dimensions).get(source);
                return BitSets.toList(aggregate.getGroupSet()).indexOf(in);
              }
              Lattice.Measure measure =
                  measures.get(source - aggregate.getGroupCount());
              int i = tileKey.measures.indexOf(measure);
              assert i >= 0;
              return tileKey.dimensions.cardinality() + i;
            }
          });
    }
    if (postProject != null) {
      rel = postProject.copy(postProject.getTraitSet(), ImmutableList.of(rel));
    }
    call.transformTo(rel);
  }

  private static AggregateCall rollUp(AggregateCall aggregateCall,
      MaterializationService.TileKey tileKey) {
    final Aggregation aggregation = aggregateCall.getAggregation();
    final Pair<Aggregation, List<Integer>> seek =
        Pair.of(aggregation, aggregateCall.getArgList());
    final int offset = tileKey.dimensions.cardinality();
    final ImmutableList<Lattice.Measure> measures = tileKey.measures;

    // First, try to satisfy the aggregation by rolling up an aggregate in the
    // materialization.
    final int i = find(measures, seek);
  tryRoll:
    if (i >= 0) {
      final Aggregation roll = getRollup(aggregation);
      if (roll == null) {
        break tryRoll;
      }
      return new AggregateCall(roll, false, ImmutableList.of(offset + i),
          aggregateCall.type, aggregateCall.name);
    }

    // Second, try to satisfy the aggregation based on group set columns.
  tryGroup:
    {
      List<Integer> newArgs = Lists.newArrayList();
      for (Integer arg : aggregateCall.getArgList()) {
        int z = BitSets.toList(tileKey.dimensions).indexOf(arg);
        if (z < 0) {
          break tryGroup;
        }
        newArgs.add(z);
      }
      return new AggregateCall(aggregation, false, newArgs, aggregateCall.type,
          aggregateCall.name);
    }

    // No roll up possible.
    return null;
  }

  private static Aggregation getRollup(Aggregation aggregation) {
    if (aggregation == SqlStdOperatorTable.SUM
        || aggregation == SqlStdOperatorTable.MIN
        || aggregation == SqlStdOperatorTable.MAX) {
      return aggregation;
    } else if (aggregation == SqlStdOperatorTable.COUNT) {
      return SqlStdOperatorTable.SUM;
    } else {
      return null;
    }
  }

  private static int find(ImmutableList<Lattice.Measure> measures,
      Pair<Aggregation, List<Integer>> seek) {
    for (int i = 0; i < measures.size(); i++) {
      Lattice.Measure measure = measures.get(i);
      if (measure.agg.equals(seek.left)
          && measure.argOrdinals().equals(seek.right)) {
        return i;
      }
    }
    return -1;
  }
}

// End AggregateStarTableRule.java
