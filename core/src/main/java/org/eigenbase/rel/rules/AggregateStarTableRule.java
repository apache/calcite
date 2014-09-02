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

import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptLattice;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;

import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.prepare.RelOptTableImpl;

import com.google.common.collect.ImmutableList;

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
      AggregateRelBase aggregate, StarTable.StarTableScan scan) {
    final RelOptCluster cluster = scan.getCluster();
    final RelOptTable table = scan.getTable();
    final RelOptLattice lattice = call.getPlanner().getLattice(table);
    OptiqSchema.TableEntry aggregateTable =
        lattice.getAggregate(call.getPlanner(), aggregate.getGroupSet(),
            aggregate.getAggCallList());
    if (aggregateTable == null) {
      return;
    }
    System.out.println(aggregateTable);
    final double rowCount = aggregate.getRows();
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(table.getRelOptSchema(),
            aggregateTable.getTable().getRowType(cluster.getTypeFactory()),
            aggregateTable, rowCount);
    RelNode rel = aggregateRelOptTable.toRel(RelOptUtil.getContext(cluster));
    if (postProject != null) {
      rel = postProject.copy(postProject.getTraitSet(), ImmutableList.of(rel));
    }
    call.transformTo(rel);
  }
}

// End AggregateStarTableRule.java
