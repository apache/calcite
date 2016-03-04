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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;

import java.util.List;

/**
 * Rules and relational operators for {@link DruidQuery}.
 */
public class DruidRules {
  private DruidRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final List<RelOptRule> RULES = ImmutableList.of(
      new DruidAggregateRule(),
      new DruidProjectRule(),
      new DruidFilterRule());

  /** Predicate that returns whether Druid can not handle an aggregate. */
  private static final Predicate<AggregateCall> BAD_AGG =
      new Predicate<AggregateCall>() {
        public boolean apply(AggregateCall aggregateCall) {
          switch (aggregateCall.getAggregation().getKind()) {
          case COUNT:
          case SUM:
          case SUM0:
          case MIN:
          case MAX:
            return false;
          default:
            return true;
          }
        }
      };

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate}
   * into a {@link DruidQuery}.
   */
  private static class DruidAggregateRule extends RelOptRule {
    private DruidAggregateRule() {
      super(
          operand(Aggregate.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (aggregate.indicator
          || aggregate.getGroupSets().size() != 1
          || Iterables.any(aggregate.getAggCallList(), BAD_AGG)) {
        return;
      }
      if (!DruidQuery.isValidSignature(query.signature() + 'a')) {
        return;
      }
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
          ImmutableList.of(Util.last(query.rels)));
      call.transformTo(extendQuery(query, newAggregate));
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Project}
   * into a {@link DruidQuery}.
   */
  private static class DruidProjectRule extends RelOptRule {
    private DruidProjectRule() {
      super(
          operand(Project.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'p')) {
        return;
      }
      if (DruidQuery.canProjectAll(project.getProjects())) {
        // All expressions can be pushed to Druid in their entirety.
        final RelNode newProject = project.copy(project.getTraitSet(),
            ImmutableList.of(Util.last(query.rels)));
        call.transformTo(extendQuery(query, newProject));
        return;
      }
      final Pair<List<RexNode>, List<RexNode>> pair =
          DruidQuery.splitProjects(project.getCluster().getRexBuilder(), query,
              project.getProjects());
      if (pair == null) {
        // We can't push anything useful to Druid.
        return;
      }
      final List<RexNode> above = pair.left;
      final List<RexNode> below = pair.right;
      final RelDataTypeFactory.FieldInfoBuilder builder =
          project.getCluster().getTypeFactory().builder();
      final RelNode input = Util.last(query.rels);
      for (RexNode e : below) {
        final String name;
        if (e instanceof RexInputRef) {
          name = input.getRowType()
              .getFieldNames()
              .get(((RexInputRef) e).getIndex());
        } else {
          name = null;
        }
        builder.add(name, e.getType());
      }
      final RelNode newProject = project.copy(project.getTraitSet(),
          input, below, builder.build());
      final DruidQuery newQuery = extendQuery(query, newProject);
      final RelNode newProject2 = project.copy(project.getTraitSet(),
          newQuery, above, project.getRowType());
      call.transformTo(newProject2);
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Filter}
   * into a {@link DruidQuery}.
   */
  private static class DruidFilterRule extends RelOptRule {
    private DruidFilterRule() {
      super(
          operand(Filter.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'f')
          || !query.isValidFilter(filter.getCondition())) {
        return;
      }
      final RelNode newFilter = filter.copy(filter.getTraitSet(),
          ImmutableList.of(Util.last(query.rels)));
      call.transformTo(extendQuery(query, newFilter));
    }
  }

  public static DruidQuery extendQuery(DruidQuery query, RelNode r) {
    final ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
    return DruidQuery.create(query.getCluster(), query.getTraitSet(),
        query.getTable(), query.druidTable,
        builder.addAll(query.rels).add(r).build());
  }
}

// End DruidRules.java
