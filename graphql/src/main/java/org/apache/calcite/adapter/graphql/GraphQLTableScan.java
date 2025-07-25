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
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A {@link RelNode} that represents a TableScan operation for a GraphQL table.
 */
public class GraphQLTableScan extends TableScan implements GraphQLRel {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLTableScan.class);
  private final GraphQLTable graphQLTable;
  final RelDataType projectRowType;
  private final @Nullable List<Integer> projectedFields;

  public GraphQLTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, GraphQLTable graphQLTable,
      RelDataType projectRowType, @Nullable List<Integer> projectedFields) {
    super(cluster,
        traitSet.replace(GraphQLRel.CONVENTION),  // Explicitly set convention here
        ImmutableList.of(),
        table);
    this.graphQLTable = graphQLTable;
    this.projectedFields = projectedFields;
    this.projectRowType = projectRowType;
    assert getConvention() == GraphQLRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new GraphQLTableScan(getCluster(), traitSet, table, graphQLTable, projectRowType, projectedFields);
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType;
  }

  /**
   * Register GraphQL rules with the specified {@link RelOptPlanner}.
   *
   * @param planner The RelOptPlanner to register the rules with.
   */
  @Override public void register(RelOptPlanner planner) {
    LOGGER.debug("Registering GraphQL rules with planner");
    planner.addRule(GraphQLRules.TO_ENUMERABLE);
    for (RelOptRule rule : GraphQLRules.RULES) {
      planner.addRule(rule);
    }
  }

  /**
   * Implement method to set table and GraphQL table in the given Implementor object.
   *
   * @param implementor The Implementor object to set the table and GraphQL table in.
   */
  @Override public void implement(Implementor implementor) {
    LOGGER.debug("GraphQLTableScan.implement() called with projectedFields: {}", projectedFields);
    implementor.table = table;
    implementor.setGraphQLTable(graphQLTable);
  }

}
