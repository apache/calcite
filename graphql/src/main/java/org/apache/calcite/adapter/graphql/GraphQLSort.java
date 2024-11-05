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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of Sort relational operator for GraphQL queries.
 */
public class GraphQLSort extends Sort implements GraphQLRel {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLSort.class);

  public GraphQLSort(RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
    LOGGER.debug("Created GraphQLSort with collation: {}, offset: {}, fetch: {}",
        collation, offset, fetch);
  }

  @Override public Sort copy(RelTraitSet traitSet,
      RelNode newInput,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new GraphQLSort(getCluster(), traitSet, newInput,
        collation, offset, fetch);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitInput(0, getInput());

    // Add ordering information
    if (!collation.getFieldCollations().isEmpty()) {
      implementor.addOrder(collation);
    }

    // Add offset if present
    if (offset != null) {
      implementor.addOffset(offset);
    }

    // Add fetch/limit if present
    if (fetch != null) {
      implementor.addFetch(fetch);
    }
  }
}
