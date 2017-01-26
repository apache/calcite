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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.adapter.pig.PigRelFactories.PigAggregateFactory;
import org.apache.calcite.adapter.pig.PigRelFactories.PigFilterFactory;
import org.apache.calcite.adapter.pig.PigRelFactories.PigJoinFactory;
import org.apache.calcite.adapter.pig.PigRelFactories.PigTableScanFactory;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 */
public class PigRelFactories {

  // prevent instantiation
  private PigRelFactories() {
  }

  public static Context getAllPigRelFactories(Schema schema) {
    return Contexts.of(new PigFilterFactory(), new PigJoinFactory(), new PigAggregateFactory(),
        new PigTableScanFactory(schema));
  }

  /**
   */
  public static class PigTableScanFactory implements RelFactories.TableScanFactory {

    private final Schema schema;

    public PigTableScanFactory(Schema schema) {
      this.schema = schema;
    }

    @Override public RelNode createScan(RelOptCluster cluster, RelOptTable table) {
      PigTable pigTable = (PigTable) schema.getTable(table.getQualifiedName().get(0));
      return new PigTableScan(cluster, cluster.traitSetOf(PigRel.CONVENTION), table, pigTable);
    }
  }

  /**
   */
  public static class PigFilterFactory implements RelFactories.FilterFactory {

    @Override public RelNode createFilter(RelNode input, RexNode condition) {
      return new PigFilter(input.getCluster(), input.getTraitSet().replace(PigRel.CONVENTION),
          input, condition);
    }
  }

  /**
   */
  public static class PigAggregateFactory implements RelFactories.AggregateFactory {

    @Override public RelNode createAggregate(RelNode input, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
      return new PigAggregate(input.getCluster(), input.getTraitSet(), input, indicator, groupSet,
          groupSets, aggCalls);
    }
  }

  /**
   */
  public static class PigJoinFactory implements RelFactories.JoinFactory {

    @Override public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone) {
      return new PigJoin(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
    }

    @Override public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone) {
      return new PigJoin(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
    }
  }
}
// End PigRelFactories.java
