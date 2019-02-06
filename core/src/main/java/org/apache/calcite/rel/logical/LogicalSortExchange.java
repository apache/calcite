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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.SortExchange} not
 * targeted at any particular engine or calling convention.
 */
public class LogicalSortExchange extends SortExchange {
  private LogicalSortExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, RelCollation collation) {
    super(cluster, traitSet, input, distribution, collation);
  }

  /**
   * Creates a LogicalSortExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param collation array of sort specifications
   */
  public static LogicalSortExchange create(
      RelNode input,
      RelDistribution distribution,
      RelCollation collation) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(distribution).replace(collation);
    return new LogicalSortExchange(cluster, traitSet, input, distribution,
        collation);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SortExchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution, RelCollation newCollation) {
    return new LogicalSortExchange(this.getCluster(), traitSet, newInput,
        newDistribution, newCollation);
  }
}

// End LogicalSortExchange.java
