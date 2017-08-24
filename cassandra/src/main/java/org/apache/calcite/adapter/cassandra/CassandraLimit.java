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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Implementation of limits in Cassandra.
 */
public class CassandraLimit extends SingleRel implements CassandraRel {
  public final RexNode offset;
  public final RexNode fetch;

  public CassandraLimit(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() == input.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // We do this so we get the limit for free
    return planner.getCostFactory().makeZeroCost();
  }

  @Override public CassandraLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new CassandraLimit(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (offset != null) {
      implementor.offset = RexLiteral.intValue(offset);
    }
    if (fetch != null) {
      implementor.fetch = RexLiteral.intValue(fetch);
    }
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("fetch", fetch, fetch != null);
    return pw;
  }
}

// End CassandraLimit.java
