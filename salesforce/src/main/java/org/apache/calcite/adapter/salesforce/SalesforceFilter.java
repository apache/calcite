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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Implementation of {@link Filter} relational expression in Salesforce.
 */
public class SalesforceFilter extends Filter implements SalesforceRel {

  public SalesforceFilter(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode condition) {
    super(cluster, traitSet, input, condition);
    assert getConvention() == SalesforceRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public SalesforceFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new SalesforceFilter(getCluster(), traitSet, input, condition);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Salesforce can push down filters effectively
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Convert filter condition to SOQL WHERE clause
    String whereClause = SOQLBuilder.buildWhereClause(condition);

    if (implementor.whereClause == null) {
      implementor.whereClause = whereClause;
    } else {
      // Combine with existing WHERE clause
      implementor.whereClause = "(" + implementor.whereClause + ") AND (" + whereClause + ")";
    }
  }
}
