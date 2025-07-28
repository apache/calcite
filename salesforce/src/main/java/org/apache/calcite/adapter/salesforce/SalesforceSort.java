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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Sort} relational expression in Salesforce.
 */
public class SalesforceSort extends Sort implements SalesforceRel {

  public SalesforceSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert getConvention() == SalesforceRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public SalesforceSort copy(RelTraitSet traitSet, RelNode input,
      RelCollation collation, RexNode offset, RexNode fetch) {
    return new SalesforceSort(getCluster(), traitSet, input, collation,
        offset, fetch);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Salesforce can push down ORDER BY and LIMIT
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Build ORDER BY clause
    if (!collation.getFieldCollations().isEmpty()) {
      List<String> orderByItems = new ArrayList<>();
      RelDataType rowType = getInput().getRowType();

      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        String fieldName = rowType.getFieldList()
            .get(fieldCollation.getFieldIndex()).getName();
        String direction = fieldCollation.getDirection().isDescending() ? "DESC" : "ASC";

        // Handle nulls direction if specified
        String nullsDirection = "";
        switch (fieldCollation.nullDirection) {
        case FIRST:
          nullsDirection = " NULLS FIRST";
          break;
        case LAST:
          nullsDirection = " NULLS LAST";
          break;
        }

        orderByItems.add(fieldName + " " + direction + nullsDirection);
      }

      implementor.orderByClause = String.join(", ", orderByItems);
    }

    // Handle LIMIT
    if (fetch != null && fetch instanceof RexLiteral) {
      RexLiteral fetchLiteral = (RexLiteral) fetch;
      implementor.limitValue = fetchLiteral.getValueAs(Integer.class);
    }

    // Handle OFFSET
    if (offset != null && offset instanceof RexLiteral) {
      RexLiteral offsetLiteral = (RexLiteral) offset;
      implementor.offsetValue = offsetLiteral.getValueAs(Integer.class);
    }
  }
}
