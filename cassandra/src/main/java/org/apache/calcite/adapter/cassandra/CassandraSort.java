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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 * relational expression in Cassandra.
 */
public class CassandraSort extends Sort implements CassandraRel {
  public CassandraSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, RelCollation collation) {
    super(cluster, traitSet, child, collation, null, null);

    assert getConvention() == CassandraRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (!collation.getFieldCollations().isEmpty()) {
      return cost.multiplyBy(0.05);
    } else {
      return cost;
    }
  }

  @Override public Sort copy(RelTraitSet traitSet, RelNode input,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new CassandraSort(getCluster(), traitSet, input, collation);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    List<RelFieldCollation> sortCollations = collation.getFieldCollations();
    List<String> fieldOrder = new ArrayList<>();
    if (!sortCollations.isEmpty()) {
      // Construct a series of order clauses from the desired collation
      final List<RelDataTypeField> fields = getRowType().getFieldList();
      for (RelFieldCollation fieldCollation : sortCollations) {
        final String name =
            fields.get(fieldCollation.getFieldIndex()).getName();
        final String direction;
        switch (fieldCollation.getDirection()) {
        case DESCENDING:
          direction = "DESC";
          break;
        default:
          direction = "ASC";
        }
        fieldOrder.add(name + " " + direction);
      }

      implementor.addOrder(fieldOrder);
    }
  }
}

// End CassandraSort.java
