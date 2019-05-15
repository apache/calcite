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
package org.apache.calcite.adapter.elasticsearch;

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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchSort extends Sort implements ElasticsearchRel {
  ElasticsearchSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, collation, offset, fetch);
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override public Sort copy(RelTraitSet traitSet, RelNode relNode, RelCollation relCollation,
      RexNode offset, RexNode fetch) {
    return new ElasticsearchSort(getCluster(), traitSet, relNode, collation, offset, fetch);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    final List<RelDataTypeField> fields = getRowType().getFieldList();

    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      final String name = fields.get(fieldCollation.getFieldIndex()).getName();
      final String rawName = implementor.expressionItemMap.getOrDefault(name, name);
      implementor.addSort(rawName, fieldCollation.getDirection());
    }

    if (offset != null) {
      implementor.offset(((RexLiteral) offset).getValueAs(Long.class));
    }

    if (fetch != null) {
      implementor.fetch(((RexLiteral) fetch).getValueAs(Long.class));
    }
  }

}

// End ElasticsearchSort.java
