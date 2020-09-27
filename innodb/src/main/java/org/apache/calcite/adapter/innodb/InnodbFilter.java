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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.alibaba.innodb.java.reader.schema.TableDef;

import java.util.Optional;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression for an InnoDB data source.
 */
public class InnodbFilter extends Filter implements InnodbRel {
  private final TableDef tableDef;
  private final IndexCondition indexCondition;
  private final Optional<String> forceIndexName;

  public InnodbFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition,
      TableDef tableDef,
      Optional<String> forceIndexName) {
    super(cluster, traitSet, child, condition);

    this.tableDef = tableDef;
    InnodbFilterTranslator translator =
        new InnodbFilterTranslator(cluster.getRexBuilder(), getRowType(),
            tableDef, forceIndexName);
    this.indexCondition = translator.translateMatch(condition);
    this.forceIndexName = forceIndexName;

    assert getConvention() == InnodbRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public InnodbFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new InnodbFilter(getCluster(), traitSet, input, condition, tableDef, forceIndexName);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.setIndexCondition(indexCondition);
  }

  public RelWriter explainTerms(RelWriter pw) {
    pw.input("input", getInput());
    pw.itemIf("condition", indexCondition, canPushDownCondition());
    return pw;
  }

  boolean canPushDownCondition() {
    return indexCondition != null && indexCondition.canPushDown();
  }

  IndexCondition getPushDownCondition() {
    return indexCondition;
  }

  /**
   * Get the resulting collation by the primary or secondary
   * indexes after filtering.
   *
   * @return the implicit collation based on the natural sorting by specific index
   */
  public RelCollation getImplicitCollation() {
    return indexCondition.getImplicitCollation();
  }
}
