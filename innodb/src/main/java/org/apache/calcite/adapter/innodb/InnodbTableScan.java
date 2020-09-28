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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.commons.collections.CollectionUtils;

import com.alibaba.innodb.java.reader.Constants;
import com.alibaba.innodb.java.reader.schema.KeyMeta;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Relational expression representing a scan of an InnoDB data source.
 */
public class InnodbTableScan extends TableScan implements InnodbRel {
  final InnodbTable innodbTable;
  final RelDataType projectRowType;
  /** Force to use one specific index from hint. */
  private final Optional<String> forceIndexName;
  /** This contains index to scan table and optional condition. */
  private final IndexCondition indexCondition;

  protected InnodbTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, InnodbTable innodbTable, RelDataType projectRowType,
      List<RelHint> hints) {
    super(cluster, traitSet, hints, table);
    this.innodbTable = innodbTable;
    this.projectRowType = projectRowType;
    this.forceIndexName = getForceIndexName(hints);
    this.indexCondition = getIndexCondition();
    assert innodbTable != null;
    assert getConvention() == InnodbRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    HintStrategyTable strategies = HintStrategyTable.builder()
        .hintStrategy("index", HintPredicates.TABLE_SCAN)
        .build();
    getCluster().setHintStrategies(strategies);

    planner.addRule(InnodbRules.TO_ENUMERABLE);
    for (RelOptRule rule : InnodbRules.RULES) {
      planner.addRule(rule);
    }
  }

  public void implement(Implementor implementor) {
    implementor.innodbTable = innodbTable;
    implementor.table = table;
    implementor.setIndexCondition(indexCondition);
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (forceIndexName.isPresent()) {
      pw.itemIf("forceIndex", forceIndexName.get(), true);
    }
    return pw;
  }

  /**
   * Infer the implicit collation from index.
   *
   * @return the implicit collation based on the natural ordering of an index
   */
  public RelCollation getImplicitCollation() {
    return indexCondition.getImplicitCollation();
  }

  private Optional<String> getForceIndexName(final List<RelHint> hints) {
    if (CollectionUtils.isEmpty(hints)) {
      return Optional.empty();
    }
    for (RelHint hint : hints) {
      if ("index".equalsIgnoreCase(hint.hintName)) {
        if (CollectionUtils.isNotEmpty(hint.listOptions)) {
          Set<String> indexesNameSet = innodbTable.getIndexesNameSet();
          Optional<String> forceIndexName = hint.listOptions.stream().findFirst();
          if (!forceIndexName.isPresent()) {
            return Optional.empty();
          }
          for (String indexName : indexesNameSet) {
            if (indexName != null && indexName.equalsIgnoreCase(forceIndexName.get())) {
              return Optional.of(indexName);
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  public Optional<String> getForceIndexName() {
    return forceIndexName;
  }

  private IndexCondition getIndexCondition() {
    // force to use a secondary index to scan table if present
    if (forceIndexName.isPresent()
        && !forceIndexName.get().equalsIgnoreCase(Constants.PRIMARY_KEY_NAME)) {
      KeyMeta skMeta = innodbTable.getTableDef()
          .getSecondaryKeyMetaMap().get(forceIndexName.get());
      if (skMeta == null) {
        throw new AssertionError("secondary index not found " + forceIndexName.get());
      }
      return IndexCondition.create(InnodbRules.innodbFieldNames(getRowType()),
          forceIndexName.get(), skMeta.getKeyColumnNames(),
          QueryType.SK_FULL_SCAN);
    }
    // by default clustering index will be used to scan table
    return IndexCondition.create(InnodbRules.innodbFieldNames(getRowType()),
        Constants.PRIMARY_KEY_NAME,
        innodbTable.getTableDef().getPrimaryKeyColumnNames(),
        QueryType.PK_FULL_SCAN);
  }
}
