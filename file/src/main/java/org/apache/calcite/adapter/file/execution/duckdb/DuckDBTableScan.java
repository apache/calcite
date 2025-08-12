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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.file.table.DuckDBParquetTable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TableScan implementation that pushes queries to DuckDB for execution.
 * This ensures that DuckDB handles the query processing rather than Calcite.
 */
public class DuckDBTableScan extends TableScan implements EnumerableRel {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBTableScan.class);
  
  private final DuckDBParquetTable duckDBTable;
  private final String schemaName;
  private final String tableName;

  public DuckDBTableScan(RelOptCluster cluster, RelOptTable table, 
                         DuckDBParquetTable duckDBTable, String schemaName, String tableName) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    this.duckDBTable = duckDBTable;
    this.schemaName = schemaName;
    this.tableName = tableName;
    LOGGER.debug("Created DuckDBTableScan for {}.{}", schemaName, tableName);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DuckDBTableScan(getCluster(), table, duckDBTable, schemaName, tableName);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    // Use reasonable estimates based on table statistics if available
    // For now, delegate to parent implementation
    return super.estimateRowCount(mq);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // DuckDB scans should be low cost since they use vectorized execution
    double rowCount = estimateRowCount(mq);
    double cpu = rowCount * 0.001; // Very low CPU cost for DuckDB
    double io = rowCount * 0.0001;  // Low IO cost for columnar format
    return planner.getCostFactory().makeCost(rowCount, cpu, io);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

    // Create the enumerable expression that uses DuckDB for query execution
    BlockBuilder builder = new BlockBuilder();

    // Generate parameters for the DuckDBEnumerator
    Expression schemaNameExpr = Expressions.constant(schemaName);
    Expression tableNameExpr = Expressions.constant(tableName);
    Expression sqlExpr = Expressions.constant("SELECT * FROM " + schemaName + "." + tableName);

    LOGGER.info("*** USING DUCKDB FOR QUERY EXECUTION *** DuckDBTableScan implementing query: SELECT * FROM {}.{}", schemaName, tableName);

    // Call DuckDBEnumerator to execute the query
    Expression enumerable = builder.append("enumerable",
        Expressions.call(
            DuckDBEnumerator.class,
            "createEnumerable",
            schemaNameExpr,
            tableNameExpr, 
            sqlExpr));

    builder.add(Expressions.return_(null, enumerable));

    return implementor.result(physType, builder.toBlock());
  }
}