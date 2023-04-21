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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

/**
 * Default Implementation of TableCreate node.
 */
public class LogicalTableCreate extends TableCreate {

  private final Schema schema;

  private final List<String> schemaPath;
  private final String tableName;
  private final boolean isReplace;

  private final SqlCreateTable.CreateTableType createTableType;

  /**
   * Creates a LogicaltableCreate Node.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits the traits
   * @param input   Input relational expression
   */
  protected LogicalTableCreate(final RelOptCluster cluster, final RelTraitSet traits,
      final RelNode input, final Schema schema, final String tableName,
      final boolean isReplace, final SqlCreateTable.CreateTableType createTableType,
      final List<String> path) {
    super(cluster, traits, input);
    this.schema = schema;
    this.tableName = tableName;
    this.isReplace = isReplace;
    this.schemaPath = path;
    this.createTableType = createTableType;
  }

  protected LogicalTableCreate(final RelOptCluster cluster, final RelTraitSet traits,
      final RelNode input, final Schema schema, final String tableName,
      final boolean isReplace,
      final List<String> path) {
    super(cluster, traits, input);
    this.schema = schema;
    this.tableName = tableName;
    this.isReplace = isReplace;
    this.schemaPath = path;
    this.createTableType = SqlCreateTable.CreateTableType.DEFAULT;
  }

  /** Creates a LogicalTableModify. */
  public static LogicalTableCreate create(final RelNode input,
      final Schema schema, final String tableName,
      final boolean isReplace, final SqlCreateTable.CreateTableType createTableType,
      final List<String> path) {

    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableCreate(cluster, traitSet, input, schema, tableName,
        isReplace, createTableType, path);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("TableName", this.tableName)
        .item("Target Schema", this.schemaPath)
        .item("IsReplace", this.isReplace)
        .item("CreateTableType", this.createTableType);
  }

  public Schema getSchema() {
    return schema;
  }

  public String getTableName() {
    return tableName;
  }

  public SqlCreateTable.CreateTableType getCreateTableType() {
    return createTableType;
  }

  public boolean isReplace() {
    return isReplace;
  }

  @Override public LogicalTableCreate copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.size() == 1;
    return new LogicalTableCreate(
        getCluster(), traitSet, inputs.get(0), this.schema, this.tableName,
        this.isReplace, this.createTableType, this.schemaPath);
  }

  public List<String> getSchemaPath() {
    return schemaPath;
  }

}
