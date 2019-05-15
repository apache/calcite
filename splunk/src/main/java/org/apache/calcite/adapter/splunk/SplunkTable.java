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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.util.List;

/**
 * Table based on Splunk.
 */
class SplunkTable extends AbstractQueryableTable implements TranslatableTable {
  public static final SplunkTable INSTANCE = new SplunkTable();

  private SplunkTable() {
    super(Object[].class);
  }

  public String toString() {
    return "SplunkTable";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType stringType =
        ((JavaTypeFactory) typeFactory).createType(String.class);
    return typeFactory.builder()
        .add("source", stringType)
        .add("sourcetype", stringType)
        .add("_extra", stringType)
        .build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new SplunkTableQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new SplunkTableScan(
        context.getCluster(),
        relOptTable,
        this,
        "search",
        null,
        null,
        relOptTable.getRowType().getFieldNames());
  }

  /** Implementation of {@link Queryable} backed by a {@link SplunkTable}.
   * Generated code uses this get a Splunk connection for executing arbitrary
   * Splunk queries.
   *
   * @param <T> element type */
  public static class SplunkTableQueryable<T>
      extends AbstractTableQueryable<T> {
    SplunkTableQueryable(QueryProvider queryProvider, SchemaPlus schema,
        SplunkTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      final SplunkQuery<T> query = createQuery("search", null, null, null);
      return query.enumerator();
    }

    public SplunkQuery<T> createQuery(String search, String earliest,
        String latest, List<String> fieldList) {
      final SplunkSchema splunkSchema = schema.unwrap(SplunkSchema.class);
      return new SplunkQuery<>(splunkSchema.splunkConnection, search,
          earliest, latest, fieldList);
    }
  }
}

// End SplunkTable.java
