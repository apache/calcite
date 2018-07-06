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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

/**
 * Table based on an Elasticsearch type.
 */
public abstract class AbstractElasticsearchTable extends AbstractQueryableTable
    implements TranslatableTable {
  protected final String indexName;
  protected final String typeName;

  /**
   * Creates an ElasticsearchTable.
   */
  public AbstractElasticsearchTable(String indexName, String typeName) {
    super(Object[].class);
    this.indexName = indexName;
    this.typeName = typeName;
  }

  @Override public String toString() {
    return "ElasticsearchTable{" + typeName + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    final RelDataType mapType = relDataTypeFactory.createMapType(
        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
        relDataTypeFactory.createTypeWithNullability(
            relDataTypeFactory.createSqlType(SqlTypeName.ANY),
            true));
    return relDataTypeFactory.builder().add("_MAP", mapType).build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new ElasticsearchQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new ElasticsearchTableScan(cluster, cluster.traitSetOf(ElasticsearchRel.CONVENTION),
        relOptTable, this, null);
  }

  /**
   * In ES 5.x scripted fields start with {@code params._source.foo} while in ES2.x
   * {@code _source.foo}. Helper method to build correct query based on runtime version of elastic.
   *
   * @see <a href="https://github.com/elastic/elasticsearch/issues/20068">_source variable</a>
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting-fields.html">Scripted Fields</a>
   */
  protected String scriptedFieldPrefix() {
    // this is default pattern starting 5.x
    return "params._source";
  }

  /** Executes a "find" operation on the underlying type.
   *
   * <p>For example,
   * <code>client.prepareSearch(index).setTypes(type)
   * .setSource("{\"fields\" : [\"state\"]}")</code></p>
   *
   * @param index Elasticsearch index
   * @param ops List of operations represented as Json strings.
   * @param fields List of fields to project; or null to return map
   * @return Enumerator of results
   */
  protected abstract Enumerable<Object> find(String index, List<String> ops,
      List<Map.Entry<String, Class>> fields);

  /**
   * Implementation of {@link Queryable} based on
   * a {@link AbstractElasticsearchTable}.
   *
   * @param <T> element type
   */
  public static class ElasticsearchQueryable<T> extends AbstractTableQueryable<T> {
    public ElasticsearchQueryable(QueryProvider queryProvider, SchemaPlus schema,
        AbstractElasticsearchTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      return null;
    }

    private String getIndex() {
      return schema.unwrap(ElasticsearchSchema.class).getIndex();
    }

    private AbstractElasticsearchTable getTable() {
      return (AbstractElasticsearchTable) table;
    }

    /** Called via code-generation.
     *
     * @see ElasticsearchMethod#ELASTICSEARCH_QUERYABLE_FIND
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(List<String> ops,
        List<Map.Entry<String, Class>> fields) {
      return getTable().find(getIndex(), ops, fields);
    }
  }
}

// End AbstractElasticsearchTable.java
