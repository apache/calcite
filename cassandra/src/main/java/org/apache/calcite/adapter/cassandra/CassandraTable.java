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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Table based on a Cassandra column family.
 */
public class CassandraTable extends AbstractQueryableTable
    implements TranslatableTable {
  final RelProtoDataType protoRowType;
  final List<String> partitionKeys;
  final List<String> clusteringKeys;
  final List<RelFieldCollation> clusteringOrder;
  private final Optional<String> keyspace;
  private final String columnFamily;

  @Deprecated // to be removed before 2.0
  public CassandraTable(CassandraSchema schema, String columnFamily, boolean isView) {
    super(Object[].class);
    this.keyspace = Optional.empty();
    this.columnFamily = columnFamily;
    this.protoRowType = schema.getRelDataType(columnFamily, isView);
    this.partitionKeys = schema.getPartitionKeys(columnFamily, isView);
    this.clusteringKeys = schema.getClusteringKeys(columnFamily, isView);
    this.clusteringOrder = schema.getClusteringOrder(columnFamily, isView);
  }

  public CassandraTable(
      CassandraSchema schema,
      String keyspace,
      String columnFamily,
      boolean isView) {
    super(Object[].class);
    this.keyspace = Optional.of(keyspace);
    this.columnFamily = columnFamily;
    this.protoRowType = schema.getRelDataType(columnFamily, isView);
    this.partitionKeys = schema.getPartitionKeys(columnFamily, isView);
    this.clusteringKeys = schema.getClusteringKeys(columnFamily, isView);
    this.clusteringOrder = schema.getClusteringOrder(columnFamily, isView);
  }

  @Deprecated // to be removed before 2.0
  public CassandraTable(CassandraSchema schema, String columnFamily) {
    this(schema, columnFamily, false);
  }

  public CassandraTable(CassandraSchema schema, String keyspace, String columnFamily) {
    this(schema, keyspace, columnFamily, false);
  }

  @Override public String toString() {
    return "CassandraTable {" + columnFamily + "}";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public List<String> getPartitionKeys() {
    return partitionKeys;
  }

  public List<String> getClusteringKeys() {
    return clusteringKeys;
  }

  public List<RelFieldCollation> getClusteringOrder() {
    return clusteringOrder;
  }

  public Enumerable<Object> query(final CqlSession session) {
    return query(session, ImmutableList.of(), ImmutableList.of(),
        ImmutableList.of(), ImmutableList.of(), 0, -1);
  }

  /** Executes a CQL query on the underlying table.
   *
   * @param session Cassandra session
   * @param fields List of fields to project
   * @param predicates A list of predicates which should be used in the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final CqlSession session, List<Map.Entry<String, Class>> fields,
        final List<Map.Entry<String, String>> selectFields, List<String> predicates,
        List<String> order, final Integer offset, final Integer fetch) {
    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    final RelDataType rowType = getRowType(typeFactory);

    Function1<String, Void> addField = fieldName -> {
      RelDataType relDataType =
          requireNonNull(rowType.getField(fieldName, true, false)).getType();
      fieldInfo.add(fieldName, relDataType).nullable(true);
      return null;
    };

    if (selectFields.isEmpty()) {
      for (Map.Entry<String, Class> field : fields) {
        addField.apply(field.getKey());
      }
    } else {
      for (Map.Entry<String, String> field : selectFields) {
        addField.apply(field.getKey());
      }
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    // Construct the list of fields to project
    final String selectString;
    if (selectFields.isEmpty()) {
      selectString = "*";
    } else {
      selectString = Util.toString(() -> {
        final Iterator<Map.Entry<String, String>> selectIterator =
            selectFields.iterator();

        return new Iterator<String>() {
          @Override public boolean hasNext() {
            return selectIterator.hasNext();
          }

          @Override public String next() {
            Map.Entry<String, String> entry = selectIterator.next();
            return entry.getKey() + " AS " + entry.getValue();
          }

          @Override public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }, "", ", ", "");
    }

    // Combine all predicates conjunctively
    String whereClause = "";
    if (!predicates.isEmpty()) {
      whereClause = " WHERE ";
      whereClause += Util.toString(predicates, "", " AND ", "");
    }

    // Build and issue the query and return an Enumerator over the results
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder
        .append(selectString)
        .append(" FROM \"");

    keyspace.ifPresent(s ->
        queryBuilder.append(s).append("\".\""));

    queryBuilder
        .append(columnFamily)
        .append("\"")
        .append(whereClause);

    if (!order.isEmpty()) {
      queryBuilder.append(Util.toString(order, " ORDER BY ", ", ", ""));
    }

    int limit = offset;
    if (fetch >= 0) {
      limit += fetch;
    }
    if (limit > 0) {
      queryBuilder.append(" LIMIT ")
          .append(limit);
    }
    queryBuilder.append(" ALLOW FILTERING");

    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        final ResultSet results = session.execute(queryBuilder.toString());
        // Skip results until we get to the right offset
        int skip = 0;
        Enumerator<Object> enumerator = new CassandraEnumerator(results, resultRowType);
        while (skip < offset && enumerator.moveNext()) {
          skip++;
        }
        return enumerator;
      }
    };
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new CassandraQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CassandraTableScan(cluster, cluster.traitSetOf(CassandraRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.cassandra.CassandraTable}.
   *
   * @param <T> element type */
  public static class CassandraQueryable<T> extends AbstractTableQueryable<T> {
    public CassandraQueryable(QueryProvider queryProvider, SchemaPlus schema,
        CassandraTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query(getSession());
      return enumerable.enumerator();
    }

    private CassandraTable getTable() {
      return (CassandraTable) table;
    }

    private CqlSession getSession() {
      return requireNonNull(schema.unwrap(CassandraSchema.class)).session;
    }

    /** Called via code-generation.
     *
     * @see org.apache.calcite.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
     */
    @SuppressWarnings("UnusedDeclaration")
    public @Nullable Enumerable<Object> query(List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields, List<String> predicates,
        List<String> order, Integer offset, Integer fetch) {
      return getTable().query(getSession(), fields, selectFields, predicates,
          order, offset, fetch);
    }
  }
}
