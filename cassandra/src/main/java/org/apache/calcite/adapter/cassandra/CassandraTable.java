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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractQueryableTable
    implements TranslatableTable {
  RelProtoDataType protoRowType;
  Pair<List<String>, List<String>> keyFields;
  List<RelFieldCollation> clusteringOrder;
  private final CassandraSchema schema;
  private final String columnFamily;

  public CassandraTable(CassandraSchema schema, String columnFamily) {
    super(Object[].class);
    this.schema = schema;
    this.columnFamily = columnFamily;
  }

  public String toString() {
    return "CassandraTable {" + columnFamily + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType == null) {
      protoRowType = schema.getRelDataType(columnFamily);
    }
    return protoRowType.apply(typeFactory);
  }

  public Pair<List<String>, List<String>> getKeyFields() {
    if (keyFields == null) {
      keyFields = schema.getKeyFields(columnFamily);
    }
    return keyFields;
  }

  public List<RelFieldCollation> getClusteringOrder() {
    if (clusteringOrder == null) {
      clusteringOrder = schema.getClusteringOrder(columnFamily);
    }
    return clusteringOrder;
  }

  public Enumerable<Object> query(final Session session) {
    return query(session, Collections.<Map.Entry<String, Class>>emptyList(),
        Collections.<String>emptyList(), Collections.<String>emptyList(), null);
  }

  /** Executes a CQL query on the underlying table.
   *
   * @param session Cassandra session
   * @param fields List of fields to project
   * @param predicates A list of predicates which should be used in the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final Session session, List<Map.Entry<String, Class>> fields,
        List<String> predicates, List<String> order, String limit) {
    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
    final RelDataType rowType = protoRowType.apply(typeFactory);
    List<String> fieldNames = new ArrayList<String>();
    for (Map.Entry<String, Class> field : fields) {
      String fieldName = field.getKey();
      fieldNames.add(fieldName);
      SqlTypeName typeName = rowType.getField(fieldName, true, false).getType().getSqlTypeName();
      fieldInfo.add(fieldName, typeFactory.createSqlType(typeName)).nullable(true);
    }
    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    // Construct the list of fields to project
    final String selectFields;
    if (fields.isEmpty()) {
      selectFields = "*";
    } else {
      selectFields = Util.toString(fieldNames, "", ", ", "");
    }

    // Combine all predicates conjunctively
    String whereClause = "";
    if (!predicates.isEmpty()) {
      whereClause = " WHERE ";
      whereClause += Util.toString(predicates, "", " AND ", "");
    }

    // Build and issue the query and return an Enumerator over the results
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder.append(selectFields);
    queryBuilder.append(" FROM \"" + columnFamily + "\"");
    queryBuilder.append(whereClause);
    if (!order.isEmpty()) {
      queryBuilder.append(Util.toString(order, " ORDER BY ", ", ", ""));
    }
    if (limit != null) {
      queryBuilder.append(" LIMIT " + limit);
    }
    queryBuilder.append(" ALLOW FILTERING");
    final String query = queryBuilder.toString();

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final ResultSet results = session.execute(query);
        return new CassandraEnumerator(results, resultRowType);
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new CassandraQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CassandraTableScan(cluster, cluster.traitSetOf(CassandraRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.cassandra.CassandraTable}. */
  public static class CassandraQueryable<T> extends AbstractTableQueryable<T> {
    public CassandraQueryable(QueryProvider queryProvider, SchemaPlus schema,
        CassandraTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query(getSession());
      return enumerable.enumerator();
    }

    private CassandraTable getTable() {
      return (CassandraTable) table;
    }

    private Session getSession() {
      return schema.unwrap(CassandraSchema.class).session;
    }

    /** Called via code-generation.
     *
     * @see org.apache.calcite.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields,
        List<String> predicates, List<String> order, String limit) {
      return getTable().query(getSession(), fields, predicates, order, limit);
    }
  }
}

// End CassandraTable.java
