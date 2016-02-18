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

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a Cassandra column family
 */
public class CassandraSchema extends AbstractSchema {
  final Session session;
  final String keyspace;

  /**
   * Creates a Cassandra schema.
   *
   * @param host Cassandra host, e.g. "localhost"
   * @param keyspace Cassandra keyspace name, e.g. "twissandra"
   */
  public CassandraSchema(String host, String keyspace) {
    super();

    this.keyspace = keyspace;
    try {
      Cluster cluster = Cluster.builder().addContactPoint(host).build();
      this.session = cluster.connect(keyspace);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  RelProtoDataType getRelDataType(String columnFamily) {
    List<ColumnMetadata> columns = getKeyspace().getTable(columnFamily).getColumns();

    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
    for (ColumnMetadata column : columns) {
      final String columnName = column.getName();
      final DataType type = column.getType();

      // TODO: This mapping of types can be done much better
      SqlTypeName typeName = SqlTypeName.ANY;
      if (type == DataType.ascii() || type == DataType.text() || type == DataType.varchar()
            || type == DataType.uuid() || type == DataType.timeuuid()) {
        typeName = SqlTypeName.CHAR;
      } else if (type == DataType.cint() || type == DataType.varint()) {
        typeName = SqlTypeName.INTEGER;
      } else if (type == DataType.bigint()) {
        typeName = SqlTypeName.BIGINT;
      } else if (type == DataType.cdouble() || type == DataType.cfloat()
          || type == DataType.decimal()) {
        typeName = SqlTypeName.DOUBLE;
      }

      fieldInfo.add(columnName, typeFactory.createSqlType(typeName)).nullable(true);
    }

    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  /**
   * Get all primary key columns from the underlying CQL table
   *
   * @return A list of field names that are part of the partition and clustering keys
   */
  Pair<List<String>, List<String>> getKeyFields(String columnFamily) {
    TableMetadata table = getKeyspace().getTable(columnFamily);

    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    List<String> pKeyFields = new ArrayList<String>();
    for (ColumnMetadata column : partitionKey) {
      pKeyFields.add(column.getName());
    }

    List<ColumnMetadata> clusteringKey = table.getClusteringColumns();
    List<String> cKeyFields = new ArrayList<String>();
    for (ColumnMetadata column : clusteringKey) {
      cKeyFields.add(column.getName());
    }

    return Pair.of((List<String>) ImmutableList.copyOf(pKeyFields),
        (List<String>) ImmutableList.copyOf(cKeyFields));
  }

  /** Get the collation of all clustering key columns.
   *
   * @return A RelCollations representing the collation of all clustering keys
   */
  public List<RelFieldCollation> getClusteringOrder(String columnFamily) {
    TableMetadata table = getKeyspace().getTable(columnFamily);
    List<TableMetadata.Order> clusteringOrder = table.getClusteringOrder();
    List<RelFieldCollation> keyCollations = new ArrayList<RelFieldCollation>();

    int i = 0;
    for (TableMetadata.Order order : clusteringOrder) {
      RelFieldCollation.Direction direction;
      switch(order) {
      case DESC:
        direction = RelFieldCollation.Direction.DESCENDING;
        break;
      case ASC:
      default:
        direction = RelFieldCollation.Direction.ASCENDING;
        break;
      }
      keyCollations.add(new RelFieldCollation(i, direction));
      i++;
    }

    return keyCollations;
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TableMetadata table : getKeyspace().getTables()) {
      String tableName = table.getName();
      builder.put(tableName, new CassandraTable(this, tableName));
    }
    return builder.build();
  }

  private KeyspaceMetadata getKeyspace() {
    return session.getCluster().getMetadata().getKeyspace(keyspace);
  }
}

// End CassandraSchema.java
