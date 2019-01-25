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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a Cassandra column family
 */
public class CassandraSchema extends AbstractSchema {
  final Session session;
  final String keyspace;
  private final SchemaPlus parentSchema;
  final String name;
  final Hook.Closeable hook;

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private static final int DEFAULT_CASSANDRA_PORT = 9042;

  /**
   * Creates a Cassandra schema.
   *
   * @param host Cassandra host, e.g. "localhost"
   * @param keyspace Cassandra keyspace name, e.g. "twissandra"
   */
  public CassandraSchema(String host, String keyspace, SchemaPlus parentSchema, String name) {
    this(host, DEFAULT_CASSANDRA_PORT, keyspace, null, null, parentSchema, name);
  }

  /**
   * Creates a Cassandra schema.
   *
   * @param host Cassandra host, e.g. "localhost"
   * @param port Cassandra port, e.g. 9042
   * @param keyspace Cassandra keyspace name, e.g. "twissandra"
   */
  public CassandraSchema(String host, int port, String keyspace,
          SchemaPlus parentSchema, String name) {
    this(host, port, keyspace, null, null, parentSchema, name);
  }

  /**
   * Creates a Cassandra schema.
   *
   * @param host Cassandra host, e.g. "localhost"
   * @param keyspace Cassandra keyspace name, e.g. "twissandra"
   * @param username Cassandra username
   * @param password Cassandra password
   */
  public CassandraSchema(String host, String keyspace, String username, String password,
        SchemaPlus parentSchema, String name) {
    this(host, DEFAULT_CASSANDRA_PORT, keyspace, null, null, parentSchema, name);
  }

  /**
   * Creates a Cassandra schema.
   *
   * @param host Cassandra host, e.g. "localhost"
   * @param port Cassandra port, e.g. 9042
   * @param keyspace Cassandra keyspace name, e.g. "twissandra"
   * @param username Cassandra username
   * @param password Cassandra password
   */
  public CassandraSchema(String host, int port, String keyspace, String username, String password,
        SchemaPlus parentSchema, String name) {
    super();

    this.keyspace = keyspace;
    try {
      Cluster cluster;
      List<InetSocketAddress> contactPoints = new ArrayList<>(1);
      contactPoints.add(new InetSocketAddress(host, port));
      if (username != null && password != null) {
        cluster = Cluster.builder().addContactPointsWithPorts(contactPoints)
            .withCredentials(username, password).build();
      } else {
        cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
      }

      this.session = cluster.connect(keyspace);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.parentSchema = parentSchema;
    this.name = name;

    this.hook = Hook.TRIMMED.add(node -> {
      CassandraSchema.this.addMaterializedViews();
    });
  }

  RelProtoDataType getRelDataType(String columnFamily, boolean view) {
    List<ColumnMetadata> columns;
    if (view) {
      columns = getKeyspace().getMaterializedView("\"" + columnFamily + "\"").getColumns();
    } else {
      columns = getKeyspace().getTable("\"" + columnFamily + "\"").getColumns();
    }

    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    for (ColumnMetadata column : columns) {
      final String columnName = column.getName();
      final DataType type = column.getType();

      // TODO: This mapping of types can be done much better
      SqlTypeName typeName = SqlTypeName.ANY;
      if (type == DataType.uuid() || type == DataType.timeuuid()) {
        // We currently rely on this in CassandraFilter to detect UUID columns.
        // That is, these fixed length literals should be unquoted in CQL.
        typeName = SqlTypeName.CHAR;
      } else if (type == DataType.ascii() || type == DataType.text()
            || type == DataType.varchar()) {
        typeName = SqlTypeName.VARCHAR;
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
  Pair<List<String>, List<String>> getKeyFields(String columnFamily, boolean view) {
    AbstractTableMetadata table;
    if (view) {
      table = getKeyspace().getMaterializedView("\"" + columnFamily + "\"");
    } else {
      table = getKeyspace().getTable("\"" + columnFamily + "\"");
    }

    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    List<String> pKeyFields = new ArrayList<>();
    for (ColumnMetadata column : partitionKey) {
      pKeyFields.add(column.getName());
    }

    List<ColumnMetadata> clusteringKey = table.getClusteringColumns();
    List<String> cKeyFields = new ArrayList<>();
    for (ColumnMetadata column : clusteringKey) {
      cKeyFields.add(column.getName());
    }

    return Pair.of(ImmutableList.copyOf(pKeyFields),
        ImmutableList.copyOf(cKeyFields));
  }

  /** Get the collation of all clustering key columns.
   *
   * @return A RelCollations representing the collation of all clustering keys
   */
  public List<RelFieldCollation> getClusteringOrder(String columnFamily, boolean view) {
    AbstractTableMetadata table;
    if (view) {
      table = getKeyspace().getMaterializedView("\"" + columnFamily + "\"");
    } else {
      table = getKeyspace().getTable("\"" + columnFamily + "\"");
    }

    List<ClusteringOrder> clusteringOrder = table.getClusteringOrder();
    List<RelFieldCollation> keyCollations = new ArrayList<>();

    int i = 0;
    for (ClusteringOrder order : clusteringOrder) {
      RelFieldCollation.Direction direction;
      switch (order) {
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

  /** Add all materialized views defined in the schema to this column family
   */
  private void addMaterializedViews() {
    // Close the hook use to get us here
    hook.close();

    for (MaterializedViewMetadata view : getKeyspace().getMaterializedViews()) {
      String tableName = view.getBaseTable().getName();
      StringBuilder queryBuilder = new StringBuilder("SELECT ");

      // Add all the selected columns to the query
      List<String> columnNames = new ArrayList<>();
      for (ColumnMetadata column : view.getColumns()) {
        columnNames.add("\"" + column.getName() + "\"");
      }
      queryBuilder.append(Util.toString(columnNames, "", ", ", ""));

      queryBuilder.append(" FROM \"" + tableName + "\"");

      // Get the where clause from the system schema
      String whereQuery = "SELECT where_clause from system_schema.views "
          + "WHERE keyspace_name='" + keyspace + "' AND view_name='" + view.getName() + "'";
      queryBuilder.append(" WHERE " + session.execute(whereQuery).one().getString(0));

      // Parse and unparse the view query to get properly quoted field names
      String query = queryBuilder.toString();
      SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
      configBuilder.setUnquotedCasing(Casing.UNCHANGED);

      SqlSelect parsedQuery;
      try {
        parsedQuery = (SqlSelect) SqlParser.create(query, configBuilder.build()).parseQuery();
      } catch (SqlParseException e) {
        LOGGER.warn("Could not parse query {} for CQL view {}.{}",
            query, keyspace, view.getName());
        continue;
      }

      StringWriter stringWriter = new StringWriter(query.length());
      PrintWriter printWriter = new PrintWriter(stringWriter);
      SqlWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT, true, printWriter);
      parsedQuery.unparse(writer, 0, 0);
      query = stringWriter.toString();

      // Add the view for this query
      String viewName = "$" + getTableNames().size();
      SchemaPlus schema = parentSchema.getSubSchema(name);
      CalciteSchema calciteSchema = CalciteSchema.from(schema);

      List<String> viewPath = calciteSchema.path(viewName);

      schema.add(viewName,
            MaterializedViewTable.create(calciteSchema, query,
            null, viewPath, view.getName(), true));
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TableMetadata table : getKeyspace().getTables()) {
      String tableName = table.getName();
      builder.put(tableName, new CassandraTable(this, tableName));

      for (MaterializedViewMetadata view : table.getViews()) {
        String viewName = view.getName();
        builder.put(viewName, new CassandraTable(this, viewName, true));
      }
    }
    return builder.build();
  }

  private KeyspaceMetadata getKeyspace() {
    return session.getCluster().getMetadata().getKeyspace(keyspace);
  }
}

// End CassandraSchema.java
