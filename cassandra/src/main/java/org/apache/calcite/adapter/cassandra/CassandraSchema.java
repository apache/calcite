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
import org.apache.calcite.rel.type.RelDataType;
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
import org.apache.calcite.sql.SqlWriterConfig;
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
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Schema mapped onto a Cassandra column family
 */
public class CassandraSchema extends AbstractSchema {
  final Session session;
  final String keyspace;
  private final SchemaPlus parentSchema;
  final String name;
  final Hook.Closeable hook;

  static final CodecRegistry CODEC_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;
  static final CqlToSqlTypeConversionRules CQL_TO_SQL_TYPE =
      CqlToSqlTypeConversionRules.instance();

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
    this(host, DEFAULT_CASSANDRA_PORT, keyspace, username, password, parentSchema, name);
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
      final SqlTypeName typeName =
          CQL_TO_SQL_TYPE.lookup(column.getType().getName());

      switch (typeName) {
      case ARRAY:
        final SqlTypeName arrayInnerType = CQL_TO_SQL_TYPE.lookup(
            column.getType().getTypeArguments().get(0).getName());

        fieldInfo.add(column.getName(),
            typeFactory.createArrayType(
                typeFactory.createSqlType(arrayInnerType), -1))
            .nullable(true);

        break;
      case MULTISET:
        final SqlTypeName multiSetInnerType = CQL_TO_SQL_TYPE.lookup(
            column.getType().getTypeArguments().get(0).getName());

        fieldInfo.add(column.getName(),
            typeFactory.createMultisetType(
                typeFactory.createSqlType(multiSetInnerType), -1)
        ).nullable(true);

        break;
      case MAP:
        final List<DataType> types = column.getType().getTypeArguments();
        final SqlTypeName keyType =
            CQL_TO_SQL_TYPE.lookup(types.get(0).getName());
        final SqlTypeName valueType =
            CQL_TO_SQL_TYPE.lookup(types.get(1).getName());

        fieldInfo.add(column.getName(),
            typeFactory.createMapType(
                typeFactory.createSqlType(keyType),
                typeFactory.createSqlType(valueType))
        ).nullable(true);

        break;
      case STRUCTURED:
        assert DataType.Name.TUPLE == column.getType().getName();

        final List<DataType> typeArgs =
            ((TupleType) column.getType()).getComponentTypes();
        final List<Map.Entry<String, RelDataType>> typesList =
            IntStream.range(0, typeArgs.size())
                .mapToObj(
                    i -> new Pair<>(
                        Integer.toString(i + 1), // 1 indexed (as ARRAY)
                        typeFactory.createSqlType(
                            CQL_TO_SQL_TYPE.lookup(typeArgs.get(i).getName()))))
                .collect(Collectors.toList());

        fieldInfo.add(column.getName(),
            typeFactory.createStructType(typesList))
            .nullable(true);

        break;
      default:
        fieldInfo.add(column.getName(), typeName).nullable(true);

        break;
      }
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

      queryBuilder.append(" FROM \"")
          .append(tableName)
          .append("\"");

      // Get the where clause from the system schema
      String whereQuery = "SELECT where_clause from system_schema.views "
          + "WHERE keyspace_name='" + keyspace + "' AND view_name='" + view.getName() + "'";
      queryBuilder.append(" WHERE ")
          .append(session.execute(whereQuery).one().getString(0));

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

      final StringBuilder buf = new StringBuilder(query.length());
      final SqlWriterConfig config = SqlPrettyWriter.config()
          .withAlwaysUseParentheses(true);
      final SqlWriter writer = new SqlPrettyWriter(config, buf);
      parsedQuery.unparse(writer, 0, 0);
      query = buf.toString();

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
