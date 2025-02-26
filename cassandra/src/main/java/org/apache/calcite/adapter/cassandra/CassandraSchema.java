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
import org.apache.calcite.util.trace.CalciteTrace;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * Schema mapped onto a Cassandra column family.
 */
public class CassandraSchema extends AbstractSchema {
  final CqlSession session;
  final String keyspace;
  private final SchemaPlus parentSchema;
  final String name;
  final Hook.Closeable hook;

  static final CqlToSqlTypeConversionRules CQL_TO_SQL_TYPE =
      CqlToSqlTypeConversionRules.instance();

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /**
   * Creates a Cassandra schema.
   *
   * @param session a Cassandra session
   * @param parentSchema the parent schema
   * @param name the schema name
   */
  public CassandraSchema(CqlSession session, SchemaPlus parentSchema, String name) {
    this(
        session,
        parentSchema,
        session.getKeyspace()
          .orElseThrow(() -> new RuntimeException("No keyspace for session " + session.getName()))
          .asInternal(),
        name);
  }

  /**
   * Creates a Cassandra schema.
   *
   * @param session a Cassandra session
   * @param parentSchema the parent schema
   * @param keyspace the keyspace name
   * @param name the schema name
   */
  public CassandraSchema(
      CqlSession session,
      SchemaPlus parentSchema,
      String keyspace,
      String name) {
    super();

    this.session = session;
    this.keyspace = keyspace;
    this.parentSchema = parentSchema;
    this.name = name;
    this.hook = prepareHook();
  }

  @SuppressWarnings("deprecation")
  private Hook.Closeable prepareHook() {
    // It adds a global hook, so it should probably be replaced with a thread-local hook
    return Hook.TRIMMED.add(node -> {
      CassandraSchema.this.addMaterializedViews();
    });
  }

  RelProtoDataType getRelDataType(String columnFamily, boolean view) {
    Map<CqlIdentifier, ColumnMetadata> columns;
    CqlIdentifier tableName = CqlIdentifier.fromInternal(columnFamily);
    if (view) {
      Optional<ViewMetadata> optionalViewMetadata = getKeyspace().getView(tableName);
      if (optionalViewMetadata.isPresent()) {
        columns = optionalViewMetadata.get().getColumns();
      } else {
        throw new IllegalStateException("Unknown view " + tableName + " in keyspace " + keyspace);
      }
    } else {
      Optional<TableMetadata> optionalTableMetadata = getKeyspace().getTable(tableName);
      if (optionalTableMetadata.isPresent()) {
        columns = optionalTableMetadata.get().getColumns();
      } else {
        throw new IllegalStateException("Unknown table " + tableName + " in keyspace " + keyspace);
      }
    }

    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    for (ColumnMetadata column : columns.values()) {
      final DataType dataType = column.getType();
      final String columnName = column.getName().asInternal();

      if (dataType instanceof ListType) {
        SqlTypeName arrayInnerType =
            CQL_TO_SQL_TYPE.lookup(((ListType) dataType).getElementType());

        fieldInfo.add(columnName,
                typeFactory.createArrayType(
                    typeFactory.createSqlType(arrayInnerType), -1))
            .nullable(true);
      } else if (dataType instanceof SetType) {
        SqlTypeName multiSetInnerType =
            CQL_TO_SQL_TYPE.lookup(((SetType) dataType).getElementType());

        fieldInfo.add(columnName,
                typeFactory.createMultisetType(
                    typeFactory.createSqlType(multiSetInnerType), -1))
            .nullable(true);
      } else if (dataType instanceof MapType) {
        MapType columnType = (MapType) dataType;
        SqlTypeName keyType = CQL_TO_SQL_TYPE.lookup(columnType.getKeyType());
        SqlTypeName valueType = CQL_TO_SQL_TYPE.lookup(columnType.getValueType());

        fieldInfo.add(columnName,
                typeFactory.createMapType(
                    typeFactory.createSqlType(keyType),
                    typeFactory.createSqlType(valueType)))
            .nullable(true);
      } else if (dataType instanceof TupleType) {
        List<DataType> typeArgs = ((TupleType) dataType).getComponentTypes();
        List<Map.Entry<String, RelDataType>> typesList =
            IntStream.range(0, typeArgs.size())
                .mapToObj(
                    i -> new Pair<>(
                        Integer.toString(i + 1), // 1 indexed (as ARRAY)
                        typeFactory.createSqlType(
                            CQL_TO_SQL_TYPE.lookup(typeArgs.get(i)))))
                .collect(Collectors.toList());

        fieldInfo.add(columnName,
                typeFactory.createStructType(typesList))
            .nullable(true);
      } else {
        SqlTypeName typeName = CQL_TO_SQL_TYPE.lookup(dataType);
        fieldInfo.add(columnName, typeName).nullable(true);
      }
    }

    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  /** Returns the partition key columns from the underlying CQL table.
   *
   * @return A list of field names that are part of the partition keys
   */
  List<String> getPartitionKeys(String columnFamily, boolean isView) {
    RelationMetadata table = getRelationMetadata(columnFamily, isView);
    return table.getPartitionKey().stream()
        .map(ColumnMetadata::getName)
        .map(CqlIdentifier::asInternal)
        .collect(Collectors.toList());
  }

  /** Returns the clustering keys from the underlying CQL table.
   *
   * @return A list of field names that are part of the clustering keys
   */
  List<String> getClusteringKeys(String columnFamily, boolean isView) {
    RelationMetadata table = getRelationMetadata(columnFamily, isView);
    return table.getClusteringColumns().keySet().stream()
        .map(ColumnMetadata::getName)
        .map(CqlIdentifier::asInternal)
        .collect(Collectors.toList());
  }

  /** Get the collation of all clustering key columns.
   *
   * @return A RelCollations representing the collation of all clustering keys
   */
  public List<RelFieldCollation> getClusteringOrder(String columnFamily, boolean isView) {
    RelationMetadata table = getRelationMetadata(columnFamily, isView);
    Collection<ClusteringOrder> clusteringOrder = table.getClusteringColumns().values();
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

  private RelationMetadata getRelationMetadata(String columnFamily, boolean isView) {
    String tableName = CqlIdentifier.fromInternal(columnFamily).asCql(false);

    if (isView) {
      return getKeyspace().getView(tableName)
          .orElseThrow(
              () -> new RuntimeException(
              "Unknown view " + columnFamily + " in keyspace " + keyspace));
    }
    return getKeyspace().getTable(tableName)
        .orElseThrow(
            () -> new RuntimeException(
            "Unknown table " + columnFamily + " in keyspace " + keyspace));
  }

  /** Adds all materialized views defined in the schema to this column family. */
  private void addMaterializedViews() {
    // Close the hook used to get us here
    hook.close();

    for (ViewMetadata view : getKeyspace().getViews().values()) {
      String tableName = view.getBaseTable().asInternal();
      StringBuilder queryBuilder = new StringBuilder("SELECT ");

      // Add all the selected columns to the query
      String columnsList = view.getColumns().values().stream()
          .map(c -> c.getName().asInternal())
          .collect(Collectors.joining(", "));
      queryBuilder.append(columnsList);

      queryBuilder.append(" FROM ")
          .append(tableName);

      // Get the where clause from the system schema
      String whereQuery = "SELECT where_clause from system_schema.views "
          + "WHERE keyspace_name='" + keyspace + "' AND view_name='"
          + view.getName().asInternal() + "'";

      Row whereClauseRow = requireNonNull(session.execute(whereQuery).one());

      queryBuilder.append(" WHERE ")
          .append(whereClauseRow.getString(0));

      // Parse and unparse the view query to get properly quoted field names
      String query = queryBuilder.toString();
      SqlParser.Config parserConfig = SqlParser.config()
          .withUnquotedCasing(Casing.UNCHANGED);

      SqlSelect parsedQuery;
      try {
        parsedQuery = (SqlSelect) SqlParser.create(query, parserConfig).parseQuery();
      } catch (SqlParseException e) {
        LOGGER.warn("Could not parse query {} for CQL view {}.{}",
            query, keyspace, view.getName().asInternal());
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
      if (schema == null) {
        throw new IllegalStateException("Cannot find schema " + name
            + " in parent schema " + parentSchema.getName());
      }
      CalciteSchema calciteSchema = CalciteSchema.from(schema);

      List<String> viewPath = calciteSchema.path(viewName);

      schema.add(viewName,
            MaterializedViewTable.create(calciteSchema, query,
            null, viewPath, view.getName().asInternal(), true));
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TableMetadata table : getKeyspace().getTables().values()) {
      String tableName = table.getName().asInternal();
      builder.put(tableName, new CassandraTable(this, keyspace, tableName));

      for (ViewMetadata view : getKeyspace().getViewsOnTable(table.getName()).values()) {
        String viewName = view.getName().asInternal();
        builder.put(viewName, new CassandraTable(this, keyspace, viewName, true));
      }
    }
    return builder.build();
  }

  private KeyspaceMetadata getKeyspace() {
    return session.getMetadata().getKeyspace(keyspace).orElseThrow(
        () -> new RuntimeException("Keyspace " + keyspace + " not found"));
  }
}
