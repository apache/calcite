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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import javax.sql.DataSource;

/**
 * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * <p>The tables in the JDBC data source appear to be tables in this schema;
 * queries against this schema are executed against those tables, pushing down
 * as much as possible of the query logic to SQL.</p>
 */
public class JdbcSchema implements Schema {
  final DataSource dataSource;
  final String catalog;
  final String schema;
  public final SqlDialect dialect;
  final JdbcConvention convention;
  private ImmutableMap<String, JdbcTable> tableMap;
  private final boolean snapshot;

  @Experimental
  public static final ThreadLocal<Foo> THREAD_METADATA = new ThreadLocal<>();

  private static final Ordering<Iterable<Integer>> VERSION_ORDERING =
      Ordering.<Integer>natural().lexicographical();

  /**
   * Creates a JDBC schema.
   *
   * @param dataSource Data source
   * @param dialect SQL dialect
   * @param convention Calling convention
   * @param catalog Catalog name, or null
   * @param schema Schema name pattern
   */
  public JdbcSchema(DataSource dataSource, SqlDialect dialect,
      JdbcConvention convention, String catalog, String schema) {
    this(dataSource, dialect, convention, catalog, schema, null);
  }

  private JdbcSchema(DataSource dataSource, SqlDialect dialect,
      JdbcConvention convention, String catalog, String schema,
      ImmutableMap<String, JdbcTable> tableMap) {
    this.dataSource = Objects.requireNonNull(dataSource);
    this.dialect = Objects.requireNonNull(dialect);
    this.convention = convention;
    this.catalog = catalog;
    this.schema = schema;
    this.tableMap = tableMap;
    this.snapshot = tableMap != null;
  }

  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      String catalog,
      String schema) {
    return create(parentSchema, name, dataSource,
        SqlDialectFactoryImpl.INSTANCE, catalog, schema);
  }

  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      SqlDialectFactory dialectFactory,
      String catalog,
      String schema) {
    final Expression expression =
        Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
    final SqlDialect dialect = createDialect(dialectFactory, dataSource);
    final JdbcConvention convention =
        JdbcConvention.of(dialect, expression, name);
    return new JdbcSchema(dataSource, dialect, convention, catalog, schema);
  }

  /**
   * Creates a JdbcSchema, taking credentials from a map.
   *
   * @param parentSchema Parent schema
   * @param name Name
   * @param operand Map of property/value pairs
   * @return A JdbcSchema
   */
  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      Map<String, Object> operand) {
    DataSource dataSource;
    try {
      final String dataSourceName = (String) operand.get("dataSource");
      if (dataSourceName != null) {
        dataSource =
            AvaticaUtils.instantiatePlugin(DataSource.class, dataSourceName);
      } else {
        final String jdbcUrl = (String) operand.get("jdbcUrl");
        final String jdbcDriver = (String) operand.get("jdbcDriver");
        final String jdbcUser = (String) operand.get("jdbcUser");
        final String jdbcPassword = (String) operand.get("jdbcPassword");
        dataSource = dataSource(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while reading dataSource", e);
    }
    String jdbcCatalog = (String) operand.get("jdbcCatalog");
    String jdbcSchema = (String) operand.get("jdbcSchema");
    String sqlDialectFactory = (String) operand.get("sqlDialectFactory");

    if (sqlDialectFactory == null || sqlDialectFactory.isEmpty()) {
      return JdbcSchema.create(
          parentSchema, name, dataSource, jdbcCatalog, jdbcSchema);
    } else {
      SqlDialectFactory factory = AvaticaUtils.instantiatePlugin(
          SqlDialectFactory.class, sqlDialectFactory);
      return JdbcSchema.create(
          parentSchema, name, dataSource, factory, jdbcCatalog, jdbcSchema);
    }
  }

  /**
   * Returns a suitable SQL dialect for the given data source.
   *
   * @param dataSource The data source
   *
   * @deprecated Use {@link #createDialect(SqlDialectFactory, DataSource)} instead
   */
  @Deprecated // to be removed before 2.0
  public static SqlDialect createDialect(DataSource dataSource) {
    return createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
  }

  /** Returns a suitable SQL dialect for the given data source. */
  public static SqlDialect createDialect(SqlDialectFactory dialectFactory,
      DataSource dataSource) {
    return JdbcUtils.DialectPool.INSTANCE.get(dialectFactory, dataSource);
  }

  /** Creates a JDBC data source with the given specification. */
  public static DataSource dataSource(String url, String driverClassName,
      String username, String password) {
    if (url.startsWith("jdbc:hsqldb:")) {
      // Prevent hsqldb from screwing up java.util.logging.
      System.setProperty("hsqldb.reconfig_logging", "false");
    }
    return JdbcUtils.DataSourcePool.INSTANCE.get(url, driverClassName, username,
        password);
  }

  public boolean isMutable() {
    return false;
  }

  public Schema snapshot(SchemaVersion version) {
    return new JdbcSchema(dataSource, dialect, convention, catalog, schema,
        tableMap);
  }

  // Used by generated code.
  public DataSource getDataSource() {
    return dataSource;
  }

  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
  }

  protected Multimap<String, Function> getFunctions() {
    // TODO: populate map from JDBC metadata
    return ImmutableMultimap.of();
  }

  public final Collection<Function> getFunctions(String name) {
    return getFunctions().get(name); // never null
  }

  public final Set<String> getFunctionNames() {
    return getFunctions().keySet();
  }

  private ImmutableMap<String, JdbcTable> computeTables() {
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      connection = dataSource.getConnection();
      final Pair<String, String> catalogSchema = getCatalogSchema(connection);
      final String catalog = catalogSchema.left;
      final String schema = catalogSchema.right;
      final Iterable<MetaImpl.MetaTable> tableDefs;
      if (THREAD_METADATA.get() != null) {
        tableDefs = THREAD_METADATA.get().apply(catalog, schema);
      } else {
        final List<MetaImpl.MetaTable> tableDefList = new ArrayList<>();
        final DatabaseMetaData metaData = connection.getMetaData();
        resultSet = metaData.getTables(catalog, schema, null, null);
        while (resultSet.next()) {
          final String catalogName = resultSet.getString(1);
          final String schemaName = resultSet.getString(2);
          final String tableName = resultSet.getString(3);
          final String tableTypeName = resultSet.getString(4);
          tableDefList.add(
              new MetaImpl.MetaTable(catalogName, schemaName, tableName,
                  tableTypeName));
        }
        tableDefs = tableDefList;
      }

      final ImmutableMap.Builder<String, JdbcTable> builder =
          ImmutableMap.builder();
      for (MetaImpl.MetaTable tableDef : tableDefs) {
        // Clean up table type. In particular, this ensures that 'SYSTEM TABLE',
        // returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
        // We know enum constants are upper-case without spaces, so we can't
        // make things worse.
        //
        // PostgreSQL returns tableTypeName==null for pg_toast* tables
        // This can happen if you start JdbcSchema off a "public" PG schema
        // The tables are not designed to be queried by users, however we do
        // not filter them as we keep all the other table types.
        final String tableTypeName2 =
            tableDef.tableType == null
            ? null
            : tableDef.tableType.toUpperCase(Locale.ROOT).replace(' ', '_');
        final TableType tableType =
            Util.enumVal(TableType.OTHER, tableTypeName2);
        if (tableType == TableType.OTHER  && tableTypeName2 != null) {
          System.out.println("Unknown table type: " + tableTypeName2);
        }
        final JdbcTable table =
            new JdbcTable(this, tableDef.tableCat, tableDef.tableSchem,
                tableDef.tableName, tableType);
        builder.put(tableDef.tableName, table);
      }
      return builder.build();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Exception while reading tables", e);
    } finally {
      close(connection, null, resultSet);
    }
  }

  /** Returns [major, minor] version from a database metadata. */
  private List<Integer> version(DatabaseMetaData metaData) throws SQLException {
    return ImmutableList.of(metaData.getJDBCMajorVersion(),
        metaData.getJDBCMinorVersion());
  }

  /** Returns a pair of (catalog, schema) for the current connection. */
  private Pair<String, String> getCatalogSchema(Connection connection)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    final List<Integer> version41 = ImmutableList.of(4, 1); // JDBC 4.1
    String catalog = this.catalog;
    String schema = this.schema;
    final boolean jdbc41OrAbove =
        VERSION_ORDERING.compare(version(metaData), version41) >= 0;
    if (catalog == null && jdbc41OrAbove) {
      // From JDBC 4.1, catalog and schema can be retrieved from the connection
      // object, hence try to get it from there if it was not specified by user
      catalog = connection.getCatalog();
    }
    if (schema == null && jdbc41OrAbove) {
      schema = connection.getSchema();
      if ("".equals(schema)) {
        schema = null; // PostgreSQL returns useless "" sometimes
      }
    }
    if ((catalog == null || schema == null)
        && metaData.getDatabaseProductName().equals("PostgreSQL")) {
      final String sql = "select current_database(), current_schema()";
      try (Statement statement = connection.createStatement();
           ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          catalog = resultSet.getString(1);
          schema = resultSet.getString(2);
        }
      }
    }
    return Pair.of(catalog, schema);
  }

  public Table getTable(String name) {
    return getTableMap(false).get(name);
  }

  private synchronized ImmutableMap<String, JdbcTable> getTableMap(
      boolean force) {
    if (force || tableMap == null) {
      tableMap = computeTables();
    }
    return tableMap;
  }

  RelProtoDataType getRelDataType(String catalogName, String schemaName,
      String tableName) throws SQLException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      DatabaseMetaData metaData = connection.getMetaData();
      return getRelDataType(metaData, catalogName, schemaName, tableName);
    } finally {
      close(connection, null, null);
    }
  }

  RelProtoDataType getRelDataType(DatabaseMetaData metaData, String catalogName,
      String schemaName, String tableName) throws SQLException {
    final ResultSet resultSet =
        metaData.getColumns(catalogName, schemaName, tableName, null);

    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    while (resultSet.next()) {
      final String columnName = resultSet.getString(4);
      final int dataType = resultSet.getInt(5);
      final String typeString = resultSet.getString(6);
      final int precision;
      final int scale;
      switch (SqlType.valueOf(dataType)) {
      case TIMESTAMP:
      case TIME:
        precision = resultSet.getInt(9); // SCALE
        scale = 0;
        break;
      default:
        precision = resultSet.getInt(7); // SIZE
        scale = resultSet.getInt(9); // SCALE
        break;
      }
      RelDataType sqlType =
          sqlType(typeFactory, dataType, precision, scale, typeString);
      boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
      fieldInfo.add(columnName, sqlType).nullable(nullable);
    }
    resultSet.close();
    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
      int precision, int scale, String typeString) {
    // Fall back to ANY if type is unknown
    final SqlTypeName sqlTypeName =
        Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
    switch (sqlTypeName) {
    case ARRAY:
      RelDataType component = null;
      if (typeString != null && typeString.endsWith(" ARRAY")) {
        // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
        // "INTEGER".
        final String remaining = typeString.substring(0,
            typeString.length() - " ARRAY".length());
        component = parseTypeString(typeFactory, remaining);
      }
      if (component == null) {
        component = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY), true);
      }
      return typeFactory.createArrayType(component, -1);
    }
    if (precision >= 0
        && scale >= 0
        && sqlTypeName.allowsPrecScale(true, true)) {
      return typeFactory.createSqlType(sqlTypeName, precision, scale);
    } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
      return typeFactory.createSqlType(sqlTypeName, precision);
    } else {
      assert sqlTypeName.allowsNoPrecNoScale();
      return typeFactory.createSqlType(sqlTypeName);
    }
  }

  /** Given "INTEGER", returns BasicSqlType(INTEGER).
   * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
   * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2). */
  private RelDataType parseTypeString(RelDataTypeFactory typeFactory,
      String typeString) {
    int precision = -1;
    int scale = -1;
    int open = typeString.indexOf("(");
    if (open >= 0) {
      int close = typeString.indexOf(")", open);
      if (close >= 0) {
        String rest = typeString.substring(open + 1, close);
        typeString = typeString.substring(0, open);
        int comma = rest.indexOf(",");
        if (comma >= 0) {
          precision = Integer.parseInt(rest.substring(0, comma));
          scale = Integer.parseInt(rest.substring(comma));
        } else {
          precision = Integer.parseInt(rest);
        }
      }
    }
    try {
      final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
      return typeName.allowsPrecScale(true, true)
          ? typeFactory.createSqlType(typeName, precision, scale)
          : typeName.allowsPrecScale(true, false)
          ? typeFactory.createSqlType(typeName, precision)
          : typeFactory.createSqlType(typeName);
    } catch (IllegalArgumentException e) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
  }

  public Set<String> getTableNames() {
    // This method is called during a cache refresh. We can take it as a signal
    // that we need to re-build our own cache.
    return getTableMap(!snapshot).keySet();
  }

  protected Map<String, RelProtoDataType> getTypes() {
    // TODO: populate map from JDBC metadata
    return ImmutableMap.of();
  }

  @Override public RelProtoDataType getType(String name) {
    return getTypes().get(name);
  }

  @Override public Set<String> getTypeNames() {
    return getTypes().keySet();
  }

  public Schema getSubSchema(String name) {
    // JDBC does not support sub-schemas.
    return null;
  }

  public Set<String> getSubSchemaNames() {
    return ImmutableSet.of();
  }

  private static void close(
      Connection connection, Statement statement, ResultSet resultSet) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /** Schema factory that creates a
   * {@link org.apache.calcite.adapter.jdbc.JdbcSchema}.
   *
   * <p>This allows you to create a jdbc schema inside a model.json file, like
   * this:
   *
   * <blockquote><pre>
   * {
   *   "version": "1.0",
   *   "defaultSchema": "FOODMART_CLONE",
   *   "schemas": [
   *     {
   *       "name": "FOODMART_CLONE",
   *       "type": "custom",
   *       "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
   *       "operand": {
   *         "jdbcDriver": "com.mysql.jdbc.Driver",
   *         "jdbcUrl": "jdbc:mysql://localhost/foodmart",
   *         "jdbcUser": "foodmart",
   *         "jdbcPassword": "foodmart"
   *       }
   *     }
   *   ]
   * }</pre></blockquote>
   */
  public static class Factory implements SchemaFactory {
    public static final Factory INSTANCE = new Factory();

    private Factory() {}

    public Schema create(
        SchemaPlus parentSchema,
        String name,
        Map<String, Object> operand) {
      return JdbcSchema.create(parentSchema, name, operand);
    }
  }

  /** Do not use */
  @Experimental
  public interface Foo
      extends BiFunction<String, String, Iterable<MetaImpl.MetaTable>> {
  }
}

// End JdbcSchema.java
