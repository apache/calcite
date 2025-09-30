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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.LazyReference;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * <p>The tables in the JDBC data source appear to be tables in this schema;
 * queries against this schema are executed against those tables, pushing down
 * as much as possible of the query logic to SQL.
 */
public class JdbcSchema extends JdbcBaseSchema implements Schema, Wrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSchema.class);

  final DataSource dataSource;
  final @Nullable String catalog;
  final @Nullable String schema;
  public final SqlDialect dialect;
  final JdbcConvention convention;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();
  private final Lookup<JdbcSchema> subSchemas = Lookup.empty();

  @Experimental
  public static final ThreadLocal<@Nullable Foo> THREAD_METADATA = new ThreadLocal<>();

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
      JdbcConvention convention, @Nullable String catalog, @Nullable String schema) {
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.dialect = requireNonNull(dialect, "dialect");
    this.convention = convention;
    this.catalog = catalog;
    this.schema = schema;
  }

  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      @Nullable String catalog,
      @Nullable String schema) {
    return create(parentSchema, name, dataSource,
        SqlDialectFactoryImpl.INSTANCE, catalog, schema);
  }

  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      SqlDialectFactory dialectFactory,
      @Nullable String catalog,
      @Nullable String schema) {
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
        final String jdbcUrl = (String) requireNonNull(operand.get("jdbcUrl"), "jdbcUrl");
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
      SqlDialectFactory factory =
          AvaticaUtils.instantiatePlugin(SqlDialectFactory.class,
              sqlDialectFactory);
      return JdbcSchema.create(parentSchema, name, dataSource, factory,
          jdbcCatalog, jdbcSchema);
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
  public static DataSource dataSource(String url, @Nullable String driverClassName,
      @Nullable String username, @Nullable String password) {
    if (url.startsWith("jdbc:hsqldb:")) {
      // Prevent hsqldb from screwing up java.util.logging.
      System.setProperty("hsqldb.reconfig_logging", "false");
    }
    return JdbcUtils.DataSourcePool.INSTANCE.get(url, driverClassName, username,
        password);
  }

  @Override public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new IgnoreCaseLookup<Table>() {
      @Override public @Nullable Table get(String name) {
        try (Stream<MetaImpl.MetaTable> s = getMetaTableStream(name)) {
          return s.findFirst().map(it -> jdbcTableMapper(it)).orElse(null);
        }
      }

      @Override public Set<String> getNames(LikePattern pattern) {
        try (Stream<MetaImpl.MetaTable> s = getMetaTableStream(pattern.pattern)) {
          return s.map(it -> it.tableName).collect(Collectors.toSet());
        }
      }
    });
  }

  @Override public Lookup<? extends Schema> subSchemas() {
    return subSchemas;
  }


  @Override public boolean isMutable() {
    return false;
  }

  @Override public Schema snapshot(SchemaVersion version) {
    return this;
  }

  // Used by generated code.
  public DataSource getDataSource() {
    return dataSource;
  }

  @Override public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    requireNonNull(parentSchema, "parentSchema must not be null for JdbcSchema");
    return Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
  }

  private Stream<MetaImpl.MetaTable> getMetaTableStream(String tableNamePattern) {
    final Pair<@Nullable String, @Nullable String> catalogSchema = getCatalogSchema();
    final Stream<MetaImpl.MetaTable> tableDefs;
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      connection = dataSource.getConnection();
      final DatabaseMetaData metaData = connection.getMetaData();
      resultSet =
          metaData.getTables(catalogSchema.left, catalogSchema.right, tableNamePattern, null);
      tableDefs = asStream(connection, resultSet)
          .map(JdbcSchema::metaDataMapper);
    } catch (SQLException e) {
      close(connection, null, resultSet);
      throw new RuntimeException(
          "Exception while reading tables", e);
    }
    return tableDefs;
  }

  private static Stream<ResultSet> asStream(Connection connection, ResultSet resultSet) {
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<ResultSet>(
            Long.MAX_VALUE, Spliterator.ORDERED) {
          @Override public boolean tryAdvance(Consumer<? super ResultSet> action) {
            try {
              if (!resultSet.next()) {
                return false;
              }
              action.accept(resultSet);
              return true;
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          }
        }, false).onClose(() -> close(connection, null, resultSet));
  }

  private JdbcTable jdbcTableMapper(MetaImpl.MetaTable tableDef) {
    return new JdbcTable(this, tableDef.tableCat, tableDef.tableSchem, tableDef.tableName,
        getTableType(tableDef.tableType));
  }

  private static MetaImpl.MetaTable metaDataMapper(ResultSet resultSet) {
    try {
      return new MetaImpl.MetaTable(resultSet.getString(1), resultSet.getString(2),
          resultSet.getString(3),
          resultSet.getString(4));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static TableType getTableType(String tableTypeName) {
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
        tableTypeName == null
            ? null
            : tableTypeName.toUpperCase(Locale.ROOT).replace(' ', '_');
    final TableType tableType =
        Util.enumVal(TableType.OTHER, tableTypeName2);
    if (tableType == TableType.OTHER && tableTypeName2 != null) {
      LOGGER.info("Unknown table type: {}", tableTypeName2);
    }
    return tableType;
  }

  /** Returns [major, minor] version from a database metadata. */
  private static List<Integer> version(DatabaseMetaData metaData) throws SQLException {
    return ImmutableList.of(metaData.getJDBCMajorVersion(),
        metaData.getJDBCMinorVersion());
  }

  /** Returns a pair of (catalog, schema) for the current connection. */
  private Pair<@Nullable String, @Nullable String> getCatalogSchema() {
    try (Connection connection = dataSource.getConnection()) {
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
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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
      final String columnName = requireNonNull(resultSet.getString(4), "columnName");
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

  private static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
      int precision, int scale, @Nullable String typeString) {
    // Fall back to ANY if type is unknown
    final SqlTypeName sqlTypeName =
        Util.first(typeString != null && typeString.toUpperCase(Locale.ROOT).contains("UNSIGNED")
            ? SqlTypeName.getNameForUnsignedJdbcType(dataType)
            : SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
    switch (sqlTypeName) {
    case ARRAY:
      RelDataType component = null;
      if (typeString != null && typeString.endsWith(" ARRAY")) {
        // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
        // "INTEGER".
        final String remaining =
            typeString.substring(0, typeString.length() - " ARRAY".length());
        component = parseTypeString(typeFactory, remaining);
      }
      if (component == null) {
        component =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true);
      }
      return typeFactory.createArrayType(component, -1);
    default:
      break;
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
  private static RelDataType parseTypeString(RelDataTypeFactory typeFactory,
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
          precision = parseInt(rest.substring(0, comma));
          scale = parseInt(rest.substring(comma));
        } else {
          precision = parseInt(rest);
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

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz == DataSource.class) {
      return clazz.cast(getDataSource());
    }
    return null;
  }


  private static void close(
      @Nullable Connection connection,
      @Nullable Statement statement,
      @Nullable ResultSet resultSet) {
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

    @Override public Schema create(
        SchemaPlus parentSchema,
        String name,
        Map<String, Object> operand) {
      return JdbcSchema.create(parentSchema, name, operand);
    }
  }

  /** Do not use. */
  @Experimental
  public interface Foo
      extends BiFunction<@Nullable String, @Nullable String, Iterable<MetaImpl.MetaTable>> {
  }
}
