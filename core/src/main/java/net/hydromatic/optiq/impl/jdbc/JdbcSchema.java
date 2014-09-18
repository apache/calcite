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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import com.google.common.collect.*;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.*;
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
    super();
    this.dataSource = dataSource;
    this.dialect = dialect;
    this.convention = convention;
    this.catalog = catalog;
    this.schema = schema;
    assert dialect != null;
    assert dataSource != null;
  }

  public static JdbcSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      String catalog,
      String schema) {
    final Expression expression =
        Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
    final SqlDialect dialect = createDialect(dataSource);
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
        final Class<?> clazz = Class.forName((String) dataSourceName);
        dataSource = (DataSource) clazz.newInstance();
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
    return JdbcSchema.create(parentSchema, name, dataSource, jdbcCatalog,
        jdbcSchema);
  }

  /** Returns a suitable SQL dialect for the given data source. */
  public static SqlDialect createDialect(DataSource dataSource) {
    return JdbcUtils.DialectPool.INSTANCE.get(dataSource);
  }

  /** Creates a JDBC data source with the given specification. */
  public static DataSource dataSource(String url, String driverClassName,
      String username, String password) {
    if (url.startsWith("jdbc:hsqldb:")) {
      // Prevent hsqldb from screwing up java.util.logging.
      System.setProperty("hsqldb.reconfig_logging", "false");
    }
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(driverClassName);
    return dataSource;
  }

  public boolean isMutable() {
    return false;
  }

  public boolean contentsHaveChangedSince(long lastCheck, long now) {
    return false;
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
      DatabaseMetaData metaData = connection.getMetaData();
      resultSet = metaData.getTables(
          catalog,
          schema,
          null,
          null);
      final ImmutableMap.Builder<String, JdbcTable> builder =
          ImmutableMap.builder();
      while (resultSet.next()) {
        final String tableName = resultSet.getString(3);
        final String catalogName = resultSet.getString(1);
        final String schemaName = resultSet.getString(2);
        final String tableTypeName = resultSet.getString(4);
        // Clean up table type. In particular, this ensures that 'SYSTEM TABLE',
        // returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
        // We know enum constants are upper-case without spaces, so we can't
        // make things worse.
        final String tableTypeName2 =
            tableTypeName.toUpperCase().replace(' ', '_');
        final TableType tableType =
            Util.enumVal(TableType.class, tableTypeName2);
        final JdbcTable table =
            new JdbcTable(this, catalogName, schemaName, tableName, tableType);
        builder.put(tableName, table);
      }
      return builder.build();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Exception while reading tables", e);
    } finally {
      close(connection, null, resultSet);
    }
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
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
    while (resultSet.next()) {
      final String columnName = resultSet.getString(4);
      final int dataType = resultSet.getInt(5);
      final String typeString = resultSet.getString(6);
      final int size = resultSet.getInt(7);
      final int scale = resultSet.getInt(9);
      RelDataType sqlType =
          sqlType(typeFactory, dataType, size, scale, typeString);
      boolean nullable = resultSet.getBoolean(11);
      fieldInfo.add(columnName, sqlType).nullable(nullable);
    }
    resultSet.close();
    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
      int precision, int scale, String typeString) {
    SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(dataType);
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
        component = typeFactory.createSqlType(SqlTypeName.ANY);
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
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
  }

  public Set<String> getTableNames() {
    // This method is called during a cache refresh. We can take it as a signal
    // that we need to re-build our own cache.
    return getTableMap(true).keySet();
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
   * {@link net.hydromatic.optiq.impl.jdbc.JdbcSchema}.
   * This allows you to create a jdbc schema inside a model.json file.
   *
   * <pre>{@code
   * {
   *   version: '1.0',
   *   defaultSchema: 'FOODMART_CLONE',
   *   schemas: [
   *     {
   *       name: 'FOODMART_CLONE',
   *       type: 'custom',
   *       factory: 'net.hydromatic.optiq.impl.clone.JdbcSchema$Factory',
   *       operand: {
   *         jdbcDriver: 'com.mysql.jdbc.Driver',
   *         jdbcUrl: 'jdbc:mysql://localhost/foodmart',
   *         jdbcUser: 'foodmart',
   *         jdbcPassword: 'foodmart'
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   */
  public static class Factory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        Map<String, Object> operand) {
      return JdbcSchema.create(parentSchema, name, operand);
    }
  }
}

// End JdbcSchema.java
