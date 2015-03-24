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
package org.apache.calcite.avatica.jdbc;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/** Implementation of {@link Meta} upon an existing JDBC data source. */
public class JdbcMeta implements Meta {
  /**
   * JDBC Types Mapped to Java Types
   *
   * @see <a href="https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html#1051555">JDBC Types Mapped to Java Types</a>
   */
  protected static final Map<Integer, Type> SQL_TYPE_TO_JAVA_TYPE =
      new HashMap<>();
  static {
    SQL_TYPE_TO_JAVA_TYPE.put(Types.CHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.VARCHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.LONGNVARCHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.NUMERIC, BigDecimal.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DECIMAL, BigDecimal.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BIT, Boolean.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TINYINT, Byte.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.SMALLINT, Short.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.INTEGER, Integer.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BIGINT, Long.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.REAL, Float.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.FLOAT, Double.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DOUBLE, Double.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.VARBINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.LONGVARBINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DATE, java.sql.Date.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TIME, java.sql.Time.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TIMESTAMP, java.sql.Timestamp.class);
    //put(Types.CLOB, Clob);
    //put(Types.BLOB, Blob);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.ARRAY, Array.class);
  }

  private static final String DEFAULT_CONN_ID =
      UUID.fromString("00000000-0000-0000-0000-000000000000").toString();

  private final String url;
  private final Properties info;
  private final Connection connection; // TODO: remove default connection
  private final Map<String, Connection> connectionMap = new HashMap<>();
  private final Map<Integer, StatementInfo> statementMap = new HashMap<>();

  /**
   * Convert from JDBC metadata to Avatica columns.
   */
  protected static List<ColumnMetaData>
  columns(ResultSetMetaData metaData) throws SQLException {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      final Type javaType =
          SQL_TYPE_TO_JAVA_TYPE.get(metaData.getColumnType(i));
      ColumnMetaData.AvaticaType t =
          ColumnMetaData.scalar(metaData.getColumnType(i),
              metaData.getColumnTypeName(i), ColumnMetaData.Rep.of(javaType));
      ColumnMetaData md =
          new ColumnMetaData(i - 1, metaData.isAutoIncrement(i),
              metaData.isCaseSensitive(i), metaData.isSearchable(i),
              metaData.isCurrency(i), metaData.isNullable(i),
              metaData.isSigned(i), metaData.getColumnDisplaySize(i),
              metaData.getColumnLabel(i), metaData.getColumnName(i),
              metaData.getSchemaName(i), metaData.getPrecision(i),
              metaData.getScale(i), metaData.getTableName(i),
              metaData.getCatalogName(i), t, metaData.isReadOnly(i),
              metaData.isWritable(i), metaData.isDefinitelyWritable(i),
              metaData.getColumnClassName(i));
      columns.add(md);
    }
    return columns;
  }

  /**
   * Converts from JDBC metadata to AvaticaParameters
   */
  protected static List<AvaticaParameter> parameters(ParameterMetaData metaData)
      throws SQLException {
    if (metaData == null) {
      return Collections.emptyList();
    }
    final List<AvaticaParameter> params = new ArrayList<>();
    for (int i = 1; i <= metaData.getParameterCount(); i++) {
      params.add(
          new AvaticaParameter(metaData.isSigned(i), metaData.getPrecision(i),
              metaData.getScale(i), metaData.getParameterType(i),
              metaData.getParameterTypeName(i),
              metaData.getParameterClassName(i), "?" + i));
    }
    return params;
  }

  protected static Signature signature(ResultSetMetaData metaData,
      ParameterMetaData parameterMetaData, String sql) throws  SQLException {
    return new Signature(columns(metaData), sql, parameters(parameterMetaData),
        null, CursorFactory.LIST /* LIST because JdbcResultSet#frame */);
  }

  protected static Signature signature(ResultSetMetaData metaData)
      throws SQLException {
    return signature(metaData, null, null);
  }

  /**
   * @param url a database url of the form
   *  <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
   */
  public JdbcMeta(String url) throws SQLException {
    this(url, new Properties());
  }

  /**
   * @param url a database url of the form
   * <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
   * @param user the database user on whose behalf the connection is being
   *   made
   * @param password the user's password
   */
  public JdbcMeta(final String url, final String user, final String password)
      throws SQLException {
    this(url, new Properties() {
      {
        put("user", user);
        put("password", password);
      }
    });
  }

  /**
   * @param url a database url of the form
   * <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
   * @param info a list of arbitrary string tag/value pairs as
   * connection arguments; normally at least a "user" and
   * "password" property should be included
   */
  public JdbcMeta(String url, Properties info) throws SQLException {
    this.url = url;
    this.info = info;
    this.connection = DriverManager.getConnection(url, info);
    this.connectionMap.put(DEFAULT_CONN_ID, connection);
  }

  public String getSqlKeywords() {
    try {
      return connection.getMetaData().getSQLKeywords();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getNumericFunctions() {
    try {
      return connection.getMetaData().getNumericFunctions();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getStringFunctions() {
    return null;
  }

  public String getSystemFunctions() {
    return null;
  }

  public String getTimeDateFunctions() {
    return null;
  }

  public MetaResultSet getTables(String catalog, Pat schemaPattern,
      Pat tableNamePattern, List<String> typeList) {
    try {
      String[] types = new String[typeList == null ? 0 : typeList.size()];
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTables(catalog, schemaPattern.s,
              tableNamePattern.s,
              typeList == null ? types : typeList.toArray(types)));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getColumns(catalog, schemaPattern.s,
              tableNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getSchemas(catalog, schemaPattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getCatalogs() {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getCatalogs());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTableTypes() {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTableTypes());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedures(String catalog, Pat schemaPattern,
      Pat procedureNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getProcedures(catalog, schemaPattern.s,
              procedureNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedureColumns(String catalog, Pat schemaPattern,
      Pat procedureNamePattern, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getProcedureColumns(catalog,
              schemaPattern.s, procedureNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumnPrivileges(String catalog, String schema,
      String table, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getColumnPrivileges(catalog, schema,
              table, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTablePrivileges(String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTablePrivileges(catalog,
              schemaPattern.s, tableNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getBestRowIdentifier(catalog, schema,
              table, scope, nullable));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getVersionColumns(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getVersionColumns(catalog, schema, table));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getPrimaryKeys(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getPrimaryKeys(catalog, schema, table));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getImportedKeys(String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getExportedKeys(String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema, String parentTable, String foreignCatalog,
      String foreignSchema, String foreignTable) {
    return null;
  }

  public MetaResultSet getTypeInfo() {
    return null;
  }

  public MetaResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) {
    return null;
  }

  public MetaResultSet getUDTs(String catalog, Pat schemaPattern,
      Pat typeNamePattern, int[] types) {
    return null;
  }

  public MetaResultSet getSuperTypes(String catalog, Pat schemaPattern,
      Pat typeNamePattern) {
    return null;
  }

  public MetaResultSet getSuperTables(String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    return null;
  }

  public MetaResultSet getAttributes(String catalog, Pat schemaPattern,
      Pat typeNamePattern, Pat attributeNamePattern) {
    return null;
  }

  public MetaResultSet getClientInfoProperties() {
    return null;
  }

  public MetaResultSet getFunctions(String catalog, Pat schemaPattern,
      Pat functionNamePattern) {
    return null;
  }

  public MetaResultSet getFunctionColumns(String catalog, Pat schemaPattern,
      Pat functionNamePattern, Pat columnNamePattern) {
    return null;
  }

  public MetaResultSet getPseudoColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    return null;
  }

  public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, List<Object> parameterValues, Frame firstFrame) {
    return null;
  }

  protected Connection getConnection(String id) throws SQLException {
    if (connectionMap.get(id) == null) {
      connectionMap.put(id, DriverManager.getConnection(url, info));
    }
    return connectionMap.get(id);
  }

  public StatementHandle createStatement(ConnectionHandle ch) {
    try {
      final Connection conn = getConnection(ch.id);
      final Statement statement = conn.createStatement();
      final int id = System.identityHashCode(statement);
      statementMap.put(id, new StatementInfo(statement));
      return new StatementHandle(ch.id, id, null);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  @Override public void closeStatement(StatementHandle h) {
    Statement stmt = statementMap.get(h.id).statement;
    if (stmt == null) {
      return;
    }
    try {
      assert stmt.getConnection() == connectionMap.get(h.connectionId);
      stmt.close();
    } catch (SQLException e) {
      throw propagate(e);
    } finally {
      statementMap.remove(h.id);
    }
  }

  private RuntimeException propagate(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    } else if (e instanceof Error) {
      throw (Error) e;
    } else {
      throw new RuntimeException(e);
    }
  }

  public StatementHandle prepare(ConnectionHandle ch, String sql,
      int maxRowCount) {
    try {
      final Connection conn = getConnection(ch.id);
      final PreparedStatement statement = conn.prepareStatement(sql);
      final int id = System.identityHashCode(statement);
      statementMap.put(id, new StatementInfo(statement));
      return new StatementHandle(ch.id, id, signature(statement.getMetaData(),
          statement.getParameterMetaData(), sql));
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public MetaResultSet prepareAndExecute(ConnectionHandle ch, String sql,
      int maxRowCount, PrepareCallback callback) {
    try {
      final Connection connection = getConnection(ch.id);
      final PreparedStatement statement = connection.prepareStatement(sql);
      final int id = System.identityHashCode(statement);
      final StatementInfo info = new StatementInfo(statement);
      statementMap.put(id, info);
      info.resultSet = statement.executeQuery();
      return JdbcResultSet.create(ch.id, id, info.resultSet);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public Frame fetch(StatementHandle h, List<Object> parameterValues,
      int offset, int fetchMaxRowCount) {
    final StatementInfo statementInfo = statementMap.get(h.id);
    try {
      assert statementInfo.statement.getConnection()
          == connectionMap.get(h.connectionId);
      if (statementInfo.resultSet == null || parameterValues != null) {
        if (statementInfo.resultSet != null) {
          statementInfo.resultSet.close();
        }
        final PreparedStatement preparedStatement =
            (PreparedStatement) statementInfo.statement;
        if (parameterValues != null) {
          for (int i = 0; i < parameterValues.size(); i++) {
            Object o = parameterValues.get(i);
            preparedStatement.setObject(i + 1, o);
          }
        }
        statementInfo.resultSet = preparedStatement.executeQuery();
      }
      return JdbcResultSet.frame(statementInfo.resultSet, offset,
          fetchMaxRowCount);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  /** All we know about a statement. */
  private static class StatementInfo {
    final Statement statement; // sometimes a PreparedStatement
    ResultSet resultSet;

    private StatementInfo(Statement statement) {
      this.statement = Objects.requireNonNull(statement);
    }
  }
}

// End JdbcMeta.java
