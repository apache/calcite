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
        null, CursorFactory.ARRAY);
  }

  protected static Signature signature(ResultSetMetaData metaData)
      throws SQLException {
    return signature(metaData, null, null);
  }

  protected final Connection connection;

  public JdbcMeta(Connection connection) {
    this.connection = connection;
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
      return JdbcResultSet.create(
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
      return JdbcResultSet.create(
          connection.getMetaData().getColumns(catalog, schemaPattern.s,
              tableNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getSchemas(catalog, schemaPattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getCatalogs() {
    try {
      return JdbcResultSet.create(connection.getMetaData().getCatalogs());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTableTypes() {
    try {
      return JdbcResultSet.create(connection.getMetaData().getTableTypes());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedures(String catalog, Pat schemaPattern,
      Pat procedureNamePattern) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getProcedures(catalog, schemaPattern.s,
              procedureNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedureColumns(String catalog, Pat schemaPattern,
      Pat procedureNamePattern, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getProcedureColumns(catalog,
              schemaPattern.s, procedureNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumnPrivileges(String catalog, String schema,
      String table, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getColumnPrivileges(catalog, schema,
              table, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTablePrivileges(String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getTablePrivileges(catalog,
              schemaPattern.s, tableNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getBestRowIdentifier(catalog, schema,
              table, scope, nullable));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getVersionColumns(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(
          connection.getMetaData().getVersionColumns(catalog, schema, table));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getPrimaryKeys(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(
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

  public StatementHandle createStatement(ConnectionHandle ch) {
    try {
      final Statement statement = connection.createStatement();
      final int id = statementMap.size();
      statementMap.put(id, new StatementInfo(statement));
      return new StatementHandle(id);
    } catch (SQLException e) {
      throw propagate(e);
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

  public Signature prepare(StatementHandle h, String sql, int maxRowCount) {
    // TODO: can't actually prepare an existing statement...
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      statementMap.put(h.id, new StatementInfo(statement));
      return signature(statement.getMetaData(),
          statement.getParameterMetaData(), sql);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public MetaResultSet prepareAndExecute(StatementHandle h, String sql,
      int maxRowCount, PrepareCallback callback) {
    final StatementInfo statementInfo = statementMap.get(h.id);
    try {
      statementInfo.resultSet = statementInfo.statement.executeQuery(sql);
      return JdbcResultSet.create(statementInfo.resultSet);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public Frame fetch(StatementHandle h, List<Object> parameterValues,
      int offset, int fetchMaxRowCount) {
    final StatementInfo statementInfo = statementMap.get(h.id);
    try {
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
