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
package org.apache.calcite.avatica;


import org.apache.calcite.avatica.remote.TypedValue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

/**
 * Test that {@code ResultSet} returned by {@code DatabaseMetaData} methods
 * conform to JDBC specification.
 */
public class MetaResultSetTest {
  /**
   * A fake test driver for test.
   */
  private static final class TestDriver extends UnregisteredDriver {

    @Override protected DriverVersion createDriverVersion() {
      return new DriverVersion("test", "test 0.0.0", "test", "test 0.0.0", false, 0, 0, 0, 0);
    }

    @Override protected String getConnectStringPrefix() {
      return "jdbc:test";
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      return new TestMetaImpl(connection);
    }
  }

  /**
   * Fake meta implementation for test driver.
   */
  public static final class TestMetaImpl extends MetaImpl {
    public TestMetaImpl(AvaticaConnection connection) {
      super(connection);
    }

    @Override public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
        long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
        long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
        throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h,
        List<String> sqlCommands) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteBatchResult executeBatch(StatementHandle h,
        List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
        throws NoSuchStatementException, MissingResultsException {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
        long maxRowCount) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
        int maxRowsInFirstFrame) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public void closeStatement(StatementHandle h) {
    }

    @Override public boolean syncResults(StatementHandle sh, QueryState state, long offset)
        throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public void commit(ConnectionHandle ch) {
      throw new UnsupportedOperationException();
    }

    @Override public void rollback(ConnectionHandle ch) {
      throw new UnsupportedOperationException();
    }
  }

  private Connection connection;

  @Before public void setUp() throws SQLException {
    connection = new TestDriver().connect("jdbc:test", null);
  }

  @After public void tearDown() throws SQLException {
    connection.close();
  }

  @Test public void testGetAttributes() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getAttributes(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(21, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TYPE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TYPE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "ATTR_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "ATTR_TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "ATTR_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "DECIMAL_DIGITS", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 9, "NUM_PREC_RADIX", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "NULLABLE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 11, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 12, "ATTR_DEF", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 13, "SQL_DATA_TYPE", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 14, "SQL_DATETIME_SUB", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 15, "CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 16, "ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 17, "IS_NULLABLE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 18, "SCOPE_CATALOG", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 19, "SCOPE_SCHEMA", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 20, "SCOPE_TABLE", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 21, "SOURCE_DATA_TYPE", Types.SMALLINT, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetBestRowIdentifier() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getBestRowIdentifier(null, null, null,
        DatabaseMetaData.bestRowUnknown, false)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(8, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "SCOPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 2, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 3, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "COLUMN_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "BUFFER_LENGTH", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 7, "DECIMAL_DIGITS", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "PSEUDO_COLUMN", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetCatalogs() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getCatalogs()) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(1, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetClientInfoProperties() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getClientInfoProperties()) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(4, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 2, "MAX_LEN", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 3, "DEFAULT_VALUE", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 4, "DESCRIPTION", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetColumnPrivileges() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getColumnPrivileges(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(8, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "GRANTOR", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "GRANTEE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "PRIVILEGE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "IS_GRANTABLE", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetColumns() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getColumns(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(24, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "COLUMN_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "BUFFER_LENGTH", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 9, "DECIMAL_DIGITS", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "NUM_PREC_RADIX", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 11, "NULLABLE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 13, "COLUMN_DEF", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "SQL_DATA_TYPE", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 15, "SQL_DATETIME_SUB", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 16, "CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 17, "ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 18, "IS_NULLABLE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 19, "SCOPE_CATALOG", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 20, "SCOPE_SCHEMA", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 21, "SCOPE_TABLE", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 22, "SOURCE_DATA_TYPE", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 23, "IS_AUTOINCREMENT", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 24, "IS_GENERATEDCOLUMN", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetCrossReference() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getCrossReference(null, null, null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(14, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "PKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "PKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "PKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "PKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "FKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "FKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "FKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "FKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "KEY_SEQ", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 10, "UPDATE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 11, "DELETE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "FK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 13, "PK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "DEFERABILITY", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetExportedKeys() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getExportedKeys(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(14, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "PKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "PKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "PKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "PKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "FKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "FKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "FKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "FKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "KEY_SEQ", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 10, "UPDATE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 11, "DELETE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "FK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 13, "PK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "DEFERABILITY", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetFunctionColumns() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getFunctionColumns(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(17, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "FUNCTION_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "FUNCTION_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "FUNCTION_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "COLUMN_TYPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "PRECISION", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 9, "LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "SCALE", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 11, "RADIX", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 12, "NULLABLE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 13, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 15, "ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 16, "IS_NULLABLE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 17, "SPECIFIC_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetFunctions() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getFunctions(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(6, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "FUNCTION_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "FUNCTION_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "FUNCTION_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 5, "FUNCTION_TYPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "SPECIFIC_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetImportedKeys() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getImportedKeys(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(14, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "PKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "PKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "PKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "PKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "FKTABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "FKTABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "FKTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "FKCOLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "KEY_SEQ", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 10, "UPDATE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 11, "DELETE_RULE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "FK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 13, "PK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "DEFERABILITY", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetIndexInfo() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getIndexInfo(null, null, null, false, false)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(13, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "NON_UNIQUE", Types.BOOLEAN, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "INDEX_QUALIFIER", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "INDEX_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "TYPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "ORDINAL_POSITION", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "ASC_OR_DESC", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 11, "CARDINALITY", Types.BIGINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "PAGES", Types.BIGINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 13, "FILTER_CONDITION", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetPrimaryKeys() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getPrimaryKeys(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(6, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "KEY_SEQ", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "PK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetProcedureColumns() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getProcedureColumns(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(20, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "PROCEDURE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "PROCEDURE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "PROCEDURE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "COLUMN_TYPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "PRECISION", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 9, "LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "SCALE", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 11, "RADIX", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 12, "NULLABLE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 13, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "COLUMN_DEF", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 15, "SQL_DATA_TYPE", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 16, "SQL_DATETIME_SUB", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 17, "CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 18, "ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 19, "IS_NULLABLE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 20, "SPECIFIC_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetProcedures() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getProcedures(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(9, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "PROCEDURE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "PROCEDURE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "PROCEDURE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "PROCEDURE_TYPE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "SPECIFIC_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetPseudoColumns() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getPseudoColumns(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(12, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "COLUMN_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "DECIMAL_DIGITS", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "NUM_PREC_RADIX", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 9, "COLUMN_USAGE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 10, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 11, "CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 12, "IS_NULLABLE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetSchemas() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getSchemas(null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(2, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 2, "TABLE_CATALOG", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetSuperTables() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getSuperTables(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(4, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "SUPERTABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetSuperTypes() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getSuperTypes(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(6, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TYPE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TYPE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "SUPERTYPE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 5, "SUPERTYPE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "SUPERTYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetTablePrivileges() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getTablePrivileges(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(7, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "GRANTOR", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 5, "GRANTEE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "PRIVILEGE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 7, "IS_GRANTABLE", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetTables() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getTables(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(10, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "TABLE_TYPE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "TYPE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "TYPE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 9, "SELF_REFERENCING_COL_NAME", Types.VARCHAR,
          DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 10, "REF_GENERATION", Types.VARCHAR, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetTableTypes() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getTableTypes()) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(1, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TABLE_TYPE", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
    }
  }

  @Test public void testGetTypeInfo() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getTypeInfo()) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(18, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 2, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 3, "PRECISION", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 4, "LITERAL_PREFIX", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 5, "LITERAL_SUFFIX", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "CREATE_PARAMS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "NULLABLE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 8, "CASE_SENSITIVE", Types.BOOLEAN, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 9, "SEARCHABLE", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 10, "UNSIGNED_ATTRIBUTE", Types.BOOLEAN, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 11, "FIXED_PREC_SCALE", Types.BOOLEAN, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 12, "AUTO_INCREMENT", Types.BOOLEAN, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 13, "LOCAL_TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 14, "MINIMUM_SCALE", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 15, "MAXIMUM_SCALE", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 16, "SQL_DATA_TYPE", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 17, "SQL_DATETIME_SUB", Types.INTEGER,
          DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 18, "NUM_PREC_RADIX", Types.INTEGER, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetUDTs() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getUDTs(null, null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(7, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "TYPE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 2, "TYPE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 3, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "CLASS_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 6, "REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "BASE_TYPE", Types.SMALLINT, DatabaseMetaData.columnNullable);
    }
  }

  @Test public void testGetVersionColumns() throws SQLException {
    DatabaseMetaData metadata = getDatabaseMetadata();
    try (ResultSet rs = metadata.getVersionColumns(null, null, null)) {
      ResultSetMetaData rsMeta = rs.getMetaData();

      assertEquals(8, rsMeta.getColumnCount());
      assertColumn(rsMeta, 1, "SCOPE", Types.SMALLINT, DatabaseMetaData.columnNullableUnknown);
      assertColumn(rsMeta, 2, "COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 3, "DATA_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 4, "TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
      assertColumn(rsMeta, 5, "COLUMN_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 6, "BUFFER_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 7, "DECIMAL_DIGITS", Types.SMALLINT, DatabaseMetaData.columnNullable);
      assertColumn(rsMeta, 8, "PSEUDO_COLUMN", Types.SMALLINT, DatabaseMetaData.columnNoNulls);
    }
  }

  private static void assertColumn(ResultSetMetaData rsMeta, int column, String name, int type,
      int nullable) throws SQLException {
    assertEquals(
        String.format(Locale.ROOT,
            "Expected column %d to be named '%s', was '%s'.",
            column, name, rsMeta.getColumnName(column)),
        name,
        rsMeta.getColumnName(column));

    assertEquals(
        String.format(Locale.ROOT,
            "Expected column %d type to be '%d', was '%d'.",
            column, type, rsMeta.getColumnType(column)),
        type,
        rsMeta.getColumnType(column));

    assertEquals(
        String.format(Locale.ROOT,
            "Expected column %d nullability to be '%d', was '%d'.",
            column, nullable, rsMeta.isNullable(column)),
        nullable,
        rsMeta.isNullable(column));
  }

  private DatabaseMetaData getDatabaseMetadata() throws SQLException {
    return connection.getMetaData();
  }

}

// End MetaResultSetTest.java
