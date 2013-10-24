/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.jdbc;

import org.eigenbase.sql.SqlJdbcFunctionCall;
import org.eigenbase.sql.parser.SqlParser;

import java.sql.*;

/**
 * Implementation of {@link java.sql.DatabaseMetaData}
 * for the Optiq engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newDatabaseMetaData}.</p>
 */
class OptiqDatabaseMetaData implements DatabaseMetaData {
  private final OptiqConnectionImpl connection;
  final Meta meta;

  OptiqDatabaseMetaData(OptiqConnectionImpl connection) {
    this.connection = connection;
    this.meta = new Meta(connection);
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return true;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public String getURL() throws SQLException {
    return connection.url;
  }

  public String getUserName() throws SQLException {
    return connection.info.getProperty("user");
  }

  public boolean isReadOnly() throws SQLException {
    return true;
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return true;
  }

  public String getDatabaseProductName() throws SQLException {
    return connection.driver.version.productName;
  }

  public String getDatabaseProductVersion() throws SQLException {
    return connection.driver.version.productVersion;
  }

  public String getDriverName() throws SQLException {
    return connection.driver.version.name;
  }

  public String getDriverVersion() throws SQLException {
    return connection.driver.version.versionString;
  }

  public int getDriverMajorVersion() {
    return connection.driver.getMajorVersion();
  }

  public int getDriverMinorVersion() {
    return connection.driver.getMinorVersion();
  }

  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  public String getSQLKeywords() throws SQLException {
    return new SqlParser("").getParserImpl().getMetadata().getJdbcKeywords();
  }

  public String getNumericFunctions() throws SQLException {
    return SqlJdbcFunctionCall.getNumericFunctions();
  }

  public String getStringFunctions() throws SQLException {
    return SqlJdbcFunctionCall.getStringFunctions();
  }

  public String getSystemFunctions() throws SQLException {
    return SqlJdbcFunctionCall.getSystemFunctions();
  }

  public String getTimeDateFunctions() throws SQLException {
    return SqlJdbcFunctionCall.getTimeDateFunctions();
  }

  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    return true;
  }

  public boolean supportsConvert(
      int fromType, int toType) throws SQLException {
    return false; // TODO: more detail
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return true;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    return true;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return true;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return true;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return true;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    return true;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  public String getProcedureTerm() throws SQLException {
    return "procedure";
  }

  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true; // except that we don't support index definitions
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true; // except that we don't support privilege definitions
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true; // except that we don't support index definitions
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return true; // except that we don't support privilege definitions
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  public boolean supportsUnion() throws SQLException {
    return true;
  }

  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  public int getMaxColumnNameLength() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  public int getMaxConnections() throws SQLException {
    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return 0;
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    return level == Connection.TRANSACTION_NONE;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly()
      throws SQLException {
    return true;
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return true;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  public ResultSet getProcedures(
      String catalog,
      String schemaPattern,
      String procedureNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaProcedure.class);
  }

  public ResultSet getProcedureColumns(
      String catalog,
      String schemaPattern,
      String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection,
        Meta.MetaProcedureColumn.class);
  }

  public ResultSet getTables(
      String catalog,
      final String schemaPattern,
      String tableNamePattern,
      String[] types) throws SQLException {
    return meta.getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  public ResultSet getSchemas(
      String catalog, String schemaPattern) throws SQLException {
    return meta.getSchemas(catalog, schemaPattern);
  }

  public ResultSet getSchemas() throws SQLException {
    return meta.getSchemas(null, null);
  }

  public ResultSet getCatalogs() throws SQLException {
    return meta.getCatalogs();
  }

  public ResultSet getTableTypes() throws SQLException {
    return meta.getTableTypes();
  }

  public ResultSet getColumns(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return meta.getColumns(
        catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  public ResultSet getColumnPrivileges(
      String catalog,
      String schema,
      String table,
      String columnNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection,
        Meta.MetaColumnPrivilege.class);
  }

  public ResultSet getTablePrivileges(
      String catalog,
      String schemaPattern,
      String tableNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaTablePrivilege.class);
  }

  public ResultSet getBestRowIdentifier(
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable) throws SQLException {
    return Meta.createEmptyResultSet(connection,
        Meta.MetaBestRowIdentifier.class);
  }

  public ResultSet getVersionColumns(
      String catalog, String schema, String table) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaVersionColumn.class);
  }

  public ResultSet getPrimaryKeys(
      String catalog, String schema, String table) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaPrimaryKey.class);
  }

  public ResultSet getImportedKeys(
      String catalog, String schema, String table) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaImportedKey.class);
  }

  public ResultSet getExportedKeys(
      String catalog, String schema, String table) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaExportedKey.class);
  }

  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaCrossReference.class);
  }

  public ResultSet getTypeInfo() throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaTypeInfo.class);
  }

  public ResultSet getIndexInfo(
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaIndexInfo.class);
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  public boolean supportsResultSetConcurrency(
      int type, int concurrency) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY
        && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    throw connection.helper.todo();
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public ResultSet getUDTs(
      String catalog,
      String schemaPattern,
      String typeNamePattern,
      int[] types) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaUdt.class);
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  public ResultSet getSuperTypes(
      String catalog,
      String schemaPattern,
      String typeNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaSuperType.class);
  }

  public ResultSet getSuperTables(
      String catalog,
      String schemaPattern,
      String tableNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaSuperTable.class);
  }

  public ResultSet getAttributes(
      String catalog,
      String schemaPattern,
      String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaAttribute.class);
  }

  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException {
    throw connection.helper.todo();
  }

  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return connection.driver.version.databaseMajorVersion;
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return connection.driver.version.databaseMinorVersion;
  }

  public int getJDBCMajorVersion() throws SQLException {
    return connection.factory.getJdbcMajorVersion();
  }

  public int getJDBCMinorVersion() throws SQLException {
    return connection.factory.getJdbcMinorVersion();
  }

  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    return true;
  }

  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax()
      throws SQLException {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    return Meta.createEmptyResultSet(connection,
        Meta.MetaClientInfoProperty.class);
  }

  public ResultSet getFunctions(
      String catalog,
      String schemaPattern,
      String functionNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaFunction.class);
  }

  public ResultSet getFunctionColumns(
      String catalog,
      String schemaPattern,
      String functionNamePattern,
      String columnNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaFunctionColumn.class);
  }

  public ResultSet getPseudoColumns(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return Meta.createEmptyResultSet(connection, Meta.MetaPseudoColumn.class);
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  // implement Wrapper

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw connection.helper.createException(
        "does not implement '" + iface + "'");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}

// End OptiqDatabaseMetaData.java
