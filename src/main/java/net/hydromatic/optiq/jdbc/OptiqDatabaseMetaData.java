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

import java.sql.*;

/**
 * Implementation of {@link java.sql.DatabaseMetaData}
 * for the OPTIQ engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newDatabaseMetaData}.</p>
 *
 * @author jhyde
 */
class OptiqDatabaseMetaData implements DatabaseMetaData {
    private static final OptiqConnectionImpl.Helper HELPER =
        OptiqConnectionImpl.HELPER;

    private final OptiqConnectionImpl connection;

    OptiqDatabaseMetaData(OptiqConnectionImpl connection) {
        this.connection = connection;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        throw HELPER.todo();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw HELPER.todo();
    }

    public String getURL() throws SQLException {
        throw HELPER.todo();
    }

    public String getUserName() throws SQLException {
        throw HELPER.todo();
    }

    public boolean isReadOnly() throws SQLException {
        throw HELPER.todo();
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        throw HELPER.todo();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        throw HELPER.todo();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        throw HELPER.todo();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw HELPER.todo();
    }

    public String getDatabaseProductName() throws SQLException {
        return connection.driver.version.productName;
    }

    public String getDatabaseProductVersion() throws SQLException {
        return connection.driver.version.productVersion;
    }

    public String getDriverName() throws SQLException {
        return connection.driver.getName();
    }

    public String getDriverVersion() throws SQLException {
        return connection.driver.getVersion();
    }

    public int getDriverMajorVersion() {
        return connection.driver.getMajorVersion();
    }

    public int getDriverMinorVersion() {
        return connection.driver.getMinorVersion();
    }

    public boolean usesLocalFiles() throws SQLException {
        throw HELPER.todo();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw HELPER.todo();
    }

    public String getIdentifierQuoteString() throws SQLException {
        throw HELPER.todo();
    }

    public String getSQLKeywords() throws SQLException {
        throw HELPER.todo();
    }

    public String getNumericFunctions() throws SQLException {
        throw HELPER.todo();
    }

    public String getStringFunctions() throws SQLException {
        throw HELPER.todo();
    }

    public String getSystemFunctions() throws SQLException {
        throw HELPER.todo();
    }

    public String getTimeDateFunctions() throws SQLException {
        throw HELPER.todo();
    }

    public String getSearchStringEscape() throws SQLException {
        throw HELPER.todo();
    }

    public String getExtraNameCharacters() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        throw HELPER.todo();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsConvert() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsConvert(
        int fromType, int toType) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsGroupBy() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw HELPER.todo();
    }

    public String getSchemaTerm() throws SQLException {
        throw HELPER.todo();
    }

    public String getProcedureTerm() throws SQLException {
        throw HELPER.todo();
    }

    public String getCatalogTerm() throws SQLException {
        throw HELPER.todo();
    }

    public boolean isCatalogAtStart() throws SQLException {
        throw HELPER.todo();
    }

    public String getCatalogSeparator() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsUnion() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsUnionAll() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxColumnsInTable() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxConnections() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxCursorNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxIndexLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxRowSize() throws SQLException {
        throw HELPER.todo();
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxStatementLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxStatements() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxTableNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxTablesInSelect() throws SQLException {
        throw HELPER.todo();
    }

    public int getMaxUserNameLength() throws SQLException {
        throw HELPER.todo();
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsTransactions() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw HELPER.todo();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getProcedures(
        String catalog,
        String schemaPattern,
        String procedureNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String[] types) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getSchemas() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getCatalogs() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getTableTypes() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getVersionColumns(
        String catalog, String schema, String table) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getPrimaryKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getImportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getExportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getCrossReference(
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getTypeInfo() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsResultSetConcurrency(
        int type, int concurrency) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsBatchUpdates() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getUDTs(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        int[] types) throws SQLException
    {
        throw HELPER.todo();
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

    public boolean supportsSavepoints() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsNamedParameters() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getSuperTypes(
        String catalog,
        String schemaPattern,
        String typeNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getSuperTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getAttributes(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException
    {
        throw HELPER.todo();
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
        throw HELPER.todo();
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        throw HELPER.todo();
    }

    public boolean supportsStatementPooling() throws SQLException {
        throw HELPER.todo();
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getSchemas(
        String catalog, String schemaPattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        throw HELPER.todo();
    }

    public ResultSet getFunctions(
        String catalog,
        String schemaPattern,
        String functionNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getFunctionColumns(
        String catalog,
        String schemaPattern,
        String functionNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public ResultSet getPseudoColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw HELPER.todo();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw HELPER.todo();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw HELPER.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}

// End OptiqDatabaseMetaData.java
