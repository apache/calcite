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

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.runtime.ColumnMetaData;
import net.hydromatic.optiq.runtime.Cursor;

import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link java.sql.DatabaseMetaData}
 * for the Optiq engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newDatabaseMetaData}.</p>
 *
 * @author jhyde
 */
class OptiqDatabaseMetaData implements DatabaseMetaData {
    private final OptiqConnectionImpl connection;
    final Meta meta;

    OptiqDatabaseMetaData(OptiqConnectionImpl connection) {
        this.connection = connection;
        this.meta = new Meta(connection);
    }

    public boolean allProceduresAreCallable() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw connection.helper.todo();
    }

    public String getURL() throws SQLException {
        throw connection.helper.todo();
    }

    public String getUserName() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw connection.helper.todo();
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
        throw connection.helper.todo();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        throw connection.helper.todo();
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
        throw connection.helper.todo();
    }

    public String getNumericFunctions() throws SQLException {
        throw connection.helper.todo();
    }

    public String getStringFunctions() throws SQLException {
        throw connection.helper.todo();
    }

    public String getSystemFunctions() throws SQLException {
        throw connection.helper.todo();
    }

    public String getTimeDateFunctions() throws SQLException {
        throw connection.helper.todo();
    }

    public String getSearchStringEscape() throws SQLException {
        throw connection.helper.todo();
    }

    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsConvert() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsConvert(
        int fromType, int toType) throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsGroupBy() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw connection.helper.todo();
    }

    public String getSchemaTerm() throws SQLException {
        throw connection.helper.todo();
    }

    public String getProcedureTerm() throws SQLException {
        throw connection.helper.todo();
    }

    public String getCatalogTerm() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean isCatalogAtStart() throws SQLException {
        throw connection.helper.todo();
    }

    public String getCatalogSeparator() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsUnion() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsUnionAll() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw connection.helper.todo();
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
        throw connection.helper.todo();
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
        throw connection.helper.todo();
    }

    public int getMaxIndexLength() throws SQLException {
        throw connection.helper.todo();
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
        throw connection.helper.todo();
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
        throw connection.helper.todo();
    }

    public boolean supportsTransactions() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw connection.helper.todo();
    }

    public ResultSet getProcedures(
        String catalog,
        String schemaPattern,
        String procedureNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getTables(
        String catalog,
        final String schemaPattern,
        String tableNamePattern,
        String[] types) throws SQLException
    {
        return meta.getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    public ResultSet getSchemas() throws SQLException {
        return createEmptyResultSet();
    }

    public ResultSet getCatalogs() throws SQLException {
        return createEmptyResultSet();
    }

    public ResultSet getTableTypes() throws SQLException {
        return createEmptyResultSet();
    }

    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException
    {
        return meta.getColumns(
            catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getVersionColumns(
        String catalog, String schema, String table) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getPrimaryKeys(
        String catalog, String schema, String table) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getImportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getExportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getCrossReference(
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getTypeInfo() throws SQLException {
        return createEmptyResultSet();
    }

    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate) throws SQLException
    {
        return createEmptyResultSet();
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    public boolean supportsResultSetConcurrency(
        int type, int concurrency) throws SQLException
    {
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
        throw connection.helper.todo();
    }

    public ResultSet getUDTs(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        int[] types) throws SQLException
    {
        return createEmptyResultSet();
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

    public boolean supportsSavepoints() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsNamedParameters() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw connection.helper.todo();
    }

    public ResultSet getSuperTypes(
        String catalog,
        String schemaPattern,
        String typeNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getSuperTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getAttributes(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException
    {
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
        throw connection.helper.todo();
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        throw connection.helper.todo();
    }

    public boolean supportsStatementPooling() throws SQLException {
        throw connection.helper.todo();
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw connection.helper.todo();
    }

    public ResultSet getSchemas(
        String catalog, String schemaPattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException
    {
        throw connection.helper.todo();
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw connection.helper.todo();
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        return createEmptyResultSet();
    }

    public ResultSet getFunctions(
        String catalog,
        String schemaPattern,
        String functionNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getFunctionColumns(
        String catalog,
        String schemaPattern,
        String functionNamePattern,
        String columnNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public ResultSet getPseudoColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException
    {
        return createEmptyResultSet();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw connection.helper.todo();
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

    // Helpers

    private ResultSet createEmptyResultSet() {
        try {
            return connection.driver.factory.newResultSet(
                connection.createStatement(),
                Collections.<ColumnMetaData>emptyList(),
                new Function0<Cursor>() {
                    public Cursor apply() {
                        return new Cursor() {
                            public List<Accessor> createAccessors(
                                List<ColumnMetaData> types)
                            {
                                assert types.isEmpty();
                                return Collections.emptyList();
                            }

                            public boolean next() {
                                return false;
                            }
                        };
                    }
                }).execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

// End OptiqDatabaseMetaData.java
