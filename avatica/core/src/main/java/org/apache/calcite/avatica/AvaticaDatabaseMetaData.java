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

import org.apache.calcite.avatica.AvaticaConnection.CallableWithoutException;
import org.apache.calcite.avatica.Meta.DatabaseProperty;
import org.apache.calcite.avatica.remote.MetaDataOperation;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static org.apache.calcite.avatica.InternalProperty.CASE_SENSITIVE;
import static org.apache.calcite.avatica.InternalProperty.NULL_SORTING;
import static org.apache.calcite.avatica.InternalProperty.NullSorting;
import static org.apache.calcite.avatica.InternalProperty.QUOTED_CASING;
import static org.apache.calcite.avatica.InternalProperty.QUOTING;
import static org.apache.calcite.avatica.InternalProperty.UNQUOTED_CASING;

/**
 * Implementation of {@link java.sql.DatabaseMetaData}
 * for the Avatica engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link AvaticaFactory#newDatabaseMetaData}.</p>
 */
public class AvaticaDatabaseMetaData implements AvaticaSpecificDatabaseMetaData {
  private final AvaticaConnection connection;

  protected  AvaticaDatabaseMetaData(AvaticaConnection connection) {
    this.connection = connection;
  }

  // Helper methods

  private NullSorting nullSorting() {
    return NULL_SORTING.getEnum(getProperties(), NullSorting.class);
  }

  private Quoting quoting() {
    return QUOTING.getEnum(getProperties(), Quoting.class);
  }

  private Casing unquotedCasing() {
    return UNQUOTED_CASING.getEnum(getProperties(), Casing.class);
  }

  private Casing quotedCasing() {
    return QUOTED_CASING.getEnum(getProperties(), Casing.class);
  }

  private boolean caseSensitive() {
    return CASE_SENSITIVE.getBoolean(getProperties());
  }

  // JDBC methods

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
    return nullSorting() == NullSorting.HIGH;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return nullSorting() == NullSorting.LOW;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return nullSorting() == NullSorting.START;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return nullSorting() == NullSorting.END;
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

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return !caseSensitive() && unquotedCasing() == Casing.UNCHANGED;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return caseSensitive() && unquotedCasing() == Casing.UNCHANGED;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return unquotedCasing() == Casing.TO_UPPER;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return unquotedCasing() == Casing.TO_LOWER;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return !caseSensitive() && quotedCasing() == Casing.UNCHANGED;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return caseSensitive() && quotedCasing() == Casing.UNCHANGED;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return quotedCasing() == Casing.TO_UPPER;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return quotedCasing() == Casing.TO_LOWER;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return quoting().string;
  }

  private Map<InternalProperty, Object> getProperties() {
    return connection.properties;
  }

  public String getSQLKeywords() throws SQLException {
    return connection.invokeWithRetries(
        new CallableWithoutException<String>() {
          public String call() {
            return Meta.DatabaseProperty.GET_S_Q_L_KEYWORDS
                .getProp(connection.meta, connection.handle, String.class);
          }
        });
  }

  public String getNumericFunctions() throws SQLException {
    return connection.invokeWithRetries(
        new CallableWithoutException<String>() {
          public String call() {
            return Meta.DatabaseProperty.GET_NUMERIC_FUNCTIONS
                .getProp(connection.meta, connection.handle, String.class);
          }
        });
  }

  public String getStringFunctions() throws SQLException {
    return connection.invokeWithRetries(
        new CallableWithoutException<String>() {
          public String call() {
            return Meta.DatabaseProperty.GET_STRING_FUNCTIONS
                .getProp(connection.meta, connection.handle, String.class);
          }
        });
  }

  public String getSystemFunctions() throws SQLException {
    return connection.invokeWithRetries(
        new CallableWithoutException<String>() {
          public String call() {
            return Meta.DatabaseProperty.GET_SYSTEM_FUNCTIONS
                .getProp(connection.meta, connection.handle, String.class);
          }
        });
  }

  public String getTimeDateFunctions() throws SQLException {
    return connection.invokeWithRetries(
        new CallableWithoutException<String>() {
          public String call() {
            return Meta.DatabaseProperty.GET_TIME_DATE_FUNCTIONS
                .getProp(connection.meta, connection.handle, String.class);
          }
        });
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

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
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
    return connection.invokeWithRetries(
        new CallableWithoutException<Integer>() {
          public Integer call() {
            return Meta.DatabaseProperty.GET_DEFAULT_TRANSACTION_ISOLATION
                .getProp(connection.meta, connection.handle, Integer.class);
          }
        });
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
      final String catalog,
      final String schemaPattern,
      final String procedureNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getProcedures(connection.handle, catalog, pat(schemaPattern),
                        pat(procedureNamePattern)),
                    new QueryState(MetaDataOperation.GET_PROCEDURES, catalog, schemaPattern,
                        procedureNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getProcedureColumns(
      final String catalog,
      final String schemaPattern,
      final String procedureNamePattern,
      final String columnNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getProcedureColumns(connection.handle, catalog,
                        pat(schemaPattern), pat(procedureNamePattern), pat(columnNamePattern)),
                    new QueryState(MetaDataOperation.GET_PROCEDURE_COLUMNS, catalog, schemaPattern,
                        procedureNamePattern, columnNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getTables(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String[] types) throws SQLException {
    final List<String> typeList = types == null ? null : Arrays.asList(types);
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getTables(connection.handle, catalog, pat(schemaPattern),
                        pat(tableNamePattern), typeList),
                    new QueryState(MetaDataOperation.GET_TABLES, catalog, schemaPattern,
                        tableNamePattern, types));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  private static Meta.Pat pat(String schemaPattern) {
    return Meta.Pat.of(schemaPattern);
  }

  public ResultSet getSchemas(
      final String catalog, final String schemaPattern) throws SQLException {
    // TODO: add a 'catch ... throw new SQLException' logic to this and other
    // getXxx methods. Right now any error will throw a RuntimeException
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getSchemas(connection.handle, catalog, pat(schemaPattern)),
                    new QueryState(MetaDataOperation.GET_SCHEMAS_WITH_ARGS, catalog,
                        schemaPattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  public ResultSet getCatalogs() throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(connection.meta.getCatalogs(connection.handle),
                    new QueryState(MetaDataOperation.GET_CATALOGS));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getTableTypes() throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(connection.meta.getTableTypes(connection.handle),
                    new QueryState(MetaDataOperation.GET_TABLE_TYPES));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getColumns(connection.handle, catalog, pat(schemaPattern),
                        pat(tableNamePattern), pat(columnNamePattern)),
                    new QueryState(MetaDataOperation.GET_COLUMNS, catalog, schemaPattern,
                        tableNamePattern, columnNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getColumnPrivileges(
      final String catalog,
      final String schema,
      final String table,
      final String columnNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getColumnPrivileges(connection.handle, catalog, schema, table,
                        pat(columnNamePattern)),
                    new QueryState(MetaDataOperation.GET_COLUMN_PRIVILEGES, catalog, schema, table,
                        columnNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getTablePrivileges(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getTablePrivileges(connection.handle, catalog,
                        pat(schemaPattern), pat(tableNamePattern)),
                    new QueryState(MetaDataOperation.GET_TABLE_PRIVILEGES, catalog, schemaPattern,
                        tableNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getBestRowIdentifier(
      final String catalog,
      final String schema,
      final String table,
      final int scope,
      final boolean nullable) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getBestRowIdentifier(connection.handle, catalog, schema, table,
                        scope, nullable),
                    new QueryState(MetaDataOperation.GET_BEST_ROW_IDENTIFIER, catalog, table, scope,
                        nullable));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getVersionColumns(
      final String catalog, final String schema, final String table) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getVersionColumns(connection.handle, catalog, schema, table),
                    new QueryState(MetaDataOperation.GET_VERSION_COLUMNS, catalog, schema, table));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getPrimaryKeys(
      final String catalog, final String schema, final String table) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getPrimaryKeys(connection.handle, catalog, schema, table),
                    new QueryState(MetaDataOperation.GET_PRIMARY_KEYS, catalog, schema, table));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getImportedKeys(
      final String catalog, final String schema, final String table) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getImportedKeys(connection.handle, catalog, schema, table),
                    new QueryState(MetaDataOperation.GET_IMPORTED_KEYS, catalog, schema, table));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getExportedKeys(
      final String catalog, final String schema, final String table) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getExportedKeys(connection.handle, catalog, schema, table),
                    new QueryState(MetaDataOperation.GET_EXPORTED_KEYS, catalog, schema, table));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getCrossReference(
      final String parentCatalog,
      final String parentSchema,
      final String parentTable,
      final String foreignCatalog,
      final String foreignSchema,
      final String foreignTable) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getCrossReference(connection.handle, parentCatalog,
                        parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable),
                    new QueryState(MetaDataOperation.GET_CROSS_REFERENCE, parentCatalog,
                        parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getTypeInfo() throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(connection.meta.getTypeInfo(connection.handle),
                    new QueryState(MetaDataOperation.GET_TYPE_INFO));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getIndexInfo(
      final String catalog,
      final String schema,
      final String table,
      final boolean unique,
      final boolean approximate) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getIndexInfo(connection.handle, catalog, schema, table, unique,
                        approximate),
                    new QueryState(MetaDataOperation.GET_INDEX_INFO, catalog, schema, table, unique,
                        approximate));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
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
    return true;
  }

  public ResultSet getUDTs(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final int[] types) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getUDTs(connection.handle, catalog, pat(schemaPattern),
                        pat(typeNamePattern), types),
                    new QueryState(MetaDataOperation.GET_UDTS, catalog, schemaPattern,
                        typeNamePattern, types));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
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
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getSuperTypes(connection.handle, catalog, pat(schemaPattern),
                        pat(typeNamePattern)),
                    new QueryState(MetaDataOperation.GET_SUPER_TYPES, catalog, schemaPattern,
                        typeNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getSuperTables(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getSuperTables(connection.handle, catalog, pat(schemaPattern),
                        pat(tableNamePattern)),
                    new QueryState(MetaDataOperation.GET_SUPER_TABLES, catalog, schemaPattern,
                        tableNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getAttributes(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final String attributeNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getAttributes(connection.handle, catalog, pat(schemaPattern),
                        pat(typeNamePattern), pat(attributeNamePattern)),
                    new QueryState(MetaDataOperation.GET_ATTRIBUTES, catalog, schemaPattern,
                        typeNamePattern, attributeNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
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
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getClientInfoProperties(connection.handle),
                    new QueryState(MetaDataOperation.GET_CLIENT_INFO_PROPERTIES));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getFunctions(
      final String catalog,
      final String schemaPattern,
      final String functionNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getFunctions(connection.handle, catalog, pat(schemaPattern),
                        pat(functionNamePattern)),
                    new QueryState(MetaDataOperation.GET_FUNCTIONS, catalog, schemaPattern,
                        functionNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getFunctionColumns(
      final String catalog,
      final String schemaPattern,
      final String functionNamePattern,
      final String columnNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getFunctionColumns(connection.handle, catalog,
                        pat(schemaPattern), pat(functionNamePattern), pat(columnNamePattern)),
                    new QueryState(MetaDataOperation.GET_FUNCTION_COLUMNS, catalog,
                        schemaPattern, functionNamePattern, columnNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public ResultSet getPseudoColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern) throws SQLException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ResultSet>() {
            public ResultSet call() {
              try {
                return connection.createResultSet(
                    connection.meta.getPseudoColumns(connection.handle, catalog, pat(schemaPattern),
                        pat(tableNamePattern), pat(columnNamePattern)),
                    new QueryState(MetaDataOperation.GET_PSEUDO_COLUMNS, catalog, schemaPattern,
                        tableNamePattern, columnNamePattern));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw e;
    }
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  // implement Wrapper

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }

    if (Properties.class.equals(iface)) {
      return iface.cast(getRemoteAvaticaProperties());
    }

    throw connection.helper.createException(
        "does not implement '" + iface + "'");
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this) || Properties.class.equals(iface);
  }

  // Not JDBC Methods

  @Override public Properties getRemoteAvaticaProperties() {
    Map<DatabaseProperty, Object> propertyMap = connection.invokeWithRetries(
        new CallableWithoutException<Map<DatabaseProperty, Object>>() {
          public Map<DatabaseProperty, Object> call() {
            return connection.meta.getDatabaseProperties(connection.handle);
          }
        });

    final Properties properties = new Properties();
    for (Entry<DatabaseProperty, Object> entry: propertyMap.entrySet()) {
      properties.setProperty(entry.getKey().name(), entry.getValue().toString());
    }

    return properties;
  }

  /**
   * Fetches the Avatica version from the given server.
   *
   * @return The Avatica version string or null if the server did not provide the version.
   */
  @Override public String getAvaticaServerVersion() {
    Map<DatabaseProperty, Object> properties = connection.invokeWithRetries(
        new CallableWithoutException<Map<DatabaseProperty, Object>>() {
          public Map<DatabaseProperty, Object> call() {
            return connection.meta.getDatabaseProperties(connection.handle);
          }
        });
    Object o = properties.get(DatabaseProperty.AVATICA_VERSION);
    if (null == o) {
      return null;
    }
    return (String) o;
  }
}

// End AvaticaDatabaseMetaData.java
