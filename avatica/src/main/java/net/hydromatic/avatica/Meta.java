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
package net.hydromatic.avatica;

import java.sql.ResultSet;
import java.util.List;

/**
 * Command handler for getting various metadata. Should be implemented by each
 * driver.
 *
 * <p>Also holds other abstract methods that are not related to metadata
 * that each provider must implement. This is not ideal.</p>
 */
public interface Meta {
  String getSqlKeywords();

  String getNumericFunctions();

  String getStringFunctions();

  String getSystemFunctions();

  String getTimeDateFunctions();

  ResultSet getTables(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  ResultSet getColumns(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  ResultSet getSchemas(String catalog, Pat schemaPattern);

  ResultSet getCatalogs();

  ResultSet getTableTypes();

  ResultSet getProcedures(
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  ResultSet getProcedureColumns(
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  ResultSet getColumnPrivileges(
      String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  ResultSet getTablePrivileges(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  ResultSet getBestRowIdentifier(
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  ResultSet getVersionColumns(
      String catalog, String schema, String table);

  ResultSet getPrimaryKeys(
      String catalog, String schema, String table);

  ResultSet getImportedKeys(
      String catalog, String schema, String table);

  ResultSet getExportedKeys(
      String catalog, String schema, String table);

  ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  ResultSet getTypeInfo();

  ResultSet getIndexInfo(
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  ResultSet getUDTs(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  ResultSet getSuperTypes(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  ResultSet getSuperTables(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  ResultSet getAttributes(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  ResultSet getClientInfoProperties();

  ResultSet getFunctions(
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  ResultSet getFunctionColumns(
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

  ResultSet getPseudoColumns(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  /** Creates a cursor for a result set. */
  Cursor createCursor(AvaticaResultSet resultSet);

  AvaticaPrepareResult prepare(AvaticaStatement statement, String sql);

  /** Wrapper to remind API calls that a parameter is a pattern (allows '%' and
   * '_' wildcards, per the JDBC spec) rather than a string to be matched
   * exactly. */
  class Pat {
    public final String s;

    private Pat(String s) {
      this.s = s;
    }

    public static Pat of(String name) {
      return new Pat(name);
    }
  }
}

// End Meta.java
