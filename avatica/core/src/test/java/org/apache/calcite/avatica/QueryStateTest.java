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

import org.apache.calcite.avatica.remote.MetaDataOperation;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * Tests that {@link QueryState} properly retains the necessary state to recreate
 * a {@link ResultSet}.
 */
public class QueryStateTest {

  private Connection conn;
  private DatabaseMetaData metadata;
  private Statement statement;


  @Before
  public void setup() throws Exception {
    conn = Mockito.mock(Connection.class);
    metadata = Mockito.mock(DatabaseMetaData.class);
    statement = Mockito.mock(Statement.class);

    Mockito.when(conn.getMetaData()).thenReturn(metadata);
  }

  @Test
  public void testMetadataGetAttributes() throws Exception {
    final String catalog = "catalog";
    final String schemaPattern = null;
    final String typeNamePattern = "%";
    final String attributeNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_ATTRIBUTES, catalog, schemaPattern,
        typeNamePattern, attributeNamePattern);

    state.invoke(conn, statement);

    Mockito.verify(metadata).getAttributes(catalog, schemaPattern, typeNamePattern,
        attributeNamePattern);
  }

  @Test
  public void testMetadataGetBestRowIdentifier() throws Exception {
    final String catalog = "catalog";
    final String schema = null;
    final String table = "table";
    final int scope = 1;
    final boolean nullable = true;

    QueryState state = new QueryState(MetaDataOperation.GET_BEST_ROW_IDENTIFIER, new Object[] {
      catalog,
      schema,
      table,
      scope,
      nullable
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  @Test
  public void testMetadataGetCatalogs() throws Exception {
    QueryState state = new QueryState(MetaDataOperation.GET_CATALOGS, new Object[0]);

    state.invoke(conn, statement);

    Mockito.verify(metadata).getCatalogs();
  }

  @Test
  public void testMetadataGetColumnPrivileges() throws Exception {
    final String catalog = null;
    final String schema = "schema";
    final String table = "table";
    final String columnNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_COLUMN_PRIVILEGES, new Object[] {
      catalog,
      schema,
      table,
      columnNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  @Test
  public void testMetadataGetColumns() throws Exception {
    final String catalog = null;
    final String schemaPattern = "%";
    final String tableNamePattern = "%";
    final String columnNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_COLUMNS, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern,
      columnNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getColumns(catalog, schemaPattern, tableNamePattern,
        columnNamePattern);
  }

  @Test
  public void testMetadataGetCrossReference() throws Exception {
    final String parentCatalog = null;
    final String parentSchema = null;
    final String parentTable = "%";
    final String foreignCatalog = null;
    final String foreignSchema = null;
    final String foreignTable = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_CROSS_REFERENCE, new Object[] {
      parentCatalog,
      parentSchema,
      parentTable,
      foreignCatalog,
      foreignSchema,
      foreignTable
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getCrossReference(parentCatalog, parentSchema, parentTable,
        foreignCatalog, foreignSchema, foreignTable);
  }

  @Test
  public void testMetadataGetExportedKeys() throws Exception {
    final String catalog = "";
    final String schema = null;
    final String table = "mytable";

    QueryState state = new QueryState(MetaDataOperation.GET_EXPORTED_KEYS, new Object[] {
      catalog,
      schema,
      table
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getExportedKeys(catalog, schema, table);
  }

  @Test
  public void testMetadataGetFunctionColumns() throws Exception {
    final String catalog = null;
    final String schemaPattern = "%";
    final String functionNamePattern = "%";
    final String columnNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_FUNCTION_COLUMNS, new Object[] {
      catalog,
      schemaPattern,
      functionNamePattern,
      columnNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getFunctionColumns(catalog, schemaPattern, functionNamePattern,
        columnNamePattern);
  }

  @Test
  public void testMetadataGetFunctions() throws Exception {
    final String catalog = null;
    final String schemaPattern = "%";
    final String functionNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_FUNCTIONS, new Object[] {
      catalog,
      schemaPattern,
      functionNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  @Test
  public void testMetadataGetImportedKeys() throws Exception {
    final String catalog = "";
    final String schema = null;
    final String table = "my_table";

    QueryState state = new QueryState(MetaDataOperation.GET_IMPORTED_KEYS, new Object[] {
      catalog,
      schema,
      table
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getImportedKeys(catalog, schema, table);
  }

  @Test
  public void testMetadataGetIndexInfo() throws Exception {
    final String catalog = "";
    final String schema = null;
    final String table = "my_table";
    final boolean unique = true;
    final boolean approximate = true;

    QueryState state = new QueryState(MetaDataOperation.GET_INDEX_INFO, new Object[] {
      catalog,
      schema,
      table,
      unique,
      approximate
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getIndexInfo(catalog, schema, table, unique, approximate);
  }

  @Test
  public void testMetadataGetPrimaryKeys() throws Exception {
    final String catalog = "";
    final String schema = null;
    final String table = "my_table";

    QueryState state = new QueryState(MetaDataOperation.GET_PRIMARY_KEYS, new Object[] {
      catalog,
      schema,
      table
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getPrimaryKeys(catalog, schema, table);
  }

  @Test
  public void testMetadataGetProcedureColumns() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String procedureNamePattern = "%";
    final String columnNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_PROCEDURE_COLUMNS, new Object[] {
      catalog,
      schemaPattern,
      procedureNamePattern,
      columnNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getProcedureColumns(catalog, schemaPattern, procedureNamePattern,
        columnNamePattern);
  }

  @Test
  public void testMetadataGetProcedures() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String procedureNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_PROCEDURES, new Object[] {
      catalog,
      schemaPattern,
      procedureNamePattern,
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  @Test
  public void testMetadataGetPseudoColumns() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String tableNamePattern = "%";
    final String columnNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_PSEUDO_COLUMNS, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern,
      columnNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getPseudoColumns(catalog, schemaPattern, tableNamePattern,
        columnNamePattern);
  }

  @Test
  public void testMetadataGetSchemas() throws Exception {
    QueryState state = new QueryState(MetaDataOperation.GET_SCHEMAS, new Object[0]);

    state.invoke(conn, statement);

    Mockito.verify(metadata).getSchemas();
  }

  @Test
  public void testMetadataGetSchemasWithArgs() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;

    QueryState state = new QueryState(MetaDataOperation.GET_SCHEMAS_WITH_ARGS, new Object[] {
      catalog,
      schemaPattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getSchemas(catalog, schemaPattern);
  }

  @Test
  public void testMetadataGetSuperTables() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String tableNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_SUPER_TABLES, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  @Test
  public void testMetadataGetSuperTypes() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String tableNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_SUPER_TYPES, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getSuperTypes(catalog, schemaPattern, tableNamePattern);
  }

  @Test
  public void testMetadataGetTablePrivileges() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String tableNamePattern = "%";

    QueryState state = new QueryState(MetaDataOperation.GET_TABLE_PRIVILEGES, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  @Test
  public void testMetadataGetTables() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String tableNamePattern = "%";
    final String[] types = new String[] {"VIEW", "TABLE"};

    QueryState state = new QueryState(MetaDataOperation.GET_TABLES, new Object[] {
      catalog,
      schemaPattern,
      tableNamePattern,
      types
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  @Test
  public void testMetadataGetTableTypes() throws Exception {
    QueryState state = new QueryState(MetaDataOperation.GET_TABLE_TYPES, new Object[0]);

    state.invoke(conn, statement);

    Mockito.verify(metadata).getTableTypes();
  }

  @Test
  public void testMetadataGetTypeInfo() throws Exception {
    QueryState state = new QueryState(MetaDataOperation.GET_TYPE_INFO, new Object[0]);

    state.invoke(conn, statement);

    Mockito.verify(metadata).getTypeInfo();
  }

  @Test
  public void testMetadataGetUDTs() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String typeNamePattern = "%";
    final int[] types = new int[] {1, 2};

    QueryState state = new QueryState(MetaDataOperation.GET_UDTS, new Object[] {
      catalog,
      schemaPattern,
      typeNamePattern,
      types
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  @Test
  public void testMetadataGetVersionColumns() throws Exception {
    final String catalog = "";
    final String schemaPattern = null;
    final String table = "my_table";

    QueryState state = new QueryState(MetaDataOperation.GET_VERSION_COLUMNS, new Object[] {
      catalog,
      schemaPattern,
      table
    });

    state.invoke(conn, statement);

    Mockito.verify(metadata).getVersionColumns(catalog, schemaPattern, table);
  }

  @Test
  public void testSerialization() throws Exception {
    final String catalog = "catalog";
    final String schema = null;
    final String table = "table";
    final int scope = 1;
    final boolean nullable = true;

    QueryState state = new QueryState(MetaDataOperation.GET_BEST_ROW_IDENTIFIER, new Object[] {
      catalog,
      schema,
      table,
      scope,
      nullable
    });

    assertEquals(state, QueryState.fromProto(state.toProto()));

    final String schemaPattern = null;
    final String typeNamePattern = "%";
    final int[] types = new int[] {1, 2};

    state = new QueryState(MetaDataOperation.GET_UDTS, new Object[] {
      catalog,
      schemaPattern,
      typeNamePattern,
      types
    });

    assertEquals(state, QueryState.fromProto(state.toProto()));

    state = new QueryState("SELECT * FROM foo");

    assertEquals(state, QueryState.fromProto(state.toProto()));
  }

}

// End QueryStateTest.java
