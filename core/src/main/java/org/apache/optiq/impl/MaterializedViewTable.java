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
package org.apache.optiq.impl;

import org.apache.optiq.*;
import org.apache.optiq.impl.enumerable.JavaTypeFactory;
import org.apache.optiq.jdbc.OptiqConnection;
import org.apache.optiq.jdbc.OptiqPrepare;
import org.apache.optiq.jdbc.OptiqSchema;
import org.apache.optiq.materialize.MaterializationKey;
import org.apache.optiq.materialize.MaterializationService;

import org.apache.optiq.rel.RelNode;
import org.apache.optiq.relopt.RelOptTable;
import org.apache.optiq.reltype.RelDataTypeImpl;
import org.apache.optiq.reltype.RelProtoDataType;

import java.lang.reflect.Type;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Table that is a materialized view.
 *
 * <p>It can exist in two states: materialized and not materialized. Over time,
 * a given materialized view may switch states. How it is expanded depends upon
 * its current state. State is managed by
 * {@link org.apache.optiq.materialize.MaterializationService}.</p>
 */
public class MaterializedViewTable extends ViewTable {

  private final MaterializationKey key;

  /**
   * Internal connection, used to execute queries to materialize views.
   * To be used only by Optiq internals. And sparingly.
   */
  public static final OptiqConnection MATERIALIZATION_CONNECTION;

  static {
    try {
      MATERIALIZATION_CONNECTION = DriverManager.getConnection("jdbc:optiq:")
          .unwrap(OptiqConnection.class);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MaterializedViewTable(Type elementType,
      RelProtoDataType relDataType,
      String viewSql,
      List<String> viewSchemaPath,
      MaterializationKey key) {
    super(elementType, relDataType, viewSql, viewSchemaPath);
    this.key = key;
  }

  /** Table macro that returns a materialized view. */
  public static MaterializedViewTableMacro create(final OptiqSchema schema,
      final String viewSql,
      final List<String> viewSchemaPath,
      final String tableName) {
    return new MaterializedViewTableMacro(schema, viewSql, viewSchemaPath,
        tableName);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final OptiqSchema.TableEntry tableEntry =
        MaterializationService.instance().checkValid(key);
    if (tableEntry != null) {
      Table materializeTable = tableEntry.getTable();
      if (materializeTable instanceof TranslatableTable) {
        TranslatableTable table = (TranslatableTable) materializeTable;
        return table.toRel(context, relOptTable);
      }
    }
    return super.toRel(context, relOptTable);
  }

  /** Table function that returns the table that materializes a view. */
  public static class MaterializedViewTableMacro
      extends ViewTableMacro {
    private final MaterializationKey key;

    private MaterializedViewTableMacro(OptiqSchema schema, String viewSql,
        List<String> viewSchemaPath, String tableName) {
      super(schema, viewSql, viewSchemaPath);
      this.key =
          MaterializationService.instance().defineMaterialization(
              schema, viewSql, schemaPath, tableName);
    }

    @Override
    public TranslatableTable apply(List<Object> arguments) {
      assert arguments.isEmpty();
      OptiqPrepare.ParseResult parsed =
          Schemas.parse(MATERIALIZATION_CONNECTION, schema, schemaPath,
              viewSql);
      final List<String> schemaPath1 =
          schemaPath != null ? schemaPath : schema.path(null);
      final JavaTypeFactory typeFactory =
          MATERIALIZATION_CONNECTION.getTypeFactory();
      return new MaterializedViewTable(typeFactory.getJavaClass(parsed.rowType),
          RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath1, key);
    }
  }
}

// End MaterializedViewTable.java
