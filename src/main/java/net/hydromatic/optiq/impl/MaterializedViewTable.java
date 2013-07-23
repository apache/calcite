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
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.materialize.MaterializationKey;
import net.hydromatic.optiq.materialize.MaterializationService;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Table that is a materialized view.
 *
 * <p>It can exist in two states: materialized and not materialized. Over time,
 * a given materialized view may switch states. How it is expanded depends upon
 * its current state. State is managed by
 * {@link net.hydromatic.optiq.materialize.MaterializationService}.</p>
 */
public class MaterializedViewTable<T> extends ViewTable<T> {

  private final MaterializationKey key;

  public MaterializedViewTable(Schema schema, Type elementType,
      RelDataType relDataType, String tableName, String viewSql,
      List<String> viewSchemaPath, MaterializationKey key) {
    super(schema, elementType, relDataType, tableName, viewSql, viewSchemaPath);
    this.key = key;
  }

  /** Table function that returns a materialized view. */
  public static Schema.TableFunctionInSchema create(final Schema schema,
      final String viewName,
      final String viewSql,
      final List<String> viewSchemaPath,
      final String tableName) {
    final MaterializedViewTableFunction tableFunction =
        new MaterializedViewTableFunction(schema, viewName, viewSql,
            viewSchemaPath, tableName);
    return new TableFunctionInSchemaImpl(schema, viewName, tableFunction);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final Schema.TableInSchema tableInSchema =
        MaterializationService.INSTANCE.checkValid(key);
    if (tableInSchema != null) {
      Table materializeTable = tableInSchema.getTable(null);
      if (materializeTable instanceof TranslatableTable) {
        TranslatableTable table = (TranslatableTable) materializeTable;
        return table.toRel(context, relOptTable);
      }
    }
    return super.toRel(context, relOptTable);
  }

  public static class MaterializedViewTableFunction<T>
      extends ViewTableFunction<T> {
    private final MaterializationKey key;

    private MaterializedViewTableFunction(Schema schema, String viewName,
        String viewSql, List<String> viewSchemaPath, String tableName) {
      super(schema, viewName, viewSql, viewSchemaPath);
      this.key =
          MaterializationService.INSTANCE.defineMaterialization(
              schema, viewSql, schemaPath, tableName);
    }

    @Override
    public Table<T> apply(List<Object> arguments) {
      assert arguments.isEmpty();
      OptiqPrepare.ParseResult parsed =
          Schemas.parse(schema, schemaPath, viewSql);
      return new MaterializedViewTable<T>(
          schema, schema.getTypeFactory().getJavaClass(parsed.rowType),
          parsed.rowType, name, viewSql, schemaPath, key);
    }
  }
}

// End MaterializedViewTable.java
