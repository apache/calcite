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
package net.hydromatic.optiq.materialize;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.Prepare;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Manages the collection of materialized tables known to the system,
 * and the process by which they become valid and invalid.
 */
public class MaterializationService {
  public static final MaterializationService INSTANCE =
      new MaterializationService();

  private final MaterializationActor actor = new MaterializationActor();

  /** Defines a new materialization. Returns its key. */
  public MaterializationKey defineMaterialization(final Schema schema,
      String viewSql, List<String> viewSchemaPath, String tableName) {
    final MaterializationKey key = new MaterializationKey();
    Schema.TableInSchema materializedTable;
    RelDataType rowType = null;
    if (tableName != null) {
      materializedTable = schema.getTables().get(tableName);
      if (materializedTable == null) {
        final OptiqPrepare.PrepareResult<Object> prepareResult =
            Schemas.prepare(schema, viewSchemaPath, viewSql);
        materializedTable =
            CloneSchema.createCloneTable((MutableSchema) schema, tableName,
                prepareResult.rowType,
                new AbstractQueryable<Object>() {
                  public Type getElementType() {
                    return prepareResult.resultClazz;
                  }

                  public Expression getExpression() {
                    return null;
                  }

                  public QueryProvider getProvider() {
                    return schema.getQueryProvider();
                  }

                  public Iterator<Object> iterator() {
                    return prepareResult.enumerable instanceof Iterable
                        ? ((Iterable) prepareResult.enumerable).iterator()
                        : Linq4j.enumeratorIterator(enumerator());
                  }

                  public Enumerator<Object> enumerator() {
                    return prepareResult.enumerable.enumerator();
                  }
            });
        ((MutableSchema) schema).addTable(materializedTable);
      }
    } else {
      materializedTable = null;
    }
    if (rowType == null) {
      // If we didn't validate the SQL by populating a table, validate it now.
      final OptiqPrepare.ParseResult parse =
          Schemas.parse(schema, viewSchemaPath, viewSql);
      rowType = parse.rowType;
    }
    final MaterializationActor.Materialization materialization =
        new MaterializationActor.Materialization(key, Schemas.root(schema),
            materializedTable, viewSql, rowType);
    actor.keyMap.put(materialization.key, materialization);
    return key;
  }

  /** Checks whether a materialization is valid, and if so, returns the table
   * where the data are stored. */
  public Schema.TableInSchema checkValid(MaterializationKey key) {
    final MaterializationActor.Materialization materialization =
        actor.keyMap.get(key);
    if (materialization != null) {
      return materialization.materializedTable;
    }
    return null;
  }

  /** Gathers a list of all materialized tables known within a given root
   * schema. (Each root schema defines a disconnected namespace, with no overlap
   * with the current schema. Especially in a test run, the contents of two
   * root schemas may look similar.) */
  public List<Prepare.Materialization> query(Schema rootSchema) {
    final List<Prepare.Materialization> list =
        new ArrayList<Prepare.Materialization>();
    for (MaterializationActor.Materialization materialization
        : actor.keyMap.values()) {
      if (materialization.rootSchema == rootSchema
          && materialization.materializedTable != null) {
        list.add(
            new Prepare.Materialization(materialization.materializedTable,
                materialization.sql));
      }
    }
    return list;
  }

  /** De-registers all materialized tables in the system. */
  public void clear() {
    actor.keyMap.clear();
  }
}

// End MaterializationService.java
