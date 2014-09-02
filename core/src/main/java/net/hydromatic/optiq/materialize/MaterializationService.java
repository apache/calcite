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
package net.hydromatic.optiq.materialize;

import net.hydromatic.avatica.ColumnMetaData;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.config.OptiqConnectionProperty;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.Hook;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeImpl;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Manages the collection of materialized tables known to the system,
 * and the process by which they become valid and invalid.
 */
public class MaterializationService {
  private static final MaterializationService INSTANCE =
      new MaterializationService();

  /** For testing. */
  private static final ThreadLocal<MaterializationService> THREAD_INSTANCE =
      new ThreadLocal<MaterializationService>() {
        @Override
        protected MaterializationService initialValue() {
          return new MaterializationService();
        }
      };

  private final MaterializationActor actor = new MaterializationActor();

  private MaterializationService() {
  }

  /** Defines a new materialization. Returns its key. */
  public MaterializationKey defineMaterialization(final OptiqSchema schema,
      String viewSql, List<String> viewSchemaPath, String tableName) {
    final MaterializationActor.QueryKey queryKey =
        new MaterializationActor.QueryKey(viewSql, schema, viewSchemaPath);
    final MaterializationKey existingKey = actor.keyBySql.get(queryKey);
    if (existingKey != null) {
      return existingKey;
    }

    final OptiqConnection connection =
        MetaImpl.connect(schema.root(), null);
    final MaterializationKey key = new MaterializationKey();
    Table materializedTable;
    RelDataType rowType = null;
    OptiqSchema.TableEntry tableEntry;
    if (tableName != null) {
      final Pair<String, Table> pair = schema.getTable(tableName, true);
      materializedTable = pair == null ? null : pair.right;
      if (materializedTable == null) {
        final ImmutableMap<OptiqConnectionProperty, String> map =
            ImmutableMap.of(OptiqConnectionProperty.CREATE_MATERIALIZATIONS,
                "false");
        final OptiqPrepare.PrepareResult<Object> prepareResult =
            Schemas.prepare(connection, schema, viewSchemaPath, viewSql, map);
        rowType = prepareResult.rowType;
        final JavaTypeFactory typeFactory = connection.getTypeFactory();
        materializedTable =
            CloneSchema.createCloneTable(typeFactory,
                RelDataTypeImpl.proto(prepareResult.rowType),
                Functions.adapt(prepareResult.structType.columns,
                    new Function1<ColumnMetaData, ColumnMetaData.Rep>() {
                      public ColumnMetaData.Rep apply(ColumnMetaData column) {
                        return column.type.representation;
                      }
                    }),
                new AbstractQueryable<Object>() {
                  public Enumerator<Object> enumerator() {
                    final DataContext dataContext =
                        Schemas.createDataContext(connection);
                    return prepareResult.enumerator(dataContext);
                  }

                  public Type getElementType() {
                    return Object.class;
                  }

                  public Expression getExpression() {
                    throw new UnsupportedOperationException();
                  }

                  public QueryProvider getProvider() {
                    return connection;
                  }

                  public Iterator<Object> iterator() {
                    final DataContext dataContext =
                        Schemas.createDataContext(connection);
                    return prepareResult.iterator(dataContext);
                  }
                });
        schema.add(tableName, materializedTable);
      }
      tableEntry = schema.add(tableName, materializedTable);
      Hook.CREATE_MATERIALIZATION.run(tableName);
    } else {
      tableEntry = null;
    }
    if (rowType == null) {
      // If we didn't validate the SQL by populating a table, validate it now.
      final OptiqPrepare.ParseResult parse =
          Schemas.parse(connection, schema, viewSchemaPath, viewSql);
      rowType = parse.rowType;
    }
    final MaterializationActor.Materialization materialization =
        new MaterializationActor.Materialization(key, schema.root(),
            tableEntry, viewSql, rowType);
    actor.keyMap.put(materialization.key, materialization);
    actor.keyBySql.put(queryKey, materialization.key);
    return key;
  }

  /** Checks whether a materialization is valid, and if so, returns the table
   * where the data are stored. */
  public OptiqSchema.TableEntry checkValid(MaterializationKey key) {
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
  public List<Prepare.Materialization> query(OptiqSchema rootSchema) {
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

  /** Used by tests, to ensure that they see their own service. */
  public static void setThreadLocal() {
    THREAD_INSTANCE.set(new MaterializationService());
  }

  /** Returns the instance of the materialization service. Usually the global
   * one, but returns a thread-local one during testing (when
   * {@link #setThreadLocal()} has been called by the current thread). */
  public static MaterializationService instance() {
    MaterializationService materializationService = THREAD_INSTANCE.get();
    if (materializationService != null) {
      return materializationService;
    }
    return INSTANCE;
  }

}

// End MaterializationService.java
