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
package org.apache.calcite.jdbc;

import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.SnapshotLookup;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Concrete implementation of {@link CalciteSchema} that caches tables,
 * functions and sub-schemas.
 */
class CachingCalciteSchema extends CalciteSchema {
  private final ConcurrentLinkedDeque<SnapshotLookup<?>> caches = new ConcurrentLinkedDeque<>();
  private final Cached<NameSet> implicitFunctionCache;
  private final Cached<NameSet> implicitTypeCache;

  private boolean cache = true;

  /** Creates a CachingCalciteSchema. */
  CachingCalciteSchema(@Nullable CalciteSchema parent, Schema schema,
      String name) {
    this(parent, schema, name, null, null, null, null, null, null, null, null);
  }

  @SuppressWarnings({"argument.type.incompatible", "return.type.incompatible"})
  private CachingCalciteSchema(@Nullable CalciteSchema parent, Schema schema,
      String name,
      @Nullable NameMap<CalciteSchema> subSchemaMap,
      @Nullable NameMap<TableEntry> tableMap,
      @Nullable NameMap<LatticeEntry> latticeMap,
      @Nullable NameMap<TypeEntry> typeMap,
      @Nullable NameMultimap<FunctionEntry> functionMap,
      @Nullable NameSet functionNames,
      @Nullable NameMap<FunctionEntry> nullaryFunctionMap,
      @Nullable List<? extends List<String>> path) {
    super(parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap,
        functionMap, functionNames, nullaryFunctionMap, path);
    this.implicitFunctionCache =
        new AbstractCached<NameSet>() {
          @Override public NameSet build() {
            return NameSet.immutableCopyOf(
                CachingCalciteSchema.this.schema.getFunctionNames());
          }
        };
    this.implicitTypeCache =
        new AbstractCached<NameSet>() {
          @Override public NameSet build() {
            return NameSet.immutableCopyOf(
                CachingCalciteSchema.this.schema.getTypeNames());
          }
        };
  }

  @Override public void setCache(boolean cache) {
    if (cache == this.cache) {
      return;
    }
    enableCaches(cache);
    final long now = System.currentTimeMillis();
    implicitFunctionCache.enable(now, cache);
    this.cache = cache;
  }

  @Override protected boolean isCacheEnabled() {
    return this.cache;
  }

  @Override protected CalciteSchema createSubSchema(Schema schema, String name) {
    return new CachingCalciteSchema(this, schema, name);
  }

  @Override protected <S> Lookup<S> enhanceLookup(Lookup<S> lookup) {
    SnapshotLookup<S> snapshotLookup = new SnapshotLookup<>(lookup);
    caches.add(snapshotLookup);
    return snapshotLookup;
  }

  /** Adds a child schema of this schema. */
  @Override public CalciteSchema add(String name, Schema schema) {
    final CalciteSchema calciteSchema =
        new CachingCalciteSchema(this, schema, name);
    subSchemaMap.put(name, calciteSchema);
    return calciteSchema;
  }

  @Override protected @Nullable TypeEntry getImplicitType(String name,
      boolean caseSensitive) {
    final long now = System.currentTimeMillis();
    final NameSet implicitTypeNames = implicitTypeCache.get(now);
    for (String typeName
        : implicitTypeNames.range(name, caseSensitive)) {
      final RelProtoDataType type = schema.getType(typeName);
      if (type != null) {
        return typeEntry(name, type);
      }
    }
    return null;
  }

  @Override protected void addImplicitFunctionsToBuilder(
      ImmutableList.Builder<Function> builder,
      String name, boolean caseSensitive) {
    // Add implicit functions, case-insensitive.
    final long now = System.currentTimeMillis();
    final NameSet set = implicitFunctionCache.get(now);
    for (String name2 : set.range(name, caseSensitive)) {
      final Collection<Function> functions = schema.getFunctions(name2);
      if (functions != null) {
        builder.addAll(functions);
      }
    }
  }

  @Override protected void addImplicitFuncNamesToBuilder(
      ImmutableSortedSet.Builder<String> builder) {
    // Add implicit functions, case-sensitive.
    final long now = System.currentTimeMillis();
    final NameSet set = implicitFunctionCache.get(now);
    builder.addAll(set.iterable());
  }

  @Override protected void addImplicitTypeNamesToBuilder(
      ImmutableSortedSet.Builder<String> builder) {
    // Add implicit types, case-sensitive.
    final long now = System.currentTimeMillis();
    final NameSet set = implicitTypeCache.get(now);
    builder.addAll(set.iterable());
  }

  @Override protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
      ImmutableSortedMap.Builder<String, Table> builder) {
    ImmutableSortedMap<String, Table> explicitTables = builder.build();

    final long now = System.currentTimeMillis();
    final NameSet set = implicitFunctionCache.get(now);
    for (String s : set.iterable()) {
      // explicit table wins.
      if (explicitTables.containsKey(s)) {
        continue;
      }
      for (Function function : schema.getFunctions(s)) {
        if (function instanceof TableMacro
            && function.getParameters().isEmpty()) {
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          builder.put(s, table);
        }
      }
    }
  }

  @Override protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(
      String tableName, boolean caseSensitive) {
    final long now = System.currentTimeMillis();
    final NameSet set = implicitFunctionCache.get(now);
    for (String s : set.range(tableName, caseSensitive)) {
      for (Function function : schema.getFunctions(s)) {
        if (function instanceof TableMacro
            && function.getParameters().isEmpty()) {
          final Table table =
              ((TableMacro) function).apply(ImmutableList.of());
          return tableEntry(tableName, table);
        }
      }
    }
    return null;
  }

  @Override protected CalciteSchema snapshot(@Nullable CalciteSchema parent,
      SchemaVersion version) {
    CalciteSchema snapshot =
        new CachingCalciteSchema(parent, schema.snapshot(version), name, null,
            tableMap, latticeMap, typeMap,
            functionMap, functionNames, nullaryFunctionMap, getPath());
    for (CalciteSchema subSchema : subSchemaMap.map().values()) {
      CalciteSchema subSchemaSnapshot = subSchema.snapshot(snapshot, version);
      snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot);
    }
    return snapshot;
  }

  @Override public boolean removeTable(String name) {
    if (cache) {
      enableCaches(false);
      enableCaches(true);
    }
    return super.removeTable(name);
  }

  @Override public boolean removeFunction(String name) {
    if (cache) {
      final long now = System.nanoTime();
      implicitFunctionCache.enable(now, false);
      implicitFunctionCache.enable(now, true);
    }
    return super.removeFunction(name);
  }

  private void enableCaches(final boolean cache) {
    for (SnapshotLookup<?> lookupCache : caches) {
      lookupCache.enable(cache);
    }
  }

  /** Strategy for caching the value of an object and re-creating it if its
   * value is out of date as of a given timestamp.
   *
   * @param <T> Type of cached object
   */
  private interface Cached<T> {
    /** Returns the value; uses cached value if valid. */
    T get(long now);

    /** Creates a new value. */
    T build();

    /** Called when CalciteSchema caching is enabled or disabled. */
    void enable(long now, boolean enabled);
  }

  /** Implementation of {@link CachingCalciteSchema.Cached}
   * that drives from {@link CachingCalciteSchema#cache}.
   *
   * @param <T> element type */
  private abstract class AbstractCached<T> implements Cached<T> {
    @Nullable T t;
    boolean built = false;

    @Override public T get(long now) {
      if (!CachingCalciteSchema.this.cache) {
        return build();
      }
      if (!built) {
        t = build();
      }
      built = true;
      return castNonNull(t);
    }

    @Override public void enable(long now, boolean enabled) {
      if (!enabled) {
        t = null;
      }
      built = false;
    }
  }
}
