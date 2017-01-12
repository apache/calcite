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

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.util.NameSet;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.Collection;
import java.util.Set;

/**
 * Concrete implementation of {@link CalciteSchema} that caches tables,
 * functions and sub-schemas.
 */
class CachingCalciteSchema extends CalciteSchema {
  private final Cached<SubSchemaCache> implicitSubSchemaCache;
  private final Cached<NameSet> implicitTableCache;
  private final Cached<NameSet> implicitFunctionCache;

  private boolean cache = true;

  /** Creates a CachingCalciteSchema. */
  CachingCalciteSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
    this.implicitSubSchemaCache =
        new AbstractCached<SubSchemaCache>() {
          public SubSchemaCache build() {
            return new SubSchemaCache(CachingCalciteSchema.this,
                CachingCalciteSchema.this.schema.getSubSchemaNames());
          }
        };
    this.implicitTableCache =
        new AbstractCached<NameSet>() {
          public NameSet build() {
            return NameSet.immutableCopyOf(
                CachingCalciteSchema.this.schema.getTableNames());
          }
        };
    this.implicitFunctionCache =
        new AbstractCached<NameSet>() {
          public NameSet build() {
            return NameSet.immutableCopyOf(
                CachingCalciteSchema.this.schema.getFunctionNames());
          }
        };
  }

  public void setCache(boolean cache) {
    if (cache == this.cache) {
      return;
    }
    final long now = System.currentTimeMillis();
    implicitSubSchemaCache.enable(now, cache);
    implicitTableCache.enable(now, cache);
    implicitFunctionCache.enable(now, cache);
    this.cache = cache;
  }

  protected boolean isCacheEnabled() {
    return this.cache;
  }

  protected CalciteSchema getImplicitSubSchema(String schemaName,
      boolean caseSensitive) {
    final long now = System.currentTimeMillis();
    final SubSchemaCache subSchemaCache =
        implicitSubSchemaCache.get(now);
    //noinspection LoopStatementThatDoesntLoop
    for (String schemaName2
        : subSchemaCache.names.range(schemaName, caseSensitive)) {
      return subSchemaCache.cache.getUnchecked(schemaName2);
    }
    return null;
  }

  /** Adds a child schema of this schema. */
  public CalciteSchema add(String name, Schema schema) {
    final CalciteSchema calciteSchema =
        new CachingCalciteSchema(this, schema, name);
    subSchemaMap.put(name, calciteSchema);
    return calciteSchema;
  }

  protected TableEntry getImplicitTable(String tableName,
      boolean caseSensitive) {
    final long now = System.currentTimeMillis();
    final NameSet implicitTableNames = implicitTableCache.get(now);
    for (String tableName2
        : implicitTableNames.range(tableName, caseSensitive)) {
      final Table table = schema.getTable(tableName2);
      if (table != null) {
        return tableEntry(tableName2, table);
      }
    }
    return null;
  }

  protected void addImplicitSubSchemaToBuilder(
      ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
    ImmutableSortedMap<String, CalciteSchema> explicitSubSchemas = builder.build();
    final long now = System.currentTimeMillis();
    final SubSchemaCache subSchemaCache = implicitSubSchemaCache.get(now);
    for (String name : subSchemaCache.names.iterable()) {
      if (explicitSubSchemas.containsKey(name)) {
        // explicit sub-schema wins.
        continue;
      }
      builder.put(name, subSchemaCache.cache.getUnchecked(name));
    }
  }

  protected void addImplicitTableToBuilder(
      ImmutableSortedSet.Builder<String> builder) {
    // Add implicit tables, case-sensitive.
    final long now = System.currentTimeMillis();
    final NameSet set = implicitTableCache.get(now);
    builder.addAll(set.iterable());
  }

  protected void addImplicitFunctionsToBuilder(
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

  protected void addImplicitFuncNamesToBuilder(
      ImmutableSortedSet.Builder<String> builder) {
    // Add implicit functions, case-sensitive.
    final long now = System.currentTimeMillis();
    final NameSet set = implicitFunctionCache.get(now);
    builder.addAll(set.iterable());
  }

  protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
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

  protected TableEntry getImplicitTableBasedOnNullaryFunction(String tableName,
      boolean caseSensitive) {
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
   * that drives from {@link CachingCalciteSchema#cache}. */
  private abstract class AbstractCached<T> implements Cached<T> {
    T t;
    long checked = Long.MIN_VALUE;

    public T get(long now) {
      if (!CachingCalciteSchema.this.cache) {
        return build();
      }
      if (checked == Long.MIN_VALUE
          || schema.contentsHaveChangedSince(checked, now)) {
        t = build();
      }
      checked = now;
      return t;
    }

    public void enable(long now, boolean enabled) {
      if (!enabled) {
        t = null;
      }
      checked = Long.MIN_VALUE;
    }
  }

  /** Information about the implicit sub-schemas of an {@link CalciteSchema}. */
  private static class SubSchemaCache {
    /** The names of sub-schemas returned from the {@link Schema} SPI. */
    final NameSet names;
    /** Cached {@link CalciteSchema} wrappers. It is
     * worth caching them because they contain maps of their own sub-objects. */
    final LoadingCache<String, CalciteSchema> cache;

    private SubSchemaCache(final CalciteSchema calciteSchema,
        Set<String> names) {
      this.names = NameSet.immutableCopyOf(names);
      this.cache = CacheBuilder.newBuilder().build(
          new CacheLoader<String, CalciteSchema>() {
            @SuppressWarnings("NullableProblems")
            @Override public CalciteSchema load(String schemaName) {
              final Schema subSchema =
                  calciteSchema.schema.getSubSchema(schemaName);
              if (subSchema == null) {
                throw new RuntimeException("sub-schema " + schemaName
                    + " not found");
              }
              return new CachingCalciteSchema(calciteSchema, subSchema, schemaName);
            }
          });
    }
  }
}

// End CachingCalciteSchema.java
