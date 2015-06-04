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
package org.apache.calcite.materialize;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.clone.CloneSchema;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

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
        @Override protected MaterializationService initialValue() {
          return new MaterializationService();
        }
      };

  private static final Comparator<Pair<CalciteSchema.TableEntry, TileKey>> C =
      new Comparator<Pair<CalciteSchema.TableEntry, TileKey>>() {
        public int compare(Pair<CalciteSchema.TableEntry, TileKey> o0,
            Pair<CalciteSchema.TableEntry, TileKey> o1) {
          // We prefer rolling up from the table with the fewest rows.
          final Table t0 = o0.left.getTable();
          final Table t1 = o1.left.getTable();
          int c = Double.compare(t0.getStatistic().getRowCount(),
              t1.getStatistic().getRowCount());
          if (c != 0) {
            return c;
          }
          // Tie-break based on table name.
          return o0.left.name.compareTo(o1.left.name);
        }
      };

  private final MaterializationActor actor = new MaterializationActor();

  private MaterializationService() {
  }

  /** Defines a new materialization. Returns its key. */
  public MaterializationKey defineMaterialization(final CalciteSchema schema,
                                                  TileKey tileKey, String viewSql,
                                                  List<String> viewSchemaPath,
                                                  final String suggestedTableName, boolean create) {
    TableFactory tableFactory =
        new MaterializationService.DefaultTableFactory(suggestedTableName);
    return defineMaterialization(schema, tileKey, viewSql, viewSchemaPath, tableFactory, create);
  }

  /** Defines a new materialization. Returns its key. */
  public MaterializationKey defineMaterialization(final CalciteSchema schema,
      TileKey tileKey, String viewSql, List<String> viewSchemaPath,
      final TableFactory tableFactory, boolean create) {
    final MaterializationActor.QueryKey queryKey =
        new MaterializationActor.QueryKey(viewSql, schema, viewSchemaPath);
    final MaterializationKey existingKey = actor.keyBySql.get(queryKey);
    if (existingKey != null) {
      return existingKey;
    }
    if (!create) {
      return null;
    }

    final CalciteConnection connection =
        CalciteMetaImpl.connect(schema.root(), null);
    CalciteSchema.TableEntry tableEntry = schema.getTableBySql(viewSql);
    RelDataType rowType = null;
    if (tableEntry == null) {
      tableEntry = tableFactory.createTable(schema, viewSql, viewSchemaPath);
      rowType = tableEntry.getTable().getRowType(connection.getTypeFactory());
    }

    if (rowType == null) {
      // If we didn't validate the SQL by populating a table, validate it now.
      final CalcitePrepare.ParseResult parse =
          Schemas.parse(connection, schema, viewSchemaPath, viewSql);
      rowType = parse.rowType;
    }
    final MaterializationKey key = new MaterializationKey();
    final MaterializationActor.Materialization materialization =
        new MaterializationActor.Materialization(key, schema.root(),
            tableEntry, viewSql, rowType);
    actor.keyMap.put(materialization.key, materialization);
    actor.keyBySql.put(queryKey, materialization.key);
    if (tileKey != null) {
      actor.keyByTile.put(tileKey, materialization.key);
    }
    return key;
  }

  /** Checks whether a materialization is valid, and if so, returns the table
   * where the data are stored. */
  public CalciteSchema.TableEntry checkValid(MaterializationKey key) {
    final MaterializationActor.Materialization materialization =
        actor.keyMap.get(key);
    if (materialization != null) {
      return materialization.materializedTable;
    }
    return null;
  }

  /**
   * Defines a tile.
   *
   * <p>Setting the {@code create} flag to false prevents a materialization
   * from being created if one does not exist. Critically, it is set to false
   * during the recursive SQL that populates a materialization. Otherwise a
   * materialization would try to create itself to populate itself!
   */
  public Pair<CalciteSchema.TableEntry, TileKey> defineTile(Lattice lattice,
      ImmutableBitSet groupSet, List<Lattice.Measure> measureList,
      CalciteSchema schema, boolean create, boolean exact) {
    TableFactory defaultTableFactory = new DefaultTableFactory("m" + groupSet);
    return defineTile(lattice, groupSet, measureList, schema, create, exact, defaultTableFactory);
  }

  public Pair<CalciteSchema.TableEntry, TileKey> defineTile(Lattice lattice,
        ImmutableBitSet groupSet, List<Lattice.Measure> measureList,
        CalciteSchema schema, boolean create, boolean exact, TableFactory tableFactory) {
    MaterializationKey materializationKey;
    final TileKey tileKey =
        new TileKey(lattice, groupSet, ImmutableList.copyOf(measureList));

    // Step 1. Look for an exact match for the tile.
    materializationKey = actor.keyByTile.get(tileKey);
    if (materializationKey != null) {
      final CalciteSchema.TableEntry tableEntry =
          checkValid(materializationKey);
      if (tableEntry != null) {
        return Pair.of(tableEntry, tileKey);
      }
    }

    // Step 2. Look for a match of the tile with the same dimensionality and an
    // acceptable list of measures.
    final TileKey tileKey0 =
        new TileKey(lattice, groupSet, ImmutableList.<Lattice.Measure>of());
    for (TileKey tileKey1 : actor.tilesByDimensionality.get(tileKey0)) {
      assert tileKey1.dimensions.equals(groupSet);
      if (allSatisfiable(measureList, tileKey1)) {
        materializationKey = actor.keyByTile.get(tileKey1);
        if (materializationKey != null) {
          final CalciteSchema.TableEntry tableEntry =
              checkValid(materializationKey);
          if (tableEntry != null) {
            return Pair.of(tableEntry, tileKey1);
          }
        }
      }
    }

    // Step 3. There's nothing at the exact dimensionality. Look for a roll-up
    // from tiles that have a super-set of dimensions and all the measures we
    // need.
    //
    // If there are several roll-ups, choose the one with the fewest rows.
    //
    // TODO: Allow/deny roll-up based on a size factor. If the source is only
    // say 2x larger than the target, don't materialize, but if it is 3x, do.
    //
    // TODO: Use a partially-ordered set data structure, so we are not scanning
    // through all tiles.
    if (!exact) {
      final PriorityQueue<Pair<CalciteSchema.TableEntry, TileKey>> queue =
          new PriorityQueue<>(1, C);
      for (Map.Entry<TileKey, MaterializationKey> entry
          : actor.keyByTile.entrySet()) {
        final TileKey tileKey2 = entry.getKey();
        if (tileKey2.lattice == lattice
            && tileKey2.dimensions.contains(groupSet)
            && !tileKey2.dimensions.equals(groupSet)
            && allSatisfiable(measureList, tileKey2)) {
          materializationKey = entry.getValue();
          final CalciteSchema.TableEntry tableEntry =
              checkValid(materializationKey);
          if (tableEntry != null) {
            queue.add(Pair.of(tableEntry, tileKey2));
          }
        }
      }
      if (!queue.isEmpty()) {
        return queue.peek();
      }
    }

    // What we need is not there. If we can't create, we're done.
    if (!create) {
      return null;
    }

    // Step 4. Create the tile we need.
    //
    // If there were any tiles at this dimensionality, regardless of
    // whether they were current, create a wider tile that contains their
    // measures plus the currently requested measures. Then we can obsolete all
    // other tiles.
    final List<TileKey> obsolete = Lists.newArrayList();
    final LinkedHashSet<Lattice.Measure> measureSet = Sets.newLinkedHashSet();
    for (TileKey tileKey1 : actor.tilesByDimensionality.get(tileKey0)) {
      measureSet.addAll(tileKey1.measures);
      obsolete.add(tileKey1);
    }
    measureSet.addAll(measureList);
    final TileKey newTileKey =
        new TileKey(lattice, groupSet, ImmutableList.copyOf(measureSet));

    final String sql = lattice.sql(groupSet, newTileKey.measures);

    System.out.println(sql);
    materializationKey =
        defineMaterialization(schema, newTileKey, sql, schema.path(null),
            tableFactory, true);
    if (materializationKey != null) {
      final CalciteSchema.TableEntry tableEntry =
          checkValid(materializationKey);
      if (tableEntry != null) {
        // Obsolete all of the narrower tiles.
        for (TileKey tileKey1 : obsolete) {
          actor.tilesByDimensionality.remove(tileKey0, tileKey1);
          actor.keyByTile.remove(tileKey1);
        }

        actor.tilesByDimensionality.put(tileKey0, newTileKey);
        actor.keyByTile.put(newTileKey, materializationKey);
        return Pair.of(tableEntry, newTileKey);
      }
    }
    return null;
  }

  private boolean allSatisfiable(List<Lattice.Measure> measureList,
      TileKey tileKey) {
    // A measure can be satisfied if it is contained in the measure list, or,
    // less obviously, if it is composed of grouping columns.
    for (Lattice.Measure measure : measureList) {
      if (!(tileKey.measures.contains(measure)
          || tileKey.dimensions.contains(measure.argBitSet()))) {
        return false;
      }
    }
    return true;
  }

  /** Gathers a list of all materialized tables known within a given root
   * schema. (Each root schema defines a disconnected namespace, with no overlap
   * with the current schema. Especially in a test run, the contents of two
   * root schemas may look similar.) */
  public List<Prepare.Materialization> query(CalciteSchema rootSchema) {
    final List<Prepare.Materialization> list = new ArrayList<>();
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

  /**
   * TableFactory Interface defines functions required by
   * MaterializationService to create tables that represent
   * a materialized view.
   */
  public interface TableFactory {
    CalciteSchema.TableEntry createTable(final CalciteSchema schema,
                                         final String viewSql,
                                         final List<String> viewSchemaPath);
    String getTableName(CalciteSchema schema);
  }

  /**
   * Default implementation of TableFactory.
   * Creates a table using CloneSchema.
   */
  public static class DefaultTableFactory implements TableFactory {
    final String tableName;

    public DefaultTableFactory(final String tableName) {
      this.tableName = tableName;
    }

    public CalciteSchema.TableEntry createTable(final CalciteSchema schema,
                                                final String viewSql,
                                                final List<String> viewSchemaPath) {
      final CalciteConnection connection =
          CalciteMetaImpl.connect(schema.root(), null);
      final ImmutableMap<CalciteConnectionProperty, String> map =
          ImmutableMap.of(CalciteConnectionProperty.CREATE_MATERIALIZATIONS,
              "false");
      final CalcitePrepare.CalciteSignature<Object> calciteSignature =
          Schemas.prepare(connection, schema, viewSchemaPath, viewSql, map);
      Table table = CloneSchema.createCloneTable(connection.getTypeFactory(),
          RelDataTypeImpl.proto(calciteSignature.rowType),
          Lists.transform(calciteSignature.columns,
              new Function<ColumnMetaData, ColumnMetaData.Rep>() {
                public ColumnMetaData.Rep apply(ColumnMetaData column) {
                  return column.type.rep;
                }
              }),
          new AbstractQueryable<Object>() {
            public Enumerator<Object> enumerator() {
              final DataContext dataContext =
                  Schemas.createDataContext(connection);
              return calciteSignature.enumerable(dataContext).enumerator();
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
              return calciteSignature.enumerable(dataContext).iterator();
            }
          });

      final String tableName = this.getTableName(schema);
      CalciteSchema.TableEntry tableEntry = schema.add(tableName, table,
          ImmutableList.of(viewSql));
      Hook.CREATE_MATERIALIZATION.run(tableName);
      return tableEntry;
    }

    public String getTableName(CalciteSchema schema) {
      return Schemas.uniqueTableName(schema, Util.first(tableName, "m"));
    }
  }
}

// End MaterializationService.java
