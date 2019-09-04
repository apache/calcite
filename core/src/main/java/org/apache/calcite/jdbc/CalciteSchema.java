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

import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Schema.
 *
 * <p>Wrapper around user-defined schema used internally.</p>
 */
public abstract class CalciteSchema {

  private final CalciteSchema parent;
  public final Schema schema;
  public final String name;
  /** Tables explicitly defined in this schema. Does not include tables in
   *  {@link #schema}. */
  protected final NameMap<TableEntry> tableMap;
  protected final NameMultimap<FunctionEntry> functionMap;
  protected final NameMap<TypeEntry> typeMap;
  protected final NameMap<LatticeEntry> latticeMap;
  protected final NameSet functionNames;
  protected final NameMap<FunctionEntry> nullaryFunctionMap;
  protected final NameMap<CalciteSchema> subSchemaMap;
  private List<? extends List<String>> path;

  protected CalciteSchema(CalciteSchema parent, Schema schema,
      String name, NameMap<CalciteSchema> subSchemaMap,
      NameMap<TableEntry> tableMap, NameMap<LatticeEntry> latticeMap, NameMap<TypeEntry> typeMap,
      NameMultimap<FunctionEntry> functionMap, NameSet functionNames,
      NameMap<FunctionEntry> nullaryFunctionMap,
      List<? extends List<String>> path) {
    this.parent = parent;
    this.schema = schema;
    this.name = name;
    if (tableMap == null) {
      this.tableMap = new NameMap<>();
    } else {
      this.tableMap = Objects.requireNonNull(tableMap);
    }
    if (latticeMap == null) {
      this.latticeMap = new NameMap<>();
    } else {
      this.latticeMap = Objects.requireNonNull(latticeMap);
    }
    if (subSchemaMap == null) {
      this.subSchemaMap = new NameMap<>();
    } else {
      this.subSchemaMap = Objects.requireNonNull(subSchemaMap);
    }
    if (functionMap == null) {
      this.functionMap = new NameMultimap<>();
      this.functionNames = new NameSet();
      this.nullaryFunctionMap = new NameMap<>();
    } else {
      // If you specify functionMap, you must also specify functionNames and
      // nullaryFunctionMap.
      this.functionMap = Objects.requireNonNull(functionMap);
      this.functionNames = Objects.requireNonNull(functionNames);
      this.nullaryFunctionMap = Objects.requireNonNull(nullaryFunctionMap);
    }
    if (typeMap == null) {
      this.typeMap = new NameMap<>();
    } else {
      this.typeMap = Objects.requireNonNull(typeMap);
    }
    this.path = path;
  }

  /** Returns a sub-schema with a given name that is defined implicitly
   * (that is, by the underlying {@link Schema} object, not explicitly
   * by a call to {@link #add(String, Schema)}), or null. */
  protected abstract CalciteSchema getImplicitSubSchema(String schemaName,
      boolean caseSensitive);

  /** Returns a table with a given name that is defined implicitly
   * (that is, by the underlying {@link Schema} object, not explicitly
   * by a call to {@link #add(String, Table)}), or null. */
  protected abstract TableEntry getImplicitTable(String tableName,
      boolean caseSensitive);

  /** Returns a type with a given name that is defined implicitly
   * (that is, by the underlying {@link Schema} object, not explicitly
   * by a call to {@link #add(String, RelProtoDataType)}), or null. */
  protected abstract TypeEntry getImplicitType(String name,
                                                boolean caseSensitive);

  /** Returns table function with a given name and zero arguments that is
   * defined implicitly (that is, by the underlying {@link Schema} object,
   * not explicitly by a call to {@link #add(String, Function)}), or null. */
  protected abstract TableEntry getImplicitTableBasedOnNullaryFunction(String tableName,
      boolean caseSensitive);

  /** Adds implicit sub-schemas to a builder. */
  protected abstract void addImplicitSubSchemaToBuilder(
      ImmutableSortedMap.Builder<String, CalciteSchema> builder);

  /** Adds implicit tables to a builder. */
  protected abstract void addImplicitTableToBuilder(
      ImmutableSortedSet.Builder<String> builder);

  /** Adds implicit functions to a builder. */
  protected abstract void addImplicitFunctionsToBuilder(
      ImmutableList.Builder<Function> builder,
      String name, boolean caseSensitive);

  /** Adds implicit function names to a builder. */
  protected abstract void addImplicitFuncNamesToBuilder(
      ImmutableSortedSet.Builder<String> builder);

  /** Adds implicit type names to a builder. */
  protected abstract void addImplicitTypeNamesToBuilder(
      ImmutableSortedSet.Builder<String> builder);

  /** Adds implicit table functions to a builder. */
  protected abstract void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
      ImmutableSortedMap.Builder<String, Table> builder);

  /** Returns a snapshot representation of this CalciteSchema. */
  protected abstract CalciteSchema snapshot(
      CalciteSchema parent, SchemaVersion version);

  protected abstract boolean isCacheEnabled();

  public abstract void setCache(boolean cache);

  /** Creates a TableEntryImpl with no SQLs. */
  protected TableEntryImpl tableEntry(String name, Table table) {
    return new TableEntryImpl(this, name, table, ImmutableList.of());
  }

  /** Creates a TableEntryImpl with no SQLs. */
  protected TypeEntryImpl typeEntry(String name, RelProtoDataType relProtoDataType) {
    return new TypeEntryImpl(this, name, relProtoDataType);
  }

  /** Defines a table within this schema. */
  public TableEntry add(String tableName, Table table) {
    return add(tableName, table, ImmutableList.of());
  }

  /** Defines a table within this schema. */
  public TableEntry add(String tableName, Table table,
      ImmutableList<String> sqls) {
    final TableEntryImpl entry =
        new TableEntryImpl(this, tableName, table, sqls);
    tableMap.put(tableName, entry);
    return entry;
  }

  /** Defines a type within this schema. */
  public TypeEntry add(String name, RelProtoDataType type) {
    final TypeEntry entry =
        new TypeEntryImpl(this, name, type);
    typeMap.put(name, entry);
    return entry;
  }

  private FunctionEntry add(String name, Function function) {
    final FunctionEntryImpl entry =
        new FunctionEntryImpl(this, name, function);
    functionMap.put(name, entry);
    functionNames.add(name);
    if (function.getParameters().isEmpty()) {
      nullaryFunctionMap.put(name, entry);
    }
    return entry;
  }

  private LatticeEntry add(String name, Lattice lattice) {
    if (latticeMap.containsKey(name, false)) {
      throw new RuntimeException("Duplicate lattice '" + name + "'");
    }
    final LatticeEntryImpl entry = new LatticeEntryImpl(this, name, lattice);
    latticeMap.put(name, entry);
    return entry;
  }

  public CalciteSchema root() {
    for (CalciteSchema schema = this;;) {
      if (schema.parent == null) {
        return schema;
      }
      schema = schema.parent;
    }
  }

  /** Returns whether this is a root schema. */
  public boolean isRoot() {
    return parent == null;
  }

  /** Returns the path of an object in this schema. */
  public List<String> path(String name) {
    final List<String> list = new ArrayList<>();
    if (name != null) {
      list.add(name);
    }
    for (CalciteSchema s = this; s != null; s = s.parent) {
      if (s.parent != null || !s.name.equals("")) {
        // Omit the root schema's name from the path if it's the empty string,
        // which it usually is.
        list.add(s.name);
      }
    }
    return ImmutableList.copyOf(Lists.reverse(list));
  }

  public final CalciteSchema getSubSchema(String schemaName,
      boolean caseSensitive) {
    // Check explicit schemas.
    //noinspection LoopStatementThatDoesntLoop
    for (Map.Entry<String, CalciteSchema> entry
        : subSchemaMap.range(schemaName, caseSensitive).entrySet()) {
      return entry.getValue();
    }
    return getImplicitSubSchema(schemaName, caseSensitive);
  }

  /** Adds a child schema of this schema. */
  public abstract CalciteSchema add(String name, Schema schema);

  /** Returns a table that materializes the given SQL statement. */
  public final TableEntry getTableBySql(String sql) {
    for (TableEntry tableEntry : tableMap.map().values()) {
      if (tableEntry.sqls.contains(sql)) {
        return tableEntry;
      }
    }
    return null;
  }

  /** Returns a table with the given name. Does not look for views. */
  public final TableEntry getTable(String tableName, boolean caseSensitive) {
    // Check explicit tables.
    //noinspection LoopStatementThatDoesntLoop
    for (Map.Entry<String, TableEntry> entry
        : tableMap.range(tableName, caseSensitive).entrySet()) {
      return entry.getValue();
    }
    return getImplicitTable(tableName, caseSensitive);
  }

  public String getName() {
    return name;
  }

  public SchemaPlus plus() {
    return new SchemaPlusImpl();
  }

  public static CalciteSchema from(SchemaPlus plus) {
    return ((SchemaPlusImpl) plus).calciteSchema();
  }

  /** Returns the default path resolving functions from this schema.
   *
   * <p>The path consists is a list of lists of strings.
   * Each list of strings represents the path of a schema from the root schema.
   * For example, [[], [foo], [foo, bar, baz]] represents three schemas: the
   * root schema "/" (level 0), "/foo" (level 1) and "/foo/bar/baz" (level 3).
   *
   * @return Path of this schema; never null, may be empty
   */
  public List<? extends List<String>> getPath() {
    if (path != null) {
      return path;
    }
    // Return a path consisting of just this schema.
    return ImmutableList.of(path(null));
  }

  /** Returns a collection of sub-schemas, both explicit (defined using
   * {@link #add(String, org.apache.calcite.schema.Schema)}) and implicit
   * (defined using {@link org.apache.calcite.schema.Schema#getSubSchemaNames()}
   * and {@link Schema#getSubSchema(String)}). */
  public final NavigableMap<String, CalciteSchema> getSubSchemaMap() {
    // Build a map of implicit sub-schemas first, then explicit sub-schemas.
    // If there are implicit and explicit with the same name, explicit wins.
    final ImmutableSortedMap.Builder<String, CalciteSchema> builder =
        new ImmutableSortedMap.Builder<>(NameSet.COMPARATOR);
    builder.putAll(subSchemaMap.map());
    addImplicitSubSchemaToBuilder(builder);
    return builder.build();
  }

  /** Returns a collection of lattices.
   *
   * <p>All are explicit (defined using {@link #add(String, Lattice)}). */
  public NavigableMap<String, LatticeEntry> getLatticeMap() {
    return ImmutableSortedMap.copyOf(latticeMap.map());
  }

  /** Returns the set of all table names. Includes implicit and explicit tables
   * and functions with zero parameters. */
  public final NavigableSet<String> getTableNames() {
    final ImmutableSortedSet.Builder<String> builder =
        new ImmutableSortedSet.Builder<>(NameSet.COMPARATOR);
    // Add explicit tables, case-sensitive.
    builder.addAll(tableMap.map().keySet());
    // Add implicit tables, case-sensitive.
    addImplicitTableToBuilder(builder);
    return builder.build();
  }

  /** Returns the set of all types names. */
  public final NavigableSet<String> getTypeNames() {
    final ImmutableSortedSet.Builder<String> builder =
        new ImmutableSortedSet.Builder<>(NameSet.COMPARATOR);
    // Add explicit types.
    builder.addAll(typeMap.map().keySet());
    // Add implicit types.
    addImplicitTypeNamesToBuilder(builder);
    return builder.build();
  }

  /** Returns a type, explicit and implicit, with a given
   * name. Never null. */
  public final TypeEntry getType(String name, boolean caseSensitive) {
    for (Map.Entry<String, TypeEntry> entry
        : typeMap.range(name, caseSensitive).entrySet()) {
      return entry.getValue();
    }
    return getImplicitType(name, caseSensitive);
  }

  /** Returns a collection of all functions, explicit and implicit, with a given
   * name. Never null. */
  public final Collection<Function> getFunctions(String name, boolean caseSensitive) {
    final ImmutableList.Builder<Function> builder = ImmutableList.builder();
    // Add explicit functions.
    for (FunctionEntry functionEntry
        : Pair.right(functionMap.range(name, caseSensitive))) {
      builder.add(functionEntry.getFunction());
    }
    // Add implicit functions.
    addImplicitFunctionsToBuilder(builder, name, caseSensitive);
    return builder.build();
  }

  /** Returns the list of function names in this schema, both implicit and
   * explicit, never null. */
  public final NavigableSet<String> getFunctionNames() {
    final ImmutableSortedSet.Builder<String> builder =
        new ImmutableSortedSet.Builder<>(NameSet.COMPARATOR);
    // Add explicit functions, case-sensitive.
    builder.addAll(functionMap.map().keySet());
    // Add implicit functions, case-sensitive.
    addImplicitFuncNamesToBuilder(builder);
    return builder.build();
  }

  /** Returns tables derived from explicit and implicit functions
   * that take zero parameters. */
  public final NavigableMap<String, Table> getTablesBasedOnNullaryFunctions() {
    ImmutableSortedMap.Builder<String, Table> builder =
        new ImmutableSortedMap.Builder<>(NameSet.COMPARATOR);
    for (Map.Entry<String, FunctionEntry> entry
        : nullaryFunctionMap.map().entrySet()) {
      final Function function = entry.getValue().getFunction();
      if (function instanceof TableMacro) {
        assert function.getParameters().isEmpty();
        final Table table = ((TableMacro) function).apply(ImmutableList.of());
        builder.put(entry.getKey(), table);
      }
    }
    // add tables derived from implicit functions
    addImplicitTablesBasedOnNullaryFunctionsToBuilder(builder);
    return builder.build();
  }

  /** Returns a tables derived from explicit and implicit functions
   * that take zero parameters. */
  public final TableEntry getTableBasedOnNullaryFunction(String tableName,
      boolean caseSensitive) {
    for (Map.Entry<String, FunctionEntry> entry
        : nullaryFunctionMap.range(tableName, caseSensitive).entrySet()) {
      final Function function = entry.getValue().getFunction();
      if (function instanceof TableMacro) {
        assert function.getParameters().isEmpty();
        final Table table = ((TableMacro) function).apply(ImmutableList.of());
        return tableEntry(tableName, table);
      }
    }
    return getImplicitTableBasedOnNullaryFunction(tableName, caseSensitive);
  }

  /** Creates a snapshot of this CalciteSchema as of the specified time. All
   * explicit objects in this CalciteSchema will be copied into the snapshot
   * CalciteSchema, while the contents of the snapshot of the underlying schema
   * should not change as specified in {@link Schema#snapshot(SchemaVersion)}.
   * Snapshots of explicit sub schemas will be created and copied recursively.
   *
   * <p>Currently, to accommodate the requirement of creating tables on the fly
   * for materializations, the snapshot will still use the same table map and
   * lattice map as in the original CalciteSchema instead of making copies.</p>
   *
   * @param version The current schema version
   *
   * @return the schema snapshot.
   */
  public CalciteSchema createSnapshot(SchemaVersion version) {
    Preconditions.checkArgument(this.isRoot(), "must be root schema");
    return snapshot(null, version);
  }

  /** Returns a subset of a map whose keys match the given string
   * case-insensitively.
   * @deprecated use NameMap
   */
  @Deprecated // to be removed before 2.0
  protected static <V> NavigableMap<String, V> find(NavigableMap<String, V> map,
      String s) {
    return NameMap.immutableCopyOf(map).range(s, false);
  }

  /** Returns a subset of a set whose values match the given string
   * case-insensitively.
   * @deprecated use NameSet
   */
  @Deprecated // to be removed before 2.0
  protected static Iterable<String> find(NavigableSet<String> set, String name) {
    return NameSet.immutableCopyOf(set).range(name, false);
  }

  /** Creates a root schema.
   *
   * <p>When <code>addMetadataSchema</code> argument is true adds a "metadata"
   * schema containing definitions of tables, columns etc. to root schema.
   * By default, creates a {@link CachingCalciteSchema}.
   */
  public static CalciteSchema createRootSchema(boolean addMetadataSchema) {
    return createRootSchema(addMetadataSchema, true);
  }

  /** Creates a root schema.
   *
   * @param addMetadataSchema Whether to add a "metadata" schema containing
   *              definitions of tables, columns etc.
   * @param cache If true create {@link CachingCalciteSchema};
   *                if false create {@link SimpleCalciteSchema}
   */
  public static CalciteSchema createRootSchema(boolean addMetadataSchema,
      boolean cache) {
    return createRootSchema(addMetadataSchema, cache, "");
  }

  /** Creates a root schema.
   *
   * @param addMetadataSchema Whether to add a "metadata" schema containing
   *              definitions of tables, columns etc.
   * @param cache If true create {@link CachingCalciteSchema};
   *                if false create {@link SimpleCalciteSchema}
   * @param name Schema name
   */
  public static CalciteSchema createRootSchema(boolean addMetadataSchema,
      boolean cache, String name) {
    final Schema rootSchema = new CalciteConnectionImpl.RootSchema();
    return createRootSchema(addMetadataSchema, cache, name, rootSchema);
  }

  @Experimental
  public static CalciteSchema createRootSchema(boolean addMetadataSchema,
      boolean cache, String name, Schema schema) {
    CalciteSchema rootSchema;
    if (cache) {
      rootSchema = new CachingCalciteSchema(null, schema, name);
    } else {
      rootSchema = new SimpleCalciteSchema(null, schema, name);
    }
    if (addMetadataSchema) {
      rootSchema.add("metadata", MetadataSchema.INSTANCE);
    }
    return rootSchema;
  }

  @Experimental
  public boolean removeSubSchema(String name) {
    return subSchemaMap.remove(name) != null;
  }

  @Experimental
  public boolean removeTable(String name) {
    return tableMap.remove(name) != null;
  }

  @Experimental
  public boolean removeFunction(String name) {
    final FunctionEntry remove = nullaryFunctionMap.remove(name);
    if (remove == null) {
      return false;
    }
    functionMap.remove(name, remove);
    return true;
  }

  @Experimental
  public boolean removeType(String name) {
    return typeMap.remove(name) != null;
  }

  /**
   * Entry in a schema, such as a table or sub-schema.
   *
   * <p>Each object's name is a property of its membership in a schema;
   * therefore in principle it could belong to several schemas, or
   * even the same schema several times, with different names. In this
   * respect, it is like an inode in a Unix file system.</p>
   *
   * <p>The members of a schema must have unique names.
   */
  public abstract static class Entry {
    public final CalciteSchema schema;
    public final String name;

    public Entry(CalciteSchema schema, String name) {
      this.schema = Objects.requireNonNull(schema);
      this.name = Objects.requireNonNull(name);
    }

    /** Returns this object's path. For example ["hr", "emps"]. */
    public final List<String> path() {
      return schema.path(name);
    }
  }

  /** Membership of a table in a schema. */
  public abstract static class TableEntry extends Entry {
    public final ImmutableList<String> sqls;

    public TableEntry(CalciteSchema schema, String name,
        ImmutableList<String> sqls) {
      super(schema, name);
      this.sqls = Objects.requireNonNull(sqls);
    }

    public abstract Table getTable();
  }

  /** Membership of a type in a schema. */
  public abstract static class TypeEntry extends Entry {
    public TypeEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract RelProtoDataType getType();
  }

  /** Membership of a function in a schema. */
  public abstract static class FunctionEntry extends Entry {
    public FunctionEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract Function getFunction();

    /** Whether this represents a materialized view. (At a given point in time,
     * it may or may not be materialized as a table.) */
    public abstract boolean isMaterialization();
  }

  /** Membership of a lattice in a schema. */
  public abstract static class LatticeEntry extends Entry {
    public LatticeEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract Lattice getLattice();

    public abstract TableEntry getStarTable();
  }

  /** Implementation of {@link SchemaPlus} based on a
   * {@link org.apache.calcite.jdbc.CalciteSchema}. */
  private class SchemaPlusImpl implements SchemaPlus {
    CalciteSchema calciteSchema() {
      return CalciteSchema.this;
    }

    public SchemaPlus getParentSchema() {
      return parent == null ? null : parent.plus();
    }

    public String getName() {
      return CalciteSchema.this.getName();
    }

    public boolean isMutable() {
      return schema.isMutable();
    }

    public void setCacheEnabled(boolean cache) {
      CalciteSchema.this.setCache(cache);
    }

    public boolean isCacheEnabled() {
      return CalciteSchema.this.isCacheEnabled();
    }

    public Schema snapshot(SchemaVersion version) {
      throw new UnsupportedOperationException();
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
      return schema.getExpression(parentSchema, name);
    }

    public Table getTable(String name) {
      final TableEntry entry = CalciteSchema.this.getTable(name, true);
      return entry == null ? null : entry.getTable();
    }

    public NavigableSet<String> getTableNames() {
      return CalciteSchema.this.getTableNames();
    }

    @Override public RelProtoDataType getType(String name) {
      final TypeEntry entry = CalciteSchema.this.getType(name, true);
      return entry == null ? null : entry.getType();
    }

    @Override public Set<String> getTypeNames() {
      return CalciteSchema.this.getTypeNames();
    }

    public Collection<Function> getFunctions(String name) {
      return CalciteSchema.this.getFunctions(name, true);
    }

    public NavigableSet<String> getFunctionNames() {
      return CalciteSchema.this.getFunctionNames();
    }

    public SchemaPlus getSubSchema(String name) {
      final CalciteSchema subSchema =
          CalciteSchema.this.getSubSchema(name, true);
      return subSchema == null ? null : subSchema.plus();
    }

    public Set<String> getSubSchemaNames() {
      return CalciteSchema.this.getSubSchemaMap().keySet();
    }

    public SchemaPlus add(String name, Schema schema) {
      final CalciteSchema calciteSchema = CalciteSchema.this.add(name, schema);
      return calciteSchema.plus();
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(CalciteSchema.this)) {
        return clazz.cast(CalciteSchema.this);
      }
      if (clazz.isInstance(CalciteSchema.this.schema)) {
        return clazz.cast(CalciteSchema.this.schema);
      }
      if (clazz == DataSource.class) {
        if (schema instanceof JdbcSchema) {
          return clazz.cast(((JdbcSchema) schema).getDataSource());
        }
        if (schema instanceof JdbcCatalogSchema) {
          return clazz.cast(((JdbcCatalogSchema) schema).getDataSource());
        }
      }
      throw new ClassCastException("not a " + clazz);
    }

    public void setPath(ImmutableList<ImmutableList<String>> path) {
      CalciteSchema.this.path = path;
    }

    public void add(String name, Table table) {
      CalciteSchema.this.add(name, table);
    }

    public void add(String name, Function function) {
      CalciteSchema.this.add(name, function);
    }

    public void add(String name, RelProtoDataType type) {
      CalciteSchema.this.add(name, type);
    }

    public void add(String name, Lattice lattice) {
      CalciteSchema.this.add(name, lattice);
    }
  }

  /**
   * Implementation of {@link CalciteSchema.TableEntry}
   * where all properties are held in fields.
   */
  public static class TableEntryImpl extends TableEntry {
    private final Table table;

    /** Creates a TableEntryImpl. */
    public TableEntryImpl(CalciteSchema schema, String name, Table table,
        ImmutableList<String> sqls) {
      super(schema, name, sqls);
      this.table = Objects.requireNonNull(table);
    }

    public Table getTable() {
      return table;
    }
  }

  /**
   * Implementation of {@link TypeEntry}
   * where all properties are held in fields.
   */
  public static class TypeEntryImpl extends TypeEntry {
    private final RelProtoDataType protoDataType;

    /** Creates a TypeEntryImpl. */
    public TypeEntryImpl(CalciteSchema schema, String name, RelProtoDataType protoDataType) {
      super(schema, name);
      this.protoDataType = protoDataType;
    }

    public RelProtoDataType getType() {
      return protoDataType;
    }
  }

  /**
   * Implementation of {@link FunctionEntry}
   * where all properties are held in fields.
   */
  public static class FunctionEntryImpl extends FunctionEntry {
    private final Function function;

    /** Creates a FunctionEntryImpl. */
    public FunctionEntryImpl(CalciteSchema schema, String name,
        Function function) {
      super(schema, name);
      this.function = function;
    }

    public Function getFunction() {
      return function;
    }

    public boolean isMaterialization() {
      return function
          instanceof MaterializedViewTable.MaterializedViewTableMacro;
    }
  }

  /**
   * Implementation of {@link LatticeEntry}
   * where all properties are held in fields.
   */
  public static class LatticeEntryImpl extends LatticeEntry {
    private final Lattice lattice;
    private final CalciteSchema.TableEntry starTableEntry;

    /** Creates a LatticeEntryImpl. */
    public LatticeEntryImpl(CalciteSchema schema, String name,
        Lattice lattice) {
      super(schema, name);
      this.lattice = lattice;

      // Star table has same name as lattice and is in same schema.
      final StarTable starTable = lattice.createStarTable();
      starTableEntry = schema.add(name, starTable);
    }

    public Lattice getLattice() {
      return lattice;
    }

    public TableEntry getStarTable() {
      return starTableEntry;
    }
  }

}

// End CalciteSchema.java
