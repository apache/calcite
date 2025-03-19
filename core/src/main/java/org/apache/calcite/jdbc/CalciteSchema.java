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
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.Named;
import org.apache.calcite.util.LazyReference;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Schema.
 *
 * <p>Wrapper around user-defined schema used internally.
 */
public abstract class CalciteSchema {

  private final @Nullable CalciteSchema parent;
  public final Schema schema;
  public final String name;
  /** Tables explicitly defined in this schema. Does not include tables in
   * {@link #schema}. */
  protected final NameMap<TableEntry> tableMap;
  private final LazyReference<Lookup<TableEntry>> tables = new LazyReference<>();
  protected final NameMultimap<FunctionEntry> functionMap;
  protected final NameMap<TypeEntry> typeMap;
  protected final NameMap<LatticeEntry> latticeMap;
  protected final NameSet functionNames;
  protected final NameMap<FunctionEntry> nullaryFunctionMap;
  protected final NameMap<CalciteSchema> subSchemaMap;
  private final LazyReference<Lookup<CalciteSchema>> subSchemas = new LazyReference<>();
  private @Nullable List<? extends List<String>> path;

  protected CalciteSchema(@Nullable CalciteSchema parent, Schema schema,
      String name,
      @Nullable NameMap<CalciteSchema> subSchemaMap,
      @Nullable NameMap<TableEntry> tableMap,
      @Nullable NameMap<LatticeEntry> latticeMap,
      @Nullable NameMap<TypeEntry> typeMap,
      @Nullable NameMultimap<FunctionEntry> functionMap,
      @Nullable NameSet functionNames,
      @Nullable NameMap<FunctionEntry> nullaryFunctionMap,
      @Nullable List<? extends List<String>> path) {
    this.parent = parent;
    this.schema = schema;
    this.name = name;
    this.tableMap = tableMap != null ? tableMap : new NameMap<>();
    this.latticeMap = latticeMap != null ? latticeMap : new NameMap<>();
    this.subSchemaMap = subSchemaMap != null ? subSchemaMap : new NameMap<>();
    if (functionMap == null) {
      this.functionMap = new NameMultimap<>();
      this.functionNames = new NameSet();
      this.nullaryFunctionMap = new NameMap<>();
    } else {
      // If you specify functionMap, you must also specify functionNames and
      // nullaryFunctionMap.
      this.functionMap = functionMap;
      this.functionNames = requireNonNull(functionNames, "functionNames");
      this.nullaryFunctionMap =
          requireNonNull(nullaryFunctionMap, "nullaryFunctionMap");
    }
    if (typeMap == null) {
      this.typeMap = new NameMap<>();
    } else {
      this.typeMap = typeMap;
    }
    this.path = path;
  }

  public Lookup<TableEntry> tables() {
    return this.tables.getOrCompute(() ->
        Lookup.concat(
            Lookup.of(this.tableMap),
            enhanceLookup(schema.tables().map((s, n) -> tableEntry(n, s)))));

  }

  public Lookup<CalciteSchema> subSchemas() {
    return subSchemas.getOrCompute(() ->
        Lookup.concat(
            Lookup.of(this.subSchemaMap),
            enhanceLookup(schema.subSchemas().map((s, n) -> createSubSchema(s, n)))));
  }

  /** The derived class is able to enhance the lookup e.g. by introducing a cache. */
  protected <S> Lookup<S> enhanceLookup(Lookup<S> lookup) {
    return lookup;
  }

  /** Creates a sub-schema with a given name that is defined implicitly. */
  protected abstract CalciteSchema createSubSchema(CalciteSchema this,
      Schema schema, String name);

  /** Returns a type with a given name that is defined implicitly
   * (that is, by the underlying {@link Schema} object, not explicitly
   * by a call to {@link #add(String, RelProtoDataType)}), or null. */
  protected abstract @Nullable TypeEntry getImplicitType(String name,
                                                boolean caseSensitive);

  /** Returns table function with a given name and zero arguments that is
   * defined implicitly (that is, by the underlying {@link Schema} object,
   * not explicitly by a call to {@link #add(String, Function)}), or null. */
  protected abstract @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(String tableName,
      boolean caseSensitive);

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
      @Nullable CalciteSchema parent, SchemaVersion version);

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
  public List<String> path(@Nullable String name) {
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
    return ImmutableList.copyOf(list).reverse();
  }

  public final @Nullable CalciteSchema getSubSchema(String schemaName,
      boolean caseSensitive) {
    return caseSensitive
        ? subSchemas().get(schemaName)
        : Named.entityOrNull(subSchemas().getIgnoreCase(schemaName));
  }

  /** Adds a child schema of this schema. */
  public abstract CalciteSchema add(String name, Schema schema);

  /** Returns a table that materializes the given SQL statement. */
  public final @Nullable TableEntry getTableBySql(String sql) {
    for (TableEntry tableEntry : tableMap.map().values()) {
      if (tableEntry.sqls.contains(sql)) {
        return tableEntry;
      }
    }
    return null;
  }

  /** Returns a table with the given name. Does not look for views. */
  public final @Nullable TableEntry getTable(String tableName, boolean caseSensitive) {
    return Lookup.get(tables(), tableName, caseSensitive);
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
   * {@link #add(String, org.apache.calcite.schema.Schema)}) and implicit. */
  public final NavigableMap<String, CalciteSchema> getSubSchemaMap() {
    final ImmutableSortedMap.Builder<String, CalciteSchema> builder =
        new ImmutableSortedMap.Builder<>(NameSet.COMPARATOR);
    final Lookup<CalciteSchema> schemas = subSchemas();
    for (String name : schemas.getNames(LikePattern.any())) {
      builder.put(name, requireNonNull(schemas.get(name)));
    }
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
  public final Set<String> getTableNames() {
    return getTableNames(LikePattern.any());
  }

  /** Returns the set of table names filtered by the given pattern.
   * Includes implicit and explicit tables and functions with zero parameters. */
  public final Set<String> getTableNames(LikePattern pattern) {
    return tables().getNames(pattern);
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
  public final @Nullable TypeEntry getType(String name, boolean caseSensitive) {
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
  public final @Nullable TableEntry getTableBasedOnNullaryFunction(String tableName,
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
   * lattice map as in the original CalciteSchema instead of making copies.
   *
   * @param version The current schema version
   *
   * @return the schema snapshot.
   */
  public CalciteSchema createSnapshot(SchemaVersion version) {
    checkArgument(this.isRoot(), "must be root schema");
    return snapshot(null, version);
  }

  /** Returns a subset of a map whose keys match the given string
   * case-insensitively.
   *
   * @deprecated use NameMap
   */
  @Deprecated // to be removed before 2.0
  protected static <V> NavigableMap<String, V> find(NavigableMap<String, V> map,
      String s) {
    return NameMap.immutableCopyOf(map).range(s, false);
  }

  /** Returns a subset of a set whose values match the given string
   * case-insensitively.
   *
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
   * respect, it is like an inode in a Unix file system.
   *
   * <p>The members of a schema must have unique names.
   */
  public abstract static class Entry {
    public final CalciteSchema schema;
    public final String name;

    protected Entry(CalciteSchema schema, String name) {
      this.schema = requireNonNull(schema, "schema");
      this.name = requireNonNull(name, "name");
    }

    /** Returns this object's path. For example ["hr", "emps"]. */
    public final List<String> path() {
      return schema.path(name);
    }
  }

  /** Membership of a table in a schema. */
  public abstract static class TableEntry extends Entry {
    public final ImmutableList<String> sqls;

    protected TableEntry(CalciteSchema schema, String name,
        ImmutableList<String> sqls) {
      super(schema, name);
      this.sqls = requireNonNull(sqls, "sqls");
    }

    public abstract Table getTable();
  }

  /** Membership of a type in a schema. */
  public abstract static class TypeEntry extends Entry {
    protected TypeEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract RelProtoDataType getType();
  }

  /** Membership of a function in a schema. */
  public abstract static class FunctionEntry extends Entry {
    protected FunctionEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract Function getFunction();

    /** Whether this represents a materialized view. (At a given point in time,
     * it may or may not be materialized as a table.) */
    public abstract boolean isMaterialization();
  }

  /** Membership of a lattice in a schema. */
  public abstract static class LatticeEntry extends Entry {
    protected LatticeEntry(CalciteSchema schema, String name) {
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

    @Override public @Nullable SchemaPlus getParentSchema() {
      return parent == null ? null : parent.plus();
    }

    @Override public String getName() {
      return CalciteSchema.this.getName();
    }

    @Override public boolean isMutable() {
      return schema.isMutable();
    }

    @Override public void setCacheEnabled(boolean cache) {
      CalciteSchema.this.setCache(cache);
    }

    @Override public boolean isCacheEnabled() {
      return CalciteSchema.this.isCacheEnabled();
    }

    @Override public Schema snapshot(SchemaVersion version) {
      throw new UnsupportedOperationException();
    }

    @Override public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
      return schema.getExpression(parentSchema, name);
    }

    @Override public Lookup<Table> tables() {
      return CalciteSchema.this.tables().map((table, name) -> table.getTable());
    }

    @Override public Lookup<? extends SchemaPlus> subSchemas() {
      return CalciteSchema.this.subSchemas().map((schema, name) -> schema.plus());
    }

    @Deprecated @Override public @Nullable Table getTable(String name) {
      final TableEntry entry = CalciteSchema.this.getTable(name, true);
      return entry == null ? null : entry.getTable();
    }

    @Deprecated @Override public Set<String> getTableNames() {
      return CalciteSchema.this.getTableNames(LikePattern.any());
    }

    @Override public @Nullable RelProtoDataType getType(String name) {
      final TypeEntry entry = CalciteSchema.this.getType(name, true);
      return entry == null ? null : entry.getType();
    }

    @Override public Set<String> getTypeNames() {
      return CalciteSchema.this.getTypeNames();
    }

    @Override public Collection<Function> getFunctions(String name) {
      return CalciteSchema.this.getFunctions(name, true);
    }

    @Override public NavigableSet<String> getFunctionNames() {
      return CalciteSchema.this.getFunctionNames();
    }

    @Deprecated @Override public @Nullable SchemaPlus getSubSchema(String name) {
      return subSchemas().get(name);
    }

    @Deprecated @Override public Set<String> getSubSchemaNames() {
      return subSchemas().getNames(LikePattern.any());
    }

    @Override public SchemaPlus add(String name, Schema schema) {
      final CalciteSchema calciteSchema = CalciteSchema.this.add(name, schema);
      return calciteSchema.plus();
    }

    @Override public <T extends Object> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(CalciteSchema.this)) {
        return clazz.cast(CalciteSchema.this);
      }
      if (clazz.isInstance(CalciteSchema.this.schema)) {
        return clazz.cast(CalciteSchema.this.schema);
      }
      if (schema instanceof Wrapper) {
        return ((Wrapper) schema).unwrapOrThrow(clazz);
      }
      throw new ClassCastException("not a " + clazz);
    }

    @Override public void setPath(ImmutableList<ImmutableList<String>> path) {
      CalciteSchema.this.path = path;
    }

    @Override public void add(String name, Table table) {
      CalciteSchema.this.add(name, table);
    }

    @Override public boolean removeTable(String name) {
      return CalciteSchema.this.removeTable(name);
    }

    @Override public void add(String name, Function function) {
      CalciteSchema.this.add(name, function);
    }

    @Override public void add(String name, RelProtoDataType type) {
      CalciteSchema.this.add(name, type);
    }

    @Override public void add(String name, Lattice lattice) {
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
      this.table = requireNonNull(table, "table");
    }

    @Override public Table getTable() {
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

    @Override public RelProtoDataType getType() {
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

    @Override public Function getFunction() {
      return function;
    }

    @Override public boolean isMaterialization() {
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

    @Override public Lattice getLattice() {
      return lattice;
    }

    @Override public TableEntry getStarTable() {
      return starTableEntry;
    }
  }

}
