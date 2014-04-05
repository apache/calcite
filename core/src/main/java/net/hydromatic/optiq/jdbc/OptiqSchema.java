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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.MaterializedViewTable;
import net.hydromatic.optiq.util.Compatible;
import net.hydromatic.optiq.util.CompositeMap;

import com.google.common.base.Predicate;
import com.google.common.collect.*;

import java.util.*;

/**
 * Schema.
 *
 * <p>Wrapper around user-defined schema used internally.</p>
 */
public class OptiqSchema {
  private final OptiqSchema parent;
  public final Schema schema;
  public final String name;
  /** Tables explicitly defined in this schema. Does not include tables in
   *  {@link #schema}. */
  public final Map<String, TableEntry> tableMap =
      new HashMap<String, TableEntry>();
  public final Map<String, TableEntry> tableMapInsensitive =
      new TreeMap<String, TableEntry>(String.CASE_INSENSITIVE_ORDER);
  private final Multimap<String, FunctionEntry> functionMap =
      LinkedListMultimap.create();
  private final Map<String, FunctionEntry> nullaryFunctionMapInsensitive =
      new TreeMap<String, FunctionEntry>(String.CASE_INSENSITIVE_ORDER);
  private final Map<String, OptiqSchema> subSchemaMap =
      new HashMap<String, OptiqSchema>();
  private final Map<String, OptiqSchema> subSchemaMapInsensitive =
      new TreeMap<String, OptiqSchema>(String.CASE_INSENSITIVE_ORDER);
  public final Map<String, Table> compositeTableMap;
  public final Multimap<String, Function> compositeFunctionMap;
  public final Map<String, OptiqSchema> compositeSubSchemaMap;
  private ImmutableList<ImmutableList<String>> path;

  public OptiqSchema(OptiqSchema parent, final Schema schema, String name) {
    this.parent = parent;
    this.schema = schema;
    this.name = name;
    assert (parent == null) == (this instanceof OptiqRootSchema);
    //noinspection unchecked
    this.compositeTableMap = CompositeMap.of(
        Maps.transformValues(
            tableMap,
            new com.google.common.base.Function<TableEntry, Table>() {
              public Table apply(TableEntry input) {
                return input.getTable();
              }
            }),
        Maps.transformValues(
            Multimaps.filterEntries(
                functionMap,
                new Predicate<Map.Entry<String, FunctionEntry>>() {
                  public boolean apply(Map.Entry<String, FunctionEntry> entry) {
                    final Function function = entry.getValue().getFunction();
                    return function instanceof TableMacro
                        && function.getParameters().isEmpty();
                  }
                }).asMap(),
            new com.google.common.base.Function<Collection<FunctionEntry>,
                Table>() {
              public Table apply(Collection<FunctionEntry> input) {
                // At most one function with zero parameters.
                final TableMacro tableMacro =
                    (TableMacro) input.iterator().next().getFunction();
                return tableMacro.apply(ImmutableList.of());
              }
            }),
        Compatible.INSTANCE.asMap(
            schema.getTableNames(),
            new com.google.common.base.Function<String, Table>() {
              public Table apply(String input) {
                return schema.getTable(input);
              }
            }));
    // TODO: include schema's functions in this map.
    this.compositeFunctionMap =
        Multimaps.transformValues(
            functionMap,
            new com.google.common.base.Function<FunctionEntry, Function>() {
              public Function apply(FunctionEntry input) {
                return input.getFunction();
              }
            });
    //noinspection unchecked
    this.compositeSubSchemaMap =
        CompositeMap.of(
            subSchemaMap,
            Compatible.INSTANCE.asMap(
                schema.getSubSchemaNames(),
                new com.google.common.base.Function<String, OptiqSchema>() {
                  public OptiqSchema apply(String name) {
                    return add(name, schema.getSubSchema(name));
                  }
                }));
  }

  /** Defines a table within this schema. */
  public TableEntry add(String tableName, Table table) {
    final TableEntryImpl entry =
        new TableEntryImpl(this, tableName, table);
    tableMap.put(tableName, entry);
    tableMapInsensitive.put(tableName, entry);
    return entry;
  }

  private FunctionEntry add(String name, Function function) {
    final FunctionEntryImpl entry =
        new FunctionEntryImpl(this, name, function);
    functionMap.put(name, entry);
    if (function.getParameters().isEmpty()) {
      nullaryFunctionMapInsensitive.put(name, entry);
    }
    return entry;
  }

  public OptiqRootSchema root() {
    for (OptiqSchema schema = this;;) {
      if (schema.parent == null) {
        return (OptiqRootSchema) schema;
      }
      schema = schema.parent;
    }
  }

  /** Returns the path of an object in this schema. */
  public List<String> path(String name) {
    final List<String> list = new ArrayList<String>();
    if (name != null) {
      list.add(name);
    }
    for (OptiqSchema s = this; s != null; s = s.parent) {
      if (s.parent != null || !s.name.equals("")) {
        // Omit the root schema's name from the path if it's the empty string,
        // which it usually is.
        list.add(s.name);
      }
    }
    return ImmutableList.copyOf(Lists.reverse(list));
  }

  public final OptiqSchema getSubSchema(String schemaName,
      boolean caseSensitive) {
    return (caseSensitive ? subSchemaMap : subSchemaMapInsensitive)
        .get(schemaName);
  }

  /** Adds a child schema of this schema. */
  public OptiqSchema add(String name, Schema schema) {
    final OptiqSchema optiqSchema = new OptiqSchema(this, schema, name);
    subSchemaMap.put(name, optiqSchema);
    subSchemaMapInsensitive.put(name, optiqSchema);
    return optiqSchema;
  }

  public final Table getTable(String tableName, boolean caseSensitive) {
    if (caseSensitive) {
      return compositeTableMap.get(tableName);
    } else {
      final TableEntry tableEntry = tableMapInsensitive.get(tableName);
      if (tableEntry != null) {
        return tableEntry.getTable();
      }
      final FunctionEntry entry =
          nullaryFunctionMapInsensitive.get(tableName);
      if (entry != null) {
        return ((TableMacro) entry.getFunction()).apply(ImmutableList.of());
      }
      for (String name : schema.getTableNames()) {
        if (name.equalsIgnoreCase(tableName)) {
          return schema.getTable(name);
        }
      }
      return null;
    }
  }

  public String getName() {
    return name;
  }

  public SchemaPlus plus() {
    return new SchemaPlusImpl();
  }

  public static OptiqSchema from(SchemaPlus plus) {
    return ((SchemaPlusImpl) plus).optiqSchema();
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
    public final OptiqSchema schema;
    public final String name;

    public Entry(OptiqSchema schema, String name) {
      Linq4j.requireNonNull(schema);
      Linq4j.requireNonNull(name);
      this.schema = schema;
      this.name = name;
    }

    /** Returns this object's path. For example ["hr", "emps"]. */
    public final List<String> path() {
      return schema.path(name);
    }
  }

  /** Membership of a table in a schema. */
  public abstract static class TableEntry extends Entry {
    public TableEntry(OptiqSchema schema, String name) {
      super(schema, name);
    }

    public abstract Table getTable();
  }

  /** Membership of a function in a schema. */
  public abstract static class FunctionEntry extends Entry {
    public FunctionEntry(OptiqSchema schema, String name) {
      super(schema, name);
    }

    public abstract Function getFunction();

    /** Whether this represents a materialized view. (At a given point in time,
     * it may or may not be materialized as a table.) */
    public abstract boolean isMaterialization();
  }

  /** Implementation of {@link SchemaPlus} based on an {@code OptiqSchema}. */
  private class SchemaPlusImpl implements SchemaPlus {
    public OptiqSchema optiqSchema() {
      return OptiqSchema.this;
    }

    public SchemaPlus getParentSchema() {
      return parent == null ? null : parent.plus();
    }

    public String getName() {
      return OptiqSchema.this.getName();
    }

    public boolean isMutable() {
      return schema.isMutable();
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
      return schema.getExpression(parentSchema, name);
    }

    public Table getTable(String name) {
      return compositeTableMap.get(name);
    }

    public Set<String> getTableNames() {
      return compositeTableMap.keySet();
    }

    public Collection<Function> getFunctions(String name) {
      return compositeFunctionMap.get(name);
    }

    public Set<String> getFunctionNames() {
      return compositeFunctionMap.keySet();
    }

    public SchemaPlus getSubSchema(String name) {
      final OptiqSchema subSchema = OptiqSchema.this.getSubSchema(name, true);
      return subSchema == null ? null : subSchema.plus();
    }

    public Set<String> getSubSchemaNames() {
      return subSchemaMap.keySet();
    }

    public SchemaPlus add(String name, Schema schema) {
      final OptiqSchema optiqSchema = OptiqSchema.this.add(name, schema);
      return optiqSchema.plus();
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(OptiqSchema.this)) {
        return clazz.cast(OptiqSchema.this);
      }
      if (clazz.isInstance(OptiqSchema.this.schema)) {
        return clazz.cast(OptiqSchema.this.schema);
      }
      throw new ClassCastException("not a " + clazz);
    }

    public void setPath(ImmutableList<ImmutableList<String>> path) {
      OptiqSchema.this.path = path;
    }

    public void add(String name, Table table) {
      OptiqSchema.this.add(name, table);
    }

    public void add(String name, net.hydromatic.optiq.Function function) {
      OptiqSchema.this.add(name, function);
    }

  }

  /**
   * Implementation of {@link net.hydromatic.optiq.jdbc.OptiqSchema.TableEntry} where all properties are
   * held in fields.
   */
  public static class TableEntryImpl extends TableEntry {
    private final Table table;

    /** Creates a TableEntryImpl. */
    public TableEntryImpl(OptiqSchema schema, String name, Table table) {
      super(schema, name);
      assert table != null;
      this.table = table;
    }

    public Table getTable() {
      return table;
    }
  }

  /**
   * Implementation of {@link FunctionEntry}
   * where all properties are held in fields.
   */
  public static class FunctionEntryImpl extends FunctionEntry {
    private final Function function;

    /** Creates a FunctionEntryImpl. */
    public FunctionEntryImpl(OptiqSchema schema, String name,
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
}

// End OptiqSchema.java
