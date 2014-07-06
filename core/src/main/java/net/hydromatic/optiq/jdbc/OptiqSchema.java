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

import org.eigenbase.util.Pair;

import com.google.common.collect.*;

import java.util.*;

/**
 * Schema.
 *
 * <p>Wrapper around user-defined schema used internally.</p>
 */
public abstract class OptiqSchema {


  public final Schema schema;
  public final String name;
  protected final OptiqSchema parent;
  private ImmutableList<ImmutableList<String>> path;

  public OptiqSchema(OptiqSchema parent, final Schema schema, String name) {
    this.parent = parent;
    this.schema = schema;
    this.name = name;
  }

  public abstract TableEntry add(String tableName, Table table);
  public abstract OptiqSchema getSubSchema(String schemaName,
      boolean caseSensitive);

  /** Adds a child schema of this schema. */
  public abstract OptiqSchema add(String name, Schema schema);

  /** Returns a table with the given name. Does not look for views. */
  public abstract Pair<String, Table> getTable(String tableName,
      boolean caseSensitive);

  public abstract Collection<TableEntry> getTableEntries();

  public abstract Set<String> getSubSchemaNames();
  public abstract Collection<OptiqSchema> getSubSchemas();

  /** Returns the set of all table names. Includes implicit and explicit tables
   * and functions with zero parameters. */
  public abstract Set<String> getTableNames();

  /** Returns a collection of all functions, explicit and implicit, with a given
   * name. Never null. */
  public abstract Collection<Function>
  getFunctions(String name, boolean caseSensitive);

  /** Returns the list of function names in this schema, both implicit and
   * explicit, never null. */
  public abstract Set<String> getFunctionNames();

  /** Returns tables derived from explicit and implicit functions
   * that take zero parameters. */
  public abstract Map<String, Table> getTablesBasedOnNullaryFunctions();

  /** Returns a tables derived from explicit and implicit functions
   * that take zero parameters. */
  public abstract Pair<String, Table> getTableBasedOnNullaryFunction(
      String tableName,
      boolean caseSensitive);

  protected abstract FunctionEntry add(String name, Function function);

  public String getName() {
    return name;
  }

  public OptiqSchema root() {
    for (OptiqSchema schema = this;;) {
      if (schema.parent == null) {
        return (OptiqSchema) schema;
      }
      schema = schema.parent;
    }
  }

  public SchemaPlus plus() {
    return new SchemaPlusImpl();
  }

  public static OptiqSchema from(SchemaPlus plus) {
    return ((SchemaPlusImpl) plus).optiqSchema();
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



  protected void setCache(boolean cache) {
    throw new UnsupportedOperationException("Schema doesn't support caching.");
  }

  protected boolean isCacheEnabled() {
    return false;
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
  protected class SchemaPlusImpl implements SchemaPlus {
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

    public void setCacheEnabled(boolean cache) {
      OptiqSchema.this.setCache(cache);
    }

    public boolean isCacheEnabled() {
      return OptiqSchema.this.isCacheEnabled();
    }

    public boolean contentsHaveChangedSince(long lastCheck, long now) {
      return schema.contentsHaveChangedSince(lastCheck, now);
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
      return schema.getExpression(parentSchema, name);
    }

    public Table getTable(String name) {
      final Pair<String, Table> pair = OptiqSchema.this.getTable(name, true);
      return pair == null ? null : pair.getValue();
    }

    public Set<String> getTableNames() {
      return OptiqSchema.this.getTableNames();
    }

    public Collection<Function> getFunctions(String name) {
      return OptiqSchema.this.getFunctions(name, true);
    }

    public Set<String> getFunctionNames() {
      return OptiqSchema.this.getFunctionNames();
    }

    public SchemaPlus getSubSchema(String name) {
      final OptiqSchema subSchema = OptiqSchema.this.getSubSchema(name, true);
      return subSchema == null ? null : subSchema.plus();
    }

    public Set<String> getSubSchemaNames() {
      return OptiqSchema.this.getSubSchemaNames();
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
   * Implementation of {@link net.hydromatic.optiq.jdbc.OptiqSchema.TableEntry}
   * where all properties are held in fields.
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
