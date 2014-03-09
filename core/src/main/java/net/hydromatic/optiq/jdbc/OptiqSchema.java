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

import org.eigenbase.util.Util;

import com.google.common.base.Function;
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
  /** Tables explicitly defined in this schema. Does not include tables in
   *  {@link #schema}. */
  public final Map<String, TableEntry> tableMap =
      new HashMap<String, TableEntry>();
  public final Map<String, TableEntry> tableMapInsensitive =
      new TreeMap<String, TableEntry>(String.CASE_INSENSITIVE_ORDER);
  private final Multimap<String, TableFunctionEntry> tableFunctionMap =
      LinkedListMultimap.create();
  private final Map<String, TableFunctionEntry>
  nullaryTableFunctionMapInsensitive =
      new TreeMap<String, TableFunctionEntry>(String.CASE_INSENSITIVE_ORDER);
  private final Map<String, OptiqSchema> subSchemaMap =
      new HashMap<String, OptiqSchema>();
  private final Map<String, OptiqSchema> subSchemaMapInsensitive =
      new TreeMap<String, OptiqSchema>(String.CASE_INSENSITIVE_ORDER);
  public final Map<String, Table> compositeTableMap;
  public final Multimap<String, TableFunction> compositeTableFunctionMap;
  public final Map<String, OptiqSchema> compositeSubSchemaMap;

  public OptiqSchema(OptiqSchema parent, final Schema schema) {
    this.parent = parent;
    this.schema = schema;
    assert (parent == null) == (this instanceof OptiqRootSchema);
    //noinspection unchecked
    this.compositeTableMap = CompositeMap.of(
        Maps.transformValues(
            tableMap,
            new Function<TableEntry, Table>() {
              public Table apply(TableEntry input) {
                return input.getTable();
              }
            }),
        Maps.transformValues(
            Multimaps.filterEntries(
                tableFunctionMap,
                new Predicate<Map.Entry<String, TableFunctionEntry>>() {
                  public boolean apply(
                      Map.Entry<String, TableFunctionEntry> input) {
                    return input.getValue().getTableFunction().getParameters()
                        .isEmpty();
                  }
                }).asMap(),
            new Function<Collection<TableFunctionEntry>, Table>() {
              public Table apply(Collection<TableFunctionEntry> input) {
                // At most one function with zero parameters.
                TableFunctionEntry entry = input.iterator().next();
                return entry.getTableFunction().apply(ImmutableList.of());
              }
            }),
        Compatible.INSTANCE.asMap(
            schema.getTableNames(),
            new Function<String, Table>() {
              public Table apply(String input) {
                return schema.getTable(input);
              }
            }));
    // TODO: include schema's table functions in this map.
    this.compositeTableFunctionMap =
        Multimaps.transformValues(tableFunctionMap,
            new Function<TableFunctionEntry, TableFunction>() {
              public TableFunction apply(TableFunctionEntry input) {
                return input.getTableFunction();
              }
            });
    //noinspection unchecked
    this.compositeSubSchemaMap =
        CompositeMap.of(
            subSchemaMap,
            Compatible.INSTANCE.asMap(
                schema.getSubSchemaNames(),
                new Function<String, OptiqSchema>() {
                  public OptiqSchema apply(String input) {
                    return addSchema(schema.getSubSchema(input));
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

  private TableFunctionEntry add(String name,
      TableFunction tableFunction) {
    final TableFunctionEntryImpl entry =
        new TableFunctionEntryImpl(this, name, tableFunction);
    tableFunctionMap.put(name, entry);
    if (tableFunction.getParameters().isEmpty()) {
      nullaryTableFunctionMapInsensitive.put(name, entry);
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

  public final OptiqSchema getSubSchema(String schemaName,
      boolean caseSensitive) {
    return (caseSensitive ? subSchemaMap : subSchemaMapInsensitive)
        .get(schemaName);
  }

  /** Adds a child schema of this schema. */
  public OptiqSchema addSchema(Schema schema) {
    final OptiqSchema optiqSchema = new OptiqSchema(this, schema);
    subSchemaMap.put(schema.getName(), optiqSchema);
    subSchemaMapInsensitive.put(schema.getName(), optiqSchema);
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
      final TableFunctionEntry entry =
          nullaryTableFunctionMapInsensitive.get(tableName);
      if (entry != null) {
        return entry.getTableFunction().apply(ImmutableList.of());
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
    return schema.getName();
  }

  public void addTableFunction(
      TableFunctionEntry tableFunctionEntry) {
    throw Util.needToImplement(tableFunctionEntry);
  }

  public SchemaPlus plus() {
    return new SchemaPlusImpl();
  }

  public static OptiqSchema from(SchemaPlus plus) {
    return ((SchemaPlusImpl) plus).optiqSchema();
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
      return Schemas.path(schema.schema, name);
    }
  }

  /** Membership of a table in a schema. */
  public abstract static class TableEntry extends Entry {
    public TableEntry(OptiqSchema schema, String name) {
      super(schema, name);
    }

    public abstract Table getTable();
  }

  /** Membership of a table-function in a schema. */
  public abstract static class TableFunctionEntry extends Entry {
    public TableFunctionEntry(OptiqSchema schema, String name) {
      super(schema, name);
    }

    public abstract TableFunction getTableFunction();

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

    public Expression getExpression() {
      return schema.getExpression();
    }

    public Table getTable(String name) {
      return compositeTableMap.get(name);
    }

    public Set<String> getTableNames() {
      return compositeTableMap.keySet();
    }

    public Collection<TableFunction> getTableFunctions(String name) {
      return compositeTableFunctionMap.get(name);
    }

    public Set<String> getTableFunctionNames() {
      return compositeTableFunctionMap.keySet();
    }

    public SchemaPlus getSubSchema(String name) {
      final OptiqSchema subSchema = OptiqSchema.this.getSubSchema(name, true);
      return subSchema == null ? null : subSchema.plus();
    }

    public Set<String> getSubSchemaNames() {
      return subSchemaMap.keySet();
    }

    public SchemaPlus add(Schema schema) {
      final OptiqSchema optiqSchema = OptiqSchema.this.addSchema(schema);
      return optiqSchema.plus();
    }

    public SchemaPlus addRecursive(Schema schema) {
      return schema.getParentSchema().add(schema);
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

    public void add(String name, Table table) {
      OptiqSchema.this.add(name, table);
    }

    public void add(String name, TableFunction tableFunction) {
      OptiqSchema.this.add(name, tableFunction);
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
   * Implementation of {@link net.hydromatic.optiq.jdbc.OptiqSchema.TableFunctionEntry}
   * where all properties are held in fields.
   */
  public static class TableFunctionEntryImpl extends TableFunctionEntry {
    private final TableFunction tableFunction;

    /** Creates a TableFunctionEntryImpl. */
    public TableFunctionEntryImpl(OptiqSchema schema, String name,
        TableFunction tableFunction) {
      super(schema, name);
      this.tableFunction = tableFunction;
    }

    public TableFunction getTableFunction() {
      return tableFunction;
    }

    public boolean isMaterialization() {
      return tableFunction
          instanceof MaterializedViewTable.MaterializedViewTableFunction;
    }
  }
}

// End OptiqSchema.java
